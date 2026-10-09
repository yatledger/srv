//! Graceful shutdown (O5).
//!
//! Единый [`Shutdown`]-сигнал на узел: HTTP-сервер завершает приём запросов,
//! затем Raft корректно останавливается и состояние финализируется на диск.
//! Сигнал взводится по `Ctrl-C`/`SIGTERM` либо программно (в тестах и при
//! управляемом рестарте узла).

use tokio::sync::watch;

/// Хэндл управления остановкой узла.
#[derive(Debug, Clone)]
pub struct Shutdown {
    sender: watch::Sender<bool>,
}

impl Shutdown {
    /// Создаёт канал остановки.
    pub fn new() -> (Self, ShutdownSignal) {
        let (sender, receiver) = watch::channel(false);
        (Self { sender }, ShutdownSignal { receiver })
    }

    /// Взводит сигнал остановки. Повторные вызовы безопасны.
    pub fn trigger(&self) {
        let _ = self.sender.send(true);
    }

    /// Возвращает независимый приёмник сигнала (для HTTP-сервера).
    pub fn signal(&self) -> ShutdownSignal {
        ShutdownSignal {
            receiver: self.sender.subscribe(),
        }
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new().0
    }
}

/// Получатель сигнала остановки (для `with_graceful_shutdown`).
#[derive(Debug, Clone)]
pub struct ShutdownSignal {
    receiver: watch::Receiver<bool>,
}

impl ShutdownSignal {
    /// Завершается, когда взведён сигнал остановки.
    pub async fn cancelled(&mut self) {
        if *self.receiver.borrow() {
            return;
        }
        // Ждём изменения; если отправитель уничтожен, считаем, что пора завершаться.
        while self.receiver.changed().await.is_ok() {
            if *self.receiver.borrow() {
                return;
            }
        }
    }
}

/// Ожидает `Ctrl-C` или `SIGTERM` и взводит [`Shutdown`].
pub async fn shutdown_signal(shutdown: Shutdown) {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!("failed to listen for ctrl-c: {e}");
            // Не даём возможности завершиться только по SIGTERM: держим ветку живой.
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(e) => {
                tracing::error!("failed to listen for SIGTERM: {e}");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }

    tracing::info!("shutdown signal received");
    shutdown.trigger();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn trigger_wakes_waiter() {
        let (shutdown, mut signal) = Shutdown::new();
        shutdown.trigger();
        // Должно завершиться немедленно, не дожидаясь внешнего таймаута.
        tokio::time::timeout(std::time::Duration::from_secs(1), signal.cancelled())
            .await
            .expect("cancelled() должен завершиться после trigger()");
    }

    #[tokio::test]
    async fn trigger_after_wait_still_wakes() {
        let (shutdown, mut signal) = Shutdown::new();
        let handle = tokio::spawn(async move {
            signal.cancelled().await;
        });
        // Даём ожидающему встать в очередь.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        shutdown.trigger();
        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("waiter должен проснуться")
            .expect("задача не должна паниковать");
    }
}
