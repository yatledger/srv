//! Фоновая очистка «тяжёлых» узлов.
//!
//! Обработку ведёт исключительно лидер: он видит актуальное состояние DAG и
//! может сразу писать команды в Raft. Followers не инициируют очистку, поэтому
//! больше нет молчаливого `continue` и пересылки самому себе. Так очистка
//! работает и в одноузловом кластере.

use tokio::time;
use tracing::{debug, info};

use crate::NodeId;
use crate::app::App;

/// Запускает цикл очистки. Один вызов на узел; активную работу выполняет лидер.
pub async fn start_processor(app: App) {
    let mut interval = time::interval(app.processor_interval);
    let node_id: NodeId = app.id;

    loop {
        interval.tick().await;

        let is_leader = app.raft.metrics().borrow().current_leader == Some(node_id);
        if !is_leader {
            // Очистку ведёт только лидер.
            continue;
        }

        // 1. Кандидаты — узлы без активных родителей (без клонирования DAG).
        let candidates =
            crate::cleanup::select_candidates(app.state_machine.as_ref(), app.cleanup_batch_size)
                .await;
        if candidates.is_empty() {
            continue;
        }
        let candidates_len = candidates.len();

        // 2. Оставляем только «тяжёлые» по весу.
        let heavy = crate::cleanup::select_heavy(
            app.state_machine.as_ref(),
            &candidates,
            app.weight_threshold,
        )
        .await;
        if heavy.is_empty() {
            continue;
        }
        let heavy_len = heavy.len();

        // 3. Архивируем в Redis и удаляем через Raft (лидер напрямую).
        match crate::cleanup::archive_and_remove(&app, heavy).await {
            Ok(removed) => {
                info!(
                    "cleanup cycle: candidates={candidates_len}, heavy={heavy_len}, removed={removed}"
                );
            }
            Err(e) => {
                debug!("cleanup cycle failed: {e}");
            }
        }
    }
}
