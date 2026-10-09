//! Ограничение частоты запросов (O3).
//!
//! Простой потокобезопасный token-bucket на ключ (обычно IP-адрес клиента).
//! Не тянет внешних зависимостей и не требует фонового таймера: пополнение
//! ведра вычисляется лениво при каждом запросе.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

/// Максимум отслеживаемых ключей до принудительной очистки «остывших» вёдер.
const MAX_TRACKED_KEYS: usize = 10_000;
/// Время простоя ключа, после которого его можно удалить при очистке.
const IDLE_TTL_SECS: u64 = 60;

struct Bucket {
    tokens: f64,
    last: Instant,
}

impl std::fmt::Debug for Bucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bucket")
            .field("tokens", &self.tokens)
            .field("last", &self.last)
            .finish()
    }
}

/// Token-bucket rate limiter.
#[derive(Debug)]
pub struct RateLimiter {
    rate_per_sec: f64,
    burst: f64,
    buckets: Mutex<HashMap<String, Bucket>>,
}

impl RateLimiter {
    /// Создаёт лимитер. `rate_per_sec == 0` означает «выключено» (пропускать всё).
    pub fn new(rate_per_sec: u32, burst: u32) -> Self {
        let burst = burst.max(1) as f64;
        Self {
            rate_per_sec: rate_per_sec as f64,
            burst,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Включён ли лимитер.
    pub fn is_enabled(&self) -> bool {
        self.rate_per_sec > 0.0
    }

    /// Проверяет запрос от `key`. Возвращает `true`, если запрос разрешён.
    pub fn check(&self, key: &str) -> bool {
        if !self.is_enabled() {
            return true;
        }
        let now = Instant::now();
        let mut buckets = self.buckets.lock().unwrap_or_else(|e| e.into_inner());

        if buckets.len() > MAX_TRACKED_KEYS {
            let ttl = std::time::Duration::from_secs(IDLE_TTL_SECS);
            buckets.retain(|_, b| now.duration_since(b.last) < ttl);
        }

        let burst = self.burst;
        let rate = self.rate_per_sec;
        let bucket = buckets.entry(key.to_string()).or_insert_with(|| Bucket {
            tokens: burst,
            last: now,
        });

        let elapsed = now.duration_since(bucket.last).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * rate).min(burst);
        bucket.last = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_limiter_allows_everything() {
        let rl = RateLimiter::new(0, 0);
        for _ in 0..1000 {
            assert!(rl.check("1.2.3.4"));
        }
    }

    #[test]
    fn burst_then_denied() {
        // burst = 3, без пополнения за время теста.
        let rl = RateLimiter::new(1, 3);
        assert!(rl.check("ip"));
        assert!(rl.check("ip"));
        assert!(rl.check("ip"));
        // Четвёртый подряд — отказано.
        assert!(!rl.check("ip"));
    }

    #[test]
    fn keys_are_independent() {
        let rl = RateLimiter::new(1, 1);
        assert!(rl.check("a"));
        assert!(!rl.check("a"));
        assert!(rl.check("b"), "другой ключ не должен быть ограничен");
    }

    #[test]
    fn tokens_refill_over_time() {
        let rl = RateLimiter::new(1000, 1);
        assert!(rl.check("ip"));
        assert!(!rl.check("ip"));
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(rl.check("ip"), "спустя время токен должен пополниться");
    }
}
