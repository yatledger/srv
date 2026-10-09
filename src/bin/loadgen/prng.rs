//! Детерминированный ГПСЧ для нагрузочного генератора.
//!
//! Реализован без новых зависимостей (xorshift64*): при фиксированном `--seed`
//! последовательность параметров транзакций воспроизводима. Криптостойкость не
//! требуется — генератор не создаёт ключи/секреты, а лишь разыгрывает нагрузку.

/// Детерминированный генератор псевдослучайных чисел (xorshift64*).
#[derive(Debug, Clone)]
pub struct Prng {
    state: u64,
}

impl Prng {
    /// Создаёт ГПСЧ с заданным seed. Нулевой seed заменяется ненулевой константой
    /// (xorshift не выходит из нулевого состояния).
    pub fn new(seed: u64) -> Self {
        let state = if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        };
        Self { state }
    }

    /// Следующее псевдослучайное `u64`.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Случайное число в диапазоне `[0, bound)` (0, если `bound == 0`).
    pub fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            0
        } else {
            self.next_u64() % bound
        }
    }

    /// Случайное число в диапазоне `[low, high]` включительно.
    pub fn range_inclusive(&mut self, low: u64, high: u64) -> u64 {
        if high <= low {
            return low;
        }
        low + self.below(high - low + 1)
    }

    /// Случайный байт.
    pub fn next_u8(&mut self) -> u8 {
        (self.next_u64() >> 56) as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_produces_same_sequence() {
        let mut a = Prng::new(42);
        let mut b = Prng::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seed_produces_different_sequence() {
        let mut a = Prng::new(1);
        let mut b = Prng::new(2);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn zero_seed_is_usable() {
        let mut p = Prng::new(0);
        // Ненулевой seed-заменитель должен давать ненулевой поток.
        assert_ne!(p.next_u64(), 0);
    }

    #[test]
    fn below_and_range_are_bounded() {
        let mut p = Prng::new(7);
        for _ in 0..1000 {
            assert!(p.below(10) < 10);
            let v = p.range_inclusive(2, 5);
            assert!((2..=5).contains(&v));
        }
        assert_eq!(p.below(0), 0);
        assert_eq!(p.range_inclusive(3, 3), 3);
    }
}
