//! Генератор валидных транзакций.
//!
//! Строит транзакции, которые гарантированно проходят детерминированную
//! валидацию state machine: `prnts` — `k` живых узлов из `/pool`; `seq` — строго
//! монотонный по адресу (у каждого аккаунта свой счётчик); `var` корректной
//! структуры; подпись — ed25519 над каноническим [`dagdb::utils::ordered_sum`].
//! Логика канонического хэша/подписи не дублируется — используется код проекта.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, MutexGuard};

use base58::ToBase58;
use ed25519_dalek::{Signer, SigningKey};
use serde_json::json;

use dagdb::Tx;
use dagdb::domain::{Address, Hash};
use dagdb::graph::dag::TRANSFER_TOKEN;
use dagdb::utils::ordered_sum;

use crate::prng::Prng;

/// Параметры генератора транзакций.
#[derive(Clone, Debug)]
pub struct GeneratorConfig {
    /// Seed для детерминированной производной ключей и параметров.
    pub seed: u64,
    /// Число аккаунтов-отправителей.
    pub accounts: usize,
    /// Желаемое число родителей на транзакцию (зажимается в 2..=min(100,|pool|)).
    pub parents: usize,
}

/// Аккаунт-отправитель: адрес, ключ и монотонный счётчик `seq`.
struct Account {
    addr: Address,
    sk: SigningKey,
    seq: AtomicU32,
    /// Гарантирует монотонность: следующий `seq` берётся только после того, как
    /// предыдущая транзакция аккаунта получила ответ (лок держится на время отправки).
    gate: Mutex<()>,
}

/// Подписанная транзакция в форме тела `POST /`.
#[derive(Clone, Debug)]
pub struct SignedTx {
    /// Транзакция.
    pub tx: Tx,
    /// Подпись ed25519 в hex.
    pub sign: String,
    /// Имя функции (входит в подписываемый контент).
    pub func: String,
}

/// Генератор валидных транзакций.
pub struct Generator {
    config: GeneratorConfig,
    accounts: Vec<Account>,
}

impl Generator {
    /// Создаёт генератор: строит `--accounts` ключей ed25519 из seed-производных
    /// байт (детерминированно при одном seed).
    pub fn new(config: GeneratorConfig) -> Self {
        let accounts = (0..config.accounts.max(1))
            .map(|i| {
                let seed_bytes = derive_seed_bytes(config.seed, i as u64);
                let sk = SigningKey::from_bytes(&seed_bytes);
                let addr = Address::from(sk.verifying_key().to_bytes().to_base58());
                Account {
                    addr,
                    sk,
                    seq: AtomicU32::new(0),
                    gate: Mutex::new(()),
                }
            })
            .collect();
        Self { config, accounts }
    }

    /// Число аккаунтов.
    pub fn account_count(&self) -> usize {
        self.accounts.len()
    }

    /// Захватывает «шлюз» аккаунта: пока он удерживается, `seq` этого аккаунта не
    /// выдаётся другой задаче, а значит порядок применения строго монотонен.
    pub fn account_gate(&self, index: usize) -> MutexGuard<'_, ()> {
        self.accounts[index]
            .gate
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Строит подписанную транзакцию для аккаунта `index` с заданными родителями.
    ///
    /// Увеличивает счётчик `seq` аккаунта. Вызывающий обязан предварительно
    /// удерживать [`Self::account_gate`] до получения ответа.
    pub fn build(&self, index: usize, parents: &[Hash], rng: &mut Prng) -> SignedTx {
        let account = &self.accounts[index];
        let seq = account.seq.fetch_add(1, Ordering::SeqCst) + 1;

        // `to` — один из известных адресов (непустой); `val`/`msg` — случайные.
        let to = self.accounts[rng.below(self.accounts.len() as u64) as usize]
            .addr
            .clone();
        let val = rng.range_inclusive(1, 1_000_000);
        let msg_len = rng.below(32) as usize;
        let msg: String = std::iter::repeat_with(|| (b'a' + rng.next_u8() % 26) as char)
            .take(msg_len)
            .collect();

        let tx = Tx::new(
            parents.to_vec(),
            account.addr.clone(),
            seq,
            json!({
                "ca": account.addr.as_str(),
                "to": to.as_str(),
                "val": val,
                "msg": msg,
            }),
        );
        let hash = ordered_sum(&tx, TRANSFER_TOKEN).expect("ordered_sum не падает на валидном Tx");
        let sign = hex::encode(account.sk.sign(hash.as_bytes()).to_bytes());
        SignedTx {
            tx,
            sign,
            func: TRANSFER_TOKEN.to_string(),
        }
    }

    /// Число родителей, которое запросит генератор при размере пула `pool_len`
    /// (`clamp(--parents, 2, min(100, |pool|))`); `None`, если пул меньше двух.
    pub fn effective_parent_count(&self, pool_len: usize) -> Option<usize> {
        if pool_len < 2 {
            return None;
        }
        let max = pool_len.min(100);
        Some(self.config.parents.clamp(2, max))
    }
}

/// Выбирает `k` уникальных родителей из пула детерминированным образом.
///
/// Пул уже отсортирован по числу активных родителей (`/pool`), поэтому берём
/// кандидатов из начала — «наименее связанных», но со случайным смещением окна,
/// чтобы нагрузка не липла к одним и тем же узлам.
pub fn choose_parents(pool: &[Hash], k: usize, rng: &mut Prng) -> Vec<Hash> {
    if pool.is_empty() || k == 0 {
        return Vec::new();
    }
    let k = k.min(pool.len());
    if k == pool.len() {
        return pool.to_vec();
    }
    // Окно из `k` последовательных элементов отсортированного пула; стартовая
    // позиция случайна, чтобы распределять потомков по разным «лёгким» узлам.
    let start = rng.below((pool.len() - k + 1) as u64) as usize;
    pool[start..start + k].to_vec()
}

/// Детерминированно выводит 32 байта ключа из общих `seed`/`index`.
fn derive_seed_bytes(seed: u64, index: u64) -> [u8; 32] {
    let mut input = [0u8; 16];
    input[..8].copy_from_slice(&seed.to_le_bytes());
    input[8..].copy_from_slice(&index.to_le_bytes());
    *blake3::hash(&input).as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generator(seed: u64, accounts: usize, parents: usize) -> Generator {
        Generator::new(GeneratorConfig {
            seed,
            accounts,
            parents,
        })
    }

    #[test]
    fn seq_is_strictly_monotonic_within_account() {
        let g = generator(1, 1, 2);
        let parents = vec![Hash::from("a"), Hash::from("b")];
        let mut rng = Prng::new(10);
        let mut last = 0u32;
        for _ in 0..100 {
            let _gate = g.account_gate(0);
            let signed = g.build(0, &parents, &mut rng);
            assert_eq!(signed.tx.sequence(), last + 1);
            last = signed.tx.sequence();
        }
    }

    #[test]
    fn parallel_issuance_never_repeats_seq_per_account() {
        use std::collections::HashSet;
        use std::sync::Arc;

        let g = Arc::new(generator(2, 4, 2));
        let parents = vec![Hash::from("a"), Hash::from("b")];
        let handles: Vec<_> = (0..4usize)
            .map(|worker| {
                let g = Arc::clone(&g);
                let parents = parents.clone();
                std::thread::spawn(move || {
                    let mut rng = Prng::new(100 + worker as u64);
                    let mut seqs = Vec::new();
                    for _ in 0..200 {
                        // Каждая задача работает со своим аккаунтом (как в драйвере).
                        let idx = worker % 4;
                        let _gate = g.account_gate(idx);
                        let signed = g.build(idx, &parents, &mut rng);
                        seqs.push((idx, signed.tx.sequence()));
                    }
                    seqs
                })
            })
            .collect();

        let mut seen: HashSet<(usize, u32)> = HashSet::new();
        for h in handles {
            for pair in h.join().unwrap() {
                assert!(seen.insert(pair), "повтор seq для аккаунта: {pair:?}");
            }
        }
        // Всего выпущено 4 * 200 уникальных (аккаунт, seq).
        assert_eq!(seen.len(), 800);
    }

    #[test]
    fn accounts_are_deterministic_for_seed() {
        let a = generator(5, 3, 2);
        let b = generator(5, 3, 2);
        for i in 0..3 {
            assert_eq!(a.accounts[i].addr, b.accounts[i].addr);
        }
        let mut rng = Prng::new(1);
        let parents = vec![Hash::from("a"), Hash::from("b")];
        let left = a.build(0, &parents, &mut rng);
        let mut rng = Prng::new(1);
        let right = b.build(0, &parents, &mut rng);
        assert_eq!(left.sign, right.sign);
    }

    #[test]
    fn effective_parent_count_is_clamped() {
        let g = generator(1, 1, 2);
        assert_eq!(g.effective_parent_count(0), None);
        assert_eq!(g.effective_parent_count(1), None);
        assert_eq!(g.effective_parent_count(2), Some(2));
        assert_eq!(g.effective_parent_count(500), Some(2));

        let g = generator(1, 1, 150);
        assert_eq!(g.effective_parent_count(3), Some(3));
        assert_eq!(g.effective_parent_count(500), Some(100));
    }

    #[test]
    fn choose_parents_respects_bounds_and_uniqueness() {
        let pool: Vec<Hash> = (0..50).map(|i| Hash::from(i.to_string())).collect();
        let mut rng = Prng::new(3);
        for k in 2..=50 {
            let parents = choose_parents(&pool, k, &mut rng);
            assert_eq!(parents.len(), k);
            let unique: std::collections::HashSet<_> = parents.iter().collect();
            assert_eq!(unique.len(), k);
        }
        assert!(choose_parents(&pool, 0, &mut rng).is_empty());
        assert!(choose_parents(&[], 2, &mut rng).is_empty());
        // k > |pool| зажимается до |pool|.
        assert_eq!(choose_parents(&pool, 999, &mut rng).len(), pool.len());
    }
}
