//! Персистентный слой хранения на `redb` (чистый Rust, без C-зависимостей).
//!
//! Используется одновременно для Raft-лога и для state machine. Все операции
//! сериализуются внешними мьютексами хранилищ, поэтому `Db` не добавляет
//! собственной блокировки. Ошибки приводятся к `String` и на уровне хранилищ
//! конвертируются в `StorageError`.

use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};

/// Таблица метаданных: vote, committed, last_purged, снимок state machine.
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
/// Таблица Raft-логов: индекс -> сериализованная запись.
const LOGS: TableDefinition<u64, &[u8]> = TableDefinition::new("logs");

/// Обёртка над `redb::Database` с набором операций, нужных Raft-хранилищам.
#[derive(Debug)]
pub struct Db {
    db: Database,
}

impl Db {
    /// Открывает (при необходимости создаёт) базу по указанному пути.
    pub fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            }
        }
        let db = Database::create(path).map_err(|e| e.to_string())?;
        // Гарантируем существование таблиц, чтобы их можно было открывать на чтение.
        let tx = db.begin_write().map_err(|e| e.to_string())?;
        {
            tx.open_table(META).map_err(|e| e.to_string())?;
            tx.open_table(LOGS).map_err(|e| e.to_string())?;
        }
        tx.commit().map_err(|e| e.to_string())?;
        Ok(Self { db })
    }

    /// Читает значение из таблицы метаданных.
    pub fn meta_get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let tx = self.db.begin_read().map_err(|e| e.to_string())?;
        let table = tx.open_table(META).map_err(|e| e.to_string())?;
        let value = table
            .get(key)
            .map_err(|e| e.to_string())?
            .map(|guard| guard.value().to_vec());
        Ok(value)
    }

    /// Записывает значение в таблицу метаданных.
    pub fn meta_set(&self, key: &str, value: &[u8]) -> Result<(), String> {
        let tx = self.db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut table = tx.open_table(META).map_err(|e| e.to_string())?;
            table.insert(key, value).map_err(|e| e.to_string())?;
        }
        tx.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Удаляет значение из таблицы метаданных.
    pub fn meta_remove(&self, key: &str) -> Result<(), String> {
        let tx = self.db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut table = tx.open_table(META).map_err(|e| e.to_string())?;
            table.remove(key).map_err(|e| e.to_string())?;
        }
        tx.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Возвращает последний индекс лога, если лог не пуст.
    pub fn logs_last_index(&self) -> Result<Option<u64>, String> {
        let tx = self.db.begin_read().map_err(|e| e.to_string())?;
        let table = tx.open_table(LOGS).map_err(|e| e.to_string())?;
        let last = table
            .range::<u64>(..)
            .map_err(|e| e.to_string())?
            .next_back()
            .transpose()
            .map_err(|e| e.to_string())?
            .map(|(key, _)| key.value());
        Ok(last)
    }

    /// Возвращает записи лога в диапазоне `[start, end)`.
    pub fn logs_range(&self, start: u64, end: u64) -> Result<Vec<(u64, Vec<u8>)>, String> {
        let tx = self.db.begin_read().map_err(|e| e.to_string())?;
        let table = tx.open_table(LOGS).map_err(|e| e.to_string())?;
        let mut result = Vec::new();
        for item in table.range(start..end).map_err(|e| e.to_string())? {
            let (key, value) = item.map_err(|e| e.to_string())?;
            result.push((key.value(), value.value().to_vec()));
        }
        Ok(result)
    }

    /// Атомарно добавляет/перезаписывает набор записей лога.
    pub fn logs_append(&self, entries: &[(u64, Vec<u8>)]) -> Result<(), String> {
        let tx = self.db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut table = tx.open_table(LOGS).map_err(|e| e.to_string())?;
            for (index, bytes) in entries {
                table
                    .insert(*index, bytes.as_slice())
                    .map_err(|e| e.to_string())?;
            }
        }
        tx.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Удаляет записи лога с индексами `>= index`.
    pub fn logs_remove_from(&self, index: u64) -> Result<(), String> {
        let tx = self.db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut table = tx.open_table(LOGS).map_err(|e| e.to_string())?;
            let keys: Vec<u64> = table
                .range(index..)
                .map_err(|e| e.to_string())?
                .map(|item| item.map(|(k, _)| k.value()).map_err(|e| e.to_string()))
                .collect::<Result<_, _>>()?;
            for key in keys {
                table.remove(key).map_err(|e| e.to_string())?;
            }
        }
        tx.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Удаляет записи лога с индексами `<= index`.
    pub fn logs_remove_through(&self, index: u64) -> Result<(), String> {
        let tx = self.db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut table = tx.open_table(LOGS).map_err(|e| e.to_string())?;
            let keys: Vec<u64> = table
                .range(..=index)
                .map_err(|e| e.to_string())?
                .map(|item| item.map(|(k, _)| k.value()).map_err(|e| e.to_string()))
                .collect::<Result<_, _>>()?;
            for key in keys {
                table.remove(key).map_err(|e| e.to_string())?;
            }
        }
        tx.commit().map_err(|e| e.to_string())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_db_path(name: &str) -> std::path::PathBuf {
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "dagdb-db-test-{}-{}-{id}",
            std::process::id(),
            name
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("db.redb")
    }

    #[test]
    fn meta_roundtrip_survives_reopen() {
        let path = temp_db_path("meta");
        {
            let db = Db::open(&path).unwrap();
            db.meta_set("k", b"v1").unwrap();
        }
        {
            let db = Db::open(&path).unwrap();
            assert_eq!(db.meta_get("k").unwrap().as_deref(), Some(b"v1".as_ref()));
            db.meta_set("k", b"v2").unwrap();
            db.meta_remove("k").unwrap();
            assert!(db.meta_get("k").unwrap().is_none());
        }
    }

    #[test]
    fn logs_roundtrip_and_trimming() {
        let path = temp_db_path("logs");
        {
            let db = Db::open(&path).unwrap();
            db.logs_append(&[
                (1, b"one".to_vec()),
                (2, b"two".to_vec()),
                (3, b"three".to_vec()),
            ])
            .unwrap();
            assert_eq!(db.logs_last_index().unwrap(), Some(3));
        }
        {
            let db = Db::open(&path).unwrap();
            let rows = db.logs_range(1, 3).unwrap();
            assert_eq!(rows, vec![(1, b"one".to_vec()), (2, b"two".to_vec())]);

            db.logs_remove_from(3).unwrap();
            assert_eq!(db.logs_last_index().unwrap(), Some(2));

            db.logs_remove_through(1).unwrap();
            assert_eq!(db.logs_range(0, 10).unwrap(), vec![(2, b"two".to_vec())]);
        }
    }
}
