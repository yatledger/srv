//! `LogStore` — in-memory реализация `RaftLogStorage` (для тестов), а также
//! персистентная реализация `PersistentLogStore` на `redb` (для эксплуатации).

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::LogState;
use openraft::RaftTypeConfig;
use openraft::StorageError;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::{RaftLogReader, RaftLogStorage};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use super::db::Db;

/// RaftLogStore implementation with a in-memory storage
#[derive(Clone, Debug, Default)]
pub struct LogStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<LogStoreInner<C>>>,
}

#[derive(Debug)]
pub struct LogStoreInner<C: RaftTypeConfig> {
    /// The last purged log id.
    last_purged_log_id: Option<LogIdOf<C>>,

    /// The Raft log.
    log: BTreeMap<u64, C::Entry>,

    /// The commit log id.
    committed: Option<LogIdOf<C>>,

    /// The current granted vote.
    vote: Option<VoteOf<C>>,
}

impl<C: RaftTypeConfig> Default for LogStoreInner<C> {
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            log: BTreeMap::new(),
            committed: None,
            vote: None,
        }
    }
}

impl<C: RaftTypeConfig> LogStoreInner<C> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C>>
    where
        C::Entry: Clone,
    {
        let response = self
            .log
            .range(range.clone())
            .map(|(_, val)| val.clone())
            .collect::<Vec<_>>();
        Ok(response)
    }

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C>> {
        let last = self.log.iter().next_back().map(|(_, ent)| ent.log_id());

        let last_purged = self.last_purged_log_id.clone();

        let last = match last {
            None => last_purged.clone(),
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<C>>,
    ) -> Result<(), StorageError<C>> {
        self.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, StorageError<C>> {
        Ok(self.committed.clone())
    }

    async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), StorageError<C>> {
        self.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, StorageError<C>> {
        Ok(self.vote.clone())
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), StorageError<C>>
    where
        I: IntoIterator<Item = C::Entry>,
    {
        // Simple implementation that calls the flush-before-return `append_to_log`.
        for entry in entries {
            self.log.insert(entry.index(), entry);
        }
        callback.io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogIdOf<C>) -> Result<(), StorageError<C>> {
        let keys = self
            .log
            .range(log_id.index()..)
            .map(|(k, _v)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            self.log.remove(&key);
        }

        Ok(())
    }

    async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), StorageError<C>> {
        {
            let ld = &mut self.last_purged_log_id;
            assert!(ld.as_ref() <= Some(&log_id));
            *ld = Some(log_id.clone());
        }

        {
            let keys = self
                .log
                .range(..=log_id.index())
                .map(|(k, _v)| *k)
                .collect::<Vec<_>>();
            for key in keys {
                self.log.remove(&key);
            }
        }

        Ok(())
    }
}

mod impl_log_store {
    use std::fmt::Debug;
    use std::ops::RangeBounds;

    use openraft::LogState;
    use openraft::RaftLogReader;
    use openraft::RaftTypeConfig;
    use openraft::StorageError;
    use openraft::alias::LogIdOf;
    use openraft::alias::VoteOf;
    use openraft::storage::IOFlushed;
    use openraft::storage::RaftLogStorage;

    use crate::LogStore;

    impl<C: RaftTypeConfig> RaftLogReader<C> for LogStore<C>
    where
        C::Entry: Clone,
    {
        async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
            &mut self,
            range: RB,
        ) -> Result<Vec<C::Entry>, StorageError<C>> {
            let mut inner = self.inner.lock().await;
            inner.try_get_log_entries(range).await
        }

        async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, StorageError<C>> {
            let mut inner = self.inner.lock().await;
            inner.read_vote().await
        }
    }

    impl<C: RaftTypeConfig> RaftLogStorage<C> for LogStore<C>
    where
        C::Entry: Clone,
    {
        type LogReader = Self;

        async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C>> {
            let mut inner = self.inner.lock().await;
            inner.get_log_state().await
        }

        async fn save_committed(
            &mut self,
            committed: Option<LogIdOf<C>>,
        ) -> Result<(), StorageError<C>> {
            let mut inner = self.inner.lock().await;
            inner.save_committed(committed).await
        }

        async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, StorageError<C>> {
            let mut inner = self.inner.lock().await;
            inner.read_committed().await
        }

        async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), StorageError<C>> {
            let mut inner = self.inner.lock().await;
            inner.save_vote(vote).await
        }

        async fn append<I>(
            &mut self,
            entries: I,
            callback: IOFlushed<C>,
        ) -> Result<(), StorageError<C>>
        where
            I: IntoIterator<Item = C::Entry>,
        {
            let mut inner = self.inner.lock().await;
            inner.append(entries, callback).await
        }

        async fn truncate(&mut self, log_id: LogIdOf<C>) -> Result<(), StorageError<C>> {
            let mut inner = self.inner.lock().await;
            inner.truncate(log_id).await
        }

        async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), StorageError<C>> {
            let mut inner = self.inner.lock().await;
            inner.purge(log_id).await
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.clone()
        }
    }
}

/// Именованные ключи в таблице метаданных для Raft-лога.
const KEY_VOTE: &str = "log_vote";
const KEY_COMMITTED: &str = "log_committed";
const KEY_LAST_PURGED: &str = "log_last_purged";

/// Персистентная реализация `RaftLogStorage` на `redb`.
///
/// Сохраняет vote, committed и все записи лога; чтение и запись сериализуются
/// `Mutex`. Требуется `C::Entry: Serialize + DeserializeOwned`, что
/// обеспечивается фичей `serde` у `openraft`.
#[derive(Clone, Debug)]
pub struct PersistentLogStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<PersistentLogStoreInner<C>>>,
}

#[derive(Debug)]
pub struct PersistentLogStoreInner<C: RaftTypeConfig> {
    db: Db,
    _marker: std::marker::PhantomData<C>,
}

impl<C: RaftTypeConfig> PersistentLogStore<C> {
    /// Открывает персистентный лог по пути `path`.
    pub fn open(path: &std::path::Path) -> Result<Self, String> {
        let db = Db::open(path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(PersistentLogStoreInner {
                db,
                _marker: std::marker::PhantomData,
            })),
        })
    }
}

fn deserialize_log_entry<C, E>(bytes: &[u8]) -> Result<E, StorageError<C>>
where
    C: RaftTypeConfig,
    E: DeserializeOwned,
{
    serde_json::from_slice(bytes).map_err(|e| {
        StorageError::new(
            openraft::ErrorSubject::Logs,
            openraft::ErrorVerb::Read,
            openraft::AnyError::error(e),
        )
    })
}

fn io_error<C>(
    subject: openraft::ErrorSubject<C>,
    verb: openraft::ErrorVerb,
    e: String,
) -> StorageError<C>
where
    C: RaftTypeConfig,
{
    StorageError::new(subject, verb, openraft::AnyError::error(e))
}

impl<C: RaftTypeConfig> RaftLogReader<C> for PersistentLogStore<C>
where
    C::Entry: Serialize + DeserializeOwned + Clone,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C>> {
        use std::ops::Bound;
        let start = match range.start_bound() {
            Bound::Included(v) => *v,
            Bound::Excluded(v) => v.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(v) => v.saturating_add(1),
            Bound::Excluded(v) => *v,
            Bound::Unbounded => u64::MAX,
        };

        let inner = self.inner.lock().await;
        let rows = inner
            .db
            .logs_range(start, end)
            .map_err(|e| io_error(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Read, e))?;
        drop(inner);

        let mut entries = Vec::with_capacity(rows.len());
        for (_, bytes) in rows {
            entries.push(deserialize_log_entry::<C, C::Entry>(&bytes)?);
        }
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, StorageError<C>> {
        let inner = self.inner.lock().await;
        match inner
            .db
            .meta_get(KEY_VOTE)
            .map_err(|e| io_error(openraft::ErrorSubject::Vote, openraft::ErrorVerb::Read, e))?
        {
            Some(bytes) => Ok(Some(deserialize_log_entry::<C, VoteOf<C>>(&bytes)?)),
            None => Ok(None),
        }
    }
}

impl<C: RaftTypeConfig> RaftLogStorage<C> for PersistentLogStore<C>
where
    C::Entry: Serialize + DeserializeOwned + Clone,
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C>> {
        let inner = self.inner.lock().await;

        let last_purged: Option<LogIdOf<C>> =
            match inner.db.meta_get(KEY_LAST_PURGED).map_err(|e| {
                io_error(openraft::ErrorSubject::Store, openraft::ErrorVerb::Read, e)
            })? {
                Some(bytes) => Some(deserialize_log_entry::<C, LogIdOf<C>>(&bytes)?),
                None => None,
            };

        let last_log = match inner
            .db
            .logs_last_index()
            .map_err(|e| io_error(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Read, e))?
        {
            Some(index) => {
                let rows = inner
                    .db
                    .logs_range(index, index.saturating_add(1))
                    .map_err(|e| {
                        io_error(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Read, e)
                    })?;
                match rows.into_iter().next() {
                    Some((_, bytes)) => {
                        let entry: C::Entry = deserialize_log_entry(&bytes)?;
                        Some(RaftEntry::log_id(&entry))
                    }
                    None => last_purged.clone(),
                }
            }
            None => last_purged.clone(),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last_log,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<C>>,
    ) -> Result<(), StorageError<C>> {
        let inner = self.inner.lock().await;
        match committed {
            Some(log_id) => {
                let bytes = serde_json::to_vec(&log_id).map_err(|e| {
                    io_error(
                        openraft::ErrorSubject::Store,
                        openraft::ErrorVerb::Write,
                        e.to_string(),
                    )
                })?;
                inner.db.meta_set(KEY_COMMITTED, &bytes).map_err(|e| {
                    io_error(openraft::ErrorSubject::Store, openraft::ErrorVerb::Write, e)
                })
            }
            None => inner.db.meta_remove(KEY_COMMITTED).map_err(|e| {
                io_error(
                    openraft::ErrorSubject::Store,
                    openraft::ErrorVerb::Delete,
                    e,
                )
            }),
        }
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, StorageError<C>> {
        let inner = self.inner.lock().await;
        match inner
            .db
            .meta_get(KEY_COMMITTED)
            .map_err(|e| io_error(openraft::ErrorSubject::Store, openraft::ErrorVerb::Read, e))?
        {
            Some(bytes) => Ok(Some(deserialize_log_entry::<C, LogIdOf<C>>(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), StorageError<C>> {
        let bytes = serde_json::to_vec(vote).map_err(|e| {
            io_error(
                openraft::ErrorSubject::Vote,
                openraft::ErrorVerb::Write,
                e.to_string(),
            )
        })?;
        let inner = self.inner.lock().await;
        inner
            .db
            .meta_set(KEY_VOTE, &bytes)
            .map_err(|e| io_error(openraft::ErrorSubject::Vote, openraft::ErrorVerb::Write, e))
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), StorageError<C>>
    where
        I: IntoIterator<Item = C::Entry>,
    {
        let mut rows = Vec::new();
        for entry in entries {
            let index = entry.index();
            let bytes = serde_json::to_vec(&entry).map_err(|e| {
                io_error(
                    openraft::ErrorSubject::LogIndex(index),
                    openraft::ErrorVerb::Write,
                    e.to_string(),
                )
            })?;
            rows.push((index, bytes));
        }

        let result = {
            let inner = self.inner.lock().await;
            inner
                .db
                .logs_append(&rows)
                .map_err(|e| io_error(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Write, e))
        };

        match result {
            Ok(()) => {
                callback.io_completed(Ok(()));
                Ok(())
            }
            Err(e) => {
                callback.io_completed(Err(std::io::Error::other(e.to_string())));
                Err(e)
            }
        }
    }

    async fn truncate(&mut self, log_id: LogIdOf<C>) -> Result<(), StorageError<C>> {
        let inner = self.inner.lock().await;
        inner
            .db
            .logs_remove_from(log_id.index)
            .map_err(|e| io_error(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Delete, e))
    }

    async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), StorageError<C>> {
        let bytes = serde_json::to_vec(&log_id).map_err(|e| {
            io_error(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Write,
                e.to_string(),
            )
        })?;
        let inner = self.inner.lock().await;
        inner
            .db
            .meta_set(KEY_LAST_PURGED, &bytes)
            .map_err(|e| io_error(openraft::ErrorSubject::Store, openraft::ErrorVerb::Write, e))?;
        inner
            .db
            .logs_remove_through(log_id.index)
            .map_err(|e| io_error(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Delete, e))
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}
