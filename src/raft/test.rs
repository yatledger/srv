use std::sync::Arc;

use super::log;
use openraft::StorageError;
use openraft::testing::log::StoreBuilder;
use openraft::testing::log::Suite;

pub type LogStore = log::LogStore<TypeConfig>;
use super::store::StateMachineStore;
use crate::TypeConfig;

struct MemDAGStoreBuilder {}

impl StoreBuilder<TypeConfig, LogStore, Arc<StateMachineStore>, ()> for MemDAGStoreBuilder {
    async fn build(
        &self,
    ) -> Result<((), LogStore, Arc<StateMachineStore>), StorageError<TypeConfig>> {
        Ok(((), LogStore::default(), Arc::default()))
    }
}

#[tokio::test]
pub async fn test_mem_store() -> Result<(), StorageError<TypeConfig>> {
    Suite::test_all(MemDAGStoreBuilder {}).await?;
    Ok(())
}
