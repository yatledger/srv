use std::sync::Arc;
use openraft::storage::Adaptor;

use crate::store::{MemStore};
use crate::NodeId;
use crate::TypeConfig;
use crate::typ;

pub async fn start(){
    let node_id: u64 = 1;
    let config = openraft::Config::default();

    let store: Arc<MemStore> = Arc::new(MemStore::new());
    let (log_store, state_machine) = Adaptor::<TypeConfig, _>::new(store);

    // Сетевая фабрика:
    let network_factory = MyNetworkFactory::new();

    // Создаём Raft:
    let _raft = openraft::Raft::<TypeConfig>::new(
        node_id,
        Arc::new(config),
        network_factory,
        log_store,
        state_machine,
    );
}

