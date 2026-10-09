//! Логика архивации и удаления «тяжёлых» узлов.
//!
//! Общая для внутреннего HTTP-хендлера (`/remove_heavy_nodes`) и фонового
//! процессора лидера. Функции выбора кандидатов не клонируют весь DAG: они
//! читают состояние под коротким read-локом и возвращают только нужные хэши.

use tracing::{debug, error, info};

use crate::app::App;
use crate::audit::{self, AuditSource};
use crate::domain::Hash;
use crate::raft::command::Request;
use crate::raft::store::StateMachineStore;

/// Отбирает узлы без активных родителей, годные к очистке, и обрезает до
/// размера батча. Весь DAG не клонируется.
pub async fn select_candidates(sm: &StateMachineStore, batch_size: usize) -> Vec<Hash> {
    let state_machine = sm.state_machine.read().await;
    let dag = &state_machine.dag;

    let mut candidates: Vec<Hash> = dag
        .get_node_keys()
        .into_iter()
        .filter(|node| {
            dag.get_nodes()
                .get(node)
                .map(|n| n.parents.is_empty())
                .unwrap_or(false)
        })
        .collect();

    // Детерминированный порядок перед обрезкой (не зависит от порядка HashMap).
    candidates.sort();
    candidates.truncate(batch_size);
    candidates
}

/// Возвращает подмножество кандидатов, чей вес превышает порог.
/// Веса считаются под коротким read-локом без клонирования DAG.
pub async fn select_heavy(
    sm: &StateMachineStore,
    candidates: &[Hash],
    weight_threshold: f64,
) -> Vec<Hash> {
    if candidates.is_empty() {
        return Vec::new();
    }
    let state_machine = sm.state_machine.read().await;
    let weights = state_machine.dag.compute_weights_for_batch(candidates);

    let mut heavy: Vec<Hash> = weights
        .into_iter()
        .filter_map(|(node, weight)| (weight > weight_threshold).then_some(node))
        .collect();
    heavy.sort();
    heavy
}

/// Узел и его сериализованные данные, подготовленные к архивации.
type NodePayload = (Hash, String);

/// Архивирует узлы в Redis и удаляет их через Raft.
///
/// Возвращает число удалённых узлов. Узлы, уже находящиеся в реестре `added`,
/// пропускаются. Данные читаются под коротким локом, запись в Redis и Raft
/// выполняется вне блокировки. `source` фиксируется в аудит-логе (O7).
pub async fn archive_and_remove(
    app: &App,
    candidates: Vec<Hash>,
    source: AuditSource,
) -> Result<usize, String> {
    if candidates.is_empty() {
        return Ok(0);
    }

    // Берём данные узлов и отсекаем те, что уже удалены.
    let (nodes_to_remove, payloads): (Vec<Hash>, Vec<NodePayload>) = {
        let state_machine = app.state_machine.state_machine.read().await;
        let dag = &state_machine.dag;

        let mut to_remove = Vec::new();
        let mut payloads = Vec::new();
        for node in &candidates {
            if dag.is_node_added(node) {
                debug!("Node {} already in added, skipping.", node);
                continue;
            }
            match dag.get_node_data(node) {
                Some(data) => match serde_json::to_string(data) {
                    Ok(json) => {
                        to_remove.push(node.clone());
                        payloads.push((node.clone(), json));
                    }
                    Err(e) => error!("Failed to serialize node {} data: {}", node, e),
                },
                None => debug!("No data found for node {}, skipping.", node),
            }
        }
        (to_remove, payloads)
    };

    if nodes_to_remove.is_empty() {
        return Ok(0);
    }

    // Пишем архив в Redis.
    let mut redis = app.redis.clone();
    let mut redis_pipe = redis::pipe();
    for (node, json) in &payloads {
        redis_pipe
            .set(format!("confirmed:{}", node.as_str()), json.clone())
            .ignore();
    }

    if let Err(e) = redis_pipe.query_async::<()>(&mut redis).await {
        return Err(format!(
            "Failed to write batch to Redis ({e}). Nodes will not be removed."
        ));
    }

    info!(
        "archiving {} node(s) to Redis before removal",
        nodes_to_remove.len()
    );

    let request = Request::Remove {
        nodes: nodes_to_remove.clone(),
    };
    match app.raft.client_write(request).await {
        Ok(_) => {
            audit::remove(source, app.id, &nodes_to_remove, true, None);
        }
        Err(e) => {
            audit::remove(
                source,
                app.id,
                &nodes_to_remove,
                false,
                Some(&e.to_string()),
            );
            return Err(format!("Raft error: {e}"));
        }
    }

    Ok(nodes_to_remove.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::Tx;
    use crate::domain::Func;
    use serde_json::json;

    fn tx(parents: Vec<Hash>) -> Tx {
        Tx {
            prnts: parents,
            addr: crate::domain::Address::from("addr"),
            seq: 0,
            var: json!({ "ca": "a", "to": "b", "val": 1, "msg": "m" }),
        }
    }

    async fn store_with_chain() -> Arc<StateMachineStore> {
        // root -> child; у root нет родителей, у child — есть.
        let store = Arc::new(StateMachineStore::default());
        {
            let mut sm = store.state_machine.write().await;
            let root = Hash::from("root");
            sm.dag
                .add_node_with_parents(root.clone(), tx(vec![]), String::new(), Func::TransferToken)
                .unwrap();
            sm.dag
                .add_node_with_parents(
                    Hash::from("child"),
                    tx(vec![root]),
                    String::new(),
                    Func::TransferToken,
                )
                .unwrap();
        }
        store
    }

    #[tokio::test]
    async fn select_candidates_only_parentless_and_truncated() {
        let store = store_with_chain().await;
        let candidates = select_candidates(store.as_ref(), 100).await;
        assert_eq!(candidates, vec![Hash::from("root")]);

        // Обрезка до батча.
        let empty = select_candidates(store.as_ref(), 0).await;
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn select_heavy_respects_threshold() {
        let store = store_with_chain().await;
        let candidates = vec![Hash::from("root")];

        // root имеет одного потомка (вес 0.5 после насыщения).
        let heavy = select_heavy(store.as_ref(), &candidates, 0.4).await;
        assert_eq!(heavy, vec![Hash::from("root")]);

        let not_heavy = select_heavy(store.as_ref(), &candidates, 0.6).await;
        assert!(not_heavy.is_empty());
    }
}
