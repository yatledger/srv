//! Бенчмарки производительности графа (O9).
//!
//! Фиксируют базовые числа для обхода потомков с весами, вычисления глубин и
//! throughput записи узлов. Запуск: `cargo bench`. В CI проверяется только
//! компиляция бенчмарков, чтобы не удлинять пайплайн.

use std::collections::{HashMap, HashSet};

use criterion::{Criterion, criterion_group, criterion_main};

use dagdb::Tx;
use dagdb::domain::{Address, Func, Hash};
use dagdb::graph::{dag::Dag, weights};

/// Строит цепочку из `n` узлов: node-0 -> node-1 -> ... -> node-{n-1}.
fn chain(n: usize) -> Dag {
    let mut dag = Dag::new();
    let mut prev: Option<Hash> = None;
    for i in 0..n {
        let hash = Hash::from(format!("node-{i}").as_str());
        let parents = prev.iter().cloned().collect::<Vec<_>>();
        dag.add_node_with_parents(
            hash.clone(),
            tx(parents),
            String::new(),
            Func::TransferToken,
        )
        .expect("valid tx");
        prev = Some(hash);
    }
    dag
}

fn tx(parents: Vec<Hash>) -> Tx {
    Tx::new(
        parents,
        Address::from("bench-addr"),
        0,
        serde_json::json!({ "ca": "0", "to": "T", "val": 1, "msg": "bench" }),
    )
}

fn bench_descendants(c: &mut Criterion) {
    let dag = chain(500);
    let roots: HashSet<Hash> = dag.get_nodes().keys().cloned().collect();
    let nodes_map: HashMap<Hash, _> = dag.get_nodes().clone();

    c.bench_function("compute_descendants_with_depth_and_weight/500", |b| {
        b.iter(|| weights::compute_descendants_with_depth_and_weight(&nodes_map, &roots));
    });
}

fn bench_nodes_by_depth(c: &mut Criterion) {
    let dag = chain(1_000);
    let nodes_map = dag.get_nodes().clone();

    c.bench_function("get_nodes_by_depth/1000", |b| {
        b.iter(|| weights::get_nodes_by_depth(&nodes_map));
    });
}

fn bench_add_throughput(c: &mut Criterion) {
    c.bench_function("add_node_with_parents", |b| {
        b.iter_with_setup(
            || {
                let mut dag = Dag::new();
                let root = Hash::from("root");
                dag.add_node_with_parents(
                    root.clone(),
                    tx(vec![]),
                    String::new(),
                    Func::TransferToken,
                )
                .expect("valid root");
                (dag, root)
            },
            |(mut dag, root)| {
                dag.add_node_with_parents(
                    Hash::from("child"),
                    tx(vec![root]),
                    String::new(),
                    Func::TransferToken,
                )
                .expect("valid child");
            },
        );
    });
}

criterion_group!(
    benches,
    bench_descendants,
    bench_nodes_by_depth,
    bench_add_throughput
);
criterion_main!(benches);
