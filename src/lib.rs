pub mod graph;
pub mod server;
pub mod cleaner;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use graph::DAG;

// Псевдоним для списка смежности графа: узел -> список его детей или родителей.
pub type Adjacency = HashMap<Arc<str>, Vec<Arc<str>>>;
pub type DagDb = Arc<RwLock<DAG>>;