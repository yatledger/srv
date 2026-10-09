pub mod api;
pub mod command;
pub mod db;
pub mod log;
pub mod network;
pub mod router;
pub mod store;

pub mod typ {
    use crate::TypeConfig;

    pub type Raft = openraft::Raft<TypeConfig>;

    pub type Vote = <TypeConfig as openraft::RaftTypeConfig>::Vote;
    pub type LogId = openraft::LogId<TypeConfig>;
    pub type Entry = <TypeConfig as openraft::RaftTypeConfig>::Entry;
    pub type StoredMembership = openraft::StoredMembership<TypeConfig>;

    pub type SnapshotMeta = openraft::SnapshotMeta<TypeConfig>;
    pub type Snapshot = openraft::Snapshot<TypeConfig>;
    pub type SnapshotData = <TypeConfig as openraft::RaftTypeConfig>::SnapshotData;

    pub type RPCError<E = openraft::error::Infallible> = openraft::error::RPCError<TypeConfig, E>;

    pub type StorageError = openraft::StorageError<TypeConfig>;
    pub type StreamingError = openraft::error::StreamingError<TypeConfig>;

    pub type VoteRequest = openraft::raft::VoteRequest<TypeConfig>;
    pub type VoteResponse = openraft::raft::VoteResponse<TypeConfig>;
    pub type AppendEntriesRequest = openraft::raft::AppendEntriesRequest<TypeConfig>;
    pub type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<TypeConfig>;
    pub type SnapshotResponse = openraft::raft::SnapshotResponse<TypeConfig>;
}

#[cfg(test)]
mod test;
