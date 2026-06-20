//! Raft type configuration for ceredb.

use std::io::Cursor;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Vec<u8>,
        R = Vec<u8>,
        NodeId = u64,
        Node = openraft::impls::BasicNode,
);
