use serde_derive::{Deserialize, Serialize};
use tokio::time::Duration;
use async_raft::{NodeId, AppData, AppDataResponse, RaftStorage};
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use async_raft::storage::{CurrentSnapshotData, HardState, InitialState};
use std::io::Cursor;
use async_raft::raft::{EntryPayload, MembershipConfig, Entry};
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard, RwLock};
use std::collections::{BTreeMap, HashMap};
use anyhow::Result;
use crate::db::Db;
use bytes::Bytes;
use thiserror::Error;
use async_trait::async_trait;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Context {
    pub db: Arc<Db>,
    pub nodes: HashMap<NodeId, Node>
}

impl Context {

    #[tracing::instrument(level = "info", skip(self))]
    pub fn apply(&mut self, data: &ClientRequest) -> anyhow::Result<ClientResponse> {
        match data.cmd {
            Cmd::AddEntry {ref key, ref value} => {
                match self.db.get(key.as_str()) {
                    Some(val) => {
                        let prev = String::from_utf8(val.to_vec())?;
                        Ok(ClientResponse::Value {
                            prev: Some(prev),
                            result: Some(String::from_utf8(value.clone())?)
                        })
                    },
                    None => {
                        self.db.set(key.clone(), Bytes::from(value.clone()), None);
                        Ok(ClientResponse::Value {
                            prev: None,
                            result: Some(String::from_utf8(value.clone())?)
                        })
                    }
                }
            },
            _ => Ok(ClientResponse::Value {prev: None, result: None}),
        }
    }
}



#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Node {
    pub name: String,
    pub address: String
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Cmd {
    AddEntry {
        key: String,
        value: Vec<u8>
    },
    RemoveEntry {
        key: String
    },
    AddNode {
        node_id: NodeId,
        node: Node
    },
    RemoveNode {
        node_id: NodeId
    }
}

impl Display for Cmd {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Cmd::AddEntry { key, value} => {
                write!(f, "add entry: key = {}", key)
            }
            Cmd::RemoveEntry { key} => {
                write!(f, "remove entry: key = {}", key)
            }
            Cmd::AddNode { node_id, node} => {
                write!(f, "add node [id: {}, name: {}, address: {}]", node_id, node.name, node.address)
            }
            Cmd::RemoveNode { node_id} => {
                write!(f, "remove node [id: {}]", node_id)
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RaftTxId {
    pub client_id: String,
    pub serial: u64
}

impl RaftTxId {
    pub fn new(client_id: &str, serial: u64) -> Self {
        Self {
            client_id: client_id.to_string(),
            serial
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientRequest {
    pub tx_id: Option<RaftTxId>,
    pub cmd: Cmd
}

impl AppData for ClientRequest {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientResponse {
    Value {
        prev: Option<String>,
        result: Option<String>
    },
    Node {
        prev: Option<Node>,
        result: Option<Node>
    }
}

impl AppDataResponse for ClientResponse {}

/// RaftStorage

/// Error used to trigger Raft shutdown from storage.
#[derive(Clone, Debug, Error)]
pub enum ShutdownError {
    #[error("unsafe storage error")]
    UnsafeStorageError,
}

/// The application snapshot type which the `MemStore` works with.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MemStoreSnapshot {
    /// The last index covered by this snapshot.
    pub index: u64,
    /// The term of the last index covered by this snapshot.
    pub term: u64,
    /// The last memberhsip config included in this snapshot.
    pub membership: MembershipConfig,
    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

/// The state machine of the `MemStore`.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct MemStoreStateMachine {
    pub last_applied_log: u64,
    /// A mapping of client IDs to their state info.
    pub client_serial_responses: HashMap<String, (u64, ClientResponse)>,
    pub context: Context
}

impl MemStoreStateMachine {

    #[tracing::instrument(level = "info", skip(self))]
    pub fn apply(&mut self, index: u64, data: &ClientRequest) -> anyhow::Result<ClientResponse> {
        self.last_applied_log = index;
        if let Some(ref tx_id) = data.tx_id {
            if let Some(ref tx_id) = data.tx_id {
                if let Some((serial, response)) = self.client_serial_responses.get(&tx_id.client_id) {
                    if serial == &tx_id.serial {
                        return Ok(response.clone());
                    }
                }
            }
        }
        let resp = self.context.apply(data)?;
        if let Some(ref tx_id) = data.tx_id {
            self.client_serial_responses.insert(tx_id.client_id.clone(), (tx_id.serial, resp.clone()));
        }
        Ok(resp)
    }
}

/// An in-memory storage system implementing the `async_raft::RaftStorage` trait.
pub struct MemStore {
    /// The ID of the Raft node for which this memory storage instances is configured.
    id: NodeId,
    /// The Raft log.
    log: RwLock<BTreeMap<u64, Entry<ClientRequest>>>,
    /// The Raft state machine.
    sm: RwLock<MemStoreStateMachine>,
    /// The current hard state.
    hs: RwLock<Option<HardState>>,
    /// The current snapshot.
    current_snapshot: RwLock<Option<MemStoreSnapshot>>,
}

impl MemStore {
    /// Create a new `MemStore` instance.
    pub fn new(id: NodeId) -> Self {
        let log = RwLock::new(BTreeMap::new());
        let sm = RwLock::new(MemStoreStateMachine::default());
        let hs = RwLock::new(None);
        let current_snapshot = RwLock::new(None);
        Self {
            id,
            log,
            sm,
            hs,
            current_snapshot,
        }
    }

    /// Create a new `MemStore` instance with some existing state (for testing).
    #[cfg(test)]
    pub fn new_with_state(
        id: NodeId, log: BTreeMap<u64, Entry<ClientRequest>>, sm: MemStoreStateMachine, hs: Option<HardState>,
        current_snapshot: Option<MemStoreSnapshot>,
    ) -> Self {
        let log = RwLock::new(log);
        let sm = RwLock::new(sm);
        let hs = RwLock::new(hs);
        let current_snapshot = RwLock::new(current_snapshot);
        Self {
            id,
            log,
            sm,
            hs,
            current_snapshot,
        }
    }

    /// Get a handle to the log for testing purposes.
    pub async fn get_log(&self) -> RwLockWriteGuard<'_, BTreeMap<u64, Entry<ClientRequest>>> {
        self.log.write().await
    }

    /// Get a handle to the state machine for testing purposes.
    pub async fn get_state_machine(&self) -> RwLockWriteGuard<'_, MemStoreStateMachine> {
        self.sm.write().await
    }

    /// Get a handle to the current hard state for testing purposes.
    pub async fn read_hard_state(&self) -> RwLockReadGuard<'_, Option<HardState>> {
        self.hs.read().await
    }
}

#[async_trait]
impl RaftStorage<ClientRequest, ClientResponse> for MemStore {
    type Snapshot = Cursor<Vec<u8>>;
    type ShutdownError = ShutdownError;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        let log = self.log.read().await;
        let cfg_opt = log.values().rev().find_map(|entry| match &entry.payload {
            EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
            EntryPayload::SnapshotPointer(snap) => Some(snap.membership.clone()),
            _ => None,
        });
        Ok(match cfg_opt {
            Some(cfg) => cfg,
            None => MembershipConfig::new_initial(self.id),
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_initial_state(&self) -> Result<InitialState> {
        let membership = self.get_membership_config().await?;
        let mut hs = self.hs.write().await;
        let log = self.log.read().await;
        let sm = self.sm.read().await;
        match &mut *hs {
            Some(inner) => {
                let (last_log_index, last_log_term) = match log.values().rev().next() {
                    Some(log) => (log.index, log.term),
                    None => (0, 0),
                };
                let last_applied_log = sm.last_applied_log;
                Ok(InitialState {
                    last_log_index,
                    last_log_term,
                    last_applied_log,
                    hard_state: inner.clone(),
                    membership,
                })
            }
            None => {
                let new = InitialState::new_initial(self.id);
                *hs = Some(new.hard_state.clone());
                Ok(new)
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self, hs))]
    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        *self.hs.write().await = Some(hs.clone());
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<ClientRequest>>> {
        // Invalid request, return empty vec.
        if start > stop {
            tracing::error!("invalid request, start > stop");
            return Ok(vec![]);
        }
        let log = self.log.read().await;
        Ok(log.range(start..stop).map(|(_, val)| val.clone()).collect())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<()> {
        if stop.as_ref().map(|stop| &start > stop).unwrap_or(false) {
            tracing::error!("invalid request, start > stop");
            return Ok(());
        }
        let mut log = self.log.write().await;

        // If a stop point was specified, delete from start until the given stop point.
        if let Some(stop) = stop.as_ref() {
            for key in start..*stop {
                log.remove(&key);
            }
            return Ok(());
        }
        // Else, just split off the remainder.
        log.split_off(&start);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, entry))]
    async fn append_entry_to_log(&self, entry: &Entry<ClientRequest>) -> Result<()> {
        let mut log = self.log.write().await;
        log.insert(entry.index, entry.clone());
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn replicate_to_log(&self, entries: &[Entry<ClientRequest>]) -> Result<()> {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.index, entry.clone());
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, data))]
    async fn apply_entry_to_state_machine(&self, index: &u64, data: &ClientRequest) -> Result<ClientResponse> {
        let mut sm = self.sm.write().await;
        let resp = sm.apply(*index, data)?;
        Ok(resp)
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn replicate_to_state_machine(&self, entries: &[(&u64, &ClientRequest)]) -> Result<()> {
        let mut sm = self.sm.write().await;
        for (index, data) in entries {
            sm.apply(**index, data)?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>> {
        let (data, last_applied_log);
        {
            // Serialize the data of the state machine.
            let sm = self.sm.read().await;
            data = serde_json::to_vec(&*sm)?;
            last_applied_log = sm.last_applied_log;
        } // Release state machine read lock.

        let membership_config;
        {
            // Go backwards through the log to find the most recent membership config <= the `through` index.
            let log = self.log.read().await;
            membership_config = log
                .values()
                .rev()
                .skip_while(|entry| entry.index > last_applied_log)
                .find_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.id));
        } // Release log read lock.

        let snapshot_bytes: Vec<u8>;
        let term;
        {
            let mut log = self.log.write().await;
            let mut current_snapshot = self.current_snapshot.write().await;
            term = log
                .get(&last_applied_log)
                .map(|entry| entry.term)
                .ok_or_else(|| anyhow::anyhow!(ERR_INCONSISTENT_LOG))?;
            *log = log.split_off(&last_applied_log);
            log.insert(
                last_applied_log,
                Entry::new_snapshot_pointer(last_applied_log, term, "".into(), membership_config.clone()),
            );

            let snapshot = MemStoreSnapshot {
                index: last_applied_log,
                term,
                membership: membership_config.clone(),
                data,
            };
            snapshot_bytes = serde_json::to_vec(&snapshot)?;
            *current_snapshot = Some(snapshot);
        } // Release log & snapshot write locks.

        tracing::trace!({ snapshot_size = snapshot_bytes.len() }, "log compaction complete");
        Ok(CurrentSnapshotData {
            term,
            index: last_applied_log,
            membership: membership_config.clone(),
            snapshot: Box::new(Cursor::new(snapshot_bytes)),
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn create_snapshot(&self) -> Result<(String, Box<Self::Snapshot>)> {
        Ok((String::from(""), Box::new(Cursor::new(Vec::new())))) // Snapshot IDs are insignificant to this storage engine.
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn finalize_snapshot_installation(
        &self, index: u64, term: u64, delete_through: Option<u64>, id: String, snapshot: Box<Self::Snapshot>,
    ) -> Result<()> {
        tracing::trace!({ snapshot_size = snapshot.get_ref().len() }, "decoding snapshot for installation");
        let raw = serde_json::to_string_pretty(snapshot.get_ref().as_slice())?;
        println!("JSON SNAP:\n{}", raw);
        let new_snapshot: MemStoreSnapshot = serde_json::from_slice(snapshot.get_ref().as_slice())?;
        // Update log.
        {
            // Go backwards through the log to find the most recent membership config <= the `through` index.
            let mut log = self.log.write().await;
            let membership_config = log
                .values()
                .rev()
                .skip_while(|entry| entry.index > index)
                .find_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.id));

            match &delete_through {
                Some(through) => {
                    *log = log.split_off(&(through + 1));
                }
                None => log.clear(),
            }
            log.insert(index, Entry::new_snapshot_pointer(index, term, id, membership_config));
        }

        // Update the state machine.
        {
            let new_sm: MemStoreStateMachine = serde_json::from_slice(&new_snapshot.data)?;
            let mut sm = self.sm.write().await;
            *sm = new_sm;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(&self) -> Result<Option<CurrentSnapshotData<Self::Snapshot>>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let reader = serde_json::to_vec(&snapshot)?;
                Ok(Some(CurrentSnapshotData {
                    index: snapshot.index,
                    term: snapshot.term,
                    membership: snapshot.membership.clone(),
                    snapshot: Box::new(Cursor::new(reader)),
                }))
            }
            None => Ok(None),
        }
    }
}

pub struct Network {
    sto: Arc<MemStore>
}

impl Network {
    pub fn new(sto: Arc<MemStore>) -> Network {
        Network { sto }
    }

    // pub async fn make_client(&self, node_id: &NodeId) -> anyhow::Result<Meta>
}