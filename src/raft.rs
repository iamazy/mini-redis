use crate::db::Db;
use crate::protobuf::mini_redis::mini_redis_service_client::MiniRedisServiceClient;
use crate::protobuf::mini_redis::mini_redis_service_server::{
    MiniRedisService, MiniRedisServiceServer,
};
use crate::protobuf::mini_redis::{GetRequest, GetResponse, RaftMessage};
use anyhow::Result;
use async_raft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteRequest, Entry, EntryPayload,
    InstallSnapshotRequest, InstallSnapshotResponse, MembershipConfig, VoteRequest, VoteResponse,
};
use async_raft::storage::{CurrentSnapshotData, HardState, InitialState};
use async_raft::{
    AppData, AppDataResponse, ClientWriteError, Config, NodeId, Raft, RaftMetrics, RaftNetwork,
    RaftStorage,
};
use async_trait::async_trait;
use bytes::Bytes;
use serde_derive::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::{TryFrom, TryInto};
use std::fmt::{self, Display, Formatter};
use std::io::Cursor;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{watch, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

const ERR_INCONSISTENT_LOG: &str =
    "a query was received which was expecting data to be in place which does not exist in the log";

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Context {
    #[serde(skip)]
    pub db: Db,
    pub nodes: HashMap<NodeId, Node>,
}

impl Context {
    #[tracing::instrument(level = "info", skip(self))]
    pub fn apply(&mut self, data: &ClientRequest) -> anyhow::Result<ClientResponse> {
        match data.cmd {
            Cmd::AddEntry { ref key, ref value } => match self.db.get(key.as_str()) {
                Some(val) => {
                    let prev = String::from_utf8(val.to_vec())?;
                    Ok(ClientResponse::Value {
                        prev: Some(prev),
                        result: Some(String::from_utf8(value.clone())?),
                    })
                }
                None => {
                    self.db.set(key.clone(), Bytes::from(value.clone()), None);
                    Ok(ClientResponse::Value {
                        prev: None,
                        result: Some(String::from_utf8(value.clone())?),
                    })
                }
            },
            _ => Ok(ClientResponse::Value {
                prev: None,
                result: None,
            }),
        }
    }

    pub fn get_node(&self, node_id: &NodeId) -> Option<Node> {
        let x = self.nodes.get(node_id);
        x.cloned()
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Node {
    pub name: String,
    pub address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Cmd {
    AddEntry { key: String, value: Vec<u8> },
    RemoveEntry { key: String },
    AddNode { node_id: NodeId, node: Node },
    RemoveNode { node_id: NodeId },
}

impl Display for Cmd {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Cmd::AddEntry { key, value } => {
                write!(f, "add entry: key = {}", key)
            }
            Cmd::RemoveEntry { key } => {
                write!(f, "remove entry: key = {}", key)
            }
            Cmd::AddNode { node_id, node } => {
                write!(
                    f,
                    "add node [id: {}, name: {}, address: {}]",
                    node_id, node.name, node.address
                )
            }
            Cmd::RemoveNode { node_id } => {
                write!(f, "remove node [id: {}]", node_id)
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientRequest {
    /// The ID of the client which has set the request
    pub client: String,
    /// The serial number of this request
    pub serial: u64,
    pub cmd: Cmd,
}

impl AppData for ClientRequest {}

impl ClientRequest {

    pub fn new_with_cmd(cmd: Cmd) -> ClientRequest {
        ClientRequest {
            client: "".to_string(),
            serial: 0,
            cmd
        }
    }
}

impl tonic::IntoRequest<RaftMessage> for ClientRequest {
    fn into_request(self) -> tonic::Request<RaftMessage> {
        let mes = RaftMessage {
            data: serde_json::to_string(&self).expect("fail to serialize"),
            error: "".to_string(),
        };
        tonic::Request::new(mes)
    }
}

impl tonic::IntoRequest<RaftMessage> for AppendEntriesRequest<ClientRequest> {
    fn into_request(self) -> tonic::Request<RaftMessage> {
        let mes = RaftMessage {
            data: serde_json::to_string(&self).expect("fail to serialize"),
            error: "".to_string(),
        };
        tonic::Request::new(mes)
    }
}

impl tonic::IntoRequest<RaftMessage> for InstallSnapshotRequest {
    fn into_request(self) -> tonic::Request<RaftMessage> {
        let mes = RaftMessage {
            data: serde_json::to_string(&self).expect("fail to serialize"),
            error: "".to_string(),
        };
        tonic::Request::new(mes)
    }
}

impl tonic::IntoRequest<RaftMessage> for VoteRequest {
    fn into_request(self) -> tonic::Request<RaftMessage> {
        let mes = RaftMessage {
            data: serde_json::to_string(&self).expect("fail to serialize"),
            error: "".to_string(),
        };
        tonic::Request::new(mes)
    }
}

impl TryFrom<RaftMessage> for ClientRequest {
    type Error = tonic::Status;

    fn try_from(mes: RaftMessage) -> Result<Self, Self::Error> {
        let req: ClientRequest =
            serde_json::from_str(&mes.data).map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(req)
    }
}

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum RetryableError {
    #[error("request must be forwarded to leader: {leader}")]
    ForwardToLeader { leader: NodeId },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientResponse {
    Value {
        prev: Option<String>,
        result: Option<String>,
    },
    Node {
        prev: Option<Node>,
        result: Option<Node>,
    },
}

impl AppDataResponse for ClientResponse {}

impl From<ClientResponse> for RaftMessage {
    fn from(msg: ClientResponse) -> Self {
        let data = serde_json::to_string(&msg).expect("fail to serialize");
        RaftMessage {
            data,
            error: "".to_string(),
        }
    }
}

impl From<RetryableError> for RaftMessage {
    fn from(err: RetryableError) -> Self {
        let error = serde_json::to_string(&err).expect("fail to serialize");
        RaftMessage {
            data: "".to_string(),
            error,
        }
    }
}

impl From<Result<ClientResponse, RetryableError>> for RaftMessage {
    fn from(rst: Result<ClientResponse, RetryableError>) -> Self {
        match rst {
            Ok(resp) => resp.into(),
            Err(err) => err.into(),
        }
    }
}

impl From<RaftMessage> for Result<ClientResponse, RetryableError> {
    fn from(msg: RaftMessage) -> Self {
        if !msg.data.is_empty() {
            let resp: ClientResponse =
                serde_json::from_str(&msg.data).expect("fail to deserialize");
            Ok(resp)
        } else {
            let err: RetryableError =
                serde_json::from_str(&msg.error).expect("fail to deserialize");
            Err(err)
        }
    }
}

impl From<(Option<Node>, Option<Node>)> for ClientResponse {
    fn from(v: (Option<Node>, Option<Node>)) -> Self {
        ClientResponse::Node {
            prev: v.0,
            result: v.1,
        }
    }
}

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
    /// The last membership config included in this snapshot.
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
    pub context: Context,
}

impl MemStoreStateMachine {
    #[tracing::instrument(level = "info", skip(self))]
    pub fn apply(&mut self, index: u64, data: &ClientRequest) -> anyhow::Result<ClientResponse> {
        self.last_applied_log = index;
        if let Some((serial, response)) = self.client_serial_responses.get(&data.client) {
            if serial == &data.serial {
                return Ok(response.clone());
            }
        }
        let resp = self.context.apply(data)?;
            self.client_serial_responses
                .insert(data.client.clone(), (data.serial, resp.clone()));
        Ok(resp)
    }

    pub fn get_db(&self) -> Db {
        self.context.db.clone()
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
        id: NodeId,
        log: BTreeMap<u64, Entry<ClientRequest>>,
        sm: MemStoreStateMachine,
        hs: Option<HardState>,
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

impl MemStore {
    pub async fn get_node(&self, node_id: &NodeId) -> Option<Node> {
        let sm = self.sm.read().await;
        sm.context.get_node(node_id)
    }

    pub async fn get_node_addr(&self, node_id: &NodeId) -> anyhow::Result<String> {
        let addr = self
            .get_node(node_id)
            .await
            .map(|n| n.address)
            .ok_or_else(|| anyhow::anyhow!("node not found {}", node_id))?;
        Ok(addr)
    }

    pub async fn list_non_voters(&self) -> HashSet<NodeId> {
        let mut rst = HashSet::new();
        let sm = self.sm.read().await;
        let ms = self
            .get_membership_config()
            .await
            .expect("fail to get membership");
        for i in sm.context.nodes.keys() {
            if !ms.contains(i) {
                rst.insert(*i);
            }
        }
        rst
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
    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        data: &ClientRequest,
    ) -> Result<ClientResponse> {
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
                Entry::new_snapshot_pointer(
                    last_applied_log,
                    term,
                    "".into(),
                    membership_config.clone(),
                ),
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

        tracing::trace!(
            { snapshot_size = snapshot_bytes.len() },
            "log compaction complete"
        );
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
        &self,
        index: u64,
        term: u64,
        delete_through: Option<u64>,
        id: String,
        snapshot: Box<Self::Snapshot>,
    ) -> Result<()> {
        tracing::trace!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );
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
            log.insert(
                index,
                Entry::new_snapshot_pointer(index, term, id, membership_config),
            );
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
    sto: Arc<MemStore>,
}

impl Network {
    pub fn new(sto: Arc<MemStore>) -> Network {
        Network { sto }
    }

    #[tracing::instrument(level = "info", skip(self), fields(myid = self.sto.id))]
    pub async fn make_client(
        &self,
        node_id: &NodeId,
    ) -> anyhow::Result<MiniRedisServiceClient<Channel>> {
        let addr = self.sto.get_node_addr(node_id).await?;
        tracing::info!("connect: id = {}: {}", node_id, addr);
        let client = MiniRedisServiceClient::connect(format!("http://{}", addr)).await?;
        tracing::info!("connected: id = {}, {}", node_id, addr);
        Ok(client)
    }
}

#[async_trait]
impl RaftNetwork<ClientRequest> for Network {
    #[tracing::instrument(level = "info", skip(self), fields(myid = self.sto.id))]
    async fn append_entries(
        &self,
        target: u64,
        rpc: AppendEntriesRequest<ClientRequest>,
    ) -> Result<AppendEntriesResponse> {
        tracing::debug!("append_entries req to: id = {}: {:?}", target, rpc);
        let mut client = self.make_client(&target).await?;
        let resp = client.append_entries(rpc).await;
        tracing::debug!("append_entries resp from: id = {}: {:?}", target, resp);

        let resp = resp?;
        let mes = resp.into_inner();
        let resp = serde_json::from_str(&mes.data)?;
        Ok(resp)
    }

    #[tracing::instrument(level = "info", skip(self), fields(myid = self.sto.id))]
    async fn install_snapshot(
        &self,
        target: u64,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        tracing::debug!("install_snapshot req to: id={}", target);

        let mut client = self.make_client(&target).await?;
        let resp = client.install_snapshot(rpc).await;
        tracing::debug!("install_snapshot resp from: id={}: {:?}", target, resp);

        let resp = resp?;
        let mes = resp.into_inner();
        let resp = serde_json::from_str(&mes.data)?;

        Ok(resp)
    }

    #[tracing::instrument(level = "info", skip(self), fields(myid = self.sto.id))]
    async fn vote(&self, target: u64, rpc: VoteRequest) -> Result<VoteResponse> {
        tracing::debug!("vote req to: id={} {:?}", target, rpc);

        let mut client = self.make_client(&target).await?;
        let resp = client.vote(rpc).await;
        tracing::info!("vote: resp from id={} {:?}", target, resp);

        let resp = resp?;
        let mes = resp.into_inner();
        let resp = serde_json::from_str(&mes.data)?;

        Ok(resp)
    }
}

pub type MiniRedisRaft = Raft<ClientRequest, ClientResponse, Network, MemStore>;

pub struct MiniRedisNode {
    pub metrics_rx: watch::Receiver<RaftMetrics>,
    pub sto: Arc<MemStore>,
    pub raft: MiniRedisRaft,
    pub running_tx: watch::Sender<()>,
    pub running_rx: watch::Receiver<()>,
    pub join_handles: Mutex<Vec<JoinHandle<anyhow::Result<()>>>>,
}

pub struct MiniRedisNodeBuilder {
    node_id: Option<NodeId>,
    config: Option<Config>,
    sto: Option<Arc<MemStore>>,
    monitor_metrics: bool,
    start_grpc_service: bool,
}

impl MiniRedisNodeBuilder {
    pub async fn build(mut self) -> anyhow::Result<Arc<MiniRedisNode>> {
        let node_id = self
            .node_id
            .ok_or_else(|| anyhow::anyhow!("node_id is not set"))?;

        let config = self
            .config
            .take()
            .ok_or_else(|| anyhow::anyhow!("config is not set"))?;

        let sto = self
            .sto
            .take()
            .ok_or_else(|| anyhow::anyhow!("sto is not set"))?;

        let net = Network::new(sto.clone());

        let raft = MiniRedisRaft::new(node_id, Arc::new(config), Arc::new(net), sto.clone());
        let metrics_rx = raft.metrics();

        let (tx, rx) = watch::channel::<()>(());

        let mn = Arc::new(MiniRedisNode {
            metrics_rx: metrics_rx.clone(),
            sto: sto.clone(),
            raft,
            running_tx: tx,
            running_rx: rx,
            join_handles: Mutex::new(Vec::new()),
        });

        if self.monitor_metrics {
            tracing::info!("about to subscribe raft metrics");
            MiniRedisNode::subscribe_metrics(mn.clone(), metrics_rx).await;
        }

        if self.start_grpc_service {
            let addr = sto.get_node_addr(&node_id).await?;
            tracing::info!("about to start grpc on {}", addr);
            MiniRedisNode::start_grpc(mn.clone(), &addr).await?;
        }
        Ok(mn)
    }

    pub fn node_id(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }
    pub fn sto(mut self, sto: Arc<MemStore>) -> Self {
        self.sto = Some(sto);
        self
    }
    pub fn start_grpc_service(mut self, b: bool) -> Self {
        self.start_grpc_service = b;
        self
    }
    pub fn monitor_metrics(mut self, b: bool) -> Self {
        self.monitor_metrics = b;
        self
    }
}

impl MiniRedisNode {
    pub fn builder() -> MiniRedisNodeBuilder {
        // Set heartbeat interval to a reasonable value.
        // The election_timeout should tolerate several heartbeat loss.
        let heartbeat = 500; // ms
        MiniRedisNodeBuilder {
            node_id: None,
            config: Some(
                Config::build("foo_cluster".into())
                    .heartbeat_interval(heartbeat)
                    .election_timeout_min(heartbeat * 4)
                    .election_timeout_max(heartbeat * 8)
                    .validate()
                    .expect("fail to build raft config"),
            ),
            sto: None,
            monitor_metrics: true,
            start_grpc_service: true,
        }
    }

    /// Start the grpc service for raft communication and meta operation API.
    #[tracing::instrument(level = "info", skip(mn))]
    pub async fn start_grpc(mn: Arc<MiniRedisNode>, addr: &str) -> anyhow::Result<()> {
        let mut rx = mn.running_rx.clone();

        let redis_srv_impl = MiniRedisServiceImpl::create(mn.clone());
        let redis_srv = MiniRedisServiceServer::new(redis_srv_impl);

        let addr_str = addr.to_string();
        let addr = addr.parse::<std::net::SocketAddr>()?;
        let node_id = mn.sto.id;

        let srv = tonic::transport::Server::builder().add_service(redis_srv);

        let h = tokio::spawn(async move {
            srv.serve_with_shutdown(addr, async move {
                let _ = rx.changed().await;
                tracing::info!(
                    "signal received, shutting down: id={} {} ",
                    node_id,
                    addr_str
                );
            })
                .await
                .map_err(|e| anyhow::anyhow!("MiniRedisNode service error: {:?}", e))?;
            Ok::<(), anyhow::Error>(())
        });

        let mut jh = mn.join_handles.lock().await;
        jh.push(h);
        Ok(())
    }

    /// Start a MetaStore node from initialized store.
    #[tracing::instrument(level = "info")]
    pub async fn new(node_id: NodeId) -> Arc<MiniRedisNode> {
        let b = MiniRedisNode::builder()
            .node_id(node_id)
            .sto(Arc::new(MemStore::new(node_id)));

        b.build().await.expect("can not fail")
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn stop(&self) -> anyhow::Result<i32> {
        // TODO need to be reentrant.

        let mut rx = self.raft.metrics();

        self.raft.shutdown().await?;
        self.running_tx.send(()).unwrap();

        // wait for raft to close the metrics tx
        loop {
            let r = rx.changed().await;
            if r.is_err() {
                break;
            }
            tracing::info!("waiting for raft to shutdown, metrics: {:?}", rx.borrow());
        }
        tracing::info!("shutdown raft");

        // raft counts 1
        let mut joined = 1;
        for j in self.join_handles.lock().await.iter_mut() {
            let _rst = j.await?;
            joined += 1;
        }

        tracing::info!("shutdown: myid={}", self.sto.id);
        Ok(joined)
    }

    // spawn a monitor to watch raft state changes such as leader changes,
    // and manually add non-voter to cluster so that non-voter receives raft logs.
    pub async fn subscribe_metrics(mn: Arc<Self>, mut metrics_rx: watch::Receiver<RaftMetrics>) {
        //TODO: return a handle for join
        // TODO: every state change triggers add_non_voter!!!
        let mut rx = mn.running_rx.clone();
        let mut jh = mn.join_handles.lock().await;

        // TODO: reduce dependency: it does not need all of the fields in MiniRedisNode
        let mn = mn.clone();

        let h = tokio::task::spawn(async move {
            loop {
                let changed = tokio::select! {
                    _ = rx.changed() => {
                       return Ok::<(), anyhow::Error>(());
                    }
                    changed = metrics_rx.changed() => {
                        changed
                    }
                };
                if changed.is_ok() {
                    let mm = metrics_rx.borrow().clone();
                    if let Some(cur) = mm.current_leader {
                        if cur == mn.sto.id {
                            // TODO: check result
                            let _rst = mn.add_configured_non_voters().await;

                            if _rst.is_err() {
                                tracing::info!(
                                    "fail to add non-voter: my id={}, rst:{:?}",
                                    mn.sto.id,
                                    _rst
                                );
                            }
                        }
                    }
                } else {
                    // shutting down
                    break;
                }
            }

            Ok::<(), anyhow::Error>(())
        });
        jh.push(h);
    }

    /// Boot up the first node to create a cluster.
    /// For every cluster this func should be called exactly once.
    /// When a node is initialized with boot or boot_non_voter, start it with MetaStore::new().
    #[tracing::instrument(level = "info")]
    pub async fn boot(node_id: NodeId, addr: String) -> anyhow::Result<Arc<MiniRedisNode>> {
        let mn = MiniRedisNode::boot_non_voter(node_id, &addr).await?;

        let mut cluster_node_ids = HashSet::new();
        cluster_node_ids.insert(node_id);

        let rst = mn
            .raft
            .initialize(cluster_node_ids)
            .await
            .map_err(|x| anyhow::anyhow!("{:?}", x))?;

        tracing::info!("booted, rst: {:?}", rst);
        mn.add_node(node_id, addr).await?;

        Ok(mn)
    }

    /// Boot a node that is going to join an existent cluster.
    /// For every node this should be called exactly once.
    /// When successfully initialized(e.g. received logs from raft leader), a node should be started with MiniRedisNode::new().
    #[tracing::instrument(level = "info")]
    pub async fn boot_non_voter(node_id: NodeId, addr: &str) -> anyhow::Result<Arc<MiniRedisNode>> {
        // TODO test MiniRedisNode::new() on a booted store.
        // TODO: Before calling this func, the node should be added as a non-voter to leader.
        // TODO: check raft initialState to see if the store is clean.

        // When booting, there is addr stored in local store.
        // Thus we need to start grpc manually.
        let sto = MemStore::new(node_id);

        let b = MiniRedisNode::builder()
            .node_id(node_id)
            .start_grpc_service(false)
            .sto(Arc::new(sto));

        let mn = b.build().await?;

        // Manually start the grpc, since no addr is stored yet.
        // We can not use the startup routine for initialized node.
        MiniRedisNode::start_grpc(mn.clone(), addr).await?;

        tracing::info!("booted non-voter: {}={}", node_id, addr);

        Ok(mn)
    }

    /// When a leader is established, it is the leader's responsibility to setup replication from itself to non-voters, AKA learners.
    /// async-raft does not persist the node set of non-voters, thus we need to do it manually.
    /// This fn should be called once a node found it becomes leader.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn add_configured_non_voters(&self) -> anyhow::Result<()> {
        // TODO after leader established, add non-voter through apis
        let node_ids = self.sto.list_non_voters().await;
        for i in node_ids.iter() {
            let x = self.raft.add_non_voter(*i).await;

            tracing::info!("add_non_voter result: {:?}", x);
            if x.is_ok() {
                tracing::info!("non-voter is added: {}", i);
            } else {
                tracing::info!("non-voter already exist: {}", i);
            }
        }
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        let sm = self.sto.sm.read().await;
        match sm.context.db.get(key) {
            Some(bytes) => Some(String::from_utf8(bytes.to_vec()).unwrap()),
            None => None,
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_node(&self, node_id: &NodeId) -> Option<Node> {
        let sm = self.sto.sm.read().await;
        sm.context.get_node(node_id)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn add_node(&self, node_id: NodeId, addr: String) -> anyhow::Result<ClientResponse> {
        let resp = self
            .write(ClientRequest::new_with_cmd(Cmd::AddNode {
                node_id,
                node: Node {
                    name: addr.clone(),
                    address: addr,
                },
            }))
            .await?;
        Ok(resp)
    }

    /// Submit a write request to the known leader. Returns the response after applying the request.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn write(&self, req: ClientRequest) -> anyhow::Result<ClientResponse> {
        let mut curr_leader = self.get_leader().await;
        loop {
            let rst = if curr_leader == self.sto.id {
                self.write_to_local_leader(req.clone()).await?
            } else {
                // forward to leader

                let addr = self.sto.get_node_addr(&curr_leader).await?;
                let mut client =
                    MiniRedisServiceClient::connect(format!("http://{}", addr)).await?;
                let resp = client.write(req.clone()).await?;
                let rst: Result<ClientResponse, RetryableError> = resp.into_inner().into();
                rst
            };

            match rst {
                Ok(resp) => return Ok(resp),
                Err(write_err) => match write_err {
                    RetryableError::ForwardToLeader { leader } => curr_leader = leader,
                },
            }
        }
    }

    /// Try to get the leader from the latest metrics of the local raft node.
    /// If leader is absent, wait for an metrics update in which a leader is set.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn get_leader(&self) -> NodeId {
        // fast path: there is a known leader

        if let Some(l) = self.metrics_rx.borrow().current_leader {
            return l;
        }

        // slow path: wait loop

        // Need to clone before calling changed() on it.
        // Otherwise other thread waiting on changed() may not receive the change event.
        let mut rx = self.metrics_rx.clone();

        loop {
            // NOTE:
            // The metrics may have already changed before we cloning it.
            // Thus we need to re-check the cloned rx.
            if let Some(l) = rx.borrow().current_leader {
                return l;
            }

            let changed = rx.changed().await;
            if changed.is_err() {
                tracing::info!("raft metrics tx closed");
                return 0;
            }
        }
    }

    /// Write a meta log through local raft node.
    /// It works only when this node is the leader,
    /// otherwise it returns ClientWriteError::ForwardToLeader error indicating the latest leader.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn write_to_local_leader(
        &self,
        req: ClientRequest,
    ) -> anyhow::Result<Result<ClientResponse, RetryableError>> {
        let write_rst = self.raft.client_write(ClientWriteRequest::new(req)).await;

        tracing::debug!("raft.client_write rst: {:?}", write_rst);

        match write_rst {
            Ok(resp) => Ok(Ok(resp.data)),
            Err(cli_write_err) => match cli_write_err {
                // fatal error
                ClientWriteError::RaftError(raft_err) => Err(anyhow::anyhow!(raft_err.to_string())),
                // retryable error
                ClientWriteError::ForwardToLeader(_, leader) => match leader {
                    Some(id) => Ok(Err(RetryableError::ForwardToLeader { leader: id })),
                    None => Err(anyhow::anyhow!("no leader to write")),
                },
            },
        }
    }
}

pub struct MiniRedisServiceImpl {
    pub mini_redis_node: Arc<MiniRedisNode>,
}

impl MiniRedisServiceImpl {
    pub fn create(mini_redis_node: Arc<MiniRedisNode>) -> Self {
        Self { mini_redis_node }
    }
}

#[async_trait::async_trait]
impl MiniRedisService for MiniRedisServiceImpl {
    /// Handles a write request.
    /// This node must be leader or an error returned.
    #[tracing::instrument(level = "info", skip(self))]
    async fn write(
        &self,
        request: tonic::Request<RaftMessage>,
    ) -> Result<tonic::Response<RaftMessage>, tonic::Status> {
        let mes = request.into_inner();
        let req: ClientRequest = mes.try_into()?;

        let rst = self
            .mini_redis_node
            .write_to_local_leader(req)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        let raft_mes = rst.into();
        Ok(tonic::Response::new(raft_mes))
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn get(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> Result<tonic::Response<GetResponse>, tonic::Status> {
        let req = request.into_inner();
        let resp = self.mini_redis_node.get(&req.key).await;
        let rst = match resp {
            Some(v) => GetResponse {
                ok: true,
                key: req.key,
                value: v,
            },
            None => GetResponse {
                ok: false,
                key: req.key,
                value: "".into(),
            },
        };

        Ok(tonic::Response::new(rst))
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn append_entries(
        &self,
        request: tonic::Request<RaftMessage>,
    ) -> Result<tonic::Response<RaftMessage>, tonic::Status> {
        let req = request.into_inner();

        let ae_req =
            serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self
            .mini_redis_node
            .raft
            .append_entries(ae_req)
            .await
            .map_err(|x| tonic::Status::internal(x.to_string()))?;
        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        let mes = RaftMessage {
            data,
            error: "".to_string(),
        };

        Ok(tonic::Response::new(mes))
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn install_snapshot(
        &self,
        request: tonic::Request<RaftMessage>,
    ) -> Result<tonic::Response<RaftMessage>, tonic::Status> {
        let req = request.into_inner();

        let is_req =
            serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self
            .mini_redis_node
            .raft
            .install_snapshot(is_req)
            .await
            .map_err(|x| tonic::Status::internal(x.to_string()))?;
        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        let mes = RaftMessage {
            data,
            error: "".to_string(),
        };

        Ok(tonic::Response::new(mes))
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn vote(
        &self,
        request: tonic::Request<RaftMessage>,
    ) -> Result<tonic::Response<RaftMessage>, tonic::Status> {
        let req = request.into_inner();

        let v_req =
            serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self
            .mini_redis_node
            .raft
            .vote(v_req)
            .await
            .map_err(|x| tonic::Status::internal(x.to_string()))?;
        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        let mes = RaftMessage {
            data,
            error: "".to_string(),
        };

        Ok(tonic::Response::new(mes))
    }
}
