use std::sync::Mutex;
use once_cell::sync::Lazy;
use std::collections::LinkedList;
use serde_derive::{Deserialize, Serialize};
use config::FileFormat;

pub static GLOBAL_CONFIG: Lazy<Mutex<MiniRedisConfig>> = Lazy::new(|| {
    let mut settings = config::Config::default();
    settings.merge(config::File::new("conf/mini-redis",
                                     FileFormat::Yaml)).unwrap();
    let config = settings.try_into::<MiniRedisConfig>().unwrap();
    tracing::info!("load mini-redis config: {:?}", config);
    Mutex::new(config)
});

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MiniRedisConfig {
    pub raft: RaftConfig
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RaftConfig {
    pub id: u64,
    pub port: u16,
    pub members: LinkedList<String>
}