use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Instant;

pub type Clients = Arc<Mutex<HashMap<SocketAddr, (u32, Instant)>>>;

#[derive(Debug, PartialEq, Clone)]
pub enum MessageType {
    Join,
    Regular,
    Dropped,
    Ack,
}

impl From<MessageType> for u8 {
    fn from(value: MessageType) -> Self {
        match value {
            MessageType::Join => 0,
            MessageType::Regular => 1,
            MessageType::Dropped => 2,
            MessageType::Ack => 3,
        }
    }
}

pub enum SequenceStatus {
    InOrder,
    Duplicate,
    Gap(u32),
    LateArrival,
}

impl SequenceStatus {
    pub fn info(&self) -> String {
        match self {
            SequenceStatus::LateArrival => "Late arrival".to_string(),
            SequenceStatus::Gap(v) => format!("Gap: {v}"),
            SequenceStatus::Duplicate => "Duplicate".to_string(),
            SequenceStatus::InOrder => "Wow Nice".to_string(),
        }
    }
}
