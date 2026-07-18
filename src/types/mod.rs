use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Instant;

pub type Clients = Arc<Mutex<HashMap<SocketAddr, Instant>>>;

#[derive(Debug, PartialEq)]
pub enum MessageType {
    Join,
    Regular,
    Dropped
}

impl From<MessageType> for u8 {
    fn from(value: MessageType) -> Self {
        match value {
            MessageType::Join => 0,
            MessageType::Regular => 1,
            MessageType::Dropped => 2
        }
    }
}
