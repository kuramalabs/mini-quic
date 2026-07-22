use crate::{MessageType, SequenceStatus};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;

pub fn decode_message(bytes: &[u8]) -> Result<(MessageType, u32, &[u8]), &str> {
    if bytes.len() < 5 {
        return Err("Packet too short");
    }

    let seq_no = u32::from_be_bytes(bytes[0..4].try_into().unwrap());

    let msg_type = match bytes[4] {
        0 => MessageType::Join,
        1 => MessageType::Regular,
        2 => MessageType::Dropped,
        3 => MessageType::Ack,
        _ => return Err("Unknown message type"),
    };

    Ok((msg_type, seq_no, &bytes[5..]))
}

pub fn encode_message(msg_type: MessageType, payload: &[u8], seq_no: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(5 + payload.len());

    buf.extend_from_slice(&seq_no.to_be_bytes());
    buf.extend_from_slice(&u8::from(msg_type).to_be_bytes());
    buf.extend_from_slice(payload);

    buf
}

pub fn classify_seq(stored_seq: u32, curr_seq_no: u32) -> SequenceStatus {
    let diff = curr_seq_no as i64 - stored_seq as i64;

    match diff {
        0 => SequenceStatus::Duplicate,
        1 => SequenceStatus::InOrder,
        n if n > 1 => SequenceStatus::Gap(n as u32),
        _ => SequenceStatus::LateArrival,
    }
}

pub async fn send_ack_empty(
    udp_socket: Arc<UdpSocket>,
    client_addr: SocketAddr,
    curr_seq_no: u32,
    log: &str,
) {
    // tokio::time::sleep(Duration::from_secs(3)).await;
    // this timer sleep was here to simulate the delay between acks to test retransmission
    let msg = encode_message(MessageType::Ack, b"", curr_seq_no);
    if let Err(e) = udp_socket.send_to(&msg, client_addr).await {
        println!("{}, Error: {}", log, e);
    }
}
