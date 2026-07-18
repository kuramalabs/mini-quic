use crate::MessageType;

pub fn decode_message(bytes: &[u8]) -> Result<(MessageType, &[u8]), &str> {
    let msg_type = match bytes.first() {
        Some(0) => MessageType::Join,
        Some(1) => MessageType::Regular,
        Some(2) => MessageType::Dropped,
        Some(_) => return Err("Unknown message type"),
        None => return Err("Empty packet"),
    };

    Ok((msg_type, &bytes[1..]))
}

pub fn encode_message(msg_type: MessageType, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + payload.len());

    buf.extend_from_slice(&u8::from(msg_type).to_be_bytes());
    buf.extend_from_slice(payload);

    buf
}
