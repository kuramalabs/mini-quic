use crate::MessageType;

pub fn decode_message(bytes: &[u8]) -> Result<(MessageType, u32, &[u8]), &str> {
    if bytes.len() < 5 {
        return Err("Packet too short");
    }

    let seq_no = u32::from_be_bytes(bytes[0..4].try_into().unwrap());

    let msg_type = match bytes[4] {
        0 => MessageType::Join,
        1 => MessageType::Regular,
        2 => MessageType::Dropped,
        _ => return Err("Unknown message type"),
    };

    Ok((msg_type, seq_no, &bytes[5..]))
}


pub fn encode_message(msg_type: MessageType, payload: &[u8], seq_no: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + payload.len());

    buf.extend_from_slice(&seq_no.to_be_bytes());
    buf.extend_from_slice(&u8::from(msg_type).to_be_bytes());
    buf.extend_from_slice(payload);

    buf
}
