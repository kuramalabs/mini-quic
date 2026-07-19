use mini_quic::{decode_message, encode_message, MessageType};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UdpSocket;

struct Sender {
    client: Arc<UdpSocket>,
    message_no: u32,
}

impl Sender {
    pub async fn new() -> Self {
        Self {
            client: Arc::new(UdpSocket::bind("0.0.0.0:0").await.unwrap()),
            message_no: 0,
        }
    }

    pub async fn form_connection(&self) {
        if let Err(e) = self.client.connect("0.0.0.0:3000").await {
            println!("Failed to connect to the server: {}", e);
        }
    }
    pub async fn send_message(&mut self, msg: &[u8], is_regular: bool) {
        self.message_no += 1;
        let msg_type = match is_regular {
            true => MessageType::Regular,
            false => MessageType::Join,
        };
        // encode the sequence number

        let msg = encode_message(msg_type, msg, self.message_no);
        if let Err(e) = self.client.send(&msg).await {
            println!("Failed to deliver the message to server : {}", e);
        }
    }
}
#[tokio::main]
async fn main() {
    let mut sender = Sender::new().await;
    sender.form_connection().await;

    let c1 = sender.client.clone();

    tokio::spawn(async move {
        let mut buf = vec![0; 256];
        loop {
            let r = c1.recv(&mut buf).await.unwrap();
            let msg = decode_message(&buf[..r]).unwrap();
            println!(
                "Received: Seq Number: {}, Message: {}",
                msg.1,
                String::from_utf8_lossy(msg.2)
            );
        }
    });

    sender.send_message(b"", false).await;
    println!("Join message sent");

    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);

    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await.unwrap() == 0 {
            break;
        }
        let msg = line.trim();
        if msg.is_empty() {
            continue;
        }
        sender.send_message(msg.as_bytes(), true).await;
    }
}
