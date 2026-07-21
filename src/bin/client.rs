use mini_quic::{decode_message, encode_message, MessageType};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::time::Instant;

// need something to track ack based messages
struct PendingMessage {
    encoded: Vec<u8>,
    sent_at: Instant,
}

type Pending = Arc<Mutex<HashMap<u32, PendingMessage>>>;
impl PendingMessage {
    pub fn new(encoded: Vec<u8>, sent_at: Instant) -> Self {
        Self { encoded, sent_at }
    }
}
struct Sender {
    client: Arc<UdpSocket>,
    message_no: u32,
    smoothed_rtt: f64,
}

impl Sender {
    pub async fn new() -> Self {
        Self {
            client: Arc::new(UdpSocket::bind("0.0.0.0:0").await.unwrap()),
            message_no: 0,
            smoothed_rtt: 333f64,
        }
    }
    pub async fn form_connection(&self) {
        if let Err(e) = self.client.connect("0.0.0.0:3000").await {
            println!("Failed to connect to the server: {}", e);
        }
    }
    pub async fn send_message(&mut self, msg: &[u8], is_regular: bool, pending: Pending) {
        self.message_no += 1;
        let msg_type = match is_regular {
            true => MessageType::Regular,
            false => MessageType::Join,
        };
        // encode the sequence number

        let msg = encode_message(msg_type.clone(), msg, self.message_no);

        if let Err(e) = self.client.send(&msg).await {
            println!("Failed to deliver the message to server : {}", e);
            return;
        }

        if msg_type != MessageType::Join {
            pending
                .lock()
                .await
                .insert(self.message_no, PendingMessage::new(msg, Instant::now()));
        }

        // Now how to copy here
    }
}
#[tokio::main]
async fn main() {
    let mut sender = Sender::new().await;
    sender.form_connection().await;

    let pending: Pending = Pending::new(Mutex::new(HashMap::new()));

    let c1 = sender.client.clone();

    let p1 = pending.clone();

    // task to receive server messages
    tokio::spawn(async move {
        let mut buf = vec![0; 256];
        loop {
            match c1.recv(&mut buf).await {
                Ok(r) => {
                    let msg = decode_message(&buf[..r]).unwrap();
                    match msg.0 {
                        MessageType::Regular => {
                            println!("Received: Message: {}", String::from_utf8_lossy(msg.2));
                        }
                        MessageType::Ack => {
                            // eject the seq number received from the server
                            match p1.lock().await.remove(&msg.1) {
                                Some(pending_msg) => {
                                    let sample = pending_msg.sent_at.elapsed().as_secs_f64();
                                    sender.smoothed_rtt =
                                        0.875 * sender.smoothed_rtt + 0.125 * sample;
                                    println!(
                                        "RTT updated: {:.4}s | smoothed: {:.4}s",
                                        sample, sender.smoothed_rtt
                                    );
                                    println!("Removed the seq_no {} as ack received.", msg.1)
                                }
                                None => {
                                    println!("Seq number not found")
                                }
                            }
                        }
                        MessageType::Dropped => {
                            println!("{}", String::from_utf8_lossy(msg.2))
                        }
                        _ => {
                            todo!()
                        }
                    }
                }
                Err(e) => {
                    println!("{}", e);
                    break;
                }
            }
        }
    });

    // spawn a new task for retransmission of pending ack messages
    let c2 = sender.client.clone();
    let p2 = pending.clone();

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;

            // push all the packets in the pending to the server
            let to_retransmit: Vec<Vec<u8>> = {
                let all_messages = p2.lock().await;
                all_messages
                    .values()
                    .filter(|m| m.sent_at.elapsed() > Duration::from_secs(5))
                    .map(|m| m.encoded.clone())
                    .collect()
            };

            for encoded in to_retransmit {
                // send the message again
                if let Err(e) = c2.send(&encoded).await {
                    println!("{}", sender.smoothed_rtt);
                    println!("Retransmit failed : {}", e);
                }
            }
        }
    });

    //send a join message here
    sender.send_message(b"", false, pending.clone()).await;
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
        sender
            .send_message(msg.as_bytes(), true, pending.clone())
            .await;
    }
}
