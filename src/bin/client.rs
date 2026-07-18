use mini_quic::{decode_message, encode_message, MessageType};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() {
    let client = Arc::new(UdpSocket::bind("0.0.0.0:0").await.unwrap());

    // connect this with the real udp server running
    client.connect("0.0.0.0:3000").await.unwrap();

    //now can directly use the send and the recv
    let c1 = client.clone();
    tokio::spawn(async move {
        let mut buf = vec![0; 256];
        loop {
            let r = c1.recv(&mut buf).await.unwrap();
            // gotta decode this message
            let msg = decode_message(&buf[..r]).unwrap();

            // this will be Regular one as there is no chance the task will receive the
            // Join as this functionality is not there in the server yet

            println!("Received: {}", String::from_utf8_lossy(msg.1));
        }
    });

    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);

    let join_msg = encode_message(MessageType::Join, b"");
    client.send(&join_msg).await.unwrap();
    println!("Join message sent");

    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await.unwrap() == 0 {
            break;
        }
        let msg = line.trim();
        if msg.is_empty() {
            continue;
        }

        // encode it and send
        let msg = encode_message(MessageType::Regular, msg.as_bytes());
        client.send(&msg).await.unwrap();
        
    }
}
