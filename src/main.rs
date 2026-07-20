mod types;

use mini_quic::{classify_seq, decode_message, encode_message, MessageType};
use std::collections::HashMap;
use std::io::Error;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::time;
use tokio::time::{Duration, Instant};
use types::Clients;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let clients = Clients::new(Mutex::new(HashMap::new()));

    let udp_socket = Arc::new(UdpSocket::bind("0.0.0.0:3000").await?);
    let mut buf = vec![0; 50];

    let c1 = clients.clone();
    let u1 = udp_socket.clone();
    tokio::spawn(async move {
        loop {
            time::sleep(Duration::from_secs(5)).await;

            let mut dropped = Vec::new();

            c1.lock().await.retain(|addr, instant| {
                let active = instant.1.elapsed() < Duration::from_secs(10);
                if !active {
                    println!("Client with {addr} is dropped");
                    dropped.push((*addr, instant.0));
                }
                active
            });

            for addr in dropped {
                let msg = encode_message(MessageType::Dropped, b"You are dropped", addr.1);
                if let Err(e) = u1.send_to(&msg, addr.0).await {
                    eprintln!("Could not notify {}: {e}", addr.0);
                }
            }
        }
    });

    loop {
        let (n, client_addr) = udp_socket.recv_from(&mut buf).await?;
        let res = decode_message(&buf[..n]);

        match res {
            Ok((msg_type, curr_seq_no, bytes)) => {
                match msg_type {
                    MessageType::Join => {
                        if clients
                            .lock()
                            .await
                            .insert(client_addr, (curr_seq_no, Instant::now()))
                            .is_none()
                        {
                            println!("New client {client_addr} connected");
                            // can notify others from here ( lets leave it for now )
                        }
                    }
                    MessageType::Regular => {
                        let mut all_clients = clients.lock().await;
                        if !all_clients.contains_key(&client_addr) {
                            eprintln!("You need to connect via Join first");
                            continue;
                        }
                        //verify the seq_no and log
                        let stored_seq = all_clients.get(&client_addr).unwrap().0;

                        let seq_status = classify_seq(stored_seq, curr_seq_no);

                        // log this
                        println!("{}", seq_status.info());

                        all_clients.insert(client_addr, (curr_seq_no, Instant::now()));

                        println!(
                            "The client {client_addr} sent: {:?}, curr_seq_no: {}",
                            String::from_utf8_lossy(bytes),
                            curr_seq_no
                        );

                        let encoded_msg = encode_message(MessageType::Regular, bytes, curr_seq_no);

                        for (addr, _) in all_clients.iter() {
                            if *addr != client_addr {
                                if let Err(e) = udp_socket.send_to(&encoded_msg, addr).await {
                                    println!("Could not send to: {addr}, Error: {e}");
                                }
                            }
                        }
                    }

                    _ => {}
                }
            }
            Err(e) => eprintln!("Error decoding from {client_addr}: {e}"),
        }
    }
}
