mod types;

use mini_quic::{decode_message, encode_message, MessageType};
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
                let active = instant.elapsed() < Duration::from_secs(10);
                if !active {
                    println!("Client with {addr} is dropped");
                    dropped.push(*addr);
                }
                active
            });

            for addr in dropped {
                let msg = encode_message(MessageType::Dropped, b"You are dropped");
                if let Err(e) = u1.send_to(&msg, addr).await {
                    eprintln!("Could not notify {addr}: {e}");
                }
            }
        }
    });

    loop {
        let (n, client_addr) = udp_socket.recv_from(&mut buf).await?;
        let res = decode_message(&buf[..n]);

        match res {
            Ok((msg_type, bytes)) => {
                match msg_type {
                    MessageType::Join => {
                        if clients
                            .lock()
                            .await
                            .insert(client_addr, Instant::now())
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
                        all_clients.insert(client_addr, Instant::now());

                        println!(
                            "The client {client_addr} sent: {:?}",
                            String::from_utf8_lossy(bytes)
                        );

                        let encoded_msg = encode_message(MessageType::Regular, bytes);

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
