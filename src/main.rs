mod types;

use std::collections::HashMap;
use std::io::Error;

use mini_quic::{decode_message, encode_message, MessageType};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::time;
use tokio::time::{Duration, Instant};
use types::Clients;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let clients = Clients::new(Mutex::new(HashMap::new()));

    let udp_socket = UdpSocket::bind("0.0.0.0:3000").await?;
    let mut buf = vec![0; 50];

    let c1 = clients.clone();
    tokio::spawn(async move {
        loop {
            time::sleep(Duration::from_secs(5)).await;
            c1.lock().await.retain(|addr, instant| {
                let active = instant.elapsed() < Duration::from_secs(10);
                if !active {
                    println!("Client with {addr} is dropped");
                }
                active
            })
        }
    });

    loop {
        let (n, client_addr) = udp_socket.recv_from(&mut buf).await?;
        eprintln!("Got {n} bytes from {client_addr}: {:?}", &buf[..n]);
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

                        for (addr, _) in clients.lock().await.iter() {
                            if *addr != client_addr {
                                if let Err(e) = udp_socket.send_to(&encoded_msg, addr).await {
                                    println!("Could not send to: {addr}, Error: {e}");
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => eprintln!("Error decoding from {client_addr}: {e}"),
        }
    }
}
