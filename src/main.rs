use mini_quic::{
    classify_seq, decode_message, encode_message, send_ack_empty, Clients, MessageType,
    SequenceStatus,
};
use std::collections::HashMap;
use std::io::Error;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::time::{self, Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let clients = Clients::new(Mutex::new(HashMap::new()));

    let udp_socket = Arc::new(UdpSocket::bind("0.0.0.0:3000").await?);
    let mut buf = vec![0; 50];

    let c1 = clients.clone();
    let u1 = udp_socket.clone();

    tokio::spawn(async move {
        loop {
            time::sleep(Duration::from_secs(10)).await;
            let mut dropped = Vec::new();

            c1.lock().await.retain(|addr, (seq, instant)| {
                let active = instant.elapsed() < Duration::from_secs(10);
                if !active {
                    println!("Client with {addr} is dropped");
                    dropped.push((*addr, *seq));
                }
                active
            });

            for (addr, seq) in dropped {
                let msg = encode_message(
                    MessageType::Dropped,
                    b"You are dropped due to inactivity",
                    seq,
                );
                if let Err(e) = u1.send_to(&msg, addr).await {
                    eprintln!("Could not notify {}: {e}", addr);
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
                            eprintln!("Try restarting the client");
                            continue;
                        }
                        //verify the seq_no and log
                        let stored_seq = all_clients.get(&client_addr).unwrap().0;

                        let seq_status = classify_seq(stored_seq, curr_seq_no);

                        // log this
                        println!("{}", seq_status.info());

                        match seq_status {
                            SequenceStatus::InOrder => {
                                // this is good
                                println!(
                                    "The client {client_addr} sent: {:?}, curr_seq_no: {}",
                                    String::from_utf8_lossy(bytes),
                                    curr_seq_no
                                );

                                all_clients.insert(client_addr, (curr_seq_no, Instant::now()));

                                let encoded_msg =
                                    encode_message(MessageType::Regular, bytes, curr_seq_no);
                                for (addr, _) in all_clients.iter() {
                                    if *addr != client_addr
                                        && let Err(e) = udp_socket.send_to(&encoded_msg, addr).await
                                    {
                                        println!("Could not send to: {addr}, Error: {e}");
                                    }
                                }

                                send_ack_empty(
                                    udp_socket.clone(),
                                    client_addr,
                                    curr_seq_no,
                                    format!(
                                        " Error sending ack to client : {} for seq_no: {}",
                                        client_addr, curr_seq_no
                                    )
                                    .as_str(),
                                )
                                .await
                            }
                            SequenceStatus::Duplicate => {
                                println!(
                                    "Duplicate seq no: {} detected from client: {}",
                                    curr_seq_no, client_addr
                                );
                                // should tell the client that it is safe to eject the curr seq number is
                                // of no use .. so we can eject it

                                send_ack_empty(
                                    udp_socket.clone(),
                                    client_addr,
                                    curr_seq_no,
                                    format!(
                                        "Error sending ack to client : {} for seq_no: {}",
                                        client_addr, curr_seq_no
                                    )
                                    .as_str(),
                                )
                                .await
                            }
                            SequenceStatus::Gap(gap) => {
                                println!(" Gap Detected between client and server: {}", gap);
                                // do not send the ack.. will send only in the case of the Inorder
                                // and also in case of duplicate
                            }
                            SequenceStatus::LateArrival => {
                                println!(
                                    " This seq no: {} is a late arrival from client :{}",
                                    curr_seq_no, client_addr
                                );
                                // this case is very particular .. maybe I will never hit it.
                                // How about blocking client side unless there is ack for the latest send
                                // lets see, lets see
                                // send ack but

                                send_ack_empty(
                                    udp_socket.clone(),
                                    client_addr,
                                    curr_seq_no,
                                    format!(
                                        " Error sending ack to client : {} for seq_no: {}",
                                        client_addr, curr_seq_no
                                    )
                                    .as_str(),
                                )
                                .await
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
