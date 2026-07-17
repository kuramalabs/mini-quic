use std::io::Error;
use tokio::net::UdpSocket;

#[tokio::main]
async fn main()-> Result<(),Error> {

    let udp_socket = UdpSocket::bind("0.0.0.0:3000").await?;

    let mut buf = vec![0;256];

    loop {
        let (n, client_addr) = udp_socket.recv_from(&mut buf).await?;
        println!("The client {client_addr} sent: {:?}", String::from_utf8_lossy(&buf[..n]));

        udp_socket.send_to(&buf[..n],client_addr).await?;
    }

}
