use std::vec;

use anyhow::Error;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{decoder::PacketTypes, server::Server};

struct Client<'a> {
    server: &'a Server,
    stream: &'a mut TcpStream,
}

impl<'a> Client<'a> {
    pub async fn send_commands(&mut self, commands: Vec<String>) {
        let commands: Vec<PacketTypes> = commands
            .iter()
            .map(|c| PacketTypes::BulkString(c.to_string()))
            .collect();
        let commands = PacketTypes::Array(commands);
        let str = commands.to_string();
        self.stream.write_all(str.as_bytes()).await.unwrap();
    }
}

pub async fn init_client(server: &Server) -> Result<(), Error> {
    let host = &server.replicat_of.as_ref().unwrap().0;
    let port = &server.replicat_of.as_ref().unwrap().1;

    let addr = format!("{}:{}", host, port);

    let mut stream = TcpStream::connect(&addr).await.unwrap();

    let mut client = Client {
        server: &server,
        stream: &mut stream,
    };

    let mut handshake_buf: [u8; 128] = [0; 128];
    println!("Debug: Connected to master at {}", &addr);

    println!("Debug: Sending ping");
    client.send_commands(vec!["ping".to_string()]).await;

    let _ = client.stream.read(&mut handshake_buf);

    client
        .send_commands(vec![
            "replconf".to_string(),
            "listening-port".to_string(),
            server.port.clone(),
        ])
        .await;

    return Ok(());
}
