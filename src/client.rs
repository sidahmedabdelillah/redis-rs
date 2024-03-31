use std::{sync::Arc, vec};

use anyhow::Error;
use clap::Command;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{decoder::PacketTypes, server::Server};

pub async fn init_client(server: &Server) -> Result<(), Error> {
    let host = &server.replicat_of.as_ref().unwrap().0;
    let port = &server.replicat_of.as_ref().unwrap().1;

    let addr = format!("{}:{}", host, port);

    let mut stream = TcpStream::connect(&addr).await.unwrap();

    println!("Debug: Connected to master at {}", &addr);
    
    let ping = PacketTypes::BulkString("ping".to_string());
    let commands: Vec<PacketTypes> = vec![ping];
    let commands = PacketTypes::Array(commands);
    let str = commands.to_string();

    println!("Debug: Sending {}", str);
    stream.write_all(str.as_bytes()).await.unwrap();

    return Ok(());
}
