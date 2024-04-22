use anyhow::Error;
use std::{sync::Arc, vec};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    decoder::{PacketTypes, Parser},
    server::Server,
    store::Store,
};

struct Client<'a> {
    stream: &'a mut TcpStream,
    is_tracking_ack: bool,
    tracked_changed: u64,
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

    pub async fn send_replconf_offset(&mut self, offset: u64) {
        self.send_commands(vec![
            "REPLCONF".to_string(),
            "ACK".to_string(),
            offset.to_string(),
        ])
        .await;
    }

    pub fn activate_replication_tracking(&mut self) {
        self.is_tracking_ack = true;
    }

    pub fn track_packed_replication(&mut self, packet: &PacketTypes){
        if self.is_tracking_ack {
            let bytes = packet.to_bytes();
            self.tracked_changed += bytes.len() as u64;
        }
    }
}

pub async fn init_client(store: &Arc<Store>, server: &Server) -> Result<(), Error> {
    let host = &server.replicat_of.as_ref().unwrap().0;
    let port = &server.replicat_of.as_ref().unwrap().1;

    let addr = format!("{}:{}", host, port);

    let mut stream = TcpStream::connect(&addr).await.unwrap();

    let mut client = Client {
        stream: &mut stream,
        is_tracking_ack: false,
        tracked_changed: 0,
    };

    let mut handshake_buf: [u8; 128] = [0; 128];
    println!("Debug: Connected to master at {}", &addr);

    println!("Debug: Sending ping");
    client.send_commands(vec!["ping".to_string()]).await;

    let _ = client.stream.read(&mut handshake_buf).await.unwrap();

    println!("Debug: Sending replconf 1 ");
    client
        .send_commands(vec![
            "replconf".to_string(),
            "listening-port".to_string(),
            server.port.clone(),
        ])
        .await;

    let _ = client.stream.read(&mut handshake_buf).await.unwrap();
    println!("Debug: Sending replconf 2 ");
    client
        .send_commands(vec![
            "replconf".to_string(),
            "capa".to_string(),
            "psync2 ".to_string(),
        ])
        .await;

    let _ = client.stream.read(&mut handshake_buf).await.unwrap();
    println!("Debug: Sending PSYNC  ");
    client
        .send_commands(vec!["PSYNC".to_string(), "?".to_string(), "-1".to_string()])
        .await;

    let store = Arc::clone(store);

    println!("Debug: handshake with master done. into the loop we go");
    loop {
        let buf = &mut [0; 2048];

        println!("Debug: loop iteration");
        let res = client.stream.read(buf).await;
        println!("DEBUG: client got data from master");
        let n = res.unwrap();
        if n == 0 {
            println!("The n is zero breaking");
            break;
        }

        let mut parser = Parser::new(buf[..n].to_vec());
        let packets = parser.parse().unwrap();
        println!("Debug: got {} packets on read from master", packets.len());
        for packet in packets {
            client.track_packed_replication(&packet);
            match packet {
                PacketTypes::Array(packets) => {
                    let packet1 = packets.get(0);
                    let packet2 = packets.get(1);
                    let packet3 = packets.get(2);

                    match (packet1, packet2, packet3) {
                        (
                            Some(PacketTypes::BulkString(bulk1)),
                            Some(PacketTypes::BulkString(bulk2)),
                            Some(PacketTypes::BulkString(bulk3)),
                        ) => {
                            let commande = bulk1.as_str().to_uppercase();
                            match commande.as_str() {
                                "SET" => {
                                    println!("Debug: got set commande from master");
                                    let key = bulk2.to_string();
                                    let value = bulk3.to_string();

                                    let cmd2 = packets.get(3);
                                    let cmd3 = packets.get(4);

                                    match (cmd2, cmd3) {
                                        (
                                            Some(PacketTypes::BulkString(bulk4)),
                                            Some(PacketTypes::BulkString(bulk5)),
                                        ) => {
                                            let commande = bulk4.as_str().to_uppercase();
                                            match commande.as_str() {
                                                "PX" => {
                                                    let expire_time =
                                                        bulk5.as_str().parse::<u64>().unwrap();
                                                    println!("debug: setting key {} with value {} and expiry {}", key, value,expire_time);
                                                    store.set_with_expiry(key, value, expire_time);
                                                }
                                                _ => {
                                                    println!(
                                                        "unsupported sub commande {} for SET",
                                                        commande
                                                    );
                                                }
                                            }
                                        }
                                        _ => {
                                            println!(
                                                "debug: setting key {} with value {}",
                                                key, value
                                            );
                                            store.set(key, value);
                                        }
                                    }
                                }
                                "REPLCONF" => {
                                    println!("Debug: got REPLCONF commande from master");
                                    let subcommand = bulk2.as_str();
                                    let value = bulk3.as_str();
                                    match (subcommand, value) {
                                        ("GETACK", "*") => {
                                            client.activate_replication_tracking();
                                            client.send_replconf_offset(client.tracked_changed).await;
                                        }
                                        any => {
                                            println!("Debug: client cant handle {} {} variation of REPLCONF", any.0,any.1)
                                        }
                                    }
                                }
                                _ => {
                                    println!("unsupported command {}", commande);
                                }
                            }
                        }
                        _ => {}
                    }
                }
                PacketTypes::SimpleString(simple_string) => {
                    println!("Debug: client got simple string {simple_string}");
                },
                PacketTypes::RDB(_) => {
                    println!("Debug: client got RDB file from server");
                }
                _ => {}
            }
        }
    }

    return Ok(());
}
