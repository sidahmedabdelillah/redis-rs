use std::sync::Arc;

use anyhow::Error;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::decoder::{self, PacketTypes};
use crate::store::Store;

#[derive(Debug, PartialEq)]
pub enum ServerRole {
    Master,
    Slave,
}

impl ServerRole {
    pub fn to_string(&self) -> String {
        match self {
            ServerRole::Master => "master".to_string(),
            ServerRole::Slave => "slave".to_string(),
        }
    }
}

pub struct Server {
    pub role: ServerRole,
    pub port: String,
    pub host: String,
    pub replicat_of: Option<(String, String)>,
    pub replid: String,
    pub master_replid: Option<String>,
}

impl Server {
    pub fn new(
        role: ServerRole,
        port: String,
        host: String,
        replicat_of: Option<(String, String)>,
        master_replid: Option<String>,
    ) -> Self {
        let replid: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(40)
            .map(char::from)
            .collect();
        Server {
            role,
            port,
            host,
            replicat_of,
            replid,
            master_replid,
        }
    }
}

pub async fn init_server(store: &Arc<Store>, server: &Arc<Server>) -> Result<(), Error> {
    let addr = format!("{}:{}", &server.host, &server.port);

    let listener = TcpListener::bind(&addr).await?;
    loop {
        let (socket, _) = listener.accept().await?;
        handle_client(socket, &store, &server);
    }
}

fn handle_client(mut stream: TcpStream, store: &Arc<Store>, server: &Arc<Server>) {
    let store = Arc::clone(store);
    let server = Arc::clone(server);

    tokio::spawn(async move {
        let mut buf = [0; 512];
        // In a loop, read data from the socket and write the data back.
        loop {
            let n = stream
                .read(&mut buf)
                .await
                .expect("failed to read data from socket");

            if n == 0 {
                return;
            }

            let (packet, _) = decoder::parse_message(&buf[..n]).unwrap();

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
                                                    send_ok(&mut stream).await.unwrap();
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
                                            send_ok(&mut stream).await.unwrap();
                                        }
                                    }
                                }
                                "REPLCONF" => {
                                    send_ok(&mut stream).await.unwrap();
                                }
                                "PSYNC" => {
                                    send_simple_string(
                                        &mut stream,
                                        &format!("+FULLRESYNC {} 0", server.replid).to_string(),
                                    )
                                    .await
                                    .unwrap();
                                }
                                _ => {
                                    println!("unsupported command {}", commande);
                                }
                            }
                        }
                        (
                            Some(PacketTypes::BulkString(bulk1)),
                            Some(PacketTypes::BulkString(bulk2)),
                            None,
                        ) => {
                            let command = bulk1.as_str();
                            match command.to_uppercase().as_str() {
                                "ECHO" => {
                                    send_bulk_string(&mut stream, &bulk2.to_string())
                                        .await
                                        .unwrap();
                                }
                                "GET" => {
                                    let key = bulk2.to_string();
                                    let value = store.get(key);
                                    if let Some(value) = value {
                                        send_bulk_string(&mut stream, &value.value).await.unwrap();
                                    } else {
                                        send_null_string(&mut stream).await.unwrap();
                                    }
                                }
                                "INFO" => {
                                    let sub = bulk2.as_str();
                                    match sub {
                                        "replication" => {
                                            send_replication_info(&mut stream, &server)
                                                .await
                                                .unwrap();
                                        }
                                        _ => {
                                            println!(
                                                "unsupported sub command for INFO : {}",
                                                command
                                            );
                                        }
                                    }
                                }
                                _ => {
                                    println!("unsupported command {}", command);
                                }
                            }
                        }
                        (Some(PacketTypes::BulkString(bulk1)), None, None) => {
                            let command = bulk1.as_str();
                            match command.to_uppercase().as_str() {
                                "PING" => {
                                    send_pong(&mut stream).await.unwrap();
                                }
                                _ => {
                                    println!("unsupported command {}", command);
                                }
                            }
                        }
                        _ => {
                            panic!("Commands must be of type array");
                        }
                    }
                }
                _ => {
                    panic!("Commands must be of type array");
                }
            }
        }
    });
}

async fn send_ok(stream: &mut TcpStream) -> Result<(), Error> {
    let res = PacketTypes::SimpleString("OK".to_string());
    let ok = res.to_string();
    stream.write_all(ok.as_bytes()).await.unwrap();
    Ok(())
}

async fn send_pong(stream: &mut TcpStream) -> Result<(), Error> {
    let res = PacketTypes::SimpleString("PONG".to_string());
    let pong = res.to_string();
    stream.write_all(pong.as_bytes()).await.unwrap();
    Ok(())
}

async fn send_bulk_string(stream: &mut TcpStream, value: &String) -> Result<(), Error> {
    let res = PacketTypes::BulkString(value.to_string());
    let res = res.to_string();
    stream.write_all(res.as_bytes()).await.unwrap();
    Ok(())
}

async fn send_null_string(stream: &mut TcpStream) -> Result<(), Error> {
    let res = PacketTypes::NullBulkString;
    let res = res.to_string();
    stream.write_all(res.as_bytes()).await.unwrap();
    Ok(())
}

async fn send_replication_info(stream: &mut TcpStream, server: &Server) -> Result<(), Error> {
    let packet = PacketTypes::new_replication_info(server);
    let info = packet.to_string();
    stream.write_all(info.as_bytes()).await.unwrap();
    Ok(())
}

async fn send_simple_string(stream: &mut TcpStream, value: &String) -> Result<(), Error> {
    let res = PacketTypes::SimpleString(value.to_string());
    let res = res.to_string();
    stream.write_all(res.as_bytes()).await.unwrap();
    Ok(())
}
