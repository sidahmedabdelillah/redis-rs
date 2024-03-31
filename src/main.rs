use std::sync::Arc;

use anyhow::Error;
use clap::Parser;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

mod decoder;
mod store;

use decoder::PacketTypes;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long)]
    port: Option<String>,

    #[arg(long)]
    host: Option<String>,

    #[clap(short, long, value_delimiter = ' ', num_args = 2)]
    pub replicaof: Option<Vec<String>>,
}

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

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let port = args.port.unwrap_or("6379".to_string());
    let host = args.host.unwrap_or("127.0.0.1".to_string());
    let addr = format!("{}:{}", host, port);

    let server = match args.replicaof {
        Some(replicatof) => {
            if replicatof.len() != 2 {
                panic!("replicatof must have 2 arguments");
            }
            let replid: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(40)
                .map(char::from)
                .collect();
            Server::new(
                ServerRole::Slave,
                port,
                host,
                Some((replicatof[0].to_string(), replicatof[1].to_string())),
                Some(replid),
            )
        }
        None => Server::new(ServerRole::Master, port, host, None, None),
    };

    let server = Arc::new(server);
    let store = Arc::new(store::Store::new());

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind(&addr).await?;

    loop {
        let (socket, _) = listener.accept().await?;
        handle_client(socket, &store, &server);
    }
}

fn handle_client(mut stream: TcpStream, store: &Arc<store::Store>, server: &Arc<Server>) {
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
                                                    let res =
                                                        PacketTypes::SimpleString("OK".to_string());
                                                    let ok = res.to_string();
                                                    stream.write_all(ok.as_bytes()).await.unwrap();
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
                                            let res = PacketTypes::SimpleString("OK".to_string());
                                            let ok = res.to_string();
                                            stream.write_all(ok.as_bytes()).await.unwrap();
                                        }
                                    }
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
                                    let res = PacketTypes::BulkString(bulk2.to_string());
                                    let echo = res.to_string();
                                    stream.write_all(echo.as_bytes()).await.unwrap();
                                }
                                "GET" => {
                                    let key = bulk2.to_string();
                                    println!("debug: handeling get for key {}", key);
                                    let value = store.get(key);
                                    if let Some(value) = value {
                                        let res = PacketTypes::BulkString(value.value);
                                        let res = res.to_string();
                                        stream.write_all(res.as_bytes()).await.unwrap();
                                    } else {
                                        let res = PacketTypes::NullBulkString;
                                        let res = res.to_string();
                                        stream.write_all(res.as_bytes()).await.unwrap();
                                    }
                                }
                                "INFO" => {
                                    let sub = bulk2.as_str();
                                    match sub {
                                        "replication" => {
                                            let packet = PacketTypes::new_replication_info(&server);
                                            let info = packet.to_string();
                                            stream.write_all(info.as_bytes()).await.unwrap();
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
                                    let res = PacketTypes::SimpleString("PONG".to_string());
                                    let pong = res.to_string();
                                    stream.write_all(pong.as_bytes()).await.unwrap();
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
