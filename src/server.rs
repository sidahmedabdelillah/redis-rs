use std::collections::HashSet;

use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex;

use anyhow::Error;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::decoder::{self, PacketTypes};
use crate::rdb::{get_rdb_bytes, EMPTY_RDB_FILE};
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
    pub slaves: Arc<Mutex<Vec<String>>>,
    pub pending_updates: Arc<Mutex<HashSet<(String, String)>>>,
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
        let slaves = Arc::new(Mutex::new(Vec::new()));
        let pending_updates = Arc::new(Mutex::new(HashSet::<(String, String)>::new()));
        let shared_pending_updates = Arc::clone(&pending_updates);
        Server {
            role,
            port,
            host,
            replicat_of,
            replid,
            master_replid,
            slaves,
            pending_updates: shared_pending_updates,
        }
    }

    async fn is_slave(&self, stream_id: &String) -> Result<bool, Error> {
        let server = self;
        let slaves = server.slaves.lock().await;
        return Ok(slaves.contains(stream_id));
    }
    pub async fn apply_pending_update(&self, conn: &Connection) {
        let stream_id = &conn.stream_id;
        if let Ok(is_slave) = self.is_slave(stream_id).await {
            if !is_slave {
                return;
            }
        }
        let server = self;
        let slaves = server.slaves.lock().await;
        let mut pending_updates = server.pending_updates.lock().await;
        let updates = pending_updates.clone();
        if slaves.len() == 0 || updates.len() == 0 {
            return;
        }
        print!("Debug: applying pending updates\n");

        println!("Debug: there is {} pending updates", updates.len());
        println!("Debug: current stream_id: {}", stream_id);
        for (slave, update) in updates.iter() {
            println!(
                "Debug: testing update for slave {} against {}",
                slave, stream_id
            );
            if slave.clone() == stream_id.clone() {
                println!("Debug: applying update {} on server {}", update, stream_id);
                pending_updates.remove(&(slave.to_string(), update.to_string()));
                conn.send_data(update.as_bytes()).await;
            }
        }
    }

    pub async fn add_pending_update(&self, update: &String) {
        print!("Debug: adding pending update {} \n", update);
        let server = &self;
        let mut pending_updates = server.pending_updates.lock().await;
        let servers = server.slaves.lock().await;
        for slave in servers.iter() {
            pending_updates.insert((slave.to_string(), update.to_string()));
        }
    }

    pub async fn send_empty_rdb(&self, conn: &Connection) -> Result<(), Error> {
        let res = PacketTypes::RDB(get_rdb_bytes().to_vec());
        let res = res.to_string();
        conn.send_data([res.as_bytes(), &EMPTY_RDB_FILE].concat().as_slice())
            .await;
        Ok(())
    }

    pub async fn send_replication_info(&self, conn: &Connection) -> Result<(), Error> {
        let packet = PacketTypes::new_replication_info(self);
        let info = packet.to_string();
        conn.send_data(info.as_bytes()).await;
        Ok(())
    }

    pub async fn add_slave(&self, conn: &Connection) {
        let stream_id = conn.stream_id.clone();
        println!("Debug: adding slave with id {}", stream_id);
        let slaves = &mut self.slaves.lock().await;
        slaves.push(stream_id.clone());
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

pub struct Connection {
    pub tcp_stream: Arc<Mutex<TcpStream>>,
    pub stream_id: String,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        let stream_id = stream.peer_addr().unwrap().to_string().clone();
        Connection {
            tcp_stream: Arc::new(Mutex::new(stream)),
            stream_id: stream_id,
        }
    }
    pub async fn send_data(&self, data: &[u8]) {
        let stream = &mut self.tcp_stream.lock().await;
        stream.write_all(data).await.unwrap();
    }
    pub async fn read_data(&self, data: &mut [u8; 512]) -> usize {
        let stream = &mut self.tcp_stream.lock().await;
        return stream.read(data).await.unwrap();
    }
}

async fn handle_client(stream: TcpStream, store: &Arc<Store>, server: &Arc<Server>) {
    let store = Arc::clone(store);
    let server = Arc::clone(&server);

    tokio::spawn(async move {
        let mut buf = [0; 512];
        let conn = Connection::new(stream);
        // In a loop, read data from the socket and write the data back.
        loop {
            tokio::select! {
                    res = conn.read_data(&mut buf) => {

                    let n = res;
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
                                            // raw command string from buf
                                            let raw_command =
                                                std::str::from_utf8(&buf[..n]).unwrap().to_string();
                                            // add to pending updates
                                            let server = Arc::clone(&server);
                                            server.add_pending_update(&raw_command).await;

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
                                                            send_ok(&conn).await.unwrap();
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
                                                    send_ok(&conn).await.unwrap();
                                                }
                                            }
                                        }
                                        "REPLCONF" => {
                                            send_ok(&conn).await.unwrap();
                                        }
                                        "PSYNC" => {
                                            send_simple_string(
                                                &conn,
                                                &format!("+FULLRESYNC {} 0", server.replid).to_string(),
                                            )
                                            .await
                                            .unwrap();
                                            let server = Arc::clone(&server);
                                            server.send_empty_rdb(&conn).await.unwrap();

                                            server.add_slave(&conn).await
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
                                            send_bulk_string(&conn, &bulk2.to_string())
                                                .await
                                                .unwrap();
                                        }
                                        "GET" => {
                                            let key = bulk2.to_string();
                                            let value = store.get(key);
                                            if let Some(value) = value {
                                                send_bulk_string(&conn, &value.value).await.unwrap();
                                            } else {
                                                send_null_string(&conn).await.unwrap();
                                            }
                                        }
                                        "INFO" => {
                                            let sub = bulk2.as_str();
                                            match sub {
                                                "replication" => {
                                                    server
                                                        .send_replication_info(&conn)
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
                                            send_pong(&conn).await.unwrap();
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
                // _ = server.apply_pending_update(&mut stream, &stream_id) => {
                //     println!("Debug: applied pending updates");
                // }
            }
        }
    });
}

async fn send_ok(conn: &Connection) -> Result<(), Error> {
    let res = PacketTypes::SimpleString("OK".to_string());
    let ok = res.to_string();
    conn.send_data(ok.as_bytes()).await;
    Ok(())
}

async fn send_pong(conn: &Connection) -> Result<(), Error> {
    let res = PacketTypes::SimpleString("PONG".to_string());
    let pong = res.to_string();
    conn.send_data(pong.as_bytes()).await;
    Ok(())
}

async fn send_bulk_string(conn: &Connection, value: &String) -> Result<(), Error> {
    let res = PacketTypes::BulkString(value.to_string());
    let res = res.to_string();
    conn.send_data(res.as_bytes());
    Ok(())
}

async fn send_null_string(conn: &Connection) -> Result<(), Error> {
    let res = PacketTypes::NullBulkString;
    let res = res.to_string();
    conn.send_data(res.as_bytes()).await;
    Ok(())
}

pub async fn send_simple_string(conn: &Connection, value: &String) -> Result<(), Error> {
    let res = PacketTypes::SimpleString(value.to_string());
    let res = res.to_string();
    conn.send_data(res.as_bytes()).await;
    Ok(())
}
