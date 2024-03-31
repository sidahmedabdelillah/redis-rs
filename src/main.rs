use std::sync::Arc;

use anyhow::Error;
use clap::Parser;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use server::{init_server, Server, ServerRole};

mod client;
mod decoder;
mod server;
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

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let port = args.port.unwrap_or("6379".to_string());
    let host = args.host.unwrap_or("127.0.0.1".to_string());

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

    if server.role == ServerRole::Slave {
        let client_server = Arc::clone(&server);
        tokio::spawn(async move {
            client::init_client(&client_server).await.unwrap();
        });
    }

    init_server(&store, &server).await?;

    return Ok(());
}
