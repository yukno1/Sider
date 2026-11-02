mod connection;
mod request;
mod resp;
mod server;
mod set;
mod storage;

use crate::connection::{ConnectionMessage, run_listener};
use crate::resp::RESP;
use crate::server::{Server, run_server};
use crate::storage::Storage;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let storage = Storage::new();
    let mut server = Server::new();
    server = server.set_storage(storage);

    let (server_sender, server_receiver) = mpsc::channel::<ConnectionMessage>(32);
    tokio::spawn(run_server(server, server_receiver));

    run_listener("127.0.0.1".to_string(), 6379, server_sender).await;

    Ok(())
}
