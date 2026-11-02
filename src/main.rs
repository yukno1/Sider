mod connection;
mod request;
mod resp;
mod server;
mod set;
mod storage;

use crate::connection::ConnectionMessage;
use crate::request::Request;
use crate::resp::{RESP, bytes_to_resp};
use crate::server::{ServerMessage, process_request};
use crate::storage::Storage;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    select,
    sync::mpsc,
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    let storage = Arc::new(Mutex::new(Storage::new()));

    let mut interval_timer = tokio::time::interval(Duration::from_millis(10));

    let (server_sender, _) = mpsc::channel::<ConnectionMessage>(32);

    loop {
        // now have both the listener and the timer to await
        tokio::select! {
            connection =listener.accept()=>{
                match connection {
                    Ok((stream, _)) => {
                        tokio::spawn(handle_conn(stream, server_sender.clone()));
                    }
                    Err(e) => {
                        eprintln!("error: {}", e);
                        continue;
                    }
                }
            }

            _ = interval_timer.tick() => {
                //  expire_keys is an asynchronous function but its true nature is that of a blocking one.
                tokio::spawn(expire_keys(storage.clone()));
            }
        }
    }
}

async fn handle_conn(mut stream: TcpStream, server_sender: mpsc::Sender<ConnectionMessage>) {
    let mut buffer = [0; 512];

    let (connection_sender, _) = mpsc::channel::<ServerMessage>(32);

    loop {
        // use the select! instead of awaiting directly stream.read.
        // preparing the handler to host multiple asynchronous actions.
        select! {
            result =stream.read(&mut buffer) => {
                match result {
                    Ok(size) if size != 0 => {
                        println!("Received: {:?}", buffer);
                        let mut index: usize = 0;

                        let resp = match bytes_to_resp(&buffer[..size].to_vec(), &mut index) {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("Error: {}", e);
                                return;
                            }
                        };

                        let request = Request {
                            value: resp,
                            sender: connection_sender.clone(),
                        };

                        match server_sender.send(ConnectionMessage::Request(request)).await {
                            Ok(()) => {},
                            Err(e) => {
                                eprintln!("Error sending request: {}", e);
                                return;
                            }
                        }
                    }
                    Ok(_) => {
                        println!("Connection closed");
                        break;
                    }
                    Err(e) => {
                        println!("Error: {}", e);
                        break;
                    }
                }
            }
        }
    }
}

async fn expire_keys(storage: Arc<Mutex<Storage>>) {
    let mut guard = storage.lock().unwrap();
    guard.expire_keys();
}
