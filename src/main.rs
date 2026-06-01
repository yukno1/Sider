use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

mod resp;

use crate::resp::RESP;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(handle_conn(stream));
            }
            Err(e) => {
                eprintln!("error: {}", e);
                continue;
            }
        }
    }
}

async fn handle_conn(mut stream: TcpStream) {
    let mut buffer = [0; 512];

    loop {
        match stream.read(&mut buffer).await {
            Ok(size) if size != 0 => {
                println!("Received: {:?}", buffer);
                let response = RESP::SimpleString(String::from("PONG"));

                if let Err(e) = stream.write_all(response.to_string().as_bytes()).await {
                    println!("Error writing to socket: {}", e);
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
