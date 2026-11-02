mod result;
pub use result::{ServerError, ServerMessage, ServerResult};

use crate::{
    RESP,
    connection::ConnectionMessage,
    request::Request,
    storage::{Storage, StorageError, StorageResult},
};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

pub struct Server {
    pub storage: Option<Storage>,
}

impl Server {
    // server can be initialised without the storage, but this is just a stylistic
    // choice.
    pub fn new() -> Self {
        Self { storage: None }
    }

    pub fn set_storage(mut self, storage: Storage) -> Self {
        self.storage = Some(storage);
        self
    }
}

pub async fn run_server(mut server: Server, mut crx: mpsc::Receiver<ConnectionMessage>) {
    loop {
        tokio::select! {
            Some(message) = crx.recv() => {
                match message {
                    ConnectionMessage::Request(request) => {
                        process_request(request, &mut server).await;
                    }
                }
            }
        }
    }
}

pub async fn process_request(request: Request, server: &mut Server) -> StorageResult<RESP> {
    let elements = match &request.value {
        RESP::Array(v) => v,
        _ => {
            // return Err(StorageError::IncorrectRequest);
            panic!()
        }
    };

    let mut command = Vec::new();
    for elem in elements.iter() {
        match elem {
            RESP::BulkString(v) => command.push(v.clone()),
            _ => {
                // return Err(StorageError::IncorrectRequest);
                panic!()
            }
        }
    }
    // match command[0].to_lowercase().as_str() {
    //     "ping" => Ok(RESP::SimpleString(String::from("PONG"))),
    //     "echo" => Ok(RESP::BulkString(command[1].clone())),
    //     _ => {
    //         return Err(ServerError::CommandError);
    //     }
    // }

    let storage = match server.storage.as_mut() {
        Some(storage) => storage,
        None => panic!(),
    };

    let response = storage.process_command(&command);
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    // #[test]
    // fn test_process_request_ping() {
    //     let (connection_sender, _) = mpsc::channel::<ServerMessage>(32);

    //     let request = Request {
    //         value: RESP::Array(vec![RESP::BulkString(String::from("PING"))]),
    //         sender: connection_sender,
    //     };

    //     let storage = Arc::new(Mutex::new(Storage::new()));

    //     let output = process_request(request, storage).unwrap();

    //     assert_eq!(output, RESP::SimpleString(String::from("PONG")));
    // }
    // #[test]
    // fn test_process_request_not_array() {
    //     let (connection_sender, _) = mpsc::channel::<ServerMessage>(32);
    //     let request = Request {
    //         value: RESP::BulkString(String::from("PING")),
    //         sender: connection_sender,
    //     };
    //     let storage = Arc::new(Mutex::new(Storage::new()));

    //     let error = process_request(request, storage).unwrap_err();
    //     assert_eq!(error, StorageError::IncorrectRequest);
    // }
    // #[test]
    // fn test_process_request_not_bulkstrings() {
    //     let (connection_sender, _) = mpsc::channel::<ServerMessage>(32);
    //     let request = Request {
    //         value: RESP::Array(vec![RESP::SimpleString(String::from("PING"))]),
    //         sender: connection_sender,
    //     };
    //     let storage = Arc::new(Mutex::new(Storage::new()));

    //     let error = process_request(request, storage).unwrap_err();
    //     assert_eq!(error, StorageError::IncorrectRequest);
    // }

    // #[test]
    // fn test_process_request_echo() {
    //     let (connection_sender, _) = mpsc::channel::<ServerMessage>(32);
    //     let request = Request {
    //         value: RESP::Array(vec![
    //             RESP::BulkString(String::from("ECHO")),
    //             RESP::BulkString(String::from("42")),
    //         ]),
    //         sender: connection_sender,
    //     };
    //     let storage = Arc::new(Mutex::new(Storage::new()));

    //     let output = process_request(request, storage).unwrap();
    //     assert_eq!(output, RESP::BulkString(String::from("42")));
    // }
}
