use crate::request::Request;
use crate::resp::RESP;
use crate::server::Server;
use crate::server::ServerValue;

pub async fn command(_server: &Server, request: &Request, command: &[String]) {
    request
        .data(ServerValue::RESP(RESP::bulk_string(&command[1])))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::ServerMessage;
    use tokio::sync::mpsc;

    #[tokio::test]
    // Test that the function command processes
    // an `ECHO` request and that it
    // responds with a copy of the input.
    async fn test_command() {
        let cmd = vec![String::from("echo"), String::from("hey")];
        let server = Server::new();
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: RESP::Null,
            sender: connection_sender,
        };

        command(&server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::RESP(RESP::bulk_string("hey")))
        );
    }
}
