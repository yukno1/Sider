use crate::{resp::RESP, server::ServerMessage};
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct Request {
    pub value: RESP,
    pub sender: mpsc::Sender<ServerMessage>,
}
