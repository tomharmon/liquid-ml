use crate::error::DFError;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;

pub struct Client {
    pub id: usize,
    pub port: usize,
    pub address: String,
    pub msg_id: usize,
    pub directory: Vec<DirEntry>,
    pub connections: Vec<TcpStream>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DirEntry {
    pub ip: String,
    pub port: usize,
}

/*
impl Client {
    async fn send_to(&self, data: &[u8], id: usize) -> Result<(), DFError> {
        match self.directory.get(id) {
            None => Err(DFError::RowIndexOutOfBounds),
            Some(x) => x.socket.write(data)
    }
    }

    async fn read_from(&self, id: usize) -> Result<(), DFError> {
        let sock = self.directory.get(id)?
    }

}
*/
