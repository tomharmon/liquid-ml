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
    fn new(master_addr: String, my_addr: String) -> Result<Self, Error> {
        let mut master_stream(master_addr).await?

    }

    async fn send_to(&self, data: &[u8], id: usize) -> Result<(), Error> {
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
