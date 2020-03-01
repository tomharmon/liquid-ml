use tokio::net::TcpStream;
use tokio::prelude::*;
use bincode;
use rspark::network::DirEntry;
use tokio::io::{self, BufStream, AsyncReadExt};
use std::io::Write;

///A test client to connect to the reguistration server

#[tokio::main]
async fn main() {
    let mut stream = TcpStream::connect("127.0.0.1:9000").await.unwrap();
    println!("created stream");
    
    let me = DirEntry{ip : "my ip".to_string(), port:123123};
    let me_ser = bincode::serialize(&me).unwrap();

    let result = stream.write(&me_ser[..]).await;
    println!("wrote to stream; success={:?}", result.is_ok());
    let mut buff = Vec::new();
    let mut buffered = BufStream::new(stream);
    buffered.read_to_end(&mut buff).await.unwrap();
    println!("read the following : {:?}", buff);
    std::io::stdout().flush();
}
