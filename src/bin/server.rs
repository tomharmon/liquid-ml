/*
async fn process_socket(
    socket: &mut BufStream<TcpStream>,
    directory: &mut Vec<DirEntry>,
) {
    // Receive and deserialize a registration message from client
    let mut buffer = Vec::new();
    socket.read_to_end(&mut buffer).await.unwrap();
    println!("Received the registration msg {:#?}", buffer);
    let registration: DirEntry = bincode::deserialize(&buffer).unwrap();
    println!("Deserialized to {:#?}", registration);
    let message = bincode::serialize(directory).unwrap();
    println!("Sending directory: {:#?}", message);
    // Send the directory
    socket.write_all(&message[..]).await.unwrap();
    // add the DirEntry registration from the client to the
    // current directory
    directory.push(registration);
    std::io::stdout().flush().unwrap();
}
*/
//https://docs.rs/tokio/0.2.13/tokio/net/struct.TcpListener.html
#[tokio::main]
async fn main() {
    /*
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9000".to_string());
    println!("Server running on {:?}", addr);
    let mut listener = TcpListener::bind(addr).await.unwrap();

    // Here we convert the `TcpListener` to a stream of incoming connections
    // with the `incoming` method.
    let server = async move {
        let mut incoming = listener.incoming();
        let mut directory: Vec<DirEntry> = Vec::new();
        while let Some(socket_res) = incoming.next().await {
            match socket_res {
                Ok(socket) => {
                    // Accepted a connection from a new client
                    println!(
                        "Accepted connection from {:?}",
                        socket.peer_addr()
                    );
                    let mut buf_stream = BufStream::new(socket);
                    process_socket(&mut buf_stream, &mut directory).await;
                }
                Err(err) => {
                    // Handle error by printing to STDOUT.
                    println!("accept error = {:?}", err);
                }
            }
        }
    };

    // Start the server and block this async fn until `server` spins down.
    server.await;*/
}
