struct x;

// Vitek's implied design
impl Application for x {
    
    fn run(kv: KVStore) {
        let key = ("data", kv.network.id);
        let df = DataFrame::from_sor()
        let df : DataFrame = kv.wait_and_get(key).await;
        /// DO some operations
        df.pmap .....
        Dataframe::fromscalar("")
        if (my nodeid == 1) {
               combine the inermediate results
        }
    }

    fn run_pmap(r: Rower, filename) {


    }

}

// Our design pt1
#[(tokio_main)]
main {
    // Starts up a client that will:
    // 1. Register with the `Server` running at the address "192.168.0.0:9000"
    // 2. Schema-on-read a chunk of the "foo.sor" file. The chunk that is read
    //    is based on the number of nodes (in this case 20) and the assigned id
    //    given to this Client by the Server. All nodes must have the entire
    //    "foo.sor" file (alternatively we could send the schema around and then
    //    we only need each node's chunk stored on each node).
    // 3. Since the `Rower` trait defines how to join chunks, the Application
    //    layer will handle running pmap/map/filter on a local chunk and joining
    //    them globally
    let app = Application::new("foo.sor", 20, "192.168.0.0:9000");
    let r = MyRower::new();
    app.run_pmap(r);
}



// Our design pt2
#[tokio-main]
fn main () {
    let app = Application::new("abc.sor", 20);
    // alternatively we can also have a run() method that takes a closure,
    // where the signature of the closure can take in a `kv` or `df` as a
    // parameter to let the user do something more advanced/custom
    app.run(|kv| {
        something complicated 
    })
    app.pmap(r);
}





