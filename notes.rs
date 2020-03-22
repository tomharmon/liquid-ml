struct x;

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

#[(tokio_main)]
main {
   let app = Application::new();
   app.run_pmap(r);
}



// -------------------------------
main.rs :

Rower r = { 

}



#[tokio-main]
fn main () {
    let app = Application::new("abc.sor", 20);
    app.run(|kv| {
        something complicated 
    })
    app.pmap(r);
}





