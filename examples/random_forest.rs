use bincode::{deserialize, serialize};
use bitvec::prelude::*;
use bytecount;
use clap::Clap;
use futures::future::try_join_all;
use liquid_ml::dataframe::{Data, Row, Rower};
use liquid_ml::error::LiquidError;
use liquid_ml::liquid_ml::LiquidML;
use log::Level;
use serde::{Deserialize, Serialize};
use simple_logger;
use std::fs::File;
use std::io::{BufRead, BufReader};

/// This is a simple example showing how to load a sor file from disk and
/// distribute it across nodes, and perform pmap
#[derive(Clap)]
#[clap(version = "1.0", author = "Samedh G. & Thomas H.")]
struct Opts {
    /// The IP:Port at which the registration server is running
    #[clap(
        short = "s",
        long = "server_addr",
        default_value = "127.0.0.1:9000":qa

    )]
    server_address: String,
    /// The IP:Port at which this application must run
    #[clap(short = "m", long = "my_addr", default_value = "127.0.0.2:9002")]
    my_address: String,
    /// The number of nodes for the distributed system
    #[clap(short = "n", long = "num_nodes", default_value = "3")]
    num_nodes: usize,
    /// The name of the data file
    #[clap(
        short = "d",
        long = "data",
        default_value = "/home/tom/code/7degrees/commits.ltgt"
    )]
    data: String,
}

/// Finds all the projects that these users have ever worked on
#[derive(Clone, Serialize, Deserialize, Debug)]
struct RandomForest {
    users: BitVec,
    projects: BitVec,
    new_projects: BitVec,
}

impl RandomForest {
    fn new(
        num_projects: usize,
        prev_users: BitVec,
        prev_projects: BitVec,
    ) -> Self {
        let v = BitVec::repeat(false, num_projects);
        ProjectRower {
            users: prev_users,
            projects: prev_projects,
            new_projects: v,
        }
    }
}

impl Rower for ProjectRower {
    fn visit(&mut self, r: &Row) -> bool {
        let pid = match r.get(0).unwrap() {
            Data::Int(x) => *x as usize,
            _ => panic!("Invalid DF"),
        };
        let uid = match r.get(1).unwrap() {
            Data::Int(x) => *x as usize,
            _ => panic!("Invalid DF"),
        };
        if *self.users.get(uid).unwrap() && !self.projects.get(pid).unwrap() {
            self.new_projects.set(pid, true);
        }
        true
    }

    fn join(mut self, other: Self) -> Self {
        self.new_projects |= other.new_projects;
        self
    }
}

/// Finds all the users that have commits on these projects
#[derive(Clone, Serialize, Deserialize, Debug)]
struct UserRower {
    users: BitVec,
    projects: BitVec,
    new_users: BitVec,
}

impl UserRower {
    fn new(
        num_users: usize,
        prev_users: BitVec,
        prev_projects: BitVec,
    ) -> Self {
        let v = BitVec::repeat(false, num_users);
        UserRower {
            users: prev_users,
            projects: prev_projects,
            new_users: v,
        }
    }
}

impl Rower for UserRower {
    fn visit(&mut self, r: &Row) -> bool {
        let pid = match r.get(0).unwrap() {
            Data::Int(x) => *x as usize,
            _ => panic!("Invalid DF"),
        };
        let uid = match r.get(1).unwrap() {
            Data::Int(x) => *x as usize,
            _ => panic!("Invalid DF"),
        };
        if *self.projects.get(pid).unwrap() && !self.users.get(uid).unwrap() {
            self.new_users.set(uid, true);
        }
        true
    }

    fn join(mut self, other: Self) -> Self {
        self.new_users |= other.new_users;
        self
    }
}

fn count_new_lines(file_name: &str) -> usize {
    let mut buf_reader = BufReader::new(File::open(file_name).unwrap());
    let mut new_lines = 0;

    loop {
        let bytes_read = buf_reader.fill_buf().unwrap();
        let len = bytes_read.len();
        if len == 0 {
            return new_lines;
        };
        new_lines += bytecount::count(bytes_read, b'\n');
        buf_reader.consume(len);
    }
}

#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let opts: Opts = Opts::parse();
    simple_logger::init_with_level(Level::Error).unwrap();
    let mut app =
        LiquidML::new(&opts.my_address, &opts.server_address, opts.num_nodes)
            .await?;
    // NOTE: IS this table needed?
    //app.df_from_sor("users", "/code/7degrees/users.ltgt").await?;
    app.df_from_sor("commits", &opts.commits).await?;
    // NOTE: IS this table needed?
    //app.df_from_sor("projects", "~/code/7degrees/projects.ltgt").await?;

    // assume the max of pid is <= num_lines
    //let num_projects = count_new_lines(&opts.projects);
    //let num_users = count_new_lines(&opts.users);

    let num_projects = 125_500_000;
    let num_users = 32_500_000;
    let mut users = BitVec::repeat(false, num_users);
    users.set(4967, true);
    let mut projects = BitVec::repeat(false, num_projects);
    for i in 0..4 {
        println!("degree {}", i);
        let mut pr = ProjectRower::new(num_projects, users, projects);
        // Node 1 will get the rower back and send it to all the other nodes
        // other nodes will wait for node 1 to send the final combined rower to
        // them
        pr = match app.map("commits", pr).await? {
            None => {
                let blob =
                    { app.blob_receiver.lock().await.recv().await.unwrap() };
                deserialize(&blob[..])?
            }
            Some(rower) => {
                let serialized = serialize(&rower)?;
                let mut futs = Vec::new();
                for i in 2..(app.num_nodes + 1) {
                    futs.push(app.kv.send_blob(i, serialized.clone()));
                }
                try_join_all(futs).await?;

                rower
            }
        };
        dbg!("finished projects rower");
        users = pr.users;
        projects = pr.new_projects;
        let mut ur = UserRower::new(num_users, users, projects);
        // Node 1 will get the rower back and send it to all the other nodes
        // other nodes will wait for node 1 to send the final combined rower to
        // them
        ur = match app.map("commits", ur).await? {
            None => {
                let blob =
                    { app.blob_receiver.lock().await.recv().await.unwrap() };
                deserialize(&blob[..])?
            }
            Some(rower) => {
                let serialized = serialize(&rower)?;
                let mut futs = Vec::new();
                // Could send concurrently does it matter?
                for i in 2..(app.num_nodes + 1) {
                    futs.push(app.kv.send_blob(i, serialized.clone()));
                }
                try_join_all(futs).await?;

                rower
            }
        };
        dbg!("finished users rower");
        users = ur.new_users;
        projects = ur.projects;
        println!("num users found: {}", users.count_ones());
    }
    app.kill_notifier.notified().await;

    Ok(())
}
