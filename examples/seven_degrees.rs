use bincode::{deserialize, serialize};
use clap::Clap;
use liquid_ml::dataframe::{Data, LocalDataFrame, Row, Rower};
use liquid_ml::error::LiquidError;
use liquid_ml::liquid::Application;
use log::Level;
use serde::{Deserialize, Serialize};
use futures::future::try_join_all;
use simple_logger;
use std::collections::HashSet;
/// This is a simple example showing how to load a sor file from disk and
/// distribute it across nodes, and perform pmap
#[derive(Clap)]
#[clap(version = "1.0", author = "Samedh G. & Thomas H.")]
struct Opts {
    /// The IP:Port at which the registration server is running
    #[clap(
        short = "s",
        long = "server_addr",
        default_value = "127.0.0.1:9000"
    )]
    server_address: String,
    /// The IP:Port at which this application must run
    #[clap(short = "m", long = "my_addr", default_value = "127.0.0.2:9002")]
    my_address: String,
}

/// Finds all the projects that these users have ever worked on
#[derive(Clone, Serialize, Deserialize, Debug)]
struct ProjectRower {
    users: HashSet<u32>,
    projects: HashSet<u32>,
    new_projects: HashSet<u32>,
}

impl Rower for ProjectRower {
    fn visit(&mut self, r: &Row) -> bool {
        let pid = match r.get(0).unwrap() {
            Data::Int(x) => *x as u32,
            _ => panic!("Invalid DF"),
        };
        let uid = match r.get(1).unwrap() {
            Data::Int(x) => *x as u32,
            _ => panic!("Invalid DF"),
        };
        if self.users.contains(&uid) && !self.projects.contains(&pid) {
            self.new_projects.insert(pid);
        }
        true
    }

    fn join(mut self, other: Self) -> Self {
        self.new_projects.extend(other.new_projects.into_iter());
        self
    }
}

/// Finds all the users that have commits on these projects
#[derive(Clone, Serialize, Deserialize, Debug)]
struct UserRower {
    users: HashSet<u32>,
    projects: HashSet<u32>,
    new_users: HashSet<u32>,
}

impl Rower for UserRower {
    fn visit(&mut self, r: &Row) -> bool {
        let pid = match r.get(0).unwrap() {
            Data::Int(x) => *x as u32,
            _ => panic!("Invalid DF"),
        };
        let uid = match r.get(1).unwrap() {
            Data::Int(x) => *x as u32,
            _ => panic!("Invalid DF"),
        };
        if self.projects.contains(&pid) && !self.users.contains(&uid) {
            self.new_users.insert(uid);
        }
        true
    }

    fn join(mut self, other: Self) -> Self {
        self.new_users.extend(other.new_users.into_iter());
        self
    }
}

#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let opts: Opts = Opts::parse();
    simple_logger::init_with_level(Level::Error).unwrap();
    let mut app =
        Application::new(&opts.my_address, &opts.server_address, 8).await?;
    // NOTE: IS this table needed?
    //app.df_from_sor("users", "/code/7degrees/users.ltgt").await?;
    app.df_from_sor("commits", "/home/tom/code/7degrees/smol_commits.ltgt")
        .await?;
    // NOTE: IS this table needed?
    //app.df_from_sor("projects", "~/code/7degrees/projects.ltgt").await?;

    let mut users = HashSet::new();
    users.insert(4967);
    let mut projects = HashSet::new();
    for i in 0..4 {
        println!("degree {}", i);
        let mut pr = ProjectRower {
            users,
            projects,
            new_projects: HashSet::new(),
        };
        // Node 1 will get the rower back and send it to all the other nodes
        // other nodes will wait for node 1 to send the final combined rower to
        // them
        pr = match app.pmap("commits", pr).await? {
            None => deserialize(
                &app.blob_receiver.lock().await.recv().await.unwrap()[..],
            )?,
            Some(rower) => {
                let serialized = serialize(&rower)?;
                let unlocked = app.kv.lock().await;
                let mut futs = Vec::new();
                for i in 2..(app.num_nodes + 1) {
                    futs.push(unlocked.send_blob(i, serialized.clone()));
                }
                try_join_all(futs).await?;

                rower
            }
        };
        users = pr.users;
        projects = pr.new_projects;
        let mut ur = UserRower {
            users,
            projects,
            new_users: HashSet::new(),
        };
        // Node 1 will get the rower back and send it to all the other nodes
        // other nodes will wait for node 1 to send the final combined rower to
        // them
        ur = match app.pmap("commits", ur).await? {
            None => deserialize(
                &app.blob_receiver.lock().await.recv().await.unwrap()[..],
            )?,
            Some(rower) => {
                let serialized = serialize(&rower)?;
                let unlocked = app.kv.lock().await;
                // Could send concurrently does it matter?
                for i in 2..(app.num_nodes + 1) {
                    unlocked.send_blob(i, serialized.clone()).await?;
                }
                rower
            }
        };
        users = ur.new_users;
        projects = ur.projects;
    }
    println!("num users found: {}", users.len());
    app.kill_notifier.notified().await;

    Ok(())
}
