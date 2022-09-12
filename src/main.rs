#[macro_use]
extern crate fstrings;

use crate::cli_args::Args;
use crate::ronin::Ronin;
use env_logger::Env;

mod cli_args;
mod mongo;
mod ronin;

async fn work(range: [u64; 2], args: Args) {
    let db = mongo::connect(&args.db_uri, &args.db_name).await;
    let ronin = Ronin::new(&args.web3_hostname, db).await;

    ronin.stream(50, args, range[0], range[1]).await;
}

#[tokio::main]
async fn main() {
    let args = cli_args::parse();

    let default_log_level = match args.debug {
        true => "on",
        false => "warn",
    };

    env_logger::Builder::from_env(Env::default().default_filter_or(default_log_level)).init();

    let db_master = mongo::connect(&args.db_uri, &args.db_name).await;
    let ronin_master = Ronin::new(&args.web3_hostname, db_master).await;
    let chain_height = ronin_master
        .provider
        .eth()
        .block_number()
        .await
        .unwrap()
        .as_u64();

    let chunk_size: u64 = 1_000_000;

    let chunks: Vec<[u64; 2]> = (0..=((chain_height - 1) / chunk_size))
        .map(|n| {
            let start = n * chunk_size;
            let end = (start + chunk_size).min(chain_height);
            [start + 1, end]
        })
        .collect();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .thread_name("decoder-thread")
        .build()
        .unwrap();

    let mut tasks = vec![];

    for chunk in chunks {
        let task = work(chunk, args.clone());
        tasks.push(rt.spawn(task));
    }

    futures::future::join_all(tasks).await;
    println!("All done!");
}
