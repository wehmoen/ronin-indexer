#[macro_use]
extern crate fstrings;

const REORG_SAFTY_OFFSET: u64 = 50;
const UPPER_THREAD_LIMIT: usize = 32;

use crate::cli_args::Args;
use crate::ronin::Ronin;
use env_logger::Env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;
use tokio::task::JoinHandle;

mod cli_args;
mod mongo;
mod ronin;

fn chunk_u64(base: u64, max: u64, chunk_size: u64) -> Vec<[u64; 2]> {
    let mut chunks: Vec<[u64; 2]> = vec![];

    let mut num = base;

    let mut complete = false;

    loop {
        let start = num.clone();
        num += chunk_size;
        let mut end = num.clone();

        if end >= max {
            end = max;
            complete = true;
        } else {
            end -= 1;
        }
        chunks.push([start, end]);
        if complete {
            break;
        }
    }

    chunks
}

async fn work(range: [u64; 2], args: Args) {
    let db = mongo::connect(&args.db_uri, &args.db_name).await;
    let ronin = Ronin::new(&args.web3_hostname, db).await;

    return ronin.stream(args, range[0], range[1]).await;
}

#[tokio::main]
async fn main() {
    let args = cli_args::parse();

    let default_log_level = match args.debug {
        true => "on",
        false => "warn",
    };

    if args.debug {
        println!("{:#?}", args);
    }

    env_logger::Builder::from_env(Env::default().default_filter_or(default_log_level)).init();

    let db_master = mongo::connect(&args.db_uri, &args.db_name).await;
    let ronin_master = Ronin::new(&args.web3_hostname, db_master).await;

    let sync_start = if args.start_block > 0 {
        args.start_block
    } else {
        1
    };

    let sync_stop = if args.stop_block > 0 {
        args.stop_block
    } else {
        ronin_master
            .provider
            .eth()
            .block_number()
            .await
            .unwrap()
            .as_u64()
            - REORG_SAFTY_OFFSET
    };

    let mut available_parallelism = std::thread::available_parallelism().unwrap().get();
    if args.threads > 0 && args.threads < available_parallelism {
        available_parallelism = args.threads
    }

    if available_parallelism > UPPER_THREAD_LIMIT {
        available_parallelism = UPPER_THREAD_LIMIT
    }

    let chunk_size_base: u64 = if (sync_stop - sync_start) > 1_000_000 {
        1_000_000
    } else {
        sync_stop - sync_start
    };

    let mut chunk_size = (chunk_size_base / available_parallelism as u64) as u64;
    if chunk_size <= 0 {
        chunk_size = 1000;
    }

    let chunks = chunk_u64(sync_start, sync_stop, chunk_size);

    println!(
        "Sync from: {} to {} in {} chunks in {} threads!",
        sync_start,
        sync_stop,
        chunks.len(),
        available_parallelism
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .worker_threads(2)
        .max_blocking_threads(1)
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("fri-{}", id)
        })
        .build()
        .unwrap();

    let mut tasks: Vec<JoinHandle<()>> = vec![];

    let mut i = 0;
    while i < chunks.len() {
        if tasks.len() >= available_parallelism {
            tasks.retain(|t| !t.is_finished());
            thread::sleep(Duration::from_millis(5000));
        }

        match chunks[i] {
            chunk => {
                println!("Spawning {}", i);
                let task = work(chunk, args.clone());
                tasks.push(rt.spawn(task));
                i += 1
            }
        }
    }

    futures::future::join_all(tasks).await;

    rt.shutdown_background();

    println!("All done!");
}
