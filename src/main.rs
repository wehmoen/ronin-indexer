#[macro_use]
extern crate fstrings;

const REORG_SAFTY_OFFSET: u64 = 50;
const UPPER_THREAD_LIMIT: usize = 32;

use crate::cli_args::Args;
use crate::ronin::Ronin;
use env_logger::Env;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::atomic::{AtomicUsize, Ordering};

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

async fn work(range: [u64; 2], args: Args, progress: ProgressBar) {
    let db = mongo::connect(&args.db_uri, &args.db_name).await;
    let ronin = Ronin::new(&args.web3_hostname, db).await;

    ronin.stream(args, range[0], range[1], Some(progress)).await;
}

#[tokio::main]
async fn main() {
    let global_progress = MultiProgress::new();

    let args = cli_args::parse();

    let default_log_level = match args.debug {
        true => "on",
        false => "warn",
    };

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

    let chunk_size = (1_000_000 / available_parallelism) as u64;

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
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("fri-{}", id)
        })
        .build()
        .unwrap();

    let mut tasks = vec![];

    let progress_bar_style = ProgressStyle::default_spinner().template("{spinner}{bar:80.cyan/blue} {percent:>3}% | [{eta_precise}][{elapsed_precise}] ETA/Elapsed | {msg}").unwrap();

    for chunk in chunks {
        let progress = global_progress.add(ProgressBar::new(chunk[1] - chunk[0]));
        progress.set_style(progress_bar_style.clone());

        let task = work(chunk, args.clone(), progress);
        tasks.push(rt.spawn(task));
    }

    futures::future::join_all(tasks).await;

    rt.shutdown_background();

    println!("All done!");
}
