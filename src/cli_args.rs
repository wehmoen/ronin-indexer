use clap::Parser;
use std::fmt::Debug;

/// Better Ronin blockchain indexer
#[derive(Parser, Debug, Clone)]
#[clap(author = "wehmoen#0001", version, about, long_about = None)]
pub struct Args {
    /// MongoDB connection URL
    #[clap(
        short = 'u',
        long,
        value_parser,
        default_value = "mongodb://127.0.0.1:27017"
    )]
    pub db_uri: String,
    /// MongoDB database name
    #[clap(short = 'd', long, value_parser, default_value = "roninchain")]
    pub db_name: String,
    /// Web3 Websocket Host
    #[clap(short = 'w', long, value_parser, default_value = "ws://localhost:8546")]
    pub web3_hostname: String,
    /// Replay - Drops the entire database and starts reindexing the chain from block 0
    #[clap(short = 'r', long, value_parser, default_value_t = false)]
    pub replay: bool,
    /// Show logs for empty blocks
    #[clap(short = 'l', long, value_parser, default_value_t = false)]
    pub empty_logs: bool,
    /// Debug mode
    #[clap(short = 'd', long, value_parser, default_value_t = false)]
    pub debug: bool,
    /// Start block
    #[clap(short = 's', long, value_parser, default_value_t = 0)]
    pub start_block: u64,
    /// Stop block
    #[clap(short = 'e', long, value_parser, default_value_t = 0)]
    pub stop_block: u64,
    /// Disable logging of wallet updates
    #[clap(short = 'w', long, value_parser, default_value_t = true)]
    pub debug_disable_wallet_updates: bool,
    /// Feature: ERC Transfers
    #[clap(long, value_parser, default_value_t = true)]
    pub feature_erc_transfers: bool,
    /// Feature: ERC 721 Sales
    #[clap(long, value_parser, default_value_t = true)]
    pub feature_erc_721_sales: bool,
    /// Feature: Transactions
    #[clap(long, value_parser, default_value_t = true)]
    pub feature_transactions: bool,
    /// Feature: Wallet Updates
    #[clap(long, value_parser, default_value_t = false)]
    pub feature_wallet_updates: bool,
    /// Max number of threads
    #[clap(long, value_parser, default_value_t = 0)]
    pub max_thread_count: usize,
}

pub fn parse() -> Args {
    Args::parse()
}
