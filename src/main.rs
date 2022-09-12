#[macro_use]
extern crate fstrings;

use crate::ronin::Ronin;
use env_logger::Env;

mod cli_args;
mod mongo;
mod ronin;

#[tokio::main]
async fn main() {
    let args = cli_args::parse();

    let default_log_level = match args.debug {
        true => "on",
        false => "warn",
    };

    env_logger::Builder::from_env(Env::default().default_filter_or(default_log_level)).init();
    let db = mongo::connect(&args.db_uri, &args.db_name).await;
    let ronin = Ronin::new(&args.web3_hostname, db).await;

    ronin.stream(50, args).await;
}
