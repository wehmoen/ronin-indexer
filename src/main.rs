#[macro_use]
extern crate fstrings;

use crate::ronin::Ronin;

mod cli_args;
mod mongo;
mod ronin;

#[tokio::main]
async fn main() {
    let args = cli_args::parse();
    let db = mongo::connect(args.db_uri, args.db_name).await;
    let ronin = Ronin::new(args.web3_hostname, db).await;

    ronin.stream(50, args.replay).await;
}
