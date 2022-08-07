use crate::mongo::collections::statistic::StatisticProvider;
use crate::mongo::collections::wallet::Wallet;
use crate::mongo::collections::{
    axie_transfer::AxieTransfer, erc_transfer::ERCTransfer, statistic::Statistic,
    transaction::Transaction, wallet::WalletProvider,
};
use mongodb::Collection;

pub struct Database {
    pub wallets: WalletProvider,
    pub transactions: Collection<Transaction>,
    pub statistics: StatisticProvider,
    pub erc_transfers: Collection<ERCTransfer>,
    pub axie_transfers: Collection<AxieTransfer>,
}

pub mod collections {
    pub type Address = String;
    pub type TransactionHash = String;
    pub type Block = u64;

    pub mod statistic {
        use mongodb::bson::doc;
        use mongodb::options::UpdateOptions;
        use mongodb::results::UpdateResult;
        use mongodb::Collection;
        pub use serde::{Deserialize, Serialize};

        use crate::mongo::collections::Block;

        #[derive(Serialize, Deserialize)]
        pub struct Statistic {
            last_block: Block,
        }

        pub struct StatisticProvider {
            collection: Collection<Statistic>,
        }
        impl StatisticProvider {
            pub fn new(collection: Collection<Statistic>) -> StatisticProvider {
                StatisticProvider { collection }
            }

            pub async fn update(&self, block: Block) -> mongodb::error::Result<UpdateResult> {
                let options = UpdateOptions::builder().upsert(Some(true)).build();
                self.collection
                    .update_one(
                        doc! {},
                        {
                            doc! {
                                "$set": {
                                    "last_block": block as i64
                                }
                            }
                        },
                        options,
                    )
                    .await
            }

            pub async fn last_block(&self) -> Option<Block> {
                let db_result = self
                    .collection
                    .find_one(None, None)
                    .await
                    .expect("Failed to load statistics from database")
                    .unwrap_or(Statistic { last_block: 0 });

                Some(db_result.last_block)
            }
        }
    }
    pub mod wallet {
        use mongodb::bson::doc;
        use mongodb::Collection;
        pub use serde::{Deserialize, Serialize};

        use crate::mongo::collections::{Address, Block, TransactionHash};

        #[derive(Serialize, Deserialize)]
        pub struct WalletActivity {
            block: Block,
            transaction: TransactionHash,
        }

        #[derive(Serialize, Deserialize)]
        pub struct Wallet {
            address: Address,
            first_seen: WalletActivity,
            last_seen: WalletActivity,
        }

        pub struct WalletProvider {
            collection: Collection<Wallet>,
        }

        impl WalletProvider {
            //Todo: Compute the actual last seen tx for any address before invoking update. Otherwise there can be multiple updates per address per block.
            pub async fn update(
                &self,
                address: Address,
                block: Block,
                transaction: TransactionHash,
            ) {
                let mut wallet = self
                    .collection
                    .find_one(doc! {"address": &address}, None)
                    .await
                    .unwrap();

                match wallet {
                    Some(found_wallet) => {
                        if (found_wallet.last_seen.block != block
                            || found_wallet.last_seen.transaction != transaction)
                            && found_wallet.last_seen.block <= block
                        {
                            self.collection
                                .update_one(
                                    doc! {"address": &address},
                                    doc! {
                                        "$set": {
                                            "last_seen": {
                                                "block": block as i64,
                                                "transaction": transaction
                                            }
                                        }
                                    },
                                    None,
                                )
                                .await
                                .expect("Failed to update existing wallet in database!");
                        }
                    }
                    None => {
                        let wallet = Wallet::new(address.clone(), block, transaction);
                        self.collection
                            .insert_one(wallet, None)
                            .await
                            .expect("Failed to insert new wallet to database!");
                    }
                }
            }

            pub fn new(collection: Collection<Wallet>) -> WalletProvider {
                WalletProvider { collection }
            }
        }

        impl Wallet {
            pub fn new(address: Address, block: Block, transaction: String) -> Wallet {
                Wallet {
                    address,
                    first_seen: WalletActivity {
                        block,
                        transaction: transaction.clone(),
                    },
                    last_seen: WalletActivity { block, transaction },
                }
            }
        }
    }
    pub mod transaction {
        pub use serde::{Deserialize, Serialize};

        use crate::mongo::collections::{Address, Block, TransactionHash};

        #[derive(Serialize, Deserialize)]
        pub struct Transaction {
            pub from: Address,
            pub to: Address,
            pub hash: TransactionHash,
            pub block: Block,
            pub timestamp: mongodb::bson::DateTime,
        }
    }
    pub mod erc_transfer {
        use serde::{Deserialize, Serialize};

        use crate::mongo::collections::{Address, Block};
        use crate::ronin::ContractType;

        use sha2::digest::Update;
        use sha2::{Digest, Sha256};

        #[derive(Serialize, Deserialize, Debug, Clone)]
        pub struct ERCTransfer {
            pub from: Address,
            pub to: Address,
            pub token: String,
            pub value_or_token_id: String,
            pub block: Block,
            pub transaction_id: String,
            pub erc: ContractType,
            pub log_index: String,
            pub log_id: String,
        }

        impl ERCTransfer {
            pub fn get_transfer_id(hash: String, index: String) -> String {
                let id = f!("{hash}-{index}");
                let mut hasher = Sha256::new();
                Update::update(&mut hasher, id.as_bytes());
                format!("{:x}", hasher.finalize())
            }
        }
    }
    pub mod axie_transfer {
        use crate::mongo::collections::{Address, Block};
        use mongodb::bson::DateTime;
        use serde::{Deserialize, Serialize};

        use sha2::digest::Update;
        use sha2::{Digest, Sha256};

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub struct AxieTransfer {
            pub from: Address,
            pub to: Address,
            pub axie: u32,
            pub block: Block,
            pub created_at: DateTime,
            pub transfer_id: String,
        }

        impl AxieTransfer {
            pub fn get_transfer_id(from: &str, to: &str, axie: &u32, block: &Block) -> String {
                let id = f!("{from}{to}{axie}{block}");
                let mut hasher = Sha256::new();
                Update::update(&mut hasher, id.as_bytes());
                format!("{:x}", hasher.finalize())
            }
        }
    }
}

pub async fn connect(hostname: String, database: String) -> Database {
    let client = mongodb::Client::with_uri_str(&hostname)
        .await
        .expect(format!("Failed to connect to mongodb at {}", &hostname).as_str());

    let db = client.database(database.as_str());

    let wallet_collection = db.collection::<Wallet>("wallets");
    let transaction_collection = db.collection::<Transaction>("transactions");
    let statistic_collection = db.collection::<Statistic>("statistics");
    let erc_transfer_collection = db.collection::<ERCTransfer>("erc_transfers");
    let axie_transfer_collection = db.collection::<AxieTransfer>("axie_transfers");

    Database {
        wallets: WalletProvider::new(wallet_collection),
        transactions: transaction_collection,
        statistics: StatisticProvider::new(statistic_collection),
        erc_transfers: erc_transfer_collection,
        axie_transfers: axie_transfer_collection,
    }
}
