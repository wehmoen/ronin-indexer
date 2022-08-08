use mongodb::{Client, ClientSession, Collection};

use crate::mongo::collections::statistic::StatisticProvider;
use crate::mongo::collections::wallet::Wallet;
use crate::mongo::collections::{
    axie_transfer::AxieTransfer, erc_transfer::ERCTransfer, statistic::Statistic,
    transaction::Transaction, wallet::WalletProvider,
};

pub struct SessionBuilder {}
impl SessionBuilder {
    pub async fn build(client: &Client) -> ClientSession {
        client
            .start_session(None)
            .await
            .expect("Failed to create session!")
    }
}

pub struct Database {
    pub wallets: WalletProvider,
    pub transactions: Collection<Transaction>,
    pub statistics: StatisticProvider,
    pub erc_transfers: Collection<ERCTransfer>,
    pub axie_transfers: Collection<AxieTransfer>,
    pub _client: Client,
}

pub mod collections {
    pub type Address = String;
    pub type TransactionHash = String;
    pub type Block = u64;

    // Todo: Convert to a key:value storage and rename to settings
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
        use mongodb::bson::{doc, Document};
        use mongodb::Collection;
        pub use serde::{Deserialize, Serialize};

        use crate::mongo::collections::transaction_pool::Pool;
        use crate::mongo::collections::{Address, Block, TransactionHash};

        #[derive(Serialize, Deserialize, Clone)]
        pub struct WalletActivity {
            pub block: Block,
            pub transaction: TransactionHash,
        }

        #[derive(Serialize, Deserialize, Clone)]
        pub struct Wallet {
            address: Address,
            last_seen: WalletActivity,
        }

        #[derive(Clone)]
        pub struct WalletProvider {
            collection: Collection<Wallet>,
        }

        impl WalletProvider {
            pub(crate) fn get_pool(&self) -> Pool<Wallet> {
                Pool::new(self.collection.clone())
            }

            pub fn update(
                &self,
                address: Address,
                block: Block,
                transaction: TransactionHash,
            ) -> [Document; 2] {
                [
                    doc! {"address": &address},
                    doc! {
                        "$set": {
                            "last_seen": {
                                "block": block as i64,
                                "transaction": transaction
                            }
                        }
                    },
                ]
            }

            pub fn new(collection: Collection<Wallet>) -> WalletProvider {
                WalletProvider { collection }
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
        use sha2::digest::Update;
        use sha2::{Digest, Sha256};

        use crate::mongo::collections::{Address, Block};
        use crate::ronin::ContractType;

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
        use mongodb::bson::DateTime;
        use serde::{Deserialize, Serialize};
        use sha2::digest::Update;
        use sha2::{Digest, Sha256};

        use crate::mongo::collections::{Address, Block};

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

    pub mod transaction_pool {
        use mongodb::bson::Document;
        use mongodb::error::Error;
        use mongodb::options::UpdateOptions;
        use mongodb::Collection;
        use serde::Serialize;

        pub struct Pool<T> {
            collection: Collection<T>,
            updates: Vec<[Document; 2]>,
        }

        impl<T> Pool<T>
        where
            T: Serialize,
        {
            pub fn new(collection: Collection<T>) -> Self {
                Pool {
                    collection,
                    updates: vec![],
                }
            }

            fn has(&self, doc: Document) -> Option<usize> {
                self.updates.clone().into_iter().position(|d| d[0].eq(&doc))
            }

            pub fn update(&mut self, update: [Document; 2]) {
                let existing = self.has(update[0].clone());

                match existing {
                    None => {
                        self.updates.push(update);
                    }
                    Some(index) => {
                        self.updates.remove(index);
                        self.updates.push(update);
                    }
                }
            }

            pub fn len(&self) -> usize {
                self.updates.len()
            }

            pub async fn commit(
                &mut self,
                mut session: mongodb::ClientSession,
                upsert: bool,
            ) -> Result<&mut Pool<T>, Error> {
                session.start_transaction(None).await?;

                let options: UpdateOptions = match upsert {
                    true => UpdateOptions::builder().upsert(Some(true)).build(),
                    false => UpdateOptions::builder().build(),
                };

                for update in self.updates.as_slice() {
                    let _ = self
                        .collection
                        .update_one_with_session(
                            update[0].to_owned(),
                            update[1].to_owned(),
                            options.to_owned(),
                            &mut session,
                        )
                        .await;
                }

                session.commit_transaction().await?;

                self.updates.clear();

                Ok(self)
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
        _client: client,
    }
}
