use mongodb::options::IndexOptions;
use mongodb::{bson::Document, Client, ClientSession};

use crate::mongo::collections::erc_transfer::ErcTransferProvider;
use crate::mongo::collections::transaction::TransactionProvider;
use crate::mongo::collections::{
    erc_transfer::ERCTransfer,
    settings::{Settings, SettingsProvider},
    transaction::Transaction,
    wallet::Wallet,
    wallet::WalletProvider,
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

pub struct IndexModel {
    pub model: Document,
    pub options: IndexOptions,
}

pub trait Indexable {
    fn index_model(&self) -> Vec<IndexModel>;
}

fn index_model(key: &'static str, unique: bool) -> IndexModel {
    let mut doc = Document::new();
    let doc = doc.insert(key, 1u32).unwrap();
    let doc = doc.as_document().unwrap();

    IndexModel {
        model: doc.to_owned(),
        options: match unique {
            true => IndexOptions::builder().unique(true).build(),
            false => Default::default(),
        },
    }
}

pub struct Database {
    pub wallets: WalletProvider,
    pub transactions: TransactionProvider,
    pub settings: SettingsProvider,
    pub erc_transfers: ErcTransferProvider,
    pub _client: Client,
}

pub mod collections {
    pub type Address = String;
    pub type TransactionHash = String;
    pub type Block = u64;

    pub mod settings {
        use mongodb::bson::doc;
        use mongodb::options::UpdateOptions;
        use mongodb::results::UpdateResult;
        use mongodb::Collection;
        pub use serde::{Deserialize, Serialize};

        use crate::mongo::{index_model, IndexModel, Indexable};

        #[derive(Serialize, Deserialize)]
        pub struct Settings {
            key: String,
            pub value: String,
        }

        pub struct SettingsProvider {
            pub collection: Collection<Settings>,
        }

        impl SettingsProvider {
            pub fn new(collection: Collection<Settings>) -> SettingsProvider {
                SettingsProvider { collection }
            }

            pub async fn get(&self, key: &'static str) -> Option<Settings> {
                self.collection
                    .find_one(
                        doc! {
                            "key": key
                        },
                        None,
                    )
                    .await
                    .unwrap()
            }

            pub async fn set(
                &self,
                key: &'static str,
                value: String,
            ) -> mongodb::error::Result<UpdateResult> {
                self.collection
                    .update_one(
                        doc! {
                            "key": key
                        },
                        doc! {
                            "$set": {
                                "key": key,
                                "value": value
                            }
                        },
                        UpdateOptions::builder().upsert(Some(true)).build(),
                    )
                    .await
            }
        }
        impl Indexable for SettingsProvider {
            fn index_model(&self) -> Vec<IndexModel> {
                vec![index_model("key", true)]
            }
        }
    }
    pub mod wallet {
        use mongodb::bson::{doc, Document};
        use mongodb::Collection;
        pub use serde::{Deserialize, Serialize};

        use crate::mongo::collections::transaction_pool::Pool;
        use crate::mongo::collections::{Address, Block, TransactionHash};
        use crate::mongo::{index_model, IndexModel, Indexable};

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
            pub collection: Collection<Wallet>,
        }

        impl Indexable for WalletProvider {
            fn index_model(&self) -> Vec<IndexModel> {
                vec![index_model("address", true)]
            }
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
        use mongodb::bson::doc;
        use mongodb::Collection;
        pub use serde::{Deserialize, Serialize};

        use crate::mongo::collections::{Address, Block, TransactionHash};
        use crate::mongo::{index_model, IndexModel, Indexable};

        #[derive(Serialize, Deserialize)]
        pub struct Transaction {
            pub from: Address,
            pub to: Address,
            pub hash: TransactionHash,
            pub block: Block,
            pub timestamp: mongodb::bson::DateTime,
        }

        pub struct TransactionProvider {
            pub(crate) collection: Collection<Transaction>,
        }

        impl TransactionProvider {
            pub fn new(collection: Collection<Transaction>) -> TransactionProvider {
                TransactionProvider { collection }
            }
        }

        impl Indexable for TransactionProvider {
            fn index_model(&self) -> Vec<IndexModel> {
                vec![
                    index_model("hash", true),
                    index_model("block", false),
                    index_model("from", false),
                    index_model("to", false),
                ]
            }
        }
    }
    pub mod erc_transfer {
        use mongodb::bson::doc;
        use mongodb::Collection;
        use serde::{Deserialize, Serialize};
        use sha2::digest::Update;
        use sha2::{Digest, Sha256};

        use crate::mongo::collections::{Address, Block};
        use crate::mongo::{index_model, IndexModel, Indexable};
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

        pub struct ErcTransferProvider {
            pub collection: Collection<ERCTransfer>,
        }

        impl Indexable for ErcTransferProvider {
            fn index_model(&self) -> Vec<IndexModel> {
                vec![
                    index_model("log_id", true),
                    index_model("from", false),
                    index_model("to", false),
                    index_model("token", false),
                    index_model("value_or_token_id", false),
                    index_model("block", false),
                    index_model("transaction_id", false),
                    index_model("erc", false),
                    index_model("log_index", false),
                ]
            }
        }

        impl ErcTransferProvider {
            pub fn new(collection: Collection<ERCTransfer>) -> ErcTransferProvider {
                ErcTransferProvider { collection }
            }
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

    let wallets = WalletProvider::new(db.collection::<Wallet>("wallets"));
    let transactions = TransactionProvider::new(db.collection::<Transaction>("transactions"));
    let erc_transfers = ErcTransferProvider::new(db.collection::<ERCTransfer>("erc_transfers"));
    let settings = SettingsProvider::new(db.collection::<Settings>("settings"));

    let database = Database {
        wallets,
        transactions,
        settings,
        erc_transfers,
        _client: client,
    };

    database.create_indexes().await;

    database
}

impl Database {
    pub async fn create_indexes(&self) {
        for model in self.settings.index_model() {
            self.settings
                .collection
                .create_index(
                    mongodb::IndexModel::builder()
                        .keys(model.model)
                        .options(model.options)
                        .build(),
                    None,
                )
                .await
                .expect("Failed to create index!");
        }

        for model in self.wallets.index_model() {
            self.wallets
                .collection
                .create_index(
                    mongodb::IndexModel::builder()
                        .keys(model.model)
                        .options(model.options)
                        .build(),
                    None,
                )
                .await
                .expect("Failed to create index!");
        }

        for model in self.transactions.index_model() {
            self.transactions
                .collection
                .create_index(
                    mongodb::IndexModel::builder()
                        .keys(model.model)
                        .options(model.options)
                        .build(),
                    None,
                )
                .await
                .expect("Failed to create index!");
        }
    }
}
