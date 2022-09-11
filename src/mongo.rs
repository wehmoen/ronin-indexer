use mongodb::options::IndexOptions;
use mongodb::{bson::Document, Client};

use crate::mongo::collections::axie_sale::{Sale, SaleProvider};
use crate::mongo::collections::erc1155_transfer::{ERC1155Transfer, Erc1155TransferProvider};
use crate::mongo::collections::erc_transfer::ErcTransferProvider;
use crate::mongo::collections::transaction::TransactionProvider;
use crate::mongo::collections::{
    erc_transfer::ERCTransfer,
    settings::{Settings, SettingsProvider},
    transaction::Transaction,
    wallet::Wallet,
    wallet::WalletProvider,
};

pub struct IndexModel {
    pub model: Document,
    pub options: IndexOptions,
}

pub trait Indexable {
    fn index_model(&self) -> Vec<IndexModel>;
    fn index_setup_key(&self) -> &'static str;
}

fn index_model(key: &'static str, unique: bool) -> IndexModel {
    let mut doc = Document::new();
    doc.insert(key, 1u32);

    IndexModel {
        model: doc,
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
    pub erc1155_transfers: Erc1155TransferProvider,
    pub erc_sales: SaleProvider,
    pub _client: Client,
    pub _database: mongodb::Database,
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

            pub async fn set<S: Into<String>>(
                &self,
                key: &'static str,
                value: S,
            ) -> mongodb::error::Result<UpdateResult> {
                self.collection
                    .update_one(
                        doc! {
                            "key": key
                        },
                        doc! {
                            "$set": {
                                "key": key,
                                "value": value.into()
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

            fn index_setup_key(&self) -> &'static str {
                "settings"
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

        #[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
        pub struct WalletActivity {
            pub block: Block,
            pub transaction: TransactionHash,
        }

        #[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
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

            fn index_setup_key(&self) -> &'static str {
                "setup.wallets"
            }
        }

        impl WalletProvider {
            pub(crate) fn get_pool(&self) -> Pool<Wallet> {
                Pool::new(self.collection.to_owned())
            }

            pub fn update(
                &self,
                address: &Address,
                block: Block,
                transaction: &TransactionHash,
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

    pub mod axie_sale {
        use mongodb::bson::DateTime;
        use mongodb::Collection;
        use serde::{Deserialize, Serialize};

        use crate::mongo::collections::transaction_pool::Pool;
        use crate::mongo::collections::Address;
        use crate::mongo::{index_model, IndexModel, Indexable};

        #[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
        pub struct Sale {
            pub seller: Address,
            pub buyer: Address,
            pub price: String,
            pub seller_received: String,
            pub token: Address,
            pub token_id: String,
            pub transaction_id: String,
            pub created_at: DateTime,
        }

        pub struct SaleProvider {
            pub(crate) collection: Collection<Sale>,
        }

        impl SaleProvider {
            pub fn new(collection: Collection<Sale>) -> SaleProvider {
                SaleProvider { collection }
            }

            pub(crate) fn get_pool(&self) -> Pool<Sale> {
                Pool::new(self.collection.to_owned())
            }
        }

        impl Indexable for SaleProvider {
            fn index_model(&self) -> Vec<IndexModel> {
                vec![
                    index_model("seller", false),
                    index_model("buyer", false),
                    index_model("token_id", false),
                    index_model("token", false),
                    index_model("created_at", false),
                    index_model("transaction_id", true),
                ]
            }

            fn index_setup_key(&self) -> &'static str {
                "setup.erc_sales"
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

            fn index_setup_key(&self) -> &'static str {
                "setup.transactions"
            }
        }
    }

    pub mod erc1155_transfer {
        use mongodb::Collection;
        use serde::{Deserialize, Serialize};
        use sha2::digest::Update;
        use sha2::{Digest, Sha256};

        use crate::mongo::collections::transaction_pool::Pool;
        use crate::mongo::collections::{Address, Block};
        use crate::mongo::{index_model, IndexModel, Indexable};

        #[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
        pub struct ERC1155Transfer {
            pub token: Address,
            pub operator: Address,
            pub from: Address,
            pub to: Address,
            pub token_id: String,
            pub value: String,
            pub block: Block,
            pub transaction_id: String,
            pub log_index: String,
            pub log_id: String,
        }

        pub struct Erc1155TransferProvider {
            pub collection: Collection<ERC1155Transfer>,
        }

        impl Indexable for Erc1155TransferProvider {
            fn index_model(&self) -> Vec<IndexModel> {
                vec![
                    index_model("log_id", true),
                    index_model("operator", false),
                    index_model("from", false),
                    index_model("to", false),
                    index_model("token_id", false),
                    index_model("value", false),
                    index_model("block", false),
                    index_model("transaction_id", false),
                ]
            }

            fn index_setup_key(&self) -> &'static str {
                "setup.erc1155_transfers"
            }
        }

        impl Erc1155TransferProvider {
            pub fn new(collection: Collection<ERC1155Transfer>) -> Erc1155TransferProvider {
                Erc1155TransferProvider { collection }
            }

            pub(crate) fn get_pool(&self) -> Pool<ERC1155Transfer> {
                Pool::new(self.collection.to_owned())
            }
        }

        impl ERC1155Transfer {
            pub fn get_transfer_id(hash: &str, index: &str) -> String {
                let mut hasher = Sha256::new();
                Update::update(&mut hasher, hash.as_bytes());
                Update::update(&mut hasher, &[b'-']);
                Update::update(&mut hasher, index.as_bytes());
                format!("{:x}", hasher.finalize())
            }
        }
    }

    pub mod erc_transfer {
        use mongodb::bson::doc;
        use mongodb::Collection;
        use serde::{Deserialize, Serialize};
        use sha2::digest::Update;
        use sha2::{Digest, Sha256};

        use crate::mongo::collections::transaction_pool::Pool;
        use crate::mongo::collections::{Address, Block};
        use crate::mongo::{index_model, IndexModel, Indexable};
        use crate::ronin::ContractType;

        #[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
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
                ]
            }

            fn index_setup_key(&self) -> &'static str {
                "setup.erc_transfers"
            }
        }

        impl ErcTransferProvider {
            pub fn new(collection: Collection<ERCTransfer>) -> ErcTransferProvider {
                ErcTransferProvider { collection }
            }

            pub(crate) fn get_pool(&self) -> Pool<ERCTransfer> {
                Pool::new(self.collection.to_owned())
            }
        }

        impl ERCTransfer {
            pub fn get_transfer_id(hash: &str, index: &str) -> String {
                let mut hasher = Sha256::new();
                Update::update(&mut hasher, hash.as_bytes());
                Update::update(&mut hasher, &[b'-']);
                Update::update(&mut hasher, index.as_bytes());
                format!("{:x}", hasher.finalize())
            }
        }
    }
    pub mod transaction_pool {
        use mongodb::bson::Document;
        use mongodb::error::Error;
        use mongodb::options::{InsertManyOptions, UpdateOptions};
        use mongodb::Collection;
        use serde::Serialize;

        pub struct Pool<T> {
            collection: Collection<T>,
            updates: Vec<[Document; 2]>,
            inserts: Vec<T>,
        }

        impl<T> Pool<T>
        where
            T: Serialize + Clone + Eq + PartialEq,
        {
            pub fn new(collection: Collection<T>) -> Self {
                Pool {
                    collection,
                    updates: vec![],
                    inserts: vec![],
                }
            }

            fn has_update(&self, doc: &Document) -> Option<usize> {
                self.updates.iter().position(|d| d[0].eq(doc))
            }

            fn has_insert(&self, doc: &T) -> Option<usize> {
                self.inserts.iter().position(|d| d.eq(doc))
            }

            pub fn insert(&mut self, insert: T) {
                let existing = self.has_insert(&insert);

                match existing {
                    None => {
                        self.inserts.push(insert);
                    }
                    Some(index) => {
                        self.inserts.remove(index);
                        self.inserts.push(insert);
                    }
                }
            }

            pub fn update(&mut self, update: [Document; 2]) {
                let existing = self.has_update(&update[0]);

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
                self.updates.len() + self.inserts.len()
            }

            pub async fn commit(&mut self, upsert: bool) -> Result<&mut Pool<T>, Error> {
                if !self.inserts.is_empty() {
                    self.collection
                        .insert_many(
                            &self.inserts,
                            InsertManyOptions::builder().ordered(false).build(),
                        )
                        .await
                        .ok(); // Todo: figure out a way how to handle errors without inserting docs one by one
                }

                if !self.updates.is_empty() {
                    let options: UpdateOptions = match upsert {
                        true => UpdateOptions::builder().upsert(Some(true)).build(),
                        false => UpdateOptions::builder().build(),
                    };

                    for update in self.updates.as_slice() {
                        match self
                            .collection
                            .update_one(
                                update[0].to_owned(),
                                update[1].to_owned(),
                                options.to_owned(),
                            )
                            .await
                        {
                            Ok(_) => {}
                            Err(error) => {
                                println!("Failed to upsert {:?} with error {:?}", update, error);
                            }
                        }
                    }
                }

                self.updates.clear();
                self.inserts.clear();

                Ok(self)
            }
        }
    }
}

pub async fn connect(hostname: &str, database: &str) -> Database {
    let client = Client::with_uri_str(&hostname)
        .await
        .unwrap_or_else(|_| panic!("Failed to connect to mongodb at {}", &hostname));

    let db = client.database(database);

    let wallets = WalletProvider::new(db.collection::<Wallet>("wallets"));
    let transactions = TransactionProvider::new(db.collection::<Transaction>("transactions"));
    let erc_transfers = ErcTransferProvider::new(db.collection::<ERCTransfer>("erc_transfers"));
    let erc1155_transfers =
        Erc1155TransferProvider::new(db.collection::<ERC1155Transfer>("erc1155_transfers"));
    let settings = SettingsProvider::new(db.collection::<Settings>("settings"));
    let erc_sales = SaleProvider::new(db.collection::<Sale>("erc721_sales"));

    let database = Database {
        wallets,
        transactions,
        settings,
        erc_sales,
        erc_transfers,
        erc1155_transfers,
        _client: client,
        _database: db,
    };

    database.create_indexes().await;

    database
}

impl Database {
    pub async fn create_indexes(&self) {
        let create_settings = match self.settings.get(self.settings.index_setup_key()).await {
            None => true,
            Some(_) => false,
        };

        let create_wallets = match self.settings.get(self.wallets.index_setup_key()).await {
            None => true,
            Some(_) => false,
        };
        let create_transactions = match self.settings.get(self.transactions.index_setup_key()).await
        {
            None => true,
            Some(_) => false,
        };
        let create_erc_transfers = match self
            .settings
            .get(self.erc_transfers.index_setup_key())
            .await
        {
            None => true,
            Some(_) => false,
        };
        let create_erc1155_transfers = match self
            .settings
            .get(self.erc1155_transfers.index_setup_key())
            .await
        {
            None => true,
            Some(_) => false,
        };
        let create_erc_sales = match self.settings.get(self.erc_sales.index_setup_key()).await {
            None => true,
            Some(_) => false,
        };

        if create_settings {
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
                    .expect("Failed to create settings index!");
            }

            self.settings
                .set(self.settings.index_setup_key(), "1")
                .await
                .expect("Failed to complete setup!");
        }
        if create_wallets {
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
                    .expect("Failed to create wallet index!");
            }
            self.settings
                .set(self.wallets.index_setup_key(), "1")
                .await
                .expect("Failed to complete setup!");
        }
        if create_transactions {
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
                    .expect("Failed to create transaction index!");
            }
            self.settings
                .set(self.transactions.index_setup_key(), "1")
                .await
                .expect("Failed to complete setup!");
        }
        if create_erc_transfers {
            for model in self.erc_transfers.index_model() {
                self.erc_transfers
                    .collection
                    .create_index(
                        mongodb::IndexModel::builder()
                            .keys(model.model)
                            .options(model.options)
                            .build(),
                        None,
                    )
                    .await
                    .expect("Failed to create erc_transfer index!");
            }
            self.settings
                .set(self.erc_transfers.index_setup_key(), "1")
                .await
                .expect("Failed to complete setup!");
        }
        if create_erc1155_transfers {
            for model in self.erc1155_transfers.index_model() {
                self.erc1155_transfers
                    .collection
                    .create_index(
                        mongodb::IndexModel::builder()
                            .keys(model.model)
                            .options(model.options)
                            .build(),
                        None,
                    )
                    .await
                    .expect("Failed to create erc1155_transfer index!");
            }
            self.settings
                .set(self.erc1155_transfers.index_setup_key(), "1")
                .await
                .expect("Failed to complete setup!");
        }
        if create_erc_sales {
            for model in self.erc_sales.index_model() {
                self.erc_sales
                    .collection
                    .create_index(
                        mongodb::IndexModel::builder()
                            .keys(model.model)
                            .options(model.options)
                            .build(),
                        None,
                    )
                    .await
                    .expect("Failed to create erc_sales index!");
            }
            self.settings
                .set(self.erc_sales.index_setup_key(), "1")
                .await
                .expect("Failed to complete setup!");
        }
    }
}
