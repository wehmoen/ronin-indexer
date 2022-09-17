use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use std::thread;
use std::time::Duration;

use log::Level::Info;
use log::{debug, info, log_enabled, warn};
use mongodb::bson::{doc, DateTime};
use serde::{Deserialize, Serialize};
use thousands::Separable;
use url::Url;
use web3::ethabi::{Event, EventParam, ParamType, RawLog};
use web3::transports::{Either, Http, WebSocket};
use web3::types::{BlockId, BlockNumber, Log, TransactionReceipt};
use web3::Web3;
use ParamType::{Address, FixedBytes, Uint};

use ContractType::{LegacyErc721Sale, MarketplaceV2, ERC1155, ERC20, ERC721};

use crate::cli_args::Args;
use crate::mongo::collections::axie_sale::Sale;
use crate::mongo::collections::erc1155_transfer::ERC1155Transfer;
use crate::mongo::collections::transaction::Transaction;
use crate::mongo::collections::transaction_pool::Pool;
use crate::mongo::collections::wallet::Wallet;
use crate::mongo::collections::{erc_transfer::ERCTransfer, Block};
use crate::mongo::Database;

const ERC_TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

const MARKETPLACE_V2_ORDER_MATCHED_TOPIC: &str =
    "0xafa0d706792fa5d4e9aaf5e456e08e2a833b1e64a201710b782f29172f6d7a3a";

const MARKETPLACE_V2_DEPLOY_BLOCK: Block = 16027461;

const MARKETPLACE_AXIE_SALE_TOPIC: &str =
    "0x0c0258cd7f0d9474f62106c6981c027ea54bee0b323ea1991f4caa7e288a5725";

const ERC1155_TRANSFER_SINGLE_TOPIC: &str =
    "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62";

const ERC1155_DEPLOY_BLOCK: Block = 16171588;

const _ERC721_TOKEN: [&str; 3] = [
    "0xcbb5cc4b59a6993d6fb1ac439761dd5bf751a8c2",
    "0xa96660f0e4a3e9bc7388925d245a6d4d79e21259",
    "0x8c811e3c958e190f5ec15fb376533a3398620500",
];

const _ERC20_TOKEN: [&str; 10] = [
    "0x97a9107c1793bc407d6f527b77e7fff4d812bece",
    "0xa8754b9fa15fc18bb59458815510e40a12cd2014",
    "0xc99a6a985ed2cac1ef41640596c5a5f9f4e19ef5",
    "0x0b7007c13325c48911f73a2dad5fa5dcbf808adc",
    "0x173a2d4fa585a63acd02c107d57f932be0a71bcc",
    "0xe514d9deb7966c8be0ca922de8a064264ea6bcd4",
    "0xc6344bc1604fcab1a5aad712d766796e2b7a70b9",
    "0x306a28279d04a47468ed83d55088d0dcd1369294",
    "0x2ecb08f87f075b5769fe543d0e52e40140575ea7",
    "0xa7964991f339668107e2b6a6f6b8e8b74aa9d017",
];

pub struct Ronin {
    database: Database,
    pub provider: Web3<Either<WebSocket, Http>>,
}

pub enum AddressPrefix {
    // Ronin,
    Ethereum,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Debug, Clone)]
pub enum ContractType {
    ERC20,
    ERC721,
    ERC1155,
    Erc1155Bulk,
    Unknown,
    MarketplaceV2,
    LegacyErc721Sale,
}

#[derive(Serialize, Deserialize)]
pub struct Contract {
    pub name: &'static str,
    pub decimals: usize,
    pub erc: ContractType,
    pub address: &'static str,
}

pub type ContractList = HashMap<&'static str, Contract>;

#[derive(Serialize, Deserialize)]
struct LargestBlock {
    number: Block,
    tx_num: u64,
}

impl Ronin {
    pub fn transfer_events() -> HashMap<ContractType, Event> {
        let mut map: HashMap<ContractType, Event> = HashMap::new();

        map.insert(
            ERC1155,
            Event {
                name: "TransferSingle".to_string(),
                inputs: vec![
                    EventParam {
                        name: "operator".to_string(),
                        kind: Address,
                        indexed: true,
                    },
                    EventParam {
                        name: "from".to_string(),
                        kind: Address,
                        indexed: true,
                    },
                    EventParam {
                        name: "to".to_string(),
                        kind: Address,
                        indexed: true,
                    },
                    EventParam {
                        name: "id".to_string(),
                        kind: Uint(256),
                        indexed: false,
                    },
                    EventParam {
                        name: "value".to_string(),
                        kind: Uint(256),
                        indexed: false,
                    },
                ],
                anonymous: false,
            },
        );

        map.insert(
            ERC20,
            Event {
                name: "Transfer".to_string(),
                inputs: vec![
                    EventParam {
                        name: "_from".to_string(),
                        kind: Address,
                        indexed: true,
                    },
                    EventParam {
                        name: "_to".to_string(),
                        kind: Address,
                        indexed: true,
                    },
                    EventParam {
                        name: "_value".to_string(),
                        kind: Uint(256),
                        indexed: false,
                    },
                ],
                anonymous: false,
            },
        );

        map.insert(
            LegacyErc721Sale,
            Event {
                name: "AuctionSuccessful".to_string(),
                inputs: vec![
                    EventParam {
                        name: "_seller".to_string(),
                        kind: Address,
                        indexed: false,
                    },
                    EventParam {
                        name: "_buyer".to_string(),
                        kind: Address,
                        indexed: false,
                    },
                    EventParam {
                        name: "_listingIndex".to_string(),
                        kind: Uint(256),
                        indexed: false,
                    },
                    EventParam {
                        name: "_token".to_string(),
                        kind: Address,
                        indexed: false,
                    },
                    EventParam {
                        name: "_totalPrice".to_string(),
                        kind: Uint(256),
                        indexed: false,
                    },
                ],
                anonymous: false,
            },
        );

        map.insert(
            ERC721,
            Event {
                name: "Transfer".to_string(),
                inputs: vec![
                    EventParam {
                        name: "_from".to_string(),
                        kind: Address,
                        indexed: true,
                    },
                    EventParam {
                        name: "_to".to_string(),
                        kind: Address,
                        indexed: true,
                    },
                    EventParam {
                        name: "_tokenId".to_string(),
                        kind: Uint(256),
                        indexed: true,
                    },
                ],
                anonymous: false,
            },
        );

        map.insert(
            MarketplaceV2,
            Event {
                name: "OrderMatched".to_string(),
                inputs: vec![
                    EventParam {
                        name: "hash".to_string(),
                        kind: FixedBytes(32),
                        indexed: false,
                    },
                    EventParam {
                        name: "maker".to_string(),
                        kind: Address,
                        indexed: false,
                    },
                    EventParam {
                        name: "matcher".to_string(),
                        kind: Address,
                        indexed: false,
                    },
                    EventParam {
                        name: "kind".to_string(),
                        kind: Uint(8),
                        indexed: false,
                    },
                    EventParam {
                        name: "bidToken".to_string(),
                        kind: Address,
                        indexed: false,
                    },
                    EventParam {
                        name: "bidPrice".to_string(),
                        kind: Uint(256),
                        indexed: false,
                    },
                    EventParam {
                        name: "paymentToken".to_string(),
                        kind: Address,
                        indexed: false,
                    },
                    EventParam {
                        name: "settlePrice".to_string(),
                        kind: Uint(256),
                        indexed: false,
                    },
                    EventParam {
                        name: "sellerReceived".to_string(),
                        kind: Uint(256),
                        indexed: false,
                    },
                    EventParam {
                        name: "marketFeePercentage".to_string(),
                        kind: Uint(256),
                        indexed: false,
                    },
                    EventParam {
                        name: "marketFeeTaken".to_string(),
                        kind: Uint(256),
                        indexed: false,
                    },
                ],
                anonymous: false,
            },
        );

        map
    }

    pub fn contract_list() -> ContractList {
        let mut map: ContractList = ContractList::new();

        map.insert(
            "0x814a9c959a3ef6ca44b5e2349e3bba9845393947",
            Contract {
                name: "CHARM",
                decimals: 0,
                erc: ERC1155,
                address: "0x814a9c959a3ef6ca44b5e2349e3bba9845393947",
            },
        );

        map.insert(
            "0xc25970724f032af21d801978c73653c440cf787c",
            Contract {
                name: "RUNE",
                decimals: 0,
                erc: ERC1155,
                address: "0xc25970724f032af21d801978c73653c440cf787c",
            },
        );

        map.insert(
            "0xc99a6a985ed2cac1ef41640596c5a5f9f4e19ef5",
            Contract {
                name: "WETH",
                decimals: 18,
                erc: ERC20,
                address: "0xc99a6a985ed2cac1ef41640596c5a5f9f4e19ef5",
            },
        );

        map.insert(
            "0x97a9107c1793bc407d6f527b77e7fff4d812bece",
            Contract {
                name: "AXS",
                decimals: 18,
                erc: ERC20,
                address: "0x97a9107c1793bc407d6f527b77e7fff4d812bece",
            },
        );

        map.insert(
            "0xa8754b9fa15fc18bb59458815510e40a12cd2014",
            Contract {
                name: "SLP",
                decimals: 0,
                erc: ERC20,
                address: "0xa8754b9fa15fc18bb59458815510e40a12cd2014",
            },
        );

        map.insert(
            "0x173a2d4fa585a63acd02c107d57f932be0a71bcc",
            Contract {
                name: "AEC",
                decimals: 0,
                erc: ERC20,
                address: "0x173a2d4fa585a63acd02c107d57f932be0a71bcc",
            },
        );

        map.insert(
            "0x0b7007c13325c48911f73a2dad5fa5dcbf808adc",
            Contract {
                name: "USDC",
                decimals: 18,
                erc: ERC20,
                address: "0x0b7007c13325c48911f73a2dad5fa5dcbf808adc",
            },
        );

        map.insert(
            "0xe514d9deb7966c8be0ca922de8a064264ea6bcd4",
            Contract {
                name: "WRON",
                decimals: 18,
                erc: ERC20,
                address: "0xe514d9deb7966c8be0ca922de8a064264ea6bcd4",
            },
        );

        map.insert(
            "0xc6344bc1604fcab1a5aad712d766796e2b7a70b9",
            Contract {
                name: "AXS-WETH-LP",
                decimals: 18,
                erc: ERC20,
                address: "0xc6344bc1604fcab1a5aad712d766796e2b7a70b9",
            },
        );

        map.insert(
            "0x306a28279d04a47468ed83d55088d0dcd1369294",
            Contract {
                name: "SLP-WETH-LP",
                decimals: 18,
                erc: ERC20,
                address: "0x306a28279d04a47468ed83d55088d0dcd1369294",
            },
        );

        map.insert(
            "0x2ecb08f87f075b5769fe543d0e52e40140575ea7",
            Contract {
                name: "RON-WETH-LP",
                decimals: 18,
                erc: ERC20,
                address: "0x2ecb08f87f075b5769fe543d0e52e40140575ea7",
            },
        );

        map.insert(
            "0xa7964991f339668107e2b6a6f6b8e8b74aa9d017",
            Contract {
                name: "USDC-WETH-LP",
                decimals: 18,
                erc: ERC20,
                address: "0xa7964991f339668107e2b6a6f6b8e8b74aa9d017",
            },
        );

        map.insert(
            "0x32950db2a7164ae833121501c797d79e7b79d74c",
            Contract {
                name: "AXIE",
                decimals: 0,
                erc: ERC721,
                address: "0x32950db2a7164ae833121501c797d79e7b79d74c",
            },
        );

        map.insert(
            "0x8c811e3c958e190f5ec15fb376533a3398620500",
            Contract {
                name: "LAND",
                decimals: 0,
                erc: ERC721,
                address: "0x8c811e3c958e190f5ec15fb376533a3398620500",
            },
        );

        map.insert(
            "0xa96660f0e4a3e9bc7388925d245a6d4d79e21259",
            Contract {
                name: "ITEM",
                decimals: 0,
                erc: ERC721,
                address: "0xa96660f0e4a3e9bc7388925d245a6d4d79e21259",
            },
        );

        map
    }

    pub fn to_string<T: serde::Serialize>(&self, request: &T) -> String {
        web3::helpers::to_string(request).replace('\"', "")
    }

    pub fn prefix<T: FromStr + Display>(&self, request: &T, prefix: AddressPrefix) -> String {
        match prefix {
            // AddressPrefix::Ronin => {
            //     f!("ronin:{request}")
            // }
            AddressPrefix::Ethereum => {
                f!("0x{request}")
            }
        }
    }

    pub async fn new(hostname: &str, database: Database) -> Ronin {
        let parsed = Url::parse(hostname)
            .unwrap_or_else(|_| panic!("Failed to parse web3 hostname: {}", &hostname));
        let provider = match parsed.scheme() {
            "ws" => {
                let provider = WebSocket::new(hostname)
                    .await
                    .expect("Failed to connect to websocket provider!");
                Either::Left(provider)
            }
            "http" => {
                Either::Right(Http::new(hostname).expect("Failed to connect to http provider!"))
            }
            "https" => {
                warn!("Consider using http as protocol for better performance!");
                Either::Right(Http::new(hostname).expect("Failed to connect to http provider!"))
            }
            _ => panic!("Invalid provider type"),
        };

        Ronin {
            provider: Web3::new(provider),
            database,
        }
    }

    async fn legacy_erc_sale(&self, tx: &TransactionReceipt) -> Option<Sale> {
        if !tx.logs.is_empty() {
            let contracts: Vec<&str> = Ronin::contract_list()
                .values()
                .filter(|c| c.erc == ERC721)
                .map(|c| c.address)
                .collect();

            let sale_log = tx
                .logs
                .iter()
                .filter(|x| {
                    match x
                        .topics
                        .iter()
                        .find(|x| self.to_string(x) == MARKETPLACE_AXIE_SALE_TOPIC)
                    {
                        None => false,
                        Some(_) => true,
                    }
                })
                .collect::<Vec<&Log>>();

            match sale_log.is_empty() {
                true => None,
                false => {
                    let transfer_log = tx
                        .logs
                        .iter()
                        .filter(|x| {
                            self.to_string(&x.topics[0]) == ERC_TRANSFER_TOPIC
                                && self.to_string(&x.address)
                                    != "0xc99a6a985ed2cac1ef41640596c5a5f9f4e19ef5"
                                && contracts.contains(&self.to_string(&x.address).as_str())
                        })
                        .collect::<Vec<&Log>>();

                    if !transfer_log.is_empty() {
                        let parsed_sale = Ronin::transfer_events()
                            .get(&LegacyErc721Sale)
                            .unwrap()
                            .parse_log(RawLog {
                                topics: sale_log[0].to_owned().topics,
                                data: sale_log[0].to_owned().data.0,
                            })
                            .unwrap();

                        let parsed_transfer = Ronin::transfer_events()
                            .get(&ERC721)
                            .unwrap()
                            .parse_log(RawLog {
                                topics: transfer_log[0].to_owned().topics,
                                data: transfer_log[0].to_owned().data.0,
                            })
                            .unwrap();

                        let block_data = self
                            .provider
                            .eth()
                            .block(BlockId::Number(BlockNumber::from(tx.block_number.unwrap())))
                            .await
                            .unwrap()
                            .unwrap();

                        Some(Sale {
                            seller: self.prefix(
                                &self.to_string(&parsed_sale.params[0].value.to_string()),
                                AddressPrefix::Ethereum,
                            ),
                            buyer: self.prefix(
                                &self.to_string(&parsed_sale.params[1].value.to_string()),
                                AddressPrefix::Ethereum,
                            ),
                            price: self.to_string(&parsed_sale.params[4].value.to_string()),
                            seller_received: self
                                .to_string(&parsed_sale.params[4].value.to_string()),
                            token: self.to_string(&transfer_log[0].address),
                            token_id: self.to_string(&parsed_transfer.params[2].value.to_string()),
                            transaction_id: self.to_string(&tx.transaction_hash),
                            created_at: DateTime::from_millis(
                                block_data.timestamp.as_u64() as i64 * 1000,
                            ),
                            block: tx.block_number.unwrap().as_u64(),
                        })
                    } else {
                        None
                    }
                }
            }
        } else {
            None
        }
    }

    fn has_order_matched(&self, logs: &Vec<Log>) -> Option<Log> {
        let log = logs
            .iter()
            .filter(|l| l.topics[0] == MARKETPLACE_V2_ORDER_MATCHED_TOPIC.parse().unwrap())
            .collect::<Vec<&Log>>();
        let log = log.first();

        match log {
            None => None,
            Some(l) => Some(l.clone().to_owned()),
        }
    }

    pub async fn order_matched(&self, tx: &TransactionReceipt) -> Option<Sale> {
        if !tx.logs.is_empty() {
            if let Some(matched_order) = self.has_order_matched(&tx.logs) {
                let contracts: Vec<&str> = Ronin::contract_list()
                    .values()
                    .filter(|c| c.erc == ERC721)
                    .map(|c| c.address)
                    .collect();

                let rl = RawLog {
                    topics: matched_order.topics,
                    data: matched_order.data.0,
                };
                let parsed_sale_data = Ronin::transfer_events()
                    .get(&MarketplaceV2)
                    .unwrap()
                    .parse_log(rl)
                    .unwrap();

                let erc_transfer_log_opt = tx
                    .logs
                    .iter()
                    .find(|c| contracts.contains(&self.to_string(&c.address).as_str()))
                    .map(|log| log.to_owned());

                if erc_transfer_log_opt != None {
                    let erc_transfer_log = erc_transfer_log_opt.unwrap();
                    let erc_transfer = Ronin::transfer_events()
                        .get(&ERC721)
                        .unwrap()
                        .parse_log(RawLog {
                            topics: erc_transfer_log.topics,
                            data: erc_transfer_log.data.0,
                        })
                        .unwrap();

                    let block_data = self
                        .provider
                        .eth()
                        .block(BlockId::Number(BlockNumber::from(tx.block_number.unwrap())))
                        .await
                        .unwrap()
                        .unwrap();
                    Some(Sale {
                        seller: self.prefix(
                            &self.to_string(&parsed_sale_data.params[1].value.to_string()),
                            AddressPrefix::Ethereum,
                        ),
                        buyer: self.prefix(
                            &self.to_string(&parsed_sale_data.params[2].value.to_string()),
                            AddressPrefix::Ethereum,
                        ),
                        price: self.to_string(&parsed_sale_data.params[7].value.to_string()),
                        seller_received: self
                            .to_string(&parsed_sale_data.params[8].value.to_string()),
                        token: self.to_string(&erc_transfer_log.address),
                        token_id: self.to_string(&erc_transfer.params[2].value.to_string()),
                        transaction_id: self.to_string(&tx.transaction_hash),
                        created_at: DateTime::from_millis(
                            block_data.timestamp.as_u64() as i64 * 1000,
                        ),
                        block: tx.block_number.unwrap().as_u64(),
                    })
                } else {
                    return None;
                }
            } else {
                return None;
            }
        } else {
            return None;
        }
    }

    pub async fn stream(&self, args: Args, start: Block, stop: Block) {
        if args.debug {
            debug!("W A R N I N G");
            debug!("DEBUG MODE ENABLED! NOT SAVING ANYTHING TO DATABASE!");
            debug!("Start Block: {}", args.start_block);
            debug!("Stop Block: {}", args.stop_block);
            thread::sleep(Duration::new(1, 0));
        }

        if args.replay {
            if args.debug {
                debug!("Can't replay while in debug mode!!!");
            }
            debug!("W A R N I N G");
            debug!("About to drop ANY data stored in the database for this app!");
            debug!("Waiting 15 seconds...");
            thread::sleep(Duration::new(15, 0));
            self.database
                .settings
                .collection
                .drop(None)
                .await
                .expect("Failed to drop settings collection");
            self.database
                .wallets
                .collection
                .drop(None)
                .await
                .expect("Failed to drop wallets collection");
            self.database
                .transactions
                .collection
                .drop(None)
                .await
                .expect("Failed to drop transactions collection");
            self.database
                .erc_transfers
                .collection
                .drop(None)
                .await
                .expect("Failed to drop erc_transfers collection");
            self.database
                .erc1155_transfers
                .collection
                .drop(None)
                .await
                .expect("Failed to drop erc1155_transfers collection");

            self.database.create_indexes().await;
        }

        let contracts = Ronin::contract_list();
        let transfer_events = Ronin::transfer_events();

        let stream_stop_block: Block = stop;

        let mut largest_block_by_tx_num: LargestBlock =
            match self.database.settings.get("largest_block_by_tx_num").await {
                None => LargestBlock {
                    number: 0,
                    tx_num: 0,
                },
                Some(settings) => serde_json::from_str(settings.value.as_str()).unwrap(),
            };

        if start > stream_stop_block {
            info!("[INFO] Offset not large enough. Exiting!");
            return;
        }

        info!("[INFO] Streaming from {} to {}", &start, &stream_stop_block);

        let mut current_block: Block = start.to_owned();
        let mut wallet_pool: Pool<Wallet> = self.database.wallets.get_pool();

        loop {
            let block = self
                .provider
                .eth()
                .block_with_txs(BlockId::Number(BlockNumber::from(current_block as u64)))
                .await
                .unwrap_or_else(|_| panic!("Failed to load block {} from provider!", current_block))
                .unwrap_or_else(|| panic!("Failed to unwrap block {} from result!", current_block));

            let block_number: u64 = block.number.unwrap().as_u64();
            let timestamp = block.timestamp.as_u64() * 1000;
            let timestamp = DateTime::from_millis(i64::try_from(timestamp).unwrap());
            let num_txs = block.transactions.len();

            if num_txs > 0 {
                if !args.debug && num_txs as u64 > largest_block_by_tx_num.tx_num {
                    largest_block_by_tx_num = LargestBlock {
                        number: block_number,
                        tx_num: num_txs as u64,
                    };
                    self.database
                        .settings
                        .set(
                            "largest_block_by_tx_num",
                            &serde_json::to_string(&largest_block_by_tx_num).unwrap(),
                        )
                        .await
                        .expect("Failed to store largest_block_by_tx_num!");
                }

                let mut tx_pool: Vec<Transaction> = vec![];
                let mut erc_pool: Pool<ERCTransfer> = self.database.erc_transfers.get_pool();
                let mut erc1155_pool: Pool<ERC1155Transfer> =
                    self.database.erc1155_transfers.get_pool();
                let mut erc_sale_pool: Pool<Sale> = self.database.erc_sales.get_pool();

                for tx in block.transactions {
                    let tx_from = self.to_string(&tx.from);
                    let tx_to = self.to_string(&tx.to);
                    let tx_hash = self.to_string(&tx.hash);

                    if args.feature_wallet_updates {
                        wallet_pool.update(self.database.wallets.update(
                            &tx_from,
                            block_number,
                            &tx_hash,
                        ));
                        wallet_pool.update(self.database.wallets.update(
                            &tx_to,
                            block_number,
                            &tx_hash,
                        ));
                    }

                    if args.debug && !args.debug_disable_wallet_updates {
                        debug!(
                            "[WALLET UPDATE] Address: {}\tBlock: {:>14}\tTransaction: {}",
                            &tx_from, &block_number, &tx_hash
                        );
                        debug!(
                            "[WALLET UPDATE] Address: {}\tBlock: {:>14}\tTransaction: {}",
                            &tx_to, &block_number, &tx_hash
                        );
                    }

                    let receipt: TransactionReceipt = self
                        .provider
                        .eth()
                        .transaction_receipt(tx.hash)
                        .await
                        .expect("Failed to retrieve transaction receipt!")
                        .expect("Failed to unwrap transaction receipt!");

                    if args.feature_erc_721_sales {
                        if current_block > MARKETPLACE_V2_DEPLOY_BLOCK {
                            match self.order_matched(&receipt).await {
                                None => {}
                                Some(sale) => {
                                    if args.debug {
                                        debug!("[MARKETPLACE V2 SALE] {:#?}", sale);
                                    }
                                    erc_sale_pool.insert(sale);
                                }
                            }
                        } else {
                            match self.legacy_erc_sale(&receipt).await {
                                None => {}
                                Some(sale) => {
                                    if args.debug {
                                        debug!("[MARKETPLACE SALE] {:#?}", sale);
                                    }
                                    erc_sale_pool.insert(sale);
                                }
                            }
                        }
                    }

                    if !receipt.logs.is_empty() {
                        for log in receipt.logs {
                            if args.feature_erc_transfers {
                                if current_block > ERC1155_DEPLOY_BLOCK {
                                    match &log.topics.clone().into_iter().find(|t| {
                                        self.to_string(t).as_str() == ERC1155_TRANSFER_SINGLE_TOPIC
                                    }) {
                                        None => {}
                                        Some(_) => {
                                            let raw_log = RawLog {
                                                topics: log.topics.clone(),
                                                data: log.data.0.clone(),
                                            };

                                            let contract_address = self.to_string(&log.address);
                                            match contracts.get(&contract_address.as_str()) {
                                                None => continue,
                                                Some(_) => {
                                                    let event_data = transfer_events
                                                        .get(&ERC1155)
                                                        .unwrap()
                                                        .to_owned()
                                                        .parse_log(raw_log)
                                                        .expect(
                                                            "Failed to parsed transaction log!",
                                                        );

                                                    let operator = self.to_string(
                                                        &event_data.params[0].value.to_string(),
                                                    );
                                                    let operator = f!("0x{operator}");

                                                    let from = self.to_string(
                                                        &event_data.params[1].value.to_string(),
                                                    );
                                                    let from = f!("0x{from}");

                                                    let to = self.to_string(
                                                        &event_data.params[2].value.to_string(),
                                                    );
                                                    let to = f!("0x{to}");

                                                    let token_id = self.to_string(
                                                        &event_data.params[3].value.to_string(),
                                                    );

                                                    let value = self.to_string(
                                                        &event_data.params[4].value.to_string(),
                                                    );

                                                    let signature =
                                                        ERC1155Transfer::get_transfer_id(
                                                            &self.to_string(&log.transaction_hash),
                                                            &self.to_string(&log.log_index),
                                                        );

                                                    let transfer = ERC1155Transfer {
                                                        token: contract_address,
                                                        operator,
                                                        from,
                                                        to,
                                                        token_id,
                                                        value,
                                                        block: block_number,
                                                        transaction_id: self
                                                            .to_string(&log.transaction_hash),
                                                        log_index: self.to_string(&log.log_index),
                                                        log_id: signature,
                                                    };
                                                    if args.debug {
                                                        debug!(
                                                            "[ERC1155 Transfer] {:#?}",
                                                            transfer
                                                        );
                                                    }

                                                    erc1155_pool.insert(transfer)
                                                }
                                            }
                                        }
                                    }
                                }

                                match log
                                    .topics
                                    .clone()
                                    .into_iter()
                                    .find(|t| self.to_string(t).as_str() == ERC_TRANSFER_TOPIC)
                                {
                                    None => {}
                                    Some(_) => {
                                        let raw_log = RawLog {
                                            topics: log.topics.clone(),
                                            data: log.data.0,
                                        };

                                        let contract_address = self.to_string(&log.address);

                                        match contracts.get(&contract_address.as_str()) {
                                            None => continue,
                                            Some(contract) => {
                                                let event_data = transfer_events
                                                    .get(&contract.erc)
                                                    .unwrap()
                                                    .to_owned()
                                                    .parse_log(raw_log)
                                                    .expect("Failed to parsed transaction log!");

                                                let from = self.to_string(
                                                    &event_data.params[0].value.to_string(),
                                                );
                                                let from = f!("0x{from}");

                                                let to = self.to_string(
                                                    &event_data.params[1].value.to_string(),
                                                );
                                                let to = f!("0x{to}");

                                                let signature = ERCTransfer::get_transfer_id(
                                                    &self.to_string(&log.transaction_hash),
                                                    &self.to_string(&log.log_index),
                                                );

                                                let transfer = ERCTransfer {
                                                    from,
                                                    to,
                                                    token: contract_address.to_owned(),
                                                    value_or_token_id: self.to_string(
                                                        &event_data.params[2].value.to_string(),
                                                    ),
                                                    block: block_number,
                                                    transaction_id: self
                                                        .to_string(&log.transaction_hash),
                                                    erc: contract.erc.to_owned(),
                                                    log_index: self.to_string(&log.log_index),
                                                    log_id: signature,
                                                };

                                                if args.debug {
                                                    debug!("[ERC Transfer] {:#?}", transfer);
                                                }

                                                erc_pool.insert(transfer);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if args.feature_transactions == true {
                        let from = f!("0x{tx_from}");
                        let to = f!("0x{tx_to}");

                        tx_pool.push(Transaction {
                            from,
                            to,
                            hash: self.to_string(&tx.hash),
                            block: current_block,
                            timestamp,
                        });
                    }
                }

                if !args.debug && args.feature_transactions {
                    self.database
                        .transactions
                        .collection
                        .insert_many(&tx_pool, None)
                        .await
                        .ok();
                }

                let erc_insert_num = erc_pool.len();
                let erc1155_insert_num = erc1155_pool.len();
                let erc_sale_num = erc_sale_pool.len();
                let wallet_update_num = wallet_pool.len();

                if !args.debug {
                    if args.feature_erc_transfers {
                        erc_pool
                            .commit(true)
                            .await
                            .expect("Failed to insert erc transfers");

                        erc1155_pool
                            .commit(true)
                            .await
                            .expect("Failed to insert erc 1155 transfers");
                    }

                    if args.feature_erc_721_sales {
                        erc_sale_pool
                            .commit(true)
                            .await
                            .expect("Failed to insert erc sales");
                    }

                    if args.feature_wallet_updates {
                        wallet_pool
                            .commit(true)
                            .await
                            .expect("Failed to update wallets");
                    }

                    if log_enabled!(Info) {
                        info!(
                        "Block: {:>12}\t\tTransactions: {:>4}\tERC Transfers: {:>5}\tERC 1155 Transfers: {:>5}\tWallet Updates: {:>5}\tERC721 Sales: {:>5}",
                        &current_block.separate_with_commas(),
                        num_txs.separate_with_commas(),
                        erc_insert_num.separate_with_commas(),
                        erc1155_insert_num.separate_with_commas(),
                        wallet_update_num.separate_with_commas(),
                        erc_sale_num.separate_with_commas()
                    );
                    } else {
                        if current_block.rem_euclid(100) == 0 {
                            println!(
                                    "Block: {:>12}\t\tTransactions: {:>4}\tERC Transfers: {:>5}\tERC 1155 Transfers: {:>5}\tWallet Updates: {:>5}\tERC721 Sales: {:>5}",
                                    &current_block.separate_with_commas(),
                                    num_txs.separate_with_commas(),
                                    erc_insert_num.separate_with_commas(),
                                    erc1155_insert_num.separate_with_commas(),
                                    wallet_update_num.separate_with_commas(),
                                    erc_sale_num.separate_with_commas()
                                );
                        }
                    }
                }
            } else if args.empty_logs && !args.debug {
                if log_enabled!(Info) {
                    info!(
                    "Block: {:>12}\t\tTransactions: {:>4}\tERC Transfers: {:>5}\tERC 1155 Transfers: {:>5}\tWallet Updates: {:>5}\tERC721 Sales: {:>5}",
                    &current_block,
                    0,
                    0,
                    0,
                    0,
                    0
                );
                } else {
                    println!(
                    "Block: {:>12}\t\tTransactions: {:>4}\tERC Transfers: {:>5}\tERC 1155 Transfers: {:>5}\tWallet Updates: {:>5}\tERC721 Sales: {:>5}",
                    &current_block,
                    0,
                    0,
                    0,
                    0,
                    0
                );
                }
            }

            if !args.debug {
                self.database
                    .settings
                    .set("last_block", current_block.to_string())
                    .await
                    .expect("Failed to store last_block!");
            }

            current_block += 1;

            if current_block > stream_stop_block {
                break;
            }
        }
    }
}
