use std::collections::HashMap;
use std::fmt::Display;
use std::ops::Add;
use std::str::FromStr;
use std::thread;
use std::time::Duration;

use mongodb::bson::{doc, DateTime};
use serde::{Deserialize, Serialize};
use url::Url;
use web3::ethabi::{Event, EventParam, ParamType, RawLog};
use web3::transports::{Either, Http, WebSocket};
use web3::types::{BlockId, BlockNumber, TransactionReceipt};
use web3::Web3;

use crate::mongo::collections::axie_sale::Sale;
use crate::mongo::collections::transaction::Transaction;
use crate::mongo::collections::transaction_pool::Pool;
use crate::mongo::collections::wallet::Wallet;
use crate::mongo::collections::{erc_transfer::ERCTransfer, Block};
use crate::mongo::Database;
use crate::ronin::ContractType::ERC721;

const ERC_TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

const MARKETPLACE_V2_ORDER_MATCHED_TOPIC: &str =
    "0xafa0d706792fa5d4e9aaf5e456e08e2a833b1e64a201710b782f29172f6d7a3a";

const MARKETPLACE_V2_DEPLOY_BLOCK: Block = 16027461;

const MARKETPLACE_AXIE_SALE_TOPIC: &str =
    "0c0258cd7f0d9474f62106c6981c027ea54bee0b323ea1991f4caa7e288a5725";

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
    Unknown,
    MarketplaceV2,
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
            ContractType::ERC20,
            Event {
                name: "Transfer".to_string(),
                inputs: vec![
                    EventParam {
                        name: "_from".to_string(),
                        kind: ParamType::Address,
                        indexed: true,
                    },
                    EventParam {
                        name: "_to".to_string(),
                        kind: ParamType::Address,
                        indexed: true,
                    },
                    EventParam {
                        name: "_value".to_string(),
                        kind: ParamType::Uint(256),
                        indexed: false,
                    },
                ],
                anonymous: false,
            },
        );

        map.insert(
            ContractType::ERC721,
            Event {
                name: "Transfer".to_string(),
                inputs: vec![
                    EventParam {
                        name: "_from".to_string(),
                        kind: ParamType::Address,
                        indexed: true,
                    },
                    EventParam {
                        name: "_to".to_string(),
                        kind: ParamType::Address,
                        indexed: true,
                    },
                    EventParam {
                        name: "_tokenId".to_string(),
                        kind: ParamType::Uint(256),
                        indexed: true,
                    },
                ],
                anonymous: false,
            },
        );

        map.insert(
            ContractType::MarketplaceV2,
            Event {
                name: "OrderMatched".to_string(),
                inputs: vec![
                    EventParam {
                        name: "hash".to_string(),
                        kind: ParamType::FixedBytes(32),
                        indexed: false,
                    },
                    EventParam {
                        name: "maker".to_string(),
                        kind: ParamType::Address,
                        indexed: false,
                    },
                    EventParam {
                        name: "matcher".to_string(),
                        kind: ParamType::Address,
                        indexed: false,
                    },
                    EventParam {
                        name: "kind".to_string(),
                        kind: ParamType::Uint(8),
                        indexed: false,
                    },
                    EventParam {
                        name: "bidToken".to_string(),
                        kind: ParamType::Address,
                        indexed: false,
                    },
                    EventParam {
                        name: "bidPrice".to_string(),
                        kind: ParamType::Uint(256),
                        indexed: false,
                    },
                    EventParam {
                        name: "paymentToken".to_string(),
                        kind: ParamType::Address,
                        indexed: false,
                    },
                    EventParam {
                        name: "settlePrice".to_string(),
                        kind: ParamType::Uint(256),
                        indexed: false,
                    },
                    EventParam {
                        name: "sellerReceived".to_string(),
                        kind: ParamType::Uint(256),
                        indexed: false,
                    },
                    EventParam {
                        name: "marketFeePercentage".to_string(),
                        kind: ParamType::Uint(256),
                        indexed: false,
                    },
                    EventParam {
                        name: "marketFeeTaken".to_string(),
                        kind: ParamType::Uint(256),
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
            "0xc99a6a985ed2cac1ef41640596c5a5f9f4e19ef5",
            Contract {
                name: "WETH",
                decimals: 18,
                erc: ContractType::ERC20,
                address: "0xc99a6a985ed2cac1ef41640596c5a5f9f4e19ef5",
            },
        );

        map.insert(
            "0xed4a9f48a62fb6fdcfb45bb00c9f61d1a436e58c",
            Contract {
                name: "AXS",
                decimals: 18,
                erc: ContractType::ERC20,
                address: "0xed4a9f48a62fb6fdcfb45bb00c9f61d1a436e58c",
            },
        );

        map.insert(
            "0xa8754b9fa15fc18bb59458815510e40a12cd2014",
            Contract {
                name: "SLP",
                decimals: 0,
                erc: ContractType::ERC20,
                address: "0xa8754b9fa15fc18bb59458815510e40a12cd2014",
            },
        );

        map.insert(
            "0x173a2d4fa585a63acd02c107d57f932be0a71bcc",
            Contract {
                name: "AEC",
                decimals: 0,
                erc: ContractType::ERC20,
                address: "0x173a2d4fa585a63acd02c107d57f932be0a71bcc",
            },
        );

        map.insert(
            "0x0b7007c13325c48911f73a2dad5fa5dcbf808adc",
            Contract {
                name: "USDC",
                decimals: 18,
                erc: ContractType::ERC20,
                address: "0x0b7007c13325c48911f73a2dad5fa5dcbf808adc",
            },
        );

        map.insert(
            "0xe514d9deb7966c8be0ca922de8a064264ea6bcd4",
            Contract {
                name: "WRON",
                decimals: 18,
                erc: ContractType::ERC20,
                address: "0xe514d9deb7966c8be0ca922de8a064264ea6bcd4",
            },
        );

        map.insert(
            "0x32950db2a7164ae833121501c797d79e7b79d74c",
            Contract {
                name: "AXIE",
                decimals: 0,
                erc: ContractType::ERC721,
                address: "0x32950db2a7164ae833121501c797d79e7b79d74c",
            },
        );

        map.insert(
            "0x8c811e3c958e190f5ec15fb376533a3398620500",
            Contract {
                name: "LAND",
                decimals: 0,
                erc: ContractType::ERC721,
                address: "0x8c811e3c958e190f5ec15fb376533a3398620500",
            },
        );

        map.insert(
            "0xa96660f0e4a3e9bc7388925d245a6d4d79e21259",
            Contract {
                name: "ITEM",
                decimals: 0,
                erc: ContractType::ERC721,
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
                println!("[WARN] Consider using http as protocol for better performance!");
                Either::Right(Http::new(hostname).expect("Failed to connect to http provider!"))
            }
            _ => panic!("Invalid provider type"),
        };

        Ronin {
            provider: Web3::new(provider),
            database,
        }
    }

    pub async fn order_matched(&self, tx: &TransactionReceipt) -> Option<Sale> {
        if !tx.logs.is_empty()
            && tx.logs[0].topics[0] == MARKETPLACE_V2_ORDER_MATCHED_TOPIC.parse().unwrap()
        {
            let contracts: Vec<&str> = Ronin::contract_list()
                .values()
                .filter(|c| c.erc == ERC721)
                .map(|c| c.address)
                .collect();

            let log = tx.logs[0].to_owned();

            let rl = RawLog {
                topics: log.topics,
                data: log.data.0,
            };

            let parsed_sale_data = Ronin::transfer_events()
                .get(&ContractType::MarketplaceV2)
                .unwrap()
                .parse_log(rl)
                .unwrap();

            let erc_transfer_log = tx
                .logs
                .iter()
                .find(|c| contracts.contains(&self.to_string(&c.address).as_str()))
                .unwrap()
                .to_owned();

            let erc_transfer = Ronin::transfer_events()
                .get(&ContractType::ERC721)
                .unwrap()
                .parse_log(RawLog {
                    topics: erc_transfer_log.topics,
                    data: erc_transfer_log.data.0,
                })
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
                seller_received: self.to_string(&parsed_sale_data.params[8].value.to_string()),
                token: self.to_string(&erc_transfer_log.address),
                token_id: self.to_string(&erc_transfer.params[2].value.to_string()),
                transaction_id: self.to_string(&tx.transaction_hash),
            })
        } else {
            None
        }
        //
        // for l in tx.logs.iter().map(|x| x.topics.iter()) {
        //     println!("{:?}", l);
        // }
        // let f = TransactionId::Hash(primary_event_hash.into());
        //
        // let e = TransactionId::from_str(primary_event_hash).unwrap();
        //
        // let tx = ronin.provider.eth().transaction_receipt(web3::helpers::).await;
        //
        // let hash = match log
        //     .topics
        //     .iter()
        //     .find(|t| web3::helpers::to_string(t).as_str() == primary_event_hash)
        // {
        //     None => return,
        //     Some(data) => data,
        // };
    }

    pub async fn stream(&self, offset: u64, replay: bool, empty_logs: bool) {
        if replay {
            println!("W A R N I N G");
            println!("About to drop ANY data stored in the database for this app!");
            println!("Waiting 15 seconds...");
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
            self.database.create_indexes().await;
        }

        let contracts = Ronin::contract_list();
        let transfer_events = Ronin::transfer_events();

        let chain_head_block = self
            .provider
            .eth()
            .block_number()
            .await
            .expect("Failed to retrieve head block number from chain!");

        let stream_stop_block: Block = chain_head_block.as_u64() - offset;

        let start = self.database.settings.get("last_block").await;
        let start: Block = match start {
            None => 1,
            Some(settings) => settings.value.parse::<u64>().unwrap(),
        };

        let mut largest_block_by_tx_num: LargestBlock =
            match self.database.settings.get("largest_block_by_tx_num").await {
                None => LargestBlock {
                    number: 0,
                    tx_num: 0,
                },
                Some(settings) => serde_json::from_str(settings.value.as_str()).unwrap(),
            };

        if start >= stream_stop_block {
            println!("[INFO] Offset not large enough. Exiting!");
            return;
        }

        println!("[INFO] Streaming from {} to {}", &start, &stream_stop_block);

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
                if num_txs as u64 > largest_block_by_tx_num.tx_num {
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
                let mut erc_sale_pool: Pool<Sale> = self.database.erc_sales.get_pool();

                for tx in block.transactions {
                    let tx_from = self.to_string(&tx.from);
                    let tx_to = self.to_string(&tx.to);
                    let tx_hash = self.to_string(&tx.hash);

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

                    let receipt: TransactionReceipt = self
                        .provider
                        .eth()
                        .transaction_receipt(tx.hash)
                        .await
                        .expect("Failed to retrieve transaction receipt!")
                        .expect("Failed to unwrap transaction receipt!");

                    if current_block > MARKETPLACE_V2_DEPLOY_BLOCK {
                        match self.order_matched(&receipt).await {
                            None => {}
                            Some(sale) => {
                                erc_sale_pool.insert(sale);
                            }
                        }
                    }

                    if !receipt.logs.is_empty() {
                        for log in receipt.logs {
                            match log
                                .topics
                                .clone()
                                .into_iter()
                                .find(|t| self.to_string(t).as_str() == ERC_TRANSFER_TOPIC)
                            {
                                None => {}
                                Some(_) => {
                                    let raw_log = RawLog {
                                        topics: log.topics,
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

                                            let from = self
                                                .to_string(&event_data.params[0].value.to_string());
                                            let from = f!("0x{from}");

                                            let to = self
                                                .to_string(&event_data.params[1].value.to_string());
                                            let to = f!("0x{to}");

                                            let signature = ERCTransfer::get_transfer_id(
                                                &self.to_string(&log.transaction_hash),
                                                &self.to_string(&log.log_index),
                                            );

                                            erc_pool.insert(ERCTransfer {
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
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }

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

                self.database
                    .transactions
                    .collection
                    .insert_many(&tx_pool, None)
                    .await
                    .ok();

                let erc_insert_num = erc_pool.len();
                let erc_sale_num = erc_sale_pool.len();
                let wallet_update_num = wallet_pool.len();

                erc_pool
                    .commit(true)
                    .await
                    .expect("Failed to insert erc transfers");

                erc_sale_pool
                    .commit(true)
                    .await
                    .expect("Failed to insert erc sales");

                wallet_pool
                    .commit(true)
                    .await
                    .expect("Failed to update wallets");

                println!(
                    "Block: {:>12}\t\tTransactions: {:>4}\tERC Transfers: {:>5}\tWallet Updates: {:>5}\tERC721 Sales: {:>5}",
                    &current_block,
                    num_txs,
                    erc_insert_num,
                    wallet_update_num,
                    erc_sale_num
                );
            } else if empty_logs {
                println!(
                    "Block: {:>12}\t\tTransactions: {:>4}\tERC Transfers: {:>5}\tWallet Updates: {:>5}\tERC721 Sales: {:>5}",
                    &current_block,
                    0,
                    0,
                    0,
                    0
                );
            }

            self.database
                .settings
                .set("last_block", current_block.to_string())
                .await
                .expect("Failed to store last_block!");

            current_block = current_block.add(1);

            if current_block >= stream_stop_block {
                break;
            }
        }
    }
}
