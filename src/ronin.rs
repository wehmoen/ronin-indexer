use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use mongodb::bson::{doc, DateTime};
use serde::{Deserialize, Serialize};
use url::Url;
use web3::ethabi::{Event, EventParam, ParamType, RawLog};
use web3::transports::{Either, Http, WebSocket};
use web3::types::{BlockId, BlockNumber};
use web3::Web3;

use crate::mongo::collections::transaction::Transaction;
use crate::mongo::collections::transaction_pool::Pool;
use crate::mongo::collections::wallet::Wallet;
use crate::mongo::collections::{erc_transfer::ERCTransfer, Block};
use crate::mongo::{Database, SessionBuilder};

const ERC_TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

//const AXIE_SALE_TOPIC: &str = "0c0258cd7f0d9474f62106c6981c027ea54bee0b323ea1991f4caa7e288a5725"; // Todo: Implement axie sale indexing

pub struct Ronin {
    database: Database,
    provider: Web3<Either<WebSocket, Http>>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Debug, Clone)]
pub enum ContractType {
    ERC20,
    ERC721,
    Unknown,
}

#[derive(Serialize, Deserialize)]
pub struct Contract {
    pub name: &'static str,
    pub decimals: usize,
    pub erc: ContractType,
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
            },
        );

        map.insert(
            "0xed4a9f48a62fb6fdcfb45bb00c9f61d1a436e58c",
            Contract {
                name: "AXS",
                decimals: 18,
                erc: ContractType::ERC20,
            },
        );

        map.insert(
            "0xa8754b9fa15fc18bb59458815510e40a12cd2014",
            Contract {
                name: "SLP",
                decimals: 0,
                erc: ContractType::ERC20,
            },
        );

        map.insert(
            "0x173a2d4fa585a63acd02c107d57f932be0a71bcc",
            Contract {
                name: "AEC",
                decimals: 0,
                erc: ContractType::ERC20,
            },
        );

        map.insert(
            "0x0b7007c13325c48911f73a2dad5fa5dcbf808adc",
            Contract {
                name: "USDC",
                decimals: 18,
                erc: ContractType::ERC20,
            },
        );

        map.insert(
            "0xe514d9deb7966c8be0ca922de8a064264ea6bcd4",
            Contract {
                name: "WRON",
                decimals: 18,
                erc: ContractType::ERC20,
            },
        );

        map.insert(
            "0x32950db2a7164ae833121501c797d79e7b79d74c",
            Contract {
                name: "AXIE",
                decimals: 0,
                erc: ContractType::ERC721,
            },
        );

        map.insert(
            "0x8c811e3c958e190f5ec15fb376533a3398620500",
            Contract {
                name: "LAND",
                decimals: 0,
                erc: ContractType::ERC721,
            },
        );

        map.insert(
            "0xa96660f0e4a3e9bc7388925d245a6d4d79e21259",
            Contract {
                name: "ITEM",
                decimals: 0,
                erc: ContractType::ERC721,
            },
        );

        map
    }

    pub async fn new(hostname: String, database: Database) -> Ronin {
        let parsed = Url::parse(&hostname)
            .expect(format!("Failed to parse web3 hostname: {}", &hostname).as_str());
        let provider = match parsed.scheme() {
            "ws" => {
                let provider = WebSocket::new(&hostname)
                    .await
                    .expect("Failed to connect to websocket provider!");
                Either::Left(provider)
            }
            "http" => {
                Either::Right(Http::new(&hostname).expect("Failed to connect to http provider!"))
            }
            "https" => {
                println!("[WARN] Consider using http as protocol for better performance!");
                Either::Right(Http::new(&hostname).expect("Failed to connect to http provider!"))
            }
            _ => panic!("Invalid provider type"),
        };

        Ronin {
            provider: Web3::new(provider),
            database,
        }
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

        let mut current_block: Block = start.clone();
        let mut wallet_pool: Pool<Wallet> = self.database.wallets.get_pool();

        loop {
            let block = self
                .provider
                .eth()
                .block_with_txs(BlockId::Number(BlockNumber::from(current_block as u64)))
                .await
                .expect(format!("Failed to load block {} from provider!", current_block).as_str())
                .expect(format!("Failed to unwrap block {} from result!", current_block).as_str());

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
                            serde_json::to_string(&largest_block_by_tx_num).unwrap(),
                        )
                        .await
                        .expect("Failed to store largest_block_by_tx_num!");
                }

                let mut tx_pool: Vec<Transaction> = vec![];
                let mut erc_transfer_pool: Vec<ERCTransfer> = vec![];

                for tx in block.transactions {
                    let tx_from = web3::helpers::to_string(&tx.from).replace("\"", "");
                    let tx_to = web3::helpers::to_string(&tx.to).replace("\"", "");
                    let tx_hash = web3::helpers::to_string(&tx.hash).replace("\"", "");

                    wallet_pool.update(self.database.wallets.update(
                        tx_from,
                        block_number,
                        tx_hash.clone(),
                    ));

                    wallet_pool.update(self.database.wallets.update(
                        tx_to,
                        block_number,
                        tx_hash.clone(),
                    ));

                    let receipt: web3::types::TransactionReceipt = self
                        .provider
                        .eth()
                        .transaction_receipt(tx.hash.clone())
                        .await
                        .expect("Failed to retrieve transaction receipt!")
                        .expect("Failed to unwrap transaction receipt!");

                    if receipt.logs.len() > 0 {
                        for log in receipt.logs {
                            match log.topics.clone().into_iter().find(|t| {
                                web3::helpers::to_string(t).replace("\"", "").as_str()
                                    == ERC_TRANSFER_TOPIC
                            }) {
                                None => {}
                                Some(_) => {
                                    let raw_log = RawLog {
                                        topics: log.topics,
                                        data: log.data.0,
                                    };

                                    let contract_address =
                                        web3::helpers::to_string(&log.address).replace("\"", "");

                                    match contracts.get(&contract_address.as_str()) {
                                        None => continue,
                                        Some(contract) => match contract.erc {
                                            ContractType::ERC20 => {
                                                let event_data = transfer_events
                                                    .get(&ContractType::ERC20)
                                                    .unwrap()
                                                    .clone()
                                                    .parse_log(raw_log)
                                                    .expect("Failed to parsed transaction log!");

                                                let from = web3::helpers::to_string(
                                                    &event_data.params[0].value.to_string(),
                                                )
                                                .replace("\"", "");
                                                let from = f!("0x{from}");

                                                let to = web3::helpers::to_string(
                                                    &event_data.params[1].value.to_string(),
                                                )
                                                .replace("\"", "");
                                                let to = f!("0x{to}");

                                                let signature = ERCTransfer::get_transfer_id(
                                                    web3::helpers::to_string(&log.transaction_hash)
                                                        .replace("\"", ""),
                                                    web3::helpers::to_string(&log.log_index)
                                                        .replace("\"", ""),
                                                );

                                                match erc_transfer_pool
                                                    .clone()
                                                    .into_iter()
                                                    .find(|t| t.log_id == signature)
                                                {
                                                    None => erc_transfer_pool.push(ERCTransfer {
                                                        from: from.clone(),
                                                        to: to.clone(),
                                                        token: contract_address,
                                                        value_or_token_id:
                                                            web3::helpers::to_string(
                                                                &event_data.params[2]
                                                                    .value
                                                                    .to_string(),
                                                            )
                                                            .replace("\"", ""),
                                                        block: block_number,
                                                        transaction_id: web3::helpers::to_string(
                                                            &log.transaction_hash,
                                                        )
                                                        .replace("\"", ""),
                                                        erc: ContractType::ERC20,
                                                        log_index: web3::helpers::to_string(
                                                            &log.log_index,
                                                        )
                                                        .replace("\"", ""),
                                                        log_id: signature,
                                                    }),
                                                    Some(_) => continue,
                                                }
                                            }
                                            ContractType::ERC721 => {
                                                let event_data = transfer_events
                                                    .get(&ContractType::ERC721)
                                                    .unwrap()
                                                    .clone()
                                                    .parse_log(raw_log)
                                                    .expect("Failed to parsed transaction log!");

                                                let from = web3::helpers::to_string(
                                                    &event_data.params[0].value.to_string(),
                                                )
                                                .replace("\"", "");
                                                let from = f!("0x{from}");

                                                let to = web3::helpers::to_string(
                                                    &event_data.params[1].value.to_string(),
                                                )
                                                .replace("\"", "");
                                                let to = f!("0x{to}");

                                                let signature = ERCTransfer::get_transfer_id(
                                                    web3::helpers::to_string(&log.transaction_hash)
                                                        .replace("\"", ""),
                                                    web3::helpers::to_string(&log.log_index)
                                                        .replace("\"", ""),
                                                );

                                                match erc_transfer_pool
                                                    .clone()
                                                    .into_iter()
                                                    .find(|t| t.log_id == signature)
                                                {
                                                    None => erc_transfer_pool.push(ERCTransfer {
                                                        from: from.clone(),
                                                        to: to.clone(),
                                                        token: contract_address.clone(),
                                                        value_or_token_id:
                                                            web3::helpers::to_string(
                                                                &event_data.params[2]
                                                                    .value
                                                                    .to_string(),
                                                            )
                                                            .replace("\"", ""),
                                                        block: block_number,
                                                        transaction_id: web3::helpers::to_string(
                                                            &log.transaction_hash,
                                                        )
                                                        .replace("\"", ""),
                                                        erc: ContractType::ERC721,
                                                        log_index: web3::helpers::to_string(
                                                            &log.log_index,
                                                        )
                                                        .replace("\"", ""),
                                                        log_id: signature,
                                                    }),
                                                    Some(_) => continue,
                                                }
                                            }
                                            ContractType::Unknown => continue,
                                        },
                                    }
                                }
                            }
                        }
                    }

                    let from = web3::helpers::to_string(&tx.from).replace("\"", "");
                    let from = f!("0x{from}");

                    let to = web3::helpers::to_string(&tx.to).replace("\"", "");
                    let to = f!("0x{to}");

                    tx_pool.push(Transaction {
                        from: from.clone(),
                        to: to.clone(),
                        hash: web3::helpers::to_string(&tx.hash).replace("\"", ""),
                        block: current_block,
                        timestamp: timestamp,
                    });
                }

                self.database
                    .transactions
                    .collection
                    .insert_many(&tx_pool, None)
                    .await
                    .ok();
                self.database
                    .erc_transfers
                    .collection
                    .insert_many(&erc_transfer_pool, None)
                    .await
                    .ok();

                let wallet_update_num = wallet_pool.len();
                let session = SessionBuilder::build(&self.database._client).await;

                wallet_pool
                    .commit(session, true)
                    .await
                    .expect("Failed to update wallets");

                println!(
                    "Block: {:>12}\t\tTransactions: {:>4}\tERC Transfers: {:>5}\tWallet Updates: {:>5}",
                    &current_block,
                    num_txs,
                    erc_transfer_pool.len(),
                    wallet_update_num
                );
            } else {
                if empty_logs {
                    println!(
                        "Block: {:>12}\t\tTransactions: {:>4}\tERC Transfers: {:>5}\tWallet Updates: {:>5}",
                        &current_block,
                        0,
                        0,
                        0
                    );
                }
            }

            self.database
                .settings
                .set("last_block", current_block.to_string())
                .await
                .expect("Failed to store last_block!");

            current_block = current_block + 1u64;

            if current_block >= stream_stop_block {
                break;
            }
        }
    }
}
