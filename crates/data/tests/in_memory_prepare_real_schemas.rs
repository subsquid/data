//! Losslessness checks for the production chunk builders.
//!
//! Every supported dataset kind is populated through its real JSON model and real table
//! builders, then prepared through both the spill and in-memory paths. The complete Arrow
//! schemas and record batches must be identical.

use std::{collections::BTreeMap, sync::Mutex};

use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use sqd_data::{
    bitcoin::{model as bitcoin, tables::BitcoinChunkBuilder},
    evm::{model as evm, tables::EvmChunkBuilder},
    hyperliquid_fills::{model as hyperliquid_fills, tables::HyperliquidFillsChunkBuilder},
    hyperliquid_replica_cmds::{model as hyperliquid_replica_cmds, tables::HyperliquidReplicaCmdsChunkBuilder},
    solana::{model as solana, tables::SolanaChunkBuilder},
    tron::{model as tron, tables::TronChunkBuilder}
};
use sqd_data_core::{BlockChunkBuilder, PreparedChunk};

static SPILL_LOCK: Mutex<()> = Mutex::new(());

fn parse<T: DeserializeOwned>(value: Value) -> T {
    serde_json::from_value(value).unwrap()
}

fn assert_lossless<B>(mut builder: B, block: B::Block, expected_rows: &[(&str, usize)])
where
    B: BlockChunkBuilder
{
    let _guard = SPILL_LOCK.lock().unwrap();
    raise_nofile_limit();
    builder.push(&block).unwrap();

    let mut processor = builder.new_chunk_processor().unwrap();
    builder.submit_to_processor(&mut processor).unwrap();
    let disk = processor.finish().unwrap();
    let mem = builder.prepare_in_memory().unwrap();

    assert_eq!(builder.max_num_rows(), 0, "in-memory prepare did not clear the builder");
    assert_chunks_equal(disk, mem, expected_rows);
}

fn raise_nofile_limit() {
    const TARGET: libc::rlim_t = 8192;

    // SAFETY: `rlimit` is valid when zero-initialized and the pointer remains writable for
    // the duration of `getrlimit`.
    let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
    // SAFETY: `limit` points to valid writable storage and RLIMIT_NOFILE is supported on
    // every Unix target on which this crate runs.
    assert_eq!(unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) }, 0);
    if limit.rlim_cur >= TARGET {
        return;
    }

    limit.rlim_cur = TARGET.min(limit.rlim_max);
    // SAFETY: the initialized soft limit does not exceed the hard limit and the pointer is
    // valid for the duration of the call.
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) }, 0);
    assert!(
        limit.rlim_cur >= 1024,
        "hard fd limit is too low for the EVM spill path"
    );
}

fn assert_chunks_equal(mut disk: PreparedChunk, mut mem: PreparedChunk, expected_rows: &[(&str, usize)]) {
    let expected: BTreeMap<_, _> = expected_rows.iter().copied().collect();
    assert_eq!(
        disk.len(),
        expected.len(),
        "spill path returned an unexpected table set"
    );
    assert_eq!(
        mem.len(),
        expected.len(),
        "memory path returned an unexpected table set"
    );

    for (name, rows) in expected {
        let disk_table = disk
            .get_mut(name)
            .unwrap_or_else(|| panic!("spill path lost table {name}"));
        let mem_table = mem
            .get_mut(name)
            .unwrap_or_else(|| panic!("memory path lost table {name}"));

        assert_eq!(disk_table.schema(), mem_table.schema(), "schema differs for {name}");
        assert_eq!(disk_table.num_rows(), rows, "spill row count differs for {name}");
        assert_eq!(mem_table.num_rows(), rows, "memory row count differs for {name}");

        let disk_batch = disk_table.read_record_batch(0, rows).unwrap();
        let mem_batch = mem_table.read_record_batch(0, rows).unwrap();
        assert_eq!(disk_batch, mem_batch, "record batch differs for {name}");
    }
}

fn hex(seed: u64) -> String {
    format!("0x{seed:064x}")
}

fn address(seed: u64) -> String {
    format!("0x{seed:040x}")
}

#[test]
fn evm_real_schema_is_lossless() {
    let block: evm::Block = parse(json!({
        "header": {
            "number": 20_000_000,
            "hash": hex(1),
            "parentHash": hex(0),
            "timestamp": 1_760_000_000,
            "transactionsRoot": hex(2),
            "receiptsRoot": hex(3),
            "stateRoot": hex(4),
            "logsBloom": "0x00",
            "sha3Uncles": hex(5),
            "extraData": "0x1234",
            "miner": address(1),
            "size": 1234,
            "gasLimit": "0x1c9c380",
            "gasUsed": "0x5208",
            "withdrawals": [{
                "address": address(2),
                "amount": "0x1",
                "index": "0x2",
                "validatorIndex": "0x3"
            }],
            "uncles": [hex(6)]
        },
        "transactions": [
            {
                "transactionIndex": 0,
                "hash": hex(10),
                "nonce": 7,
                "from": address(3),
                "to": address(4),
                "input": "0x11223344aabbccdd",
                "value": "0x5",
                "type": 3,
                "gas": "0x5208",
                "gasPrice": "0x3b9aca00",
                "maxFeePerGas": "0x4",
                "maxPriorityFeePerGas": "0x2",
                "accessList": [{
                    "address": address(5),
                    "storageKeys": [hex(11), hex(12)]
                }],
                "blobVersionedHashes": [hex(13)],
                "authorizationList": [{
                    "chainId": "1",
                    "address": address(6),
                    "nonce": "9",
                    "yParity": 1,
                    "r": hex(14),
                    "s": hex(15)
                }],
                "cumulativeGasUsed": "0x5208",
                "effectiveGasPrice": "0x3b9aca00",
                "gasUsed": "0x5208",
                "logsBloom": "0x00",
                "status": 1
            },
            {
                "transactionIndex": 1,
                "hash": hex(16),
                "nonce": 8,
                "from": address(10),
                "type": 118,
                "gas": "0x8000",
                "calls": [
                    {"to": address(11), "value": "0x1", "input": "0xabcdef01"},
                    {"value": "0x0", "input": "0x6000"}
                ],
                "nonceKey": "0x02",
                "feeToken": address(12),
                "feePayerSignature": {"v": 27, "r": hex(17), "s": hex(18)},
                "signature": {
                    "userAddress": address(13),
                    "version": "v1",
                    "signature": {
                        "type": "p256",
                        "r": hex(19),
                        "s": hex(20),
                        "pubKeyX": hex(21),
                        "pubKeyY": hex(22),
                        "preHash": true
                    }
                },
                "validBefore": "0x100",
                "validAfter": "0x10",
                "aaAuthorizationList": [{
                    "chainId": "0x1",
                    "address": address(14),
                    "nonce": 3,
                    "signature": {
                        "type": "secp256k1",
                        "r": hex(23),
                        "s": hex(24),
                        "yParity": 1
                    }
                }],
                "keyAuthorization": {
                    "chainId": "0x1",
                    "keyType": "webauthn",
                    "keyId": hex(25),
                    "expiry": "0x200",
                    "limits": [{"token": address(15), "limit": "0xff"}],
                    "signature": {
                        "type": "webAuthn",
                        "r": hex(26),
                        "s": hex(27),
                        "pubKeyX": hex(28),
                        "pubKeyY": hex(29),
                        "webauthnData": "0xaabbcc"
                    }
                },
                "cumulativeGasUsed": "0xd208",
                "gasUsed": "0x8000",
                "logsBloom": "0x00",
                "status": 1
            }
        ],
        "logs": [{
            "logIndex": 0,
            "transactionIndex": 0,
            "transactionHash": hex(10),
            "address": address(7),
            "data": "0xdeadbeef",
            "topics": [hex(20), hex(21)]
        }],
        "traces": [
            {
                "transactionIndex": 0,
                "traceAddress": [0],
                "subtraces": 0,
                "type": "call",
                "action": {
                    "from": address(3),
                    "to": address(4),
                    "value": "0x5",
                    "gas": "0x5208",
                    "input": "0x11223344",
                    "callType": "call"
                },
                "result": {"gasUsed": "0x5100", "output": "0xaabb"}
            },
            {
                "transactionIndex": 0,
                "traceAddress": [1],
                "subtraces": 0,
                "type": "create",
                "action": {
                    "from": address(3),
                    "value": "0x0",
                    "gas": "0x10000",
                    "init": "0x6000"
                },
                "result": {"gasUsed": "0x9000", "code": "0x6001", "address": address(8)}
            },
            {
                "transactionIndex": 0,
                "traceAddress": [2],
                "subtraces": 0,
                "type": "selfdestruct",
                "action": {
                    "address": address(16),
                    "refundAddress": address(17),
                    "balance": "0x42"
                }
            },
            {
                "transactionIndex": 0,
                "traceAddress": [3],
                "subtraces": 0,
                "type": "reward",
                "action": {
                    "author": address(18),
                    "value": "0x2a",
                    "rewardType": "block"
                }
            }
        ],
        "stateDiffs": [
            {
                "transactionIndex": 0,
                "address": address(9),
                "key": "balance",
                "kind": "+",
                "next": "0x10"
            },
            {
                "transactionIndex": 0,
                "address": address(9),
                "key": hex(22),
                "kind": "*",
                "prev": "0x01",
                "next": "0x02"
            }
        ]
    }));

    assert_lossless(
        EvmChunkBuilder::new(),
        block,
        &[
            ("blocks", 1),
            ("transactions", 2),
            ("logs", 1),
            ("traces", 4),
            ("statediffs", 2)
        ]
    );
}

#[test]
fn solana_real_schema_is_lossless() {
    let accounts = [
        "11111111111111111111111111111111",
        "Vote111111111111111111111111111111111111111",
        "SysvarRent111111111111111111111111111111111",
        "SysvarC1ock11111111111111111111111111111111"
    ];
    let block: solana::Block = parse(json!({
        "header": {
            "number": 300_000_000,
            "hash": "5HueCGU8rMjxEXxiPuD5BDu",
            "parentNumber": 299_999_998,
            "parentHash": "4vJ9JU1bJJE96FWSJKvHs",
            "height": 280_000_000,
            "timestamp": 1_760_000_000
        },
        "accounts": accounts,
        "transactions": [{
            "transactionIndex": 0,
            "version": "legacy",
            "accountKeys": [0, 1],
            "addressTableLookups": [{
                "accountKey": 2,
                "readonlyIndexes": [1, 2],
                "writableIndexes": [3]
            }],
            "numReadonlySignedAccounts": 0,
            "numReadonlyUnsignedAccounts": 1,
            "numRequiredSignatures": 1,
            "recentBlockhash": "4vJ9JU1bJJE96FWSJKvHs",
            "signatures": ["3Bxs4NN8M2Yn4TLb"],
            "err": {"InstructionError": [0, "Custom"]},
            "computeUnitsConsumed": "12345",
            "costUnits": "13000",
            "fee": "5000",
            "loadedAddresses": {"readonly": [2], "writable": [3]},
            "hasDroppedLogMessages": false
        }],
        "instructions": [{
            "transactionIndex": 0,
            "instructionAddress": [0, 1],
            "programId": 1,
            "accounts": [0, 1, 2, 3],
            "data": "11111111111111111",
            "computeUnitsConsumed": "1000",
            "error": null,
            "isCommitted": true,
            "hasDroppedLogMessages": false
        }],
        "logs": [{
            "transactionIndex": 0,
            "logIndex": 0,
            "instructionAddress": [0, 1],
            "programId": 1,
            "kind": "log",
            "message": "Program log: all fields survive"
        }],
        "balances": [{"transactionIndex": 0, "account": 0, "pre": "100", "post": "90"}],
        "tokenBalances": [{
            "transactionIndex": 0,
            "account": 0,
            "preMint": 1,
            "postMint": 1,
            "preDecimals": 6,
            "postDecimals": 6,
            "preProgramId": 2,
            "postProgramId": 2,
            "preOwner": 3,
            "postOwner": 3,
            "preAmount": "10",
            "postAmount": "9"
        }],
        "rewards": [{
            "pubkey": 1,
            "lamports": "42",
            "postBalance": "1042",
            "rewardType": "voting",
            "commission": 7
        }]
    }));

    assert_lossless(
        SolanaChunkBuilder::new(),
        block,
        &[
            ("blocks", 1),
            ("transactions", 1),
            ("instructions", 1),
            ("logs", 1),
            ("balances", 1),
            ("token_balances", 1),
            ("rewards", 1)
        ]
    );
}

#[test]
fn bitcoin_real_schema_is_lossless() {
    let block: bitcoin::Block = parse(json!({
        "header": {
            "number": 900_000,
            "hash": hex(100),
            "parentHash": hex(99),
            "timestamp": 1_760_000_000,
            "medianTime": 1_759_999_000,
            "version": 2,
            "merkleRoot": hex(101),
            "nonce": 42,
            "target": hex(102),
            "bits": "0x1d00ffff",
            "difficulty": 123.5,
            "chainWork": hex(103),
            "strippedSize": 900,
            "size": 1000,
            "weight": 3900
        },
        "transactions": [{
            "hex": "0x010203",
            "txid": hex(110),
            "hash": hex(111),
            "size": 200,
            "vsize": 150,
            "weight": 600,
            "version": 2,
            "locktime": 0,
            "vin": [
                {"coinbase": "0x03abcdef", "sequence": 4294967295u64, "txInWitness": ["0x01"]},
                {
                    "txid": hex(112),
                    "vout": 1,
                    "scriptSig": {"hex": "0x160014", "asm": "0 0011"},
                    "sequence": 4294967294u64,
                    "txInWitness": ["0xaa", "0xbb"],
                    "prevout": {
                        "generated": false,
                        "height": 899_999,
                        "value": 0.125,
                        "scriptPubKey": {
                            "hex": "0x0014",
                            "asm": "0 abcd",
                            "desc": "addr(test)",
                            "type": "witness_v0_keyhash",
                            "address": "bc1qexample"
                        }
                    }
                }
            ],
            "vout": [{
                "value": 0.124,
                "n": 0,
                "scriptPubKey": {
                    "hex": "0x76a9",
                    "asm": "OP_DUP OP_HASH160",
                    "desc": "pkh(test)",
                    "type": "pubkeyhash",
                    "address": "1Example"
                }
            }]
        }]
    }));

    assert_lossless(
        BitcoinChunkBuilder::new(),
        block,
        &[("blocks", 1), ("transactions", 1), ("inputs", 2), ("outputs", 1)]
    );
}

#[test]
fn tron_real_schema_is_lossless() {
    let block: tron::Block = parse(json!({
        "header": {
            "height": 70_000_000,
            "hash": hex(200),
            "parentHash": hex(199),
            "txTrieRoot": hex(201),
            "version": 29,
            "timestamp": 1_760_000_000_000i64,
            "witnessAddress": "0x41aa",
            "witnessSignature": "0xbb"
        },
        "transactions": [{
            "transactionIndex": 0,
            "hash": hex(210),
            "ret": [{"contractRet": "SUCCESS"}],
            "signature": ["0xdead", "0xbeef"],
            "type": "TriggerSmartContract",
            "parameter": {"value": {
                "owner_address": "0x41aa",
                "contract_address": "0x41bb",
                "data": "0xa9059cbb0011"
            }},
            "permissionId": 2,
            "refBlockBytes": "0x1234",
            "refBlockHash": "0xabcd",
            "feeLimit": "1000000",
            "expiration": 1_760_000_060_000i64,
            "timestamp": "1760000000000",
            "rawDataHex": "0x01020304",
            "fee": "123",
            "contractResult": "0x01",
            "contractAddress": "0x41bb",
            "result": "SUCCESS",
            "energyFee": "10",
            "energyUsage": "20",
            "energyUsageTotal": "30",
            "netUsage": "40",
            "netFee": "50"
        }],
        "logs": [{
            "transactionIndex": 0,
            "logIndex": 0,
            "address": "0x41bb",
            "data": "0x0102",
            "topics": [hex(211), hex(212)]
        }],
        "internalTransactions": [{
            "transactionIndex": 0,
            "internalTransactionIndex": 0,
            "hash": hex(213),
            "callerAddress": "0x41aa",
            "transferToAddress": "0x41cc",
            "callValueInfo": [{"callValue": "7", "tokenId": "1002000"}],
            "note": "0x63616c6c",
            "rejected": false,
            "extra": "0x99"
        }]
    }));

    assert_lossless(
        TronChunkBuilder::new(),
        block,
        &[
            ("blocks", 1),
            ("transactions", 1),
            ("logs", 1),
            ("internal_transactions", 1)
        ]
    );
}

#[test]
fn hyperliquid_fills_real_schema_is_lossless() {
    let block: hyperliquid_fills::Block = parse(json!({
        "header": {
            "number": 10_000,
            "hash": hex(300),
            "parentHash": hex(299),
            "timestamp": 1_760_000_000_000i64
        },
        "fills": [{
            "fillIndex": 0,
            "user": address(20),
            "coin": "BTC",
            "px": 123.25,
            "sz": 0.5,
            "side": "B",
            "time": 1_760_000_000_000i64,
            "startPosition": 1.25,
            "dir": "Open Long",
            "closedPnl": 0.25,
            "hash": hex(301),
            "oid": 42,
            "crossed": true,
            "fee": 0.01,
            "builderFee": 0.001,
            "tid": 43,
            "cloid": "client-order-id",
            "feeToken": "USDC",
            "builder": address(21),
            "twapId": 44
        }]
    }));

    assert_lossless(
        HyperliquidFillsChunkBuilder::new(),
        block,
        &[("blocks", 1), ("fills", 1)]
    );
}

#[test]
fn hyperliquid_replica_commands_real_schema_is_lossless() {
    let block: hyperliquid_replica_cmds::Block = parse(json!({
        "header": {
            "height": 10_001,
            "hash": hex(400),
            "parentHash": hex(399),
            "round": 12,
            "parentRound": 11,
            "proposer": address(30),
            "timestamp": 1_760_000_000_000i64,
            "hardfork": {"version": 3, "round": 10}
        },
        "actions": [
            {
                "actionIndex": 0,
                "signature": {"r": hex(401), "s": hex(402), "v": 27},
                "action": {"type": "order", "orders": [{"a": 1, "c": "cloid-a"}, {"a": 2}]},
                "nonce": 100,
                "vaultAddress": address(31),
                "user": address(32),
                "status": "ok",
                "response": {"status": "ok", "data": {"statuses": ["resting"]}}
            },
            {
                "actionIndex": 1,
                "signature": {"r": hex(403), "s": hex(404), "v": 28},
                "action": {"type": "cancelByCloid", "cancels": [{"asset": 1, "cloid": "cloid-a"}]},
                "nonce": 101,
                "status": "err",
                "response": {"status": "err", "message": "already filled"}
            },
            {
                "actionIndex": 2,
                "signature": {"r": hex(405), "s": hex(406), "v": 27},
                "action": {"type": "cancel", "cancels": [{"a": 3}, {"a": 3}, {"a": 4}]},
                "nonce": 102,
                "user": address(32),
                "status": "ok",
                "response": {"status": "ok"}
            },
            {
                "actionIndex": 3,
                "signature": {"r": hex(407), "s": hex(408), "v": 28},
                "action": {
                    "type": "batchModify",
                    "modifies": [
                        {"oid": 1, "order": {"a": 5, "c": "cloid-b"}},
                        {"oid": 2, "order": {"a": 6}}
                    ]
                },
                "nonce": 103,
                "vaultAddress": address(31),
                "status": "ok",
                "response": {"status": "ok"}
            }
        ]
    }));

    assert_lossless(
        HyperliquidReplicaCmdsChunkBuilder::new(),
        block,
        &[("blocks", 1), ("actions", 4)]
    );
}
