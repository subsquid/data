# EVM Query Syntax Reference

This document describes the JSON query syntax for querying EVM (Ethereum Virtual Machine) blockchain data.

## Query Structure

An EVM query is a JSON object with the following top-level structure:

```json
{
  "type": "evm",
  "fromBlock": 17881390,
  "toBlock": 17882786,
  "parentBlockHash": "0x...",
  "includeAllBlocks": false,
  "fields": { ... },
  "transactions": [ ... ],
  "logs": [ ... ],
  "traces": [ ... ],
  "stateDiffs": [ ... ]
}
```

## Top-Level Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | Yes | - | Must be `"evm"` |
| `fromBlock` | integer | Yes | - | First block number to query (inclusive) |
| `toBlock` | integer | No | - | Last block number to query (inclusive). If omitted, query is open-ended |
| `parentBlockHash` | string | No | - | Expected parent hash of `fromBlock`. Used for chain continuity validation |
| `includeAllBlocks` | boolean | No | `false` | When `true`, includes all blocks in the range even if they have no matching items |
| `fields` | object | No | `{}` | Specifies which fields to include in the output for each entity type |
| `transactions` | array | No | `[]` | Transaction filter requests |
| `logs` | array | No | `[]` | Log filter requests |
| `traces` | array | No | `[]` | Trace filter requests |
| `stateDiffs` | array | No | `[]` | State diff filter requests |

### Validation Rules

- `fromBlock` must be less than or equal to `toBlock` (if `toBlock` is specified)
- The total number of item requests (`transactions.length + logs.length + traces.length + stateDiffs.length`) must not exceed 100

## Field Selection

The `fields` object controls which fields are included in the output for each entity type. Each field is a boolean - set to `true` to include it.

```json
{
  "fields": {
    "block": { ... },
    "transaction": { ... },
    "log": { ... },
    "trace": { ... },
    "stateDiff": { ... }
  }
}
```

### Block Fields

| Field | Description |
|-------|-------------|
| `number` | Block number |
| `hash` | Block hash |
| `parentHash` | Parent block hash |
| `timestamp` | Block timestamp (Unix seconds) |
| `transactionsRoot` | Merkle root of transactions |
| `receiptsRoot` | Merkle root of receipts |
| `stateRoot` | Merkle root of state |
| `logsBloom` | Bloom filter for logs |
| `sha3Uncles` | SHA3 hash of uncles |
| `extraData` | Extra data field |
| `miner` | Miner/validator address |
| `nonce` | Block nonce |
| `mixHash` | Mix hash (PoW) |
| `size` | Block size in bytes |
| `gasLimit` | Block gas limit |
| `gasUsed` | Total gas used in block |
| `difficulty` | Block difficulty |
| `totalDifficulty` | Cumulative chain difficulty |
| `baseFeePerGas` | Base fee per gas (EIP-1559) |
| `blobGasUsed` | Blob gas used (EIP-4844) |
| `excessBlobGas` | Excess blob gas (EIP-4844) |
| `l1BlockNumber` | L1 block number (L2 chains only) |

### Transaction Fields

| Field | Description |
|-------|-------------|
| `transactionIndex` | Index of transaction in block |
| `hash` | Transaction hash |
| `nonce` | Sender's nonce |
| `from` | Sender address |
| `to` | Recipient address (null for contract creation) |
| `input` | Transaction input data |
| `value` | Value transferred in wei |
| `gas` | Gas limit |
| `gasPrice` | Gas price (legacy transactions) |
| `maxFeePerGas` | Max fee per gas (EIP-1559) |
| `maxPriorityFeePerGas` | Max priority fee (EIP-1559) |
| `v` | Signature v value |
| `r` | Signature r value |
| `s` | Signature s value |
| `yParity` | Signature y parity (EIP-2930) |
| `chainId` | Chain ID |
| `sighash` | First 4 bytes of input (function selector) |
| `contractAddress` | Created contract address (if contract creation) |
| `gasUsed` | Actual gas used |
| `cumulativeGasUsed` | Cumulative gas used up to this tx |
| `effectiveGasPrice` | Effective gas price paid |
| `type` | Transaction type (0=legacy, 1=EIP-2930, 2=EIP-1559, 3=EIP-4844) |
| `status` | Execution status (1=success, 0=failure) |
| `maxFeePerBlobGas` | Max fee per blob gas (EIP-4844) |
| `blobVersionedHashes` | Blob versioned hashes (EIP-4844) |
| `l1Fee` | L1 fee (L2 chains) |
| `l1FeeScalar` | L1 fee scalar (L2 chains) |
| `l1GasPrice` | L1 gas price (L2 chains) |
| `l1GasUsed` | L1 gas used (L2 chains) |
| `l1BlobBaseFee` | L1 blob base fee (L2 chains) |
| `l1BlobBaseFeeScalar` | L1 blob base fee scalar (L2 chains) |
| `l1BaseFeeScalar` | L1 base fee scalar (L2 chains) |

### Log Fields

| Field | Description |
|-------|-------------|
| `logIndex` | Index of log in block |
| `transactionIndex` | Index of parent transaction |
| `transactionHash` | Hash of parent transaction |
| `address` | Contract address that emitted the log |
| `data` | Log data (non-indexed parameters) |
| `topics` | Array of log topics (indexed parameters) |

### Trace Fields

Trace output structure varies by trace type. Common fields are always available; type-specific fields appear under `action` and `result` objects.

**Common fields:**

| Field | Description |
|-------|-------------|
| `transactionIndex` | Index of parent transaction |
| `traceAddress` | Position in trace tree (array of integers) |
| `subtraces` | Number of child traces |
| `type` | Trace type: `"create"`, `"call"`, `"suicide"`, or `"reward"` |
| `error` | Error message if trace failed |
| `revertReason` | Revert reason if available |

**Create trace fields** (output under `action` and `result`):

| Field | Output Location | Description |
|-------|-----------------|-------------|
| `createFrom` | `action.from` | Creator address |
| `createValue` | `action.value` | Value sent to new contract |
| `createGas` | `action.gas` | Gas provided |
| `createInit` | `action.init` | Contract init code |
| `createResultGasUsed` | `result.gasUsed` | Gas used |
| `createResultCode` | `result.code` | Deployed bytecode |
| `createResultAddress` | `result.address` | Created contract address |

**Call trace fields** (output under `action` and `result`):

| Field | Output Location | Description |
|-------|-----------------|-------------|
| `callFrom` | `action.from` | Caller address |
| `callTo` | `action.to` | Called address |
| `callValue` | `action.value` | Value transferred |
| `callGas` | `action.gas` | Gas provided |
| `callInput` | `action.input` | Call input data |
| `callSighash` | `action.sighash` | Function selector (first 4 bytes) |
| `callType` | `action.type` | Call type (deprecated, use `callCallType`) |
| `callCallType` | `action.callType` | Call type: `"call"`, `"delegatecall"`, `"staticcall"`, `"callcode"` |
| `callResultGasUsed` | `result.gasUsed` | Gas used |
| `callResultOutput` | `result.output` | Return data |

**Suicide trace fields** (output under `action`):

| Field | Output Location | Description |
|-------|-----------------|-------------|
| `suicideAddress` | `action.address` | Self-destructed contract |
| `suicideRefundAddress` | `action.refundAddress` | Address receiving remaining balance |
| `suicideBalance` | `action.balance` | Balance transferred |

**Reward trace fields** (output under `action`):

| Field | Output Location | Description |
|-------|-----------------|-------------|
| `rewardAuthor` | `action.author` | Reward recipient |
| `rewardValue` | `action.value` | Reward amount |
| `rewardType` | `action.type` | Reward type (e.g., `"block"`, `"uncle"`) |

### StateDiff Fields

| Field | Description |
|-------|-------------|
| `transactionIndex` | Index of parent transaction |
| `address` | Account address |
| `key` | Storage key (null for balance changes) |
| `kind` | Change type: `"+"` (added), `"-"` (removed), `"*"` (modified) |
| `prev` | Previous value |
| `next` | New value |

## Item Requests

Item requests define filters for selecting blockchain data. Multiple requests of the same type are combined with OR logic - an item matches if it satisfies any request. Within a single request, all specified filters are combined with AND logic.

### Transaction Request

```json
{
  "transactions": [
    {
      "from": ["0x..."],
      "to": ["0x..."],
      "sighash": ["0x..."],
      "firstNonce": 100,
      "lastNonce": 200,
      "logs": true,
      "traces": true,
      "stateDiffs": true
    }
  ]
}
```

**Filter fields:**

| Field | Type | Description |
|-------|------|-------------|
| `from` | string[] | Match transactions from any of these addresses |
| `to` | string[] | Match transactions to any of these addresses |
| `sighash` | string[] | Match transactions with any of these function selectors (first 4 bytes of input) |
| `firstNonce` | integer | Match transactions with nonce >= this value |
| `lastNonce` | integer | Match transactions with nonce <= this value |

**Relation fields** (include related data):

| Field | Type | Description |
|-------|------|-------------|
| `logs` | boolean | Include all logs emitted by matching transactions |
| `traces` | boolean | Include all traces from matching transactions |
| `stateDiffs` | boolean | Include all state diffs from matching transactions |

### Log Request

```json
{
  "logs": [
    {
      "address": ["0x..."],
      "topic0": ["0x..."],
      "topic1": ["0x..."],
      "topic2": ["0x..."],
      "topic3": ["0x..."],
      "transaction": true,
      "transactionTraces": true,
      "transactionLogs": true,
      "transactionStateDiffs": true
    }
  ]
}
```

**Filter fields:**

| Field | Type | Description |
|-------|------|-------------|
| `address` | string[] | Match logs from any of these contract addresses |
| `topic0` | string[] | Match logs with topic0 (event signature) in this list |
| `topic1` | string[] | Match logs with topic1 in this list |
| `topic2` | string[] | Match logs with topic2 in this list |
| `topic3` | string[] | Match logs with topic3 in this list |

**Relation fields:**

| Field | Type | Description |
|-------|------|-------------|
| `transaction` | boolean | Include the parent transaction |
| `transactionTraces` | boolean | Include all traces from the parent transaction |
| `transactionLogs` | boolean | Include all logs from the parent transaction |
| `transactionStateDiffs` | boolean | Include all state diffs from the parent transaction |

### Trace Request

```json
{
  "traces": [
    {
      "type": ["call", "create"],
      "createFrom": ["0x..."],
      "createResultAddress": ["0x..."],
      "callFrom": ["0x..."],
      "callTo": ["0x..."],
      "callSighash": ["0x..."],
      "suicideAddress": ["0x..."],
      "suicideRefundAddress": ["0x..."],
      "rewardAuthor": ["0x..."],
      "transaction": true,
      "transactionLogs": true,
      "subtraces": true,
      "parents": true
    }
  ]
}
```

**Filter fields:**

| Field | Type | Description |
|-------|------|-------------|
| `type` | string[] | Match traces of these types: `"create"`, `"call"`, `"suicide"`, `"reward"` |
| `createFrom` | string[] | Match create traces from any of these addresses |
| `createResultAddress` | string[] | Match create traces that deployed contracts at these addresses |
| `callFrom` | string[] | Match call traces from any of these addresses |
| `callTo` | string[] | Match call traces to any of these addresses |
| `callSighash` | string[] | Match call traces with any of these function selectors |
| `suicideAddress` | string[] | Match suicide traces for any of these contract addresses |
| `suicideRefundAddress` | string[] | Match suicide traces refunding to any of these addresses |
| `rewardAuthor` | string[] | Match reward traces to any of these authors |

**Relation fields:**

| Field | Type | Description |
|-------|------|-------------|
| `transaction` | boolean | Include the parent transaction |
| `transactionLogs` | boolean | Include all logs from the parent transaction |
| `subtraces` | boolean | Include all child traces (recursive) |
| `parents` | boolean | Include all parent traces up to the root |

### StateDiff Request

```json
{
  "stateDiffs": [
    {
      "address": ["0x..."],
      "key": ["0x..."],
      "kind": ["+", "*"],
      "transaction": true
    }
  ]
}
```

**Filter fields:**

| Field | Type | Description |
|-------|------|-------------|
| `address` | string[] | Match state diffs for any of these account addresses |
| `key` | string[] | Match state diffs for any of these storage keys |
| `kind` | string[] | Match state diffs of these change types: `"+"`, `"-"`, `"*"`, `"="` |

**Relation fields:**

| Field | Type | Description |
|-------|------|-------------|
| `transaction` | boolean | Include the parent transaction |

## Data Format

### Hex Strings

All binary data (addresses, hashes, keys, etc.) must be hex-encoded strings with a `0x` prefix:

- Addresses: 20 bytes, e.g., `"0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d"`
- Hashes: 32 bytes, e.g., `"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"`
- Sighash: 4 bytes, e.g., `"0xa9059cbb"`

Hex strings must have even length after the `0x` prefix.

### Case Sensitivity

Address and hash filters are **case-insensitive**. The following are equivalent:
- `"0xBC4CA0EDA7647A8AB7C2061C2E118A18A936F13D"`
- `"0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d"`

## Examples

### Query ERC-20 Transfer Events

Query Transfer events from a specific token contract:

```json
{
  "type": "evm",
  "fromBlock": 17881390,
  "toBlock": 17882786,
  "logs": [
    {
      "address": ["0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d"],
      "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
      "transaction": true
    }
  ],
  "fields": {
    "block": {
      "number": true,
      "hash": true,
      "timestamp": true
    },
    "log": {
      "logIndex": true,
      "transactionIndex": true,
      "address": true,
      "topics": true,
      "data": true
    },
    "transaction": {
      "hash": true,
      "from": true,
      "to": true
    }
  }
}
```

### Query Transactions by Function Selector

Query transactions calling specific functions on a contract:

```json
{
  "type": "evm",
  "fromBlock": 17881390,
  "toBlock": 17882786,
  "transactions": [
    {
      "to": ["0x0a252663dbcc0b073063d6420a40319e438cfa59"],
      "sighash": ["0xecef9201", "0xf5878b9b"]
    }
  ],
  "fields": {
    "block": {
      "number": true,
      "timestamp": true
    },
    "transaction": {
      "hash": true,
      "from": true,
      "to": true,
      "input": true,
      "sighash": true
    }
  }
}
```

### Query Call Traces to a Contract

Query all call traces to a specific contract with related transaction data:

```json
{
  "type": "evm",
  "fromBlock": 17881390,
  "toBlock": 17882786,
  "traces": [
    {
      "type": ["call"],
      "callTo": ["0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d"],
      "transaction": true
    }
  ],
  "fields": {
    "block": {
      "number": true,
      "timestamp": true
    },
    "trace": {
      "transactionIndex": true,
      "traceAddress": true,
      "type": true,
      "callFrom": true,
      "callTo": true,
      "callSighash": true,
      "error": true
    },
    "transaction": {
      "hash": true,
      "from": true,
      "to": true
    }
  }
}
```

### Query State Changes for an Account

Query all state changes for a specific address:

```json
{
  "type": "evm",
  "fromBlock": 17881390,
  "toBlock": 17882786,
  "stateDiffs": [
    {
      "address": ["0xffdbe829d5a9afcd50482c23eb387b9050f1f825"],
      "transaction": true
    }
  ],
  "fields": {
    "block": {
      "number": true,
      "hash": true
    },
    "stateDiff": {
      "transactionIndex": true,
      "address": true,
      "key": true,
      "kind": true,
      "prev": true,
      "next": true
    },
    "transaction": {
      "hash": true,
      "from": true,
      "to": true
    }
  }
}
```

### Query with Trace Hierarchy

Query traces matching a filter and include their child traces and parent traces:

```json
{
  "type": "evm",
  "fromBlock": 17881390,
  "toBlock": 17881870,
  "traces": [
    {
      "callSighash": ["0x590a41f5"],
      "subtraces": true,
      "parents": true
    }
  ],
  "fields": {
    "block": {
      "number": true
    },
    "trace": {
      "transactionIndex": true,
      "traceAddress": true,
      "type": true,
      "callFrom": true,
      "callTo": true,
      "callSighash": true
    }
  }
}
```

### Query Contract Deployments

Query for contracts deployed at specific addresses:

```json
{
  "type": "evm",
  "fromBlock": 17881390,
  "toBlock": 17882786,
  "traces": [
    {
      "createResultAddress": [
        "0x28d16daf8999504db54cb16eb0b30e9c62a152a2",
        "0x2ab2c9bf0ef689a02dbcc51b5e7cf7c5db3edb60"
      ]
    }
  ],
  "fields": {
    "block": {
      "number": true
    },
    "trace": {
      "type": true,
      "transactionIndex": true,
      "traceAddress": true,
      "createResultAddress": true
    }
  }
}
```

### Query Multiple Event Types (OR Logic)

Query for multiple different event signatures (each log request is OR'd):

```json
{
  "type": "evm",
  "fromBlock": 17881390,
  "toBlock": 17881395,
  "logs": [
    {
      "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]
    },
    {
      "topic0": [
        "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62",
        "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
      ]
    }
  ],
  "fields": {
    "block": {
      "number": true,
      "timestamp": true
    },
    "log": {
      "logIndex": true,
      "address": true,
      "topics": true,
      "data": true
    }
  }
}
```

### Query Transactions by Nonce Range

Query transactions from a specific sender within a nonce range:

```json
{
  "type": "evm",
  "fromBlock": 17881390,
  "toBlock": 17882786,
  "transactions": [
    {
      "from": ["0x1f9090aae28b8a3dceadf281b0f12828e676c326"],
      "firstNonce": 200505,
      "lastNonce": 200510
    }
  ],
  "fields": {
    "block": {
      "number": true
    },
    "transaction": {
      "hash": true,
      "from": true,
      "to": true,
      "nonce": true
    }
  }
}
```

### Include All Blocks in Range

Include all blocks even if they have no matching items:

```json
{
  "type": "evm",
  "fromBlock": 17881390,
  "toBlock": 17882786,
  "includeAllBlocks": true,
  "fields": {
    "block": {
      "number": true,
      "hash": true,
      "timestamp": true,
      "gasUsed": true,
      "gasLimit": true
    }
  }
}
```

### Combined Query: Logs with Transaction Traces

Query logs and include all traces from their parent transactions:

```json
{
  "type": "evm",
  "fromBlock": 17881390,
  "toBlock": 17882786,
  "logs": [
    {
      "address": ["0xffdbe829d5a9afcd50482c23eb387b9050f1f825"],
      "transactionTraces": true
    }
  ],
  "fields": {
    "block": {
      "number": true,
      "timestamp": true
    },
    "log": {
      "logIndex": true,
      "address": true,
      "topics": true,
      "data": true
    },
    "trace": {
      "transactionIndex": true,
      "traceAddress": true,
      "type": true,
      "revertReason": true
    }
  }
}
```
