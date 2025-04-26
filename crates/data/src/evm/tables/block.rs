use crate::evm::model::BlockHeader;
use crate::evm::tables::common::*;
use sqd_array::builder::{TimestampSecondBuilder, UInt64Builder};
use sqd_data_core::table_builder;


table_builder! {
    BlockBuilder {
        number: UInt64Builder,
        hash: HexBytesBuilder,
        parent_hash: HexBytesBuilder,
        timestamp: TimestampSecondBuilder,
        transactions_root: HexBytesBuilder,
        receipts_root: HexBytesBuilder,
        state_root: HexBytesBuilder,
        logs_bloom: HexBytesBuilder,
        sha3_uncles: HexBytesBuilder,
        extra_data: HexBytesBuilder,
        miner: HexBytesBuilder,
        nonce: HexBytesBuilder,
        mix_hash: HexBytesBuilder,
        size: UInt64Builder,
        gas_limit: HexBytesBuilder,
        gas_used: HexBytesBuilder,
        difficulty: HexBytesBuilder,
        total_difficulty: HexBytesBuilder,
        base_fee_per_gas: HexBytesBuilder,
        blob_gas_used: HexBytesBuilder,
        excess_blob_gas: HexBytesBuilder,
        l1_block_number: UInt64Builder,
    }

    description(d) {
        d.downcast.block_number = vec!["number"];
        d.sort_key = vec!["number"];
        d.options.add_stats("number");
        d.options.row_group_size = 5_000;
    }
}


impl BlockBuilder {
    pub fn push(&mut self, row: &BlockHeader) {
        self.number.append(row.number);
        self.hash.append(&row.hash);
        self.parent_hash.append(&row.parent_hash);
        self.timestamp.append(row.timestamp);
        self.transactions_root.append(&row.transactions_root);
        self.receipts_root.append(&row.receipts_root);
        self.state_root.append(&row.state_root);
        self.logs_bloom.append(&row.logs_bloom);
        self.sha3_uncles.append(&row.sha3_uncles);
        self.extra_data.append(&row.extra_data);
        self.miner.append(&row.miner);
        self.nonce.append_option(row.nonce.as_deref());
        self.mix_hash.append_option(row.mix_hash.as_deref());
        self.size.append(row.size);
        self.gas_limit.append(&row.gas_limit);
        self.gas_used.append(&row.gas_used);
        self.difficulty.append_option(row.difficulty.as_deref());
        self.total_difficulty.append_option(row.total_difficulty.as_deref());
        self.base_fee_per_gas.append_option(row.base_fee_per_gas.as_deref());
        self.blob_gas_used.append_option(row.blob_gas_used.as_deref());
        self.excess_blob_gas.append_option(row.excess_blob_gas.as_deref());
        self.l1_block_number.append_option(row.l1_block_number);
    }
}
