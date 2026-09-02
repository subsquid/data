use crate::evm::model::BlockHeader;
use crate::evm::tables::common::*;
use sqd_array::builder::{ListBuilder, TimestampSecondBuilder, UInt64Builder, Int32Builder};
use sqd_data_core::{struct_builder, table_builder};

type WithdrawalListBuilder = ListBuilder<WithdrawalBuilder>;
struct_builder! {
    WithdrawalBuilder {
        address: HexBytesBuilder,
        amount: HexBytesBuilder,
        index: HexBytesBuilder,
        validator_index: HexBytesBuilder,
    }
}

table_builder! {
    BlockBuilder {
        number: Int32Builder,
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
        uncles: UncleListBuilder,
        withdrawals: WithdrawalListBuilder,
        withdrawals_root: HexBytesBuilder,
        blob_gas_used: HexBytesBuilder,
        excess_blob_gas: HexBytesBuilder,
        parent_beacon_block_root: HexBytesBuilder,
        requests_hash: HexBytesBuilder,
        l1_block_number: UInt64Builder,
        // Tempo-specific block header fields
        main_block_general_gas_limit: HexBytesBuilder,
        shared_gas_limit: HexBytesBuilder,
        timestamp_millis_part: HexBytesBuilder,
        // Avalanche-specific block header fields
        block_extra_data: HexBytesBuilder,
        block_gas_cost: HexBytesBuilder,
        ext_data_gas_used: HexBytesBuilder,
        ext_data_hash: HexBytesBuilder,
        min_delay_excess: HexBytesBuilder,
        timestamp_milliseconds: HexBytesBuilder,
        target_exponent: HexBytesBuilder,
        min_price_exponent: HexBytesBuilder,
        settled_height: HexBytesBuilder,
        settled_gas_unix: HexBytesBuilder,
        settled_gas_numerator: HexBytesBuilder,
        settled_excess: HexBytesBuilder,

        extra_data_size: UInt64Builder,
        block_extra_data_size: UInt64Builder,
        withdrawals_size: UInt64Builder,
    }

    description(d) {
        d.sort_key = vec!["number"];
        d.options.add_stats("number");
        d.options.use_dictionary("withdrawals.list.element.address");
        d.options.row_group_size = 5_000;
    }
}

impl BlockBuilder {
    pub fn push(&mut self, row: &BlockHeader) {
        self.number.append(row.number as i32);
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

        for uncle in row.uncles.iter().flatten() {
            self.uncles.values().append(uncle);
        }
        self.uncles.append();

        for withdrawal in row.withdrawals.iter().flatten() {
            let item = self.withdrawals.values();
            item.address.append(&withdrawal.address);
            item.amount.append(&withdrawal.amount);
            item.index.append(&withdrawal.index);
            item.validator_index.append(&withdrawal.validator_index);
            item.append_valid();
        }
        self.withdrawals.append();

        self.withdrawals_root.append_option(row.withdrawals_root.as_deref());
        self.blob_gas_used.append_option(row.blob_gas_used.as_deref());
        self.excess_blob_gas.append_option(row.excess_blob_gas.as_deref());
        self.parent_beacon_block_root
            .append_option(row.parent_beacon_block_root.as_deref());
        self.requests_hash.append_option(row.requests_hash.as_deref());
        self.l1_block_number.append_option(row.l1_block_number);
        self.main_block_general_gas_limit
            .append_option(row.main_block_general_gas_limit.as_deref());
        self.shared_gas_limit.append_option(row.shared_gas_limit.as_deref());
        self.timestamp_millis_part
            .append_option(row.timestamp_millis_part.as_deref());
        self.block_extra_data.append_option(row.block_extra_data.as_deref());
        self.block_gas_cost.append_option(row.block_gas_cost.as_deref());
        self.ext_data_gas_used.append_option(row.ext_data_gas_used.as_deref());
        self.ext_data_hash.append_option(row.ext_data_hash.as_deref());
        self.min_delay_excess.append_option(row.min_delay_excess.as_deref());
        self.timestamp_milliseconds
            .append_option(row.timestamp_milliseconds.as_deref());
        self.target_exponent.append_option(row.target_exponent.as_deref());
        self.min_price_exponent.append_option(row.min_price_exponent.as_deref());
        self.settled_height.append_option(row.settled_height.as_deref());
        self.settled_gas_unix.append_option(row.settled_gas_unix.as_deref());
        self.settled_gas_numerator
            .append_option(row.settled_gas_numerator.as_deref());
        self.settled_excess.append_option(row.settled_excess.as_deref());

        self.extra_data_size.append(row.extra_data.len() as u64);
        self.block_extra_data_size
            .append(row.block_extra_data.as_ref().map_or(0, |v| v.len()) as u64);

        let withdrawals_size = row.withdrawals.as_ref().map_or(0, |val| val.len()) * 56;
        self.withdrawals_size.append(withdrawals_size as u64);
    }
}
