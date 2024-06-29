use std::sync::Arc;

use arrow::array::{StructArray, RecordBatch};
use arrow_schema::Schema;

use crate::row_processor::{RowArrayBuilder, RowProcessor};
use crate::solana::model::Block;
use crate::sink::Writer;
use crate::fs::Fs;

use super::tables::block::{BlockBuilder, BlockProcessor};
use super::tables::transaction::{TransactionBuilder, TransactionProcessor};
use super::tables::instruction::{InstructionBuilder, InstructionProcessor};
use super::tables::balance::{BalanceBuilder, BalanceProcessor};
use super::tables::token_balance::{TokenBalanceBuilder, TokenBalanceProcessor};
use super::tables::log_message::{LogMessageBuilder, LogMessageProcessor};
use super::tables::reward::{RewardBuilder, RewardProcessor};

pub struct ParquetWriter {
    block_builder: RowArrayBuilder<BlockProcessor>,
    transaction_builder: RowArrayBuilder<TransactionProcessor>,
    instruction_builder: RowArrayBuilder<InstructionProcessor>,
    log_message_builder: RowArrayBuilder<LogMessageProcessor>,
    balance_builder: RowArrayBuilder<BalanceProcessor>,
    token_balance_builder: RowArrayBuilder<TokenBalanceProcessor>,
    reward_builder: RowArrayBuilder<RewardProcessor>,
}

impl ParquetWriter {
    pub fn new() -> ParquetWriter {
        ParquetWriter {
            block_builder: BlockProcessor::default().into_builder(),
            transaction_builder: TransactionProcessor::default().into_builder(),
            instruction_builder: InstructionProcessor::default().into_builder(),
            log_message_builder: LogMessageProcessor::default().into_builder(),
            balance_builder: BalanceProcessor::default().into_builder(),
            token_balance_builder: TokenBalanceProcessor::default().into_builder(),
            reward_builder: RewardProcessor::default().into_builder(),
        }
    }
}

impl Writer<Block> for ParquetWriter {
    fn push(&mut self, mut block: Block) {
        self.block_builder.push(&block.header);

        for transaction in &mut block.transactions {
            transaction.block_number = block.header.height;
            self.transaction_builder.push(transaction);
        }

        for instruction in &mut block.instructions {
            instruction.block_number = block.header.height;
            self.instruction_builder.push(instruction);
        }

        for log_message in &mut block.logs {
            log_message.block_number = block.header.height;
            self.log_message_builder.push(log_message);
        }

        for balance in &mut block.balances {
            balance.block_number = block.header.height;
            self.balance_builder.push(balance);
        }

        for token_balance in &mut block.token_balances {
            token_balance.block_number = block.header.height;
            self.token_balance_builder.push(token_balance);
        }

        for reward in &mut block.rewards {
            reward.block_number = block.header.height;
            self.reward_builder.push(reward);
        }
    }

    fn buffered_bytes(&self) -> usize {
        self.block_builder.n_bytes() + self.transaction_builder.n_bytes() + self.instruction_builder.n_bytes() + self.log_message_builder.n_bytes() + self.balance_builder.n_bytes() + self.token_balance_builder.n_bytes() + self.reward_builder.n_bytes()
    }

    fn flush(&mut self, fs: Box<dyn Fs>) {
        let zstd_level = parquet::basic::ZstdLevel::try_new(3).unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(zstd_level))
            .set_data_page_size_limit(32 * 1024)
            .set_dictionary_page_size_limit(192 * 1024)
            .set_write_batch_size(50)
            .set_max_row_group_size(5_000)
            .build();
        let mut schema = Arc::new(Schema::new(TransactionBuilder::data_fields()));
        let iter = self.transaction_builder.finish();
        let mut batches: Box<dyn Iterator<Item = RecordBatch>> = Box::new(iter.map(|row| {
            let data = row.to_data();
            let struct_array = StructArray::from(data);
            let batch = RecordBatch::from(struct_array);
            batch
        }));
        if let Some(batch) = batches.next() {
            schema = batch.schema();
            batches = Box::new(std::iter::once(batch).chain(batches));
        }

        fs.write_parquet("transactions.parquet", batches, schema, Some(props)).unwrap();

        let zstd_level = parquet::basic::ZstdLevel::try_new(3).unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(zstd_level))
            .set_data_page_size_limit(32 * 1024)
            .set_dictionary_page_size_limit(192 * 1024)
            .set_write_batch_size(50)
            .set_max_row_group_size(20_000)
            .build();
        let mut schema = Arc::new(Schema::new(InstructionBuilder::data_fields()));
        let iter = self.instruction_builder.finish();
        let mut batches: Box<dyn Iterator<Item = RecordBatch>> = Box::new(iter.map(|row| {
            let data = row.to_data();
            let struct_array = StructArray::from(data);
            let batch = RecordBatch::from(struct_array);
            batch
        }));
        if let Some(batch) = batches.next() {
            schema = batch.schema();
            batches = Box::new(std::iter::once(batch).chain(batches));
        }
        fs.write_parquet("instructions.parquet", batches, schema, Some(props)).unwrap();

        let zstd_level = parquet::basic::ZstdLevel::try_new(3).unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(zstd_level))
            .set_data_page_size_limit(32 * 1024)
            .set_dictionary_page_size_limit(192 * 1024)
            .set_write_batch_size(50)
            .set_max_row_group_size(5_000)
            .build();
        let mut schema = Arc::new(Schema::new(LogMessageBuilder::data_fields()));
        let iter = self.log_message_builder.finish();
        let mut batches: Box<dyn Iterator<Item = RecordBatch>> = Box::new(iter.map(|row| {
            let data = row.to_data();
            let struct_array = StructArray::from(data);
            let batch = RecordBatch::from(struct_array);
            batch
        }));
        if let Some(batch) = batches.next() {
            schema = batch.schema();
            batches = Box::new(std::iter::once(batch).chain(batches));
        }
        fs.write_parquet("logs.parquet", batches, schema, Some(props)).unwrap();

        let zstd_level = parquet::basic::ZstdLevel::try_new(3).unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(zstd_level))
            .set_data_page_size_limit(32 * 1024)
            .set_dictionary_page_size_limit(192 * 1024)
            .set_write_batch_size(50)
            .set_max_row_group_size(5_000)
            .build();
        let mut schema = Arc::new(Schema::new(BalanceBuilder::data_fields()));
        let iter = self.balance_builder.finish();
        let mut batches: Box<dyn Iterator<Item = RecordBatch>> = Box::new(iter.map(|row| {
            let data = row.to_data();
            let struct_array = StructArray::from(data);
            let batch = RecordBatch::from(struct_array);
            batch
        }));
        if let Some(batch) = batches.next() {
            schema = batch.schema();
            batches = Box::new(std::iter::once(batch).chain(batches));
        }
        fs.write_parquet("balances.parquet", batches, schema, Some(props)).unwrap();

        let zstd_level = parquet::basic::ZstdLevel::try_new(3).unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(zstd_level))
            .set_data_page_size_limit(32 * 1024)
            .set_dictionary_page_size_limit(192 * 1024)
            .set_write_batch_size(50)
            .set_max_row_group_size(5_000)
            .build();
        let mut schema = Arc::new(Schema::new(TokenBalanceBuilder::data_fields()));
        let iter = self.token_balance_builder.finish();
        let mut batches: Box<dyn Iterator<Item = RecordBatch>> = Box::new(iter.map(|row| {
            let data = row.to_data();
            let struct_array = StructArray::from(data);
            let batch = RecordBatch::from(struct_array);
            batch
        }));
        if let Some(batch) = batches.next() {
            schema = batch.schema();
            batches = Box::new(std::iter::once(batch).chain(batches));
        }
        fs.write_parquet("token_balances.parquet", batches, schema, Some(props)).unwrap();

        let zstd_level = parquet::basic::ZstdLevel::try_new(3).unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(zstd_level))
            .set_data_page_size_limit(32 * 1024)
            .set_dictionary_page_size_limit(192 * 1024)
            .set_write_batch_size(50)
            .set_max_row_group_size(5_000)
            .build();
        let mut schema = Arc::new(Schema::new(RewardBuilder::data_fields()));
        let iter = self.reward_builder.finish();
        let mut batches: Box<dyn Iterator<Item = RecordBatch>> = Box::new(iter.map(|row| {
            let data = row.to_data();
            let struct_array = StructArray::from(data);
            let batch = RecordBatch::from(struct_array);
            batch
        }));
        if let Some(batch) = batches.next() {
            schema = batch.schema();
            batches = Box::new(std::iter::once(batch).chain(batches));
        }
        fs.write_parquet("rewards.parquet", batches, schema, Some(props)).unwrap();

        let zstd_level = parquet::basic::ZstdLevel::try_new(3).unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(zstd_level))
            .set_data_page_size_limit(32 * 1024)
            .set_dictionary_page_size_limit(192 * 1024)
            .set_write_batch_size(50)
            .set_max_row_group_size(5_000)
            .build();
        let mut schema = Arc::new(Schema::new(BlockBuilder::data_fields()));
        let iter = self.block_builder.finish();
        let mut batches: Box<dyn Iterator<Item = RecordBatch>> = Box::new(iter.map(|row| {
            let data = row.to_data();
            let struct_array = StructArray::from(data);
            let batch = RecordBatch::from(struct_array);
            batch
        }));
        if let Some(batch) = batches.next() {
            schema = batch.schema();
            batches = Box::new(std::iter::once(batch).chain(batches));
        }
        fs.write_parquet("blocks.parquet", batches, schema, Some(props)).unwrap();
    }

    fn get_block_height(&self, block: &Block) -> u64 {
        block.header.height
    }

    fn get_block_hash<'a>(&self, block: &'a Block) -> &'a String {
        &block.header.hash
    }
}
