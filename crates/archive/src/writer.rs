use arrow::array::RecordBatch;
use sqd_data_core::{Downcast, PreparedTable, TableProcessor};
use parquet::basic::{ZstdLevel, Compression};
use parquet::file::properties::WriterProperties;

use crate::fs::Fs;
use crate::sink::Writer;

use sqd_data::solana::model::Block;
use sqd_data::solana::tables::{
    BlockBuilder, BalanceBuilder, InstructionBuilder, LogMessageBuilder,
    RewardBuilder, TokenBalanceBuilder, TransactionBuilder
};
use sqd_array::slice::AsSlice;

pub struct ParquetWriter {
    memory_treshold: usize,

    block_builder: BlockBuilder,
    transaction_builder: TransactionBuilder,
    instruction_builder: InstructionBuilder,
    log_message_builder: LogMessageBuilder,
    balance_builder: BalanceBuilder,
    token_balance_builder: TokenBalanceBuilder,
    reward_builder: RewardBuilder,

    block_processor: TableProcessor,
    transaction_processor: TableProcessor,
    instruction_processor: TableProcessor,
    log_message_processor: TableProcessor,
    balance_processor: TableProcessor,
    token_balance_processor: TableProcessor,
    reward_processor: TableProcessor,
}

impl ParquetWriter {
    pub fn new() -> ParquetWriter {
        let block_builder = BlockBuilder::new();
        let transaction_builder = TransactionBuilder::new();
        let instruction_builder = InstructionBuilder::new();
        let log_message_builder = LogMessageBuilder::new();
        let balance_builder = BalanceBuilder::new();
        let token_balance_builder = TokenBalanceBuilder::new();
        let reward_builder = RewardBuilder::new();

        let block_processor = TableProcessor::new(
            Downcast::new(),
            block_builder.schema(),
            BlockBuilder::table_description(),
        )
        .unwrap();
        let transaction_processor = TableProcessor::new(
            Downcast::new(),
            transaction_builder.schema(),
            TransactionBuilder::table_description(),
        )
        .unwrap();
        let instruction_processor = TableProcessor::new(
            Downcast::new(),
            instruction_builder.schema(),
            InstructionBuilder::table_description(),
        )
        .unwrap();
        let log_message_processor = TableProcessor::new(
            Downcast::new(),
            log_message_builder.schema(),
            LogMessageBuilder::table_description(),
        )
        .unwrap();
        let balance_processor = TableProcessor::new(
            Downcast::new(),
            balance_builder.schema(),
            BalanceBuilder::table_description(),
        )
        .unwrap();
        let token_balance_processor = TableProcessor::new(
            Downcast::new(),
            token_balance_builder.schema(),
            TokenBalanceBuilder::table_description(),
        )
        .unwrap();
        let reward_processor = TableProcessor::new(
            Downcast::new(),
            reward_builder.schema(),
            RewardBuilder::table_description(),
        )
        .unwrap();

        ParquetWriter {
            memory_treshold: 10 * 1024 * 1024,

            block_builder,
            transaction_builder,
            instruction_builder,
            log_message_builder,
            balance_builder,
            token_balance_builder,
            reward_builder,

            block_processor,
            transaction_processor,
            instruction_processor,
            log_message_processor,
            balance_processor,
            token_balance_processor,
            reward_processor,
        }
    }

    fn spill_on_disk(&mut self) {
        let slice = self.block_builder.as_slice();
        self.block_processor.push_batch(&slice).unwrap();
        self.block_builder.clear();

        let slice = self.transaction_builder.as_slice();
        self.transaction_processor.push_batch(&slice).unwrap();
        self.transaction_builder.clear();

        let slice = self.instruction_builder.as_slice();
        self.instruction_processor.push_batch(&slice).unwrap();
        self.instruction_builder.clear();

        let slice = self.log_message_builder.as_slice();
        self.log_message_processor.push_batch(&slice).unwrap();
        self.log_message_builder.clear();

        let slice = self.balance_builder.as_slice();
        self.balance_processor.push_batch(&slice).unwrap();
        self.balance_builder.clear();

        let slice = self.token_balance_builder.as_slice();
        self.token_balance_processor.push_batch(&slice).unwrap();
        self.token_balance_builder.clear();

        let slice = self.reward_builder.as_slice();
        self.reward_processor.push_batch(&slice).unwrap();
        self.reward_builder.clear();
    }

    fn builder_byte_size(&mut self) -> usize {
        self.block_builder.byte_size()
            + self.transaction_builder.byte_size()
            + self.instruction_builder.byte_size()
            + self.log_message_builder.byte_size()
            + self.balance_builder.byte_size()
            + self.token_balance_builder.byte_size()
            + self.reward_builder.byte_size()
    }
}

impl Writer<Block> for ParquetWriter {
    fn push(&mut self, block: Block) {
        let block_number = block.header.height;
        self.block_builder.push(&block.header);

        for transaction in &block.transactions {
            self.transaction_builder.push(block_number, transaction);
        }

        for instruction in &block.instructions {
            self.instruction_builder.push(block_number, instruction);
        }

        for log_message in &block.logs {
            self.log_message_builder.push(block_number, log_message);
        }

        for balance in &block.balances {
            self.balance_builder.push(block_number, balance);
        }

        for token_balance in &block.token_balances {
            self.token_balance_builder.push(block_number, token_balance);
        }

        for reward in &block.rewards {
            self.reward_builder.push(block_number, reward);
        }

        if self.builder_byte_size() > self.memory_treshold {
            self.spill_on_disk();
        }
    }

    fn buffered_bytes(&self) -> usize {
        self.block_builder.byte_size()
            + self.transaction_builder.byte_size()
            + self.instruction_builder.byte_size()
            + self.log_message_builder.byte_size()
            + self.balance_builder.byte_size()
            + self.token_balance_builder.byte_size()
            + self.reward_builder.byte_size()
            + self.block_processor.byte_size()
            + self.transaction_processor.byte_size()
            + self.instruction_processor.byte_size()
            + self.log_message_processor.byte_size()
            + self.balance_processor.byte_size()
            + self.token_balance_processor.byte_size()
            + self.reward_processor.byte_size()
    }

    fn flush(&mut self, fs: Box<dyn Fs>) -> anyhow::Result<()> {
        if self.builder_byte_size() > 300 {
            self.spill_on_disk();
        }

        let zstd_level = ZstdLevel::try_new(3)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(zstd_level))
            .set_data_page_size_limit(32 * 1024)
            .set_dictionary_page_size_limit(192 * 1024)
            .set_write_batch_size(50)
            .set_max_row_group_size(5_000)
            .build();
        let desc = TransactionBuilder::table_description();
        let new_processor = TableProcessor::new(Downcast::new(), self.transaction_builder.schema(), desc)?;
        let processor = std::mem::replace(&mut self.transaction_processor, new_processor);
        let table = processor.finish()?;
        let schema = table.schema();
        let batches = iterator(table);
        fs.write_parquet("transactions.parquet", batches, schema, Some(props))?;

        let zstd_level = ZstdLevel::try_new(3)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(zstd_level))
            .set_data_page_size_limit(32 * 1024)
            .set_dictionary_page_size_limit(192 * 1024)
            .set_write_batch_size(50)
            .set_max_row_group_size(20_000)
            .build();
        let desc = InstructionBuilder::table_description();
        let new_processor = TableProcessor::new(Downcast::new(), self.instruction_builder.schema(), desc)?;
        let processor = std::mem::replace(&mut self.instruction_processor, new_processor);
        let table = processor.finish()?;
        let schema = table.schema();
        let batches = iterator(table);
        fs.write_parquet("instructions.parquet", batches, schema, Some(props))?;

        let zstd_level = ZstdLevel::try_new(3)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(zstd_level))
            .set_data_page_size_limit(32 * 1024)
            .set_dictionary_page_size_limit(192 * 1024)
            .set_write_batch_size(50)
            .set_max_row_group_size(5_000)
            .build();
        let desc = LogMessageBuilder::table_description();
        let new_processor = TableProcessor::new(Downcast::new(), self.log_message_builder.schema(), desc)?;
        let processor = std::mem::replace(&mut self.log_message_processor, new_processor);
        let table = processor.finish()?;
        let schema = table.schema();
        let batches = iterator(table);
        fs.write_parquet("logs.parquet", batches, schema, Some(props))?;

        let zstd_level = ZstdLevel::try_new(3)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(zstd_level))
            .set_data_page_size_limit(32 * 1024)
            .set_dictionary_page_size_limit(192 * 1024)
            .set_write_batch_size(50)
            .set_max_row_group_size(5_000)
            .build();
        let desc = BalanceBuilder::table_description();
        let new_processor = TableProcessor::new(Downcast::new(), self.balance_builder.schema(), desc)?;
        let processor = std::mem::replace(&mut self.balance_processor, new_processor);
        let table = processor.finish()?;
        let schema = table.schema();
        let batches = iterator(table);
        fs.write_parquet("balances.parquet", batches, schema, Some(props))?;

        let zstd_level = ZstdLevel::try_new(3)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(zstd_level))
            .set_data_page_size_limit(32 * 1024)
            .set_dictionary_page_size_limit(192 * 1024)
            .set_write_batch_size(50)
            .set_max_row_group_size(5_000)
            .build();
        let desc = TokenBalanceBuilder::table_description();
        let new_processor = TableProcessor::new(Downcast::new(), self.token_balance_builder.schema(), desc)?;
        let processor = std::mem::replace(&mut self.token_balance_processor, new_processor);
        let table = processor.finish()?;
        let schema = table.schema();
        let batches = iterator(table);
        fs.write_parquet("token_balances.parquet", batches, schema, Some(props))?;

        let zstd_level = ZstdLevel::try_new(3)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(zstd_level))
            .set_data_page_size_limit(32 * 1024)
            .set_dictionary_page_size_limit(192 * 1024)
            .set_write_batch_size(50)
            .set_max_row_group_size(5_000)
            .build();
        let desc = RewardBuilder::table_description();
        let new_processor = TableProcessor::new(Downcast::new(), self.reward_builder.schema(), desc)?;
        let processor = std::mem::replace(&mut self.reward_processor, new_processor);
        let table = processor.finish()?;
        let schema = table.schema();
        let batches = iterator(table);
        fs.write_parquet("rewards.parquet", batches, schema, Some(props))?;

        let zstd_level = ZstdLevel::try_new(3)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(zstd_level))
            .set_data_page_size_limit(32 * 1024)
            .set_dictionary_page_size_limit(192 * 1024)
            .set_write_batch_size(50)
            .set_max_row_group_size(5_000)
            .build();
        let desc = BlockBuilder::table_description();
        let new_processor = TableProcessor::new(Downcast::new(), self.block_builder.schema(), desc)?;
        let processor = std::mem::replace(&mut self.block_processor, new_processor);
        let table = processor.finish()?;
        let schema = table.schema();
        let batches = iterator(table);
        fs.write_parquet("blocks.parquet", batches, schema, Some(props))?;

        Ok(())
    }

    fn get_block_height(&self, block: &Block) -> u64 {
        block.header.height
    }

    fn get_block_hash<'a>(&self, block: &'a Block) -> &'a String {
        &block.header.hash
    }
}


fn iterator(mut table: PreparedTable) -> Box<dyn Iterator<Item = RecordBatch>> {
    let step = 50;
    let mut offset = 0;
    let num_rows = table.num_rows();
    Box::new(std::iter::from_fn(move || {
        let len = std::cmp::min(num_rows - offset, step);
        debug_assert!(offset <= num_rows);
        if offset == num_rows {
            None
        } else {
            let batch = table.read_record_batch(offset, len).unwrap();
            offset += len;
            Some(batch)
        }
    }))
}
