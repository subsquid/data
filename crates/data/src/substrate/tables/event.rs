use crate::substrate::model::Event;
use crate::substrate::tables::common::{BytesBuilder, CallAddressListBuilder, JsonBuilder, TopicsListBuilder};
use sqd_array::builder::{StringBuilder, UInt32Builder, UInt64Builder};
use sqd_data_core::table_builder;
use sqd_primitives::BlockNumber;


table_builder! {
    EventBuilder {
        block_number: UInt64Builder,
        index: UInt32Builder,
        name: StringBuilder,
        args: JsonBuilder,
        phase: StringBuilder,
        extrinsic_index: UInt32Builder,
        call_address: CallAddressListBuilder,
        topics: TopicsListBuilder,
        _evm_log_address: BytesBuilder,
        _evm_log_topic0: BytesBuilder,
        _evm_log_topic1: BytesBuilder,
        _evm_log_topic2: BytesBuilder,
        _evm_log_topic3: BytesBuilder,
        _contract_address: BytesBuilder,
        _gear_program_id: BytesBuilder,
        args_size: UInt64Builder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["index", "extrinsic_index", "call_address"];
        d.sort_key = vec![
            "name",
            "_evm_log_address",
            "_evm_log_topic0",
            "_contract_address",
            "_gear_program_id",
            "block_number",
            "index"
        ];
        d.options.add_stats("name");
        d.options.add_stats("_evm_log_address");
        d.options.add_stats("_evm_log_topic0");
        d.options.add_stats("_contract_address");
        d.options.add_stats("_gear_program_id");
        d.options.add_stats("block_number");
        d.options.use_dictionary("name");
        d.options.use_dictionary("phase");
        d.options.use_dictionary("_evm_log_address");
        d.options.use_dictionary("_evm_log_topic0");
        d.options.use_dictionary("_contract_address");
        d.options.use_dictionary("_gear_program_id");
        d.options.row_group_size = 20_000;
    }
}


impl EventBuilder {
    pub fn push(&mut self, block_number: BlockNumber, row: &Event) {
        self.block_number.append(block_number);
        self.index.append(row.index);
        self.name.append(&row.name);

        let args = row.args.to_string();
        self.args.append(&args);
        self.args_size.append(args.len() as u64);

        self.phase.append(&serde_json::to_string(&row.phase).unwrap());
        self.extrinsic_index.append(row.extrinsic_index);

        if let Some(call_address) = &row.call_address {
            for address in call_address {
                self.call_address.values().append(*address);
            }
        }
        self.call_address.append();

        for topic in &row.topics {
            self.topics.values().append(topic);
        }
        self.topics.append();

        self._evm_log_address.append_option(row._evm_log_address.as_ref().map(|val| val.as_str()));
        self._evm_log_topic0.append_option(row.topics.first().map(|val| val.as_str()));
        self._evm_log_topic1.append_option(row.topics.get(1).map(|val| val.as_str()));
        self._evm_log_topic2.append_option(row.topics.get(2).map(|val| val.as_str()));
        self._evm_log_topic3.append_option(row.topics.get(3).map(|val| val.as_str()));
        self._contract_address.append_option(row._contract_address.as_ref().map(|val| val.as_str()));
        self._gear_program_id.append_option(row._gear_program_id.as_ref().map(|val| val.as_str()));
    }
}
