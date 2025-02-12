use crate::substrate::model::Call;
use crate::substrate::tables::common::{BytesBuilder, CallAddressListBuilder, JsonBuilder};
use sqd_array::builder::{BooleanBuilder, StringBuilder, UInt32Builder, UInt64Builder};
use sqd_data_core::table_builder;
use sqd_primitives::BlockNumber;


table_builder! {
    CallBuilder {
        block_number: UInt64Builder,
        extrinsic_index: UInt32Builder,
        address: CallAddressListBuilder,
        name: StringBuilder,
        args: JsonBuilder,
        origin: JsonBuilder,
        error: JsonBuilder,
        success: BooleanBuilder,
        _ethereum_transact_to: BytesBuilder,
        _ethereum_transact_sighash: BytesBuilder,
        args_size: UInt64Builder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["extrinsic_index", "address"];
        d.sort_key = vec![
            "name",
            "_ethereum_transact_to",
            "_ethereum_transact_sighash",
            "block_number",
            "extrinsic_index"
        ];
        d.options.add_stats("block_number");
        d.options.add_stats("name");
        d.options.add_stats("_ethereum_transact_to");
        d.options.add_stats("_ethereum_transact_sighash");
        d.options.use_dictionary("name");
        d.options.use_dictionary("_ethereum_transact_to");
        d.options.use_dictionary("_ethereum_transact_sighash");
        d.options.row_group_size = 20_000;
    }
}


impl CallBuilder {
    pub fn push(&mut self, block_number: BlockNumber, row: &Call) {
        self.block_number.append(block_number);
        self.extrinsic_index.append(row.extrinsic_index);

        for address in &row.address {
            self.address.values().append(*address);
        }
        self.address.append();

        self.name.append(&row.name);

        let args = row.args.to_string();
        self.args.append(&args);
        self.args_size.append(args.len() as u64);

        {
            let origin = row.origin.as_ref().map(|val| val.to_string());
            self.origin.append_option(origin.as_ref().map(|s| s.as_ref()));
        }

        {
            let error = row.error.as_ref().map(|val| val.to_string());
            self.error.append_option(error.as_ref().map(|s| s.as_ref()));
        }

        self.success.append(row.success);
        self._ethereum_transact_to.append_option(row._ethereum_transact_to.as_ref().map(|val| val.as_str()));
        self._ethereum_transact_sighash.append_option(row._ethereum_transact_sighash.as_ref().map(|val| val.as_str()));
    }
}
