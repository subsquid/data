use crate::substrate::model::Extrinsic;
use crate::substrate::tables::common::{BytesBuilder, JsonBuilder};
use sqd_array::builder::{BooleanBuilder, Decimal128Builder, UInt32Builder, UInt64Builder};
use sqd_data_core::table_builder;
use sqd_primitives::BlockNumber;


table_builder! {
    ExtrinsicBuilder {
        block_number: UInt64Builder,
        index: UInt32Builder,
        version: UInt32Builder,
        signature: JsonBuilder,
        fee: Decimal128Builder,
        tip: Decimal128Builder,
        error: JsonBuilder,
        success: BooleanBuilder,
        hash: BytesBuilder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["index"];
        d.options.add_stats("block_number");
        d.options.row_group_size = 5_000;
    }
}


impl ExtrinsicBuilder {
    pub fn push(&mut self, block_number: BlockNumber, row: &Extrinsic) {
        self.block_number.append(block_number);
        self.index.append(row.index);
        self.version.append(row.version);

        {
            let signature = row.signature.as_ref().map(|val| val.to_string());
            self.signature.append_option(signature.as_ref().map(|val| val.as_str()));
        }

        self.fee.append_option(row.fee.map(|val| val.try_into().unwrap()));
        self.tip.append_option(row.tip.map(|val| val.try_into().unwrap()));

        {
            let error = row.error.as_ref().map(|val| val.to_string());
            self.error.append_option(error.as_ref().map(|s| s.as_ref()));
        }

        self.success.append(row.success);
        self.hash.append(&row.hash);
    }
}
