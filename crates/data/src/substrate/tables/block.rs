use crate::substrate::model::BlockHeader;
use crate::substrate::tables::common::*;
use sqd_array::builder::{TimestampMillisecondBuilder, UInt64Builder, UInt32Builder, StringBuilder};
use sqd_data_core::table_builder;


table_builder! {
    BlockBuilder {
        number: UInt64Builder,
        hash: BytesBuilder,
        parent_hash: BytesBuilder,
        state_root: BytesBuilder,
        extrinsics_root: BytesBuilder,
        digest: JsonBuilder,
        spec_name: StringBuilder,
        spec_version: UInt32Builder,
        impl_name: StringBuilder,
        impl_version: UInt32Builder,
        timestamp: TimestampMillisecondBuilder,
        validator: BytesBuilder,
    }

    description(d) {
        d.downcast.block_number = vec!["number"];
        d.sort_key = vec!["number"];
        d.options.add_stats("number");
        d.options.use_dictionary("spec_name");
        d.options.use_dictionary("impl_name");
        d.options.use_dictionary("validator");
        d.options.row_group_size = 5_000;
    }
}


impl BlockBuilder {
    pub fn push(&mut self, row: &BlockHeader) {
        self.number.append(row.height);
        self.hash.append(&row.hash);
        self.parent_hash.append(&row.parent_hash);
        self.state_root.append(&row.state_root);
        self.extrinsics_root.append(&row.extrinsics_root);
        self.digest.append(&serde_json::to_string(&row.digest).unwrap());
        self.spec_name.append(&row.spec_name);
        self.spec_version.append(row.spec_version);
        self.impl_name.append(&row.impl_name);
        self.impl_version.append(row.impl_version);
        self.timestamp.append_option(row.timestamp);
        self.validator.append_option(row.validator.as_ref().map(|v| v.as_str()));
    }
}
