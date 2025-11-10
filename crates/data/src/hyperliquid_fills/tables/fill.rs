use crate::hyperliquid_fills::model::{Block, Fill, Side};
use sqd_array::builder::{BooleanBuilder, Float64Builder, StringBuilder, TimestampMillisecondBuilder, UInt32Builder, UInt64Builder};
use sqd_data_core::table_builder;


table_builder! {
    FillBuilder {
        block_number: UInt64Builder,
        fill_index: UInt32Builder,
        user: StringBuilder,
        coin: StringBuilder,
        px: Float64Builder,
        sz: Float64Builder,
        side: StringBuilder,
        time: TimestampMillisecondBuilder,
        start_position: Float64Builder,
        dir: StringBuilder,
        closed_pnl: Float64Builder,
        hash: StringBuilder,
        oid: UInt64Builder,
        crossed: BooleanBuilder,
        fee: Float64Builder,
        builder_fee: Float64Builder,
        tid: UInt64Builder,
        cloid: StringBuilder,
        fee_token: StringBuilder,
        builder: StringBuilder,
        twap_id: UInt64Builder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["fill_index"];
        d.sort_key = vec![
            "user",
            "coin",
            "block_number",
            "fill_index"
        ];
        d.options.add_stats("user");
        d.options.add_stats("coin");
        d.options.add_stats("cloid");
        d.options.add_stats("side");
        d.options.add_stats("dir");
        d.options.add_stats("fee_token");
        d.options.add_stats("builder");
        d.options.add_stats("block_number");
        d.options.use_dictionary("user");
        d.options.use_dictionary("coin");
        d.options.use_dictionary("side");
        d.options.use_dictionary("dir");
        d.options.use_dictionary("fee_token");
        d.options.use_dictionary("builder");
        d.options.row_group_size = 20_000;
    }
}


impl FillBuilder {
    pub fn push(&mut self, block: &Block, fill: &Fill) -> anyhow::Result<()> {
        self.block_number.append(block.header.number);
        self.fill_index.append(fill.fill_index);
        self.user.append(&fill.user);
        self.coin.append(&fill.coin);
        self.px.append(fill.px);
        self.sz.append(fill.sz);
        self.side.append(match fill.side {
            Side::A => "A",
            Side::B => "B"
        });
        self.time.append(fill.time);
        self.start_position.append(fill.start_position);
        self.dir.append(&fill.dir);
        self.closed_pnl.append(fill.closed_pnl);
        self.hash.append(&fill.hash);
        self.oid.append(fill.oid);
        self.crossed.append(fill.crossed);
        self.fee.append(fill.fee);
        self.builder_fee.append_option(fill.builder_fee);
        self.tid.append(fill.tid);
        self.cloid.append_option(fill.cloid.as_deref());
        self.fee_token.append(&fill.fee_token);
        self.builder.append_option(fill.builder.as_deref());
        self.twap_id.append_option(fill.twap_id);
        Ok(())
    }
}
