use crate::hyperliquid::model::{Block, Action, Status};
use sqd_array::builder::{ListBuilder, StringBuilder, UInt32Builder, UInt64Builder};
use sqd_data_core::table_builder;


type JsonBuilder = StringBuilder;
type AssetList = ListBuilder<UInt64Builder>;


table_builder! {
    ActionBuilder {
        block_number: UInt64Builder,
        action_index: UInt32Builder,
        user: StringBuilder,
        action_type: StringBuilder,
        action: JsonBuilder,
        action_size: UInt64Builder,
        signature: JsonBuilder,
        nonce: UInt64Builder,
        vault_address: StringBuilder,
        status: StringBuilder,
        response: JsonBuilder,
        response_size: UInt64Builder,

        order_assets: AssetList,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["action_index"];
        d.sort_key = vec!["action_type", "vault_address", "user", "block_number", "action_index"];
        d.options.add_stats("action_type");
        d.options.add_stats("vault_address");
        d.options.add_stats("user");
        d.options.add_stats("block_number");
        d.options.use_dictionary("action_type");
        d.options.use_dictionary("user");
        d.options.use_dictionary("vault_address");
        d.options.use_dictionary("status");
        d.options.row_group_size = 10_000;
    }
}


impl ActionBuilder {
    pub fn push(&mut self, block: &Block, action: &Action) -> anyhow::Result<()> {
        self.block_number.append(block.header.height);
        self.action_index.append(action.action_index as u32);
        self.user.append_option(action.user.as_deref());
        self.action_type.append(&action.action.r#type);

        let action_json = serde_json::to_string(&action.action)?;
        self.action.append(&action_json);
        self.action_size.append(action_json.len() as u64);

        let signature = serde_json::to_string(&action.signature)?;
        self.signature.append(&signature);

        self.nonce.append(action.nonce);
        self.vault_address.append_option(action.vault_address.as_deref());

        let status = match action.status {
            Status::Ok => "ok",
            Status::Err => "err",
        };
        self.status.append(status);

        let response_json = serde_json::to_string(&action.response)?;
        self.response.append(&response_json);
        self.response_size.append(response_json.len() as u64);

        if action.action.r#type == "order" {
            for order in action.action.data.get("orders").unwrap().as_array().unwrap() {
                self.order_assets.values().append(order.get("a").unwrap().as_u64().unwrap());
            }
            self.order_assets.append();
        } else {
            self.order_assets.append_null();
        }

        Ok(())
    }
}
