use crate::hyperliquid_replica_cmds::model::{Action, ActionData, Block, Status};
use anyhow::Context;
use sqd_array::builder::{ListBuilder, StringBuilder, UInt32Builder, UInt64Builder};
use sqd_data_core::table_builder;


type JsonBuilder = StringBuilder;
type AssetListBuilder = ListBuilder<UInt32Builder>;
type CloidListBuilder = ListBuilder<StringBuilder>;


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

        order_asset: AssetListBuilder,
        order_cloid: CloidListBuilder,

        cancel_asset: AssetListBuilder,

        cancel_by_cloid_asset: AssetListBuilder,
        cancel_by_cloid_cloid: CloidListBuilder,

        batch_modify_asset: AssetListBuilder,
        batch_modify_cloid: CloidListBuilder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["action_index"];
        d.sort_key = vec!["action_type", "user", "vault_address", "block_number", "action_index"];
        d.options.add_stats("action_type");
        d.options.add_stats("user");
        d.options.add_stats("vault_address");
        d.options.add_stats("status");
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

        // the list of possible types
        // https://github.com/hyperliquid-dex/hyperliquid-rust-sdk/blob/aac75585daf12d0a3761126cc7da7a5e035b5853/src/exchange/exchange_client.rs#L66
        self.append_order(&action.action)?;
        self.append_cancel(&action.action)?;
        self.append_cancel_by_cloid(&action.action)?;
        self.append_batch_modify(&action.action)?;

        Ok(())
    }

    fn append_order(&mut self, action: &ActionData) -> anyhow::Result<()> {
        if action.r#type == "order" {
            let orders = action
                .data
                .get("orders")
                .and_then(|val| val.as_array())
                .context("failed to parse orders")?;

            let mut order_asset = vec![];
            let mut order_cloid = vec![];
            for order in orders {
                let asset = order
                    .get("a")
                    .and_then(|val| val.as_u64())
                    .and_then(|asset| u32::try_from(asset).ok())
                    .context("failed to parse asset")?;

                if !order_asset.contains(&asset) {
                    order_asset.push(asset);
                }

                if let Some(value) = order.get("c") {
                    let cloid = value.as_str().context("failed to parse cloid")?;
                    if !order_cloid.contains(&cloid) {
                        order_cloid.push(cloid);
                    }
                }
            }

            for asset in order_asset {
                self.order_asset.values().append(asset);
            }
            self.order_asset.append();

            for cloid in order_cloid {
                self.order_cloid.values().append(cloid);
            }
            self.order_cloid.append();
        } else {
            self.order_asset.append_null();
            self.order_cloid.append_null();
        }

        Ok(())
    }

    fn append_cancel(&mut self, action: &ActionData) -> anyhow::Result<()> {
        if action.r#type == "cancel" {
            let cancels = action
                .data
                .get("cancels")
                .and_then(|val| val.as_array())
                .context("failed to parse cancels")?;

            let mut cancel_asset = vec![];
            for cancel in cancels {
                let asset = cancel
                    .get("a")
                    .and_then(|val| val.as_u64())
                    .and_then(|asset| u32::try_from(asset).ok())
                    .context("failed to parse asset")?;

                if !cancel_asset.contains(&asset) {
                    cancel_asset.push(asset);
                }
            }

            for asset in cancel_asset {
                self.cancel_asset.values().append(asset);
            }
            self.cancel_asset.append();
        } else {
            self.cancel_asset.append_null();
        }

        Ok(())
    }

    fn append_cancel_by_cloid(&mut self, action: &ActionData) -> anyhow::Result<()> {
        if action.r#type == "cancelByCloid" {
            let cancels = action
                .data
                .get("cancels")
                .and_then(|val| val.as_array())
                .context("failed to parse cancels")?;

            let mut cancel_by_cloid_asset = vec![];
            let mut cancel_by_cloid_cloid = vec![];
            for cancel in cancels {
                let asset = cancel
                    .get("asset")
                    .and_then(|val| val.as_u64())
                    .and_then(|asset| u32::try_from(asset).ok())
                    .context("failed to parse asset")?;

                if !cancel_by_cloid_asset.contains(&asset) {
                    cancel_by_cloid_asset.push(asset);
                }

                if let Some(value) = cancel.get("cloid") {
                    let cloid = value.as_str().context("failed to parse cloid")?;
                    if !cancel_by_cloid_cloid.contains(&cloid) {
                        cancel_by_cloid_cloid.push(cloid);
                    }
                }
            }

            for asset in cancel_by_cloid_asset {
                self.cancel_by_cloid_asset.values().append(asset);
            }
            self.cancel_by_cloid_asset.append();

            for cloid in cancel_by_cloid_cloid {
                self.cancel_by_cloid_cloid.values().append(cloid);
            }
            self.cancel_by_cloid_cloid.append();
        } else {
            self.cancel_by_cloid_asset.append_null();
            self.cancel_by_cloid_cloid.append_null();
        }

        Ok(())
    }

    fn append_batch_modify(&mut self, action: &ActionData) -> anyhow::Result<()> {
        if action.r#type == "batchModify" {
            let modifies = action
                .data
                .get("modifies")
                .and_then(|val| val.as_array())
                .context("failed to parse modifies")?;

            let mut batch_modify_asset = vec![];
            let mut batch_modify_cloid = vec![];
            for modify in modifies {
                let order = modify.get("order").context("failed to parse modify")?;
                let asset = order
                    .get("a")
                    .and_then(|val| val.as_u64())
                    .and_then(|asset| u32::try_from(asset).ok())
                    .context("failed to parse asset")?;

                if !batch_modify_asset.contains(&asset) {
                    batch_modify_asset.push(asset);
                }

                if let Some(value) = order.get("c") {
                    let cloid = value.as_str().context("failed to parse cloid")?;
                    if !batch_modify_cloid.contains(&cloid) {
                        batch_modify_cloid.push(cloid);
                    }
                }
            }

            for asset in batch_modify_asset {
                self.batch_modify_asset.values().append(asset);
            }
            self.batch_modify_asset.append();

            for cloid in batch_modify_cloid {
                self.batch_modify_cloid.values().append(cloid);
            }
            self.batch_modify_cloid.append();
        } else {
            self.batch_modify_asset.append_null();
            self.batch_modify_cloid.append_null();
        }

        Ok(())
    }
}
