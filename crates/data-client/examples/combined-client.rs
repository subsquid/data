use std::thread::sleep;
use std::time::Duration;

use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use sqd_data_client::CombinedClient;
use sqd_data_types::BlockNumber;

pub type Base58Bytes = String;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BlockHeader {
    pub hash: Base58Bytes,
    pub height: BlockNumber,
    pub slot: BlockNumber,
    pub parent_slot: BlockNumber,
    pub parent_hash: Base58Bytes,
    pub timestamp: i64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub header: BlockHeader,
}

impl sqd_data_types::Block for Block {
    fn number(&self) -> sqd_data_types::BlockNumber {
        self.header.height
    }

    fn hash(&self) -> &str {
        &self.header.hash
    }

    fn parent_number(&self) -> sqd_data_types::BlockNumber {
        self.number().saturating_sub(1)
    }

    fn parent_hash(&self) -> &str {
        &self.header.parent_hash
    }
}


#[tokio::main]
async fn main() {
    let req = reqwest::get("http://127.0.0.1:3000/head").await.unwrap();
    let res = req.json::<Value>().await.unwrap();
    sleep(Duration::from_secs(3));
    let start = res.get("number").unwrap().as_u64().unwrap();
    let client: CombinedClient<Block> = CombinedClient::from_url("http://127.0.0.1:3001", "http://127.0.0.1:3000");
    let mut stream = client.stream(start, "").await.unwrap();
    println!("S: {:?}", stream.finalized_head());
    while let Some(msg) = stream.next().await {
        println!("MSG: {:?}", msg);
    }
}
