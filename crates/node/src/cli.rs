use clap::Parser;


#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct CLI {
    /// Config file to get dataset specs from
    #[arg(short, long, value_name = "FILE")]
    pub config: String,
    
    /// Database directory
    #[arg(long = "db", default_value = "node.db")]
    pub database_dir: String
}