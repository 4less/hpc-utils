use batchelor::{run, Cli};
use clap::Parser;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    run(cli)
}
