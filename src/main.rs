use anyhow::Result;
use clap::Parser;
use mimalloc::MiMalloc;

pub mod arrow_ext;
pub mod json_rpc;
pub mod stellar;
pub mod cli;
pub mod json_result;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<()> {
    cli::CLI::parse().run()
}