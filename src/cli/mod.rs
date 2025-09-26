mod table_function;
mod function;

use clap::{Subcommand,Parser};
use anyhow::Result;
use tokio::runtime::Builder;
use crate::cli::table_function::TableFunction;
use crate::cli::function::Function;

#[derive(Debug, Subcommand)]
pub enum Command {
    TableFunction(TableFunction),
    Function(Function)
}

#[derive(Parser)]
pub struct CLI {
    #[command(subcommand)]
    pub cmd: Command,
}

impl CLI {
    pub fn run(&self) -> Result<()> {
        Builder::new_multi_thread()
            .enable_time()
            .enable_io()
            .build()?
            .block_on(async {
                match &self.cmd {
                    Command::TableFunction(cmd) => cmd.run().await,
                    Command::Function(cmd) => cmd.run().await
                }
            })
    }
}
