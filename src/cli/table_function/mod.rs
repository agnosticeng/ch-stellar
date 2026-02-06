mod stellar_galexie;
mod stellar_rpc;

use anyhow::Result;
use clap::{Args, Subcommand};
use stellar_galexie::StellarGalexieCommand;
use stellar_rpc::StellarRpcCommand;

#[derive(Debug, Clone, Subcommand)]
pub enum TableFunctionCommand {
    Galexie(StellarGalexieCommand),
    Rpc(StellarRpcCommand),
}

#[derive(Clone, Debug, Args)]
pub struct TableFunction {
    #[command(subcommand)]
    pub cmd: TableFunctionCommand,
}

impl TableFunction {
    pub async fn run(&self) -> Result<()> {
        match &self.cmd {
            TableFunctionCommand::Galexie(cmd) => cmd.run().await,
            TableFunctionCommand::Rpc(cmd) => cmd.run().await,
        }
    }
}
