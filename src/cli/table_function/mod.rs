mod stellar_galexie;
mod stellar_rpc;

use clap::{Args, Subcommand};
use anyhow::Result;
use stellar_galexie::StellarGalexieCommand;
use stellar_rpc::StellarRpcCommand;


#[derive(Debug, Clone, Subcommand)]
pub enum TableFunctionCommand {
    StellarGalexie(StellarGalexieCommand),
    StellarRpc(StellarRpcCommand)
}

#[derive(Clone, Debug, Args)]
pub struct TableFunction {
    #[command(subcommand)]
    pub cmd: TableFunctionCommand
}

impl TableFunction {
    pub async fn run(&self) -> Result<()> {
        match &self.cmd {
            TableFunctionCommand::StellarGalexie(cmd) => cmd.run().await,
            TableFunctionCommand::StellarRpc(cmd) => cmd.run().await
        }
    }
}

