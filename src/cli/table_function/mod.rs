mod stellar_galexie;
mod stellar_galexie_normalized;
mod stellar_rpc;

use anyhow::Result;
use clap::{Args, Subcommand};
use stellar_galexie::StellarGalexieCommand;
use stellar_galexie_normalized::StellarGalexieNormalizedCommand;
use stellar_rpc::StellarRpcCommand;

#[derive(Debug, Clone, Subcommand)]
pub enum TableFunctionCommand {
    Galexie(StellarGalexieCommand),
    GalexieNormalized(StellarGalexieNormalizedCommand),
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
            TableFunctionCommand::GalexieNormalized(cmd) => cmd.run().await,
            TableFunctionCommand::Rpc(cmd) => cmd.run().await,
        }
    }
}
