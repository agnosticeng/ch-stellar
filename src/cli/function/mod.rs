mod stellar_galexie_tip;
mod stellar_hash_transaction;
mod stellar_rpc;
mod stellar_xdr_decode;
mod stellar_id;

use anyhow::Result;
use clap::{Args, Subcommand};
use stellar_galexie_tip::StellarGalexieTipCommand;
use stellar_hash_transaction::StellarHashTransactionCommand;
use stellar_rpc::StellarRpcCommand;
use stellar_xdr_decode::StellarXdrDecodeCommand;
use stellar_id::StellarIdCommand;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Subcommand)]
pub enum FunctionCommand {
    StellarRpc(StellarRpcCommand),
    StellarXdrDecode(StellarXdrDecodeCommand),
    StellarGalexieTip(StellarGalexieTipCommand),
    StellarHashTransaction(StellarHashTransactionCommand),
    StellarId(StellarIdCommand)
}

#[derive(Clone, Debug, Args)]
pub struct Function {
    #[command(subcommand)]
    pub cmd: FunctionCommand,
}

impl Function {
    pub async fn run(&self) -> Result<()> {
        match &self.cmd {
            FunctionCommand::StellarRpc(cmd) => cmd.run().await,
            FunctionCommand::StellarXdrDecode(cmd) => cmd.run().await,
            FunctionCommand::StellarGalexieTip(cmd) => cmd.run().await,
            FunctionCommand::StellarHashTransaction(cmd) => cmd.run().await,
            FunctionCommand::StellarId(cmd) => cmd.run().await
        }
    }
}
