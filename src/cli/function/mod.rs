mod stellar_rpc;
mod stellar_xdr_decode;

use clap::{Args, Subcommand};
use anyhow::Result;
use stellar_rpc::StellarRpcCommand;
use stellar_xdr_decode::StellarXdrDecodeCommand;

#[derive(Debug, Clone, Subcommand)]
pub enum FunctionCommand {
    StellarRpc(StellarRpcCommand),
    StellarXdrDecode(StellarXdrDecodeCommand)
}

#[derive(Clone, Debug, Args)]
pub struct Function {
    #[command(subcommand)]
    pub cmd: FunctionCommand
}

impl Function {
    pub async fn run(&self) -> Result<()> {
        match &self.cmd {
            FunctionCommand::StellarRpc(cmd) => cmd.run().await,
            FunctionCommand::StellarXdrDecode(cmd) => cmd.run().await
        }
    }
}

