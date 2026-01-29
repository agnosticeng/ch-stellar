use super::result::{Result, StellarError};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use stellar_xdr::curr::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedLedger {
    pub ext: LedgerCloseMetaExt,
    pub ledger_header: LedgerHeaderHistoryEntry,
    pub tx_set: VecM<TransactionEnvelope>,
    pub tx_processing: VecM<TransactionResultMetaV1>,
    pub upgrades_processing: VecM<UpgradeEntryMeta>,
    pub scp_info: VecM<ScpHistoryEntry>,
    pub total_byte_size_of_live_soroban_state: u64,
    pub evicted_keys: VecM<LedgerKey>,
}

impl NormalizedLedger {
    pub fn try_from_ledger_close_meta(lcm: LedgerCloseMeta, network_id: [u8; 32]) -> Result<Self> {
        let normalized_ledger = match lcm {
            LedgerCloseMeta::V0(lcmv0) => {
                let tx_processing = lcmv0
                    .tx_processing
                    .into_vec()
                    .into_iter()
                    .map(upgrade_transaction_result_meta)
                    .collect::<Vec<_>>()
                    .try_into()?;

                let tx_set = transform_tx_set(lcmv0.tx_set, &tx_processing, network_id)?;

                NormalizedLedger {
                    ext: LedgerCloseMetaExt::default(),
                    ledger_header: lcmv0.ledger_header,
                    tx_set,
                    tx_processing,
                    upgrades_processing: lcmv0.upgrades_processing,
                    scp_info: lcmv0.scp_info,
                    total_byte_size_of_live_soroban_state: 0,
                    evicted_keys: VecM::default(),
                }
            }
            LedgerCloseMeta::V1(lcmv1) => {
                let tx_processing = lcmv1
                    .tx_processing
                    .into_vec()
                    .into_iter()
                    .map(upgrade_transaction_result_meta)
                    .collect::<Vec<_>>()
                    .try_into()?;

                let tx_set = transform_generalized_transaction_set(
                    lcmv1.tx_set,
                    &tx_processing,
                    network_id,
                )?;

                NormalizedLedger {
                    ext: lcmv1.ext,
                    ledger_header: lcmv1.ledger_header,
                    tx_set,
                    tx_processing,
                    upgrades_processing: lcmv1.upgrades_processing,
                    scp_info: lcmv1.scp_info,
                    total_byte_size_of_live_soroban_state: lcmv1
                        .total_byte_size_of_live_soroban_state,
                    evicted_keys: lcmv1.evicted_keys,
                }
            }
            LedgerCloseMeta::V2(lcmv2) => {
                let tx_set = transform_generalized_transaction_set(
                    lcmv2.tx_set,
                    &lcmv2.tx_processing,
                    network_id,
                )?;

                NormalizedLedger {
                    ext: lcmv2.ext,
                    ledger_header: lcmv2.ledger_header,
                    tx_set,
                    tx_processing: lcmv2.tx_processing,
                    upgrades_processing: lcmv2.upgrades_processing,
                    scp_info: lcmv2.scp_info,
                    total_byte_size_of_live_soroban_state: lcmv2
                        .total_byte_size_of_live_soroban_state,
                    evicted_keys: lcmv2.evicted_keys,
                }
            }
        };

        if normalized_ledger.tx_processing.len() != normalized_ledger.tx_set.len() {
            return Err(StellarError::UnmatchedTxEnvelope);
        }

        Ok(normalized_ledger)
    }
}

fn upgrade_transaction_result_meta(trm: TransactionResultMeta) -> TransactionResultMetaV1 {
    TransactionResultMetaV1 {
        ext: ExtensionPoint::V0,
        result: trm.result,
        fee_processing: trm.fee_processing,
        tx_apply_processing: trm.tx_apply_processing,
        post_tx_apply_fee_processing: LedgerEntryChanges::default(),
    }
}

fn transform_tx_set(
    tx_set: TransactionSet,
    tx_processing: &VecM<TransactionResultMetaV1>,
    network_id: [u8; 32],
) -> Result<VecM<TransactionEnvelope>> {
    let hash_to_index: HashMap<Hash, usize> = tx_processing
        .iter()
        .enumerate()
        .map(|(i, v)| (v.result.transaction_hash.clone(), i))
        .collect();

    Ok(tx_set
        .txs
        .into_vec()
        .into_iter()
        .map(|e| {
            let h = e.hash(network_id)?;
            let i = hash_to_index
                .get(&Hash(h))
                .ok_or(StellarError::UnmatchedTxEnvelope)?;
            Ok((i, e))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .sorted_by_cached_key(|(i, _)| *i)
        .map(|(_, e)| e)
        .collect::<Vec<_>>()
        .try_into()?)
}

fn transform_generalized_transaction_set(
    tx_set: GeneralizedTransactionSet,
    tx_processing: &VecM<TransactionResultMetaV1>,
    network_id: [u8; 32],
) -> Result<VecM<TransactionEnvelope>> {
    let hash_to_index: HashMap<Hash, usize> = tx_processing
        .iter()
        .enumerate()
        .map(|(i, v)| (v.result.transaction_hash.clone(), i))
        .collect();

    let mut txss = Vec::new();

    match tx_set {
        GeneralizedTransactionSet::V1(set) => {
            set.phases
                .into_vec()
                .into_iter()
                .for_each(|phase| match phase {
                    TransactionPhase::V0(components) => {
                        components
                            .into_vec()
                            .into_iter()
                            .for_each(|comp| match comp {
                                TxSetComponent::TxsetCompTxsMaybeDiscountedFee(ts) => {
                                    txss.push(ts.txs)
                                }
                            })
                    }
                    TransactionPhase::V1(par_components) => par_components
                        .execution_stages
                        .into_vec()
                        .into_iter()
                        .for_each(|stage| match stage {
                            ParallelTxExecutionStage(tx_cluster) => tx_cluster
                                .into_vec()
                                .into_iter()
                                .for_each(|cluster| match cluster {
                                    DependentTxCluster(txs) => txss.push(txs),
                                }),
                        }),
                })
        }
    }

    Ok(txss
        .into_iter()
        .flat_map(|v| v.into_vec())
        .map(|e: TransactionEnvelope| {
            let h = e.hash(network_id)?;
            let i = hash_to_index
                .get(&Hash(h))
                .ok_or(StellarError::UnmatchedTxEnvelope)?;
            Ok((i, e))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .sorted_by_cached_key(|(i, _)| *i)
        .map(|(_, e)| e)
        .collect::<Vec<_>>()
        .try_into()?)
}
