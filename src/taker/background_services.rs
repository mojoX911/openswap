//! Background service threads for the Taker.
//!
//! Contains `RecoveryLoop` (periodic recovery retry) and `BreachDetector`
//! (adversarial spend monitoring) — standalone structs with their own
//! background threads, `Arc` state, and `Drop` impls.

use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering::Relaxed},
        Arc, Mutex, RwLock,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use bitcoin::{OutPoint, Txid};

use crate::{
    utill::HEART_BEAT_INTERVAL,
    wallet::{Blockchain, RecoveryReport, Wallet},
    watch_tower::{service::WatchService, watcher::WatcherEvent},
};

use super::swap_tracker::{ContractOutcome, ContractResolution, RecoveryPhase, SwapTracker};

/// Interval between recovery retry attempts.
#[cfg(not(feature = "integration-test"))]
const RECOVERY_LOOP_INTERVAL: Duration = Duration::from_secs(60);
#[cfg(feature = "integration-test")]
const RECOVERY_LOOP_INTERVAL: Duration = Duration::from_secs(10);

/// Background thread that periodically retries wallet-level recovery
/// (hashlock sweep + timelock recovery) until all contract UTXOs are resolved.
///
/// Spawned at the end of `recover_active_swap()` or `init_recover_incomplete()`
/// when some contracts remain unresolved (e.g. timelocks not yet mature).
pub(crate) struct RecoveryLoop {
    shutdown: Arc<AtomicBool>,
    complete: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl RecoveryLoop {
    /// Spawn the background recovery thread.
    ///
    /// The `swap_tracker` is used to update per-contract resolution outcomes
    /// as contracts are resolved in the background.
    pub(crate) fn start(
        wallet: Arc<RwLock<Wallet>>,
        swap_tracker: Arc<Mutex<SwapTracker>>,
        data_dir: PathBuf,
    ) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let complete = Arc::new(AtomicBool::new(false));

        let shutdown_clone = shutdown.clone();
        let complete_clone = complete.clone();

        let handle = thread::Builder::new()
            .name("Recovery loop".to_string())
            .spawn(move || {
                log::info!("Recovery loop started");
                while !shutdown_clone.load(Relaxed) {
                    // Sync wallet to refresh chain state
                    if let Ok(mut w) = wallet.write() {
                        if let Err(e) = w.sync_and_save() {
                            log::warn!("Recovery loop: sync failed: {:?}", e);
                        }
                    }

                    // Try hashlock sweep (incoming)
                    let incoming_result = if let Ok(mut w) = wallet.write() {
                        match w.sweep_incoming_swapcoins(2.0) {
                            Ok(ref swept) if !swept.is_empty() => {
                                log::info!(
                                    "Recovery loop: swept {} incoming swapcoins",
                                    swept.resolved.len()
                                );
                                Some(swept.clone())
                            }
                            Err(e) => {
                                log::debug!("Recovery loop: incoming sweep: {:?}", e);
                                None
                            }
                            _ => None,
                        }
                    } else {
                        None
                    };

                    // Try timelock recovery (outgoing)
                    let outgoing_result = if let Ok(mut w) = wallet.write() {
                        match w.recover_timelocked_swapcoins(2.0) {
                            Ok(ref recovered) if !recovered.is_empty() => {
                                log::info!(
                                    "Recovery loop: recovered {} timelocked swapcoins",
                                    recovered.len()
                                );
                                Some(recovered.clone())
                            }
                            Err(e) => {
                                log::debug!("Recovery loop: timelock recovery: {:?}", e);
                                None
                            }
                            _ => None,
                        }
                    } else {
                        None
                    };

                    // Update tracker outcomes from recovery results
                    if incoming_result.is_some() || outgoing_result.is_some() {
                        if let Ok(mut tracker) = swap_tracker.lock() {
                            Self::update_tracker_outcomes(
                                &mut tracker,
                                incoming_result.as_ref(),
                                outgoing_result.as_ref(),
                            );
                        }
                    }

                    // Check if all contract outpoints are resolved
                    let all_resolved = match wallet.read() {
                        Ok(w) => {
                            let outgoing = w.outgoing_contract_outpoints();
                            let incoming = w.incoming_contract_outpoints();
                            if outgoing.is_empty() && incoming.is_empty() {
                                true
                            } else {
                                outgoing.iter().chain(incoming.iter()).all(|op| {
                                    // Explicitly match that the contract transactions are spent.
                                    matches!(
                                        w.blockchain.get_tx_out(&op.txid, op.vout, None),
                                        Ok(None)
                                    )
                                })
                            }
                        }
                        Err(_) => false,
                    };

                    if all_resolved {
                        log::info!("Recovery loop: all contracts resolved");
                        // Clean up wallet entries and update tracker
                        let swap_ids: Vec<String> = swap_tracker
                            .lock()
                            .ok()
                            .map(|t| {
                                t.incomplete_swaps()
                                    .iter()
                                    .map(|r| r.swap_id.clone())
                                    .collect()
                            })
                            .unwrap_or_default();

                        if let Ok(mut w) = wallet.write() {
                            for swap_id in &swap_ids {
                                let keys = w.outgoing_keys_for_swap(swap_id);
                                for key in &keys {
                                    w.remove_outgoing_swapcoin(key);
                                }
                                w.remove_watchonly_swapcoins(swap_id);
                            }
                            let _ = w.save_to_disk();
                        }

                        if let Ok(mut tracker) = swap_tracker.lock() {
                            // Emit recovery reports before marking as cleaned up
                            for record in tracker.incomplete_swaps() {
                                let network = wallet
                                    .read()
                                    .map(|w| w.store.network.to_string())
                                    .unwrap_or_default();
                                let all_outcomes = record
                                    .recovery
                                    .incoming
                                    .iter()
                                    .chain(record.recovery.outgoing.iter());
                                let mut hashlock_txids: Vec<String> = Vec::new();
                                let mut timelock_txids: Vec<String> = Vec::new();
                                for o in all_outcomes {
                                    if let Some(txid) = o.spending_txid {
                                        match o.resolution {
                                            ContractResolution::Hashlock => {
                                                hashlock_txids.push(txid.to_string())
                                            }
                                            ContractResolution::Timelock => {
                                                timelock_txids.push(txid.to_string())
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                if !hashlock_txids.is_empty() {
                                    RecoveryReport::emit_taker(
                                        &data_dir,
                                        record.swap_id.clone(),
                                        network.clone(),
                                        "hashlock".to_string(),
                                        hashlock_txids,
                                    );
                                }
                                if !timelock_txids.is_empty() {
                                    RecoveryReport::emit_taker(
                                        &data_dir,
                                        record.swap_id.clone(),
                                        network,
                                        "timelock".to_string(),
                                        timelock_txids,
                                    );
                                }
                            }

                            for swap_id in &swap_ids {
                                let _ = tracker.update_and_save(swap_id, |r| {
                                    r.recovery.phase = RecoveryPhase::CleanedUp;
                                });
                            }
                        }
                        complete_clone.store(true, Relaxed);
                        return;
                    }

                    thread::sleep(RECOVERY_LOOP_INTERVAL);
                }
                log::info!("Recovery loop shut down");
            })
            .expect("failed to spawn recovery loop thread");

        Self {
            shutdown,
            complete,
            handle: Some(handle),
        }
    }

    /// Match resolved contract txids against tracker records and update outcomes.
    fn update_tracker_outcomes(
        tracker: &mut SwapTracker,
        incoming: Option<&crate::wallet::RecoveryOutcome>,
        outgoing: Option<&crate::wallet::RecoveryOutcome>,
    ) {
        let swap_ids: Vec<String> = tracker
            .incomplete_swaps()
            .iter()
            .map(|r| r.swap_id.clone())
            .collect();

        for swap_id in swap_ids {
            let mut changed = false;

            let _ = tracker.update_and_save(&swap_id, |record| {
                // Update incoming outcomes from sweep results
                if let Some(swept) = incoming {
                    for (contract_txid, spending_txid) in &swept.resolved {
                        if record.incoming_contract_txids.contains(contract_txid) {
                            // Find existing outcome or add new one
                            if let Some(outcome) = record
                                .recovery
                                .incoming
                                .iter_mut()
                                .find(|o| o.contract_txid == *contract_txid)
                            {
                                if outcome.resolution == ContractResolution::Unresolved {
                                    outcome.resolution = ContractResolution::Hashlock;
                                    outcome.spending_txid = Some(*spending_txid);
                                    changed = true;
                                }
                            } else {
                                record.recovery.incoming.push(ContractOutcome {
                                    contract_txid: *contract_txid,
                                    resolution: ContractResolution::Hashlock,
                                    spending_txid: Some(*spending_txid),
                                });
                                changed = true;
                            }
                        }
                    }
                }

                // Update outgoing outcomes from timelock recovery results
                if let Some(recovered) = outgoing {
                    for (contract_txid, spending_txid) in &recovered.resolved {
                        if record.outgoing_contract_txids.contains(contract_txid) {
                            if let Some(outcome) = record
                                .recovery
                                .outgoing
                                .iter_mut()
                                .find(|o| o.contract_txid == *contract_txid)
                            {
                                if outcome.resolution == ContractResolution::Unresolved {
                                    outcome.resolution = ContractResolution::Timelock;
                                    outcome.spending_txid = Some(*spending_txid);
                                    changed = true;
                                }
                            } else {
                                record.recovery.outgoing.push(ContractOutcome {
                                    contract_txid: *contract_txid,
                                    resolution: ContractResolution::Timelock,
                                    spending_txid: Some(*spending_txid),
                                });
                                changed = true;
                            }
                        }
                    }
                    for contract_txid in &recovered.discarded {
                        if record.outgoing_contract_txids.contains(contract_txid) {
                            if let Some(outcome) = record
                                .recovery
                                .outgoing
                                .iter_mut()
                                .find(|o| o.contract_txid == *contract_txid)
                            {
                                if outcome.resolution == ContractResolution::Unresolved {
                                    outcome.resolution = ContractResolution::Discarded;
                                    changed = true;
                                }
                            } else {
                                record.recovery.outgoing.push(ContractOutcome {
                                    contract_txid: *contract_txid,
                                    resolution: ContractResolution::Discarded,
                                    spending_txid: None,
                                });
                                changed = true;
                            }
                        }
                    }
                }

                // Advance recovery phase based on what was resolved
                if changed {
                    let all_incoming_done = record
                        .recovery
                        .incoming
                        .iter()
                        .all(|o| o.resolution != ContractResolution::Unresolved);
                    let all_outgoing_done = record
                        .recovery
                        .outgoing
                        .iter()
                        .all(|o| o.resolution != ContractResolution::Unresolved);

                    if all_outgoing_done && record.recovery.phase < RecoveryPhase::OutgoingRecovered
                    {
                        record.recovery.phase = RecoveryPhase::OutgoingRecovered;
                    } else if all_incoming_done
                        && record.recovery.phase < RecoveryPhase::IncomingRecovered
                    {
                        record.recovery.phase = RecoveryPhase::IncomingRecovered;
                    }
                }
            });
        }
    }

    /// Check whether recovery is complete.
    pub(crate) fn is_complete(&self) -> bool {
        self.complete.load(Relaxed)
    }
}

impl Drop for RecoveryLoop {
    fn drop(&mut self) {
        self.shutdown.store(true, Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// One watched contract outpoint and what a spend of it would prove.
#[derive(Debug, Clone)]
pub(crate) enum Sentinel {
    /// Legacy: the pre-signed contract tx is the fingerprint of a breach.
    Legacy {
        outpoint: OutPoint,
        contract_txid: Txid,
        /// The hop's parties, for the log.
        label: String,
        /// Set when exactly one peer besides us holds the contract tx.
        blame: Option<String>,
    },
    /// Taproot: the leaf script in the spending witness names the role.
    Taproot {
        outpoint: OutPoint,
        hashlock: bitcoin::ScriptBuf,
        timelock: bitcoin::ScriptBuf,
        /// The funder's address; None when the contract is ours.
        funder: Option<String>,
    },
}

impl Sentinel {
    fn outpoint(&self) -> OutPoint {
        match self {
            Sentinel::Legacy { outpoint, .. } | Sentinel::Taproot { outpoint, .. } => *outpoint,
        }
    }
}

/// The three ways a taproot contract output can be spent.
#[derive(Debug, PartialEq)]
enum TaprootSpend {
    Timelock,
    Hashlock,
    KeyPath,
}

/// Judge a taproot spend by the leaf script in its witness.
/// The annex rides last and starts with 0x50; drop it first, or a spender
/// hides the leaf by appending one.
fn classify_taproot_spend(
    witness: &[Vec<u8>],
    hashlock: &bitcoin::ScriptBuf,
    timelock: &bitcoin::ScriptBuf,
) -> TaprootSpend {
    let items = match witness {
        [rest @ .., last] if witness.len() >= 2 && last.first() == Some(&0x50) => rest,
        other => other,
    };
    match items {
        [.., leaf, _control] if leaf.as_slice() == timelock.as_bytes() => TaprootSpend::Timelock,
        [.., leaf, _control] if leaf.as_slice() == hashlock.as_bytes() => TaprootSpend::Hashlock,
        _ => TaprootSpend::KeyPath,
    }
}

/// Background thread that monitors sentinel outpoints for adversarial spends
/// via the WatchService, and hands out the verdict itself: the spend names the
/// culprit here, and nowhere else. Callers only abort on `is_breached()`.
pub(crate) struct BreachDetector {
    breached: Arc<AtomicBool>,
    sentinels: Arc<Mutex<Vec<Sentinel>>>,
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl BreachDetector {
    /// Spawn a background thread that polls the WatchService for sentinel spends.
    pub(crate) fn start(
        watch_service: WatchService,
        offerbook: super::offers::OfferBookHandle,
    ) -> Self {
        let breached = Arc::new(AtomicBool::new(false));
        let sentinels: Arc<Mutex<Vec<Sentinel>>> = Arc::new(Mutex::new(Vec::new()));
        let shutdown = Arc::new(AtomicBool::new(false));

        let breached_clone = breached.clone();
        let sentinels_clone = sentinels.clone();
        let shutdown_clone = shutdown.clone();

        let handle = thread::Builder::new()
            .name("Breach detector thread".to_string())
            .spawn(move || {
                while !shutdown_clone.load(Relaxed) {
                    thread::sleep(HEART_BEAT_INTERVAL);

                    let current_sentinels = match sentinels_clone.lock() {
                        Ok(guard) => guard.clone(),
                        Err(_) => continue,
                    };

                    for sentinel in &current_sentinels {
                        let outpoint = sentinel.outpoint();
                        // If a watch request fails, log the error, don't panic.
                        if let Err(e) = watch_service.watch_request(outpoint) {
                            log::error!("watch request for {outpoint} failed (watcher gone): {e}");
                            continue;
                        }
                        let Some(WatcherEvent::UtxoSpent {
                            spending_tx: Some(ref tx),
                            ..
                        }) = watch_service.wait_for_event()
                        else {
                            continue;
                        };

                        match sentinel {
                            Sentinel::Legacy {
                                contract_txid,
                                label,
                                blame,
                                ..
                            } => {
                                let actual_txid = tx.compute_txid();
                                if actual_txid != *contract_txid {
                                    // A different txid is the cooperative sweep.
                                    log::info!(
                                        "Breach detector: cooperative spend on {outpoint} (tx {actual_txid})"
                                    );
                                    continue;
                                }
                                match blame {
                                    Some(addr) => {
                                        log::warn!(
                                            "Breach detector: contract tx broadcast on {label}, banning {addr}"
                                        );
                                        offerbook.add_bad_maker(addr);
                                    }
                                    // Both hop parties hold the pre-signed tx, so a ban
                                    // would hit the victim as often as the cheat.
                                    None => log::warn!(
                                        "Breach detector: contract tx broadcast on {label}, either party could have"
                                    ),
                                }
                                breached_clone.store(true, Relaxed);
                                return;
                            }
                            Sentinel::Taproot {
                                hashlock,
                                timelock,
                                funder,
                                ..
                            } => {
                                let Some(input) =
                                    tx.input.iter().find(|i| i.previous_output == outpoint)
                                else {
                                    continue;
                                };
                                let witness: Vec<Vec<u8>> =
                                    input.witness.iter().map(|w| w.to_vec()).collect();
                                match classify_taproot_spend(&witness, hashlock, timelock) {
                                    TaprootSpend::Timelock => {
                                        match funder {
                                            Some(addr) => {
                                                log::warn!(
                                                    "Breach detector: timelock spend on {outpoint}, banning funder {addr}"
                                                );
                                                offerbook.add_bad_maker(addr);
                                            }
                                            // Our own contract: a timelock spend is our refund.
                                            None => log::warn!(
                                                "Breach detector: timelock spend on our own contract {outpoint}"
                                            ),
                                        }
                                        breached_clone.store(true, Relaxed);
                                        return;
                                    }
                                    // The receiver claiming with the preimage is the
                                    // protocol working, never a breach.
                                    TaprootSpend::Hashlock => log::info!(
                                        "Breach detector: hashlock spend on {outpoint}"
                                    ),
                                    TaprootSpend::KeyPath => log::info!(
                                        "Breach detector: cooperative key-path spend on {outpoint}"
                                    ),
                                }
                            }
                        }
                    }
                }
            })
            .expect("failed to spawn breach detector thread");

        Self {
            breached,
            sentinels,
            shutdown,
            handle: Some(handle),
        }
    }

    /// Register sentinels with the WatchService. Each carries the script pubkey
    /// of its outpoint so the watcher knows what to scan for.
    pub(crate) fn add_sentinels(
        &self,
        watch_service: &WatchService,
        sentinels: Vec<(Sentinel, bitcoin::ScriptBuf)>,
    ) {
        for (sentinel, spk) in &sentinels {
            let outpoint = sentinel.outpoint();
            // If a watch request fails, log the error, don't panic.
            if let Err(e) = watch_service.register_watch_request(outpoint, spk.clone()) {
                log::error!("sentinel registration for {outpoint} failed (watcher gone): {e}");
            }
        }
        if let Ok(mut guard) = self.sentinels.lock() {
            #[cfg(debug_assertions)]
            log::debug!(
                "[WATCH_STATE] Source: taker::background_services::add_sentinels | Action: register_breach_sentinels | Added: {} | Total: {}",
                sentinels.len(),
                guard.len()
            );
            guard.extend(sentinels.into_iter().map(|(s, _)| s));
        }
    }

    /// Check whether an adversarial spend has been detected.
    pub(crate) fn is_breached(&self) -> bool {
        self.breached.load(Relaxed)
    }

    /// Signal the background thread to stop and wait for it to finish.
    pub(crate) fn stop(mut self) {
        self.shutdown.store(true, Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for BreachDetector {
    fn drop(&mut self) {
        self.shutdown.store(true, Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::ScriptBuf;

    fn scripts() -> (ScriptBuf, ScriptBuf) {
        (
            ScriptBuf::from_bytes(vec![0x01, 0x02, 0x03]),
            ScriptBuf::from_bytes(vec![0x04, 0x05, 0x06]),
        )
    }

    #[test]
    fn a_leaf_spend_names_its_path() {
        let (hashlock, timelock) = scripts();
        let sig = vec![0xaa; 64];
        let control = vec![0xc0; 33];

        let timelock_wit = vec![sig.clone(), timelock.to_bytes(), control.clone()];
        assert_eq!(
            classify_taproot_spend(&timelock_wit, &hashlock, &timelock),
            TaprootSpend::Timelock
        );

        let hashlock_wit = vec![sig.clone(), hashlock.to_bytes(), control.clone()];
        assert_eq!(
            classify_taproot_spend(&hashlock_wit, &hashlock, &timelock),
            TaprootSpend::Hashlock
        );

        let keypath_wit = vec![sig];
        assert_eq!(
            classify_taproot_spend(&keypath_wit, &hashlock, &timelock),
            TaprootSpend::KeyPath
        );
    }

    #[test]
    fn an_annex_cannot_hide_the_leaf() {
        let (hashlock, timelock) = scripts();
        let sig = vec![0xaa; 64];
        let control = vec![0xc0; 33];
        let annex = vec![0x50, 0xde, 0xad];

        // A timelock spend with an annex tacked on still reads as a timelock spend.
        let hidden = vec![sig.clone(), timelock.to_bytes(), control, annex.clone()];
        assert_eq!(
            classify_taproot_spend(&hidden, &hashlock, &timelock),
            TaprootSpend::Timelock
        );

        // A key-path spend with an annex stays a key-path spend.
        let keypath = vec![sig, annex];
        assert_eq!(
            classify_taproot_spend(&keypath, &hashlock, &timelock),
            TaprootSpend::KeyPath
        );
    }
}
