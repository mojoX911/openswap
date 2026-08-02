//! Requests a maker must refuse, and the one abort point that happens before
//! negotiation even starts.
//!
//! The amount bounds are guarded at two taker-side layers, reached by different
//! routes:
//!
//! 1. The offerbook filter (`taker/api.rs:1150-1156`) drops unsuitable makers
//!    during selection and fails with `NotEnoughMakersInOfferBook`.
//! 2. `with_preferred_makers` skips that filter — addresses are used as given —
//!    so selection succeeds and the bounds are caught one stage later, when
//!    negotiation validates the received offer (`taker/api.rs:1474-1487`).
//!
//! The maker's own bounds check (`maker/api.rs:834-840`) sits behind both, and
//! an honest taker cannot reach it: layer 2 rejects before any `SwapDetails` is
//! sent. Covering it would need a taker behavior hook that skips `validate_offer`.
//!
//! No funds move in any case here, so they all share one fixture and the test
//! ends by asserting balances are untouched. That is the whole point of a
//! fail-closed guard.

use bitcoin::Amount;
use coinswap::{
    maker::{start_server, MakerBehavior},
    protocol::common_messages::ProtocolVersion,
    taker::{error::TakerError, SwapParams, TakerBehavior},
    wallet::AddressType,
};

use super::test_framework::*;

use log::{info, warn};
use std::{sync::atomic::Ordering::Relaxed, thread};

#[test]
fn test_maker_rejects_out_of_bounds_swap_details() {
    warn!("Running Test: Maker Rejection of SwapDetails + CloseEarly");

    let makers_config_map = vec![(9202, Some(21501)), (19202, Some(21502))];
    let taker_behavior = vec![TakerBehavior::Normal];
    let maker_behaviors = vec![MakerBehavior::Normal, MakerBehavior::Normal];

    let (test_framework, mut takers, makers, block_generation_handle) =
        TestFramework::init::<BitcoindBackend>(makers_config_map, taker_behavior, maker_behaviors);

    let bitcoind = &test_framework.bitcoind;
    let taker = takers.get_mut(0).unwrap();

    // 4 UTXOs, not the usual 3: the above-maximum cases need the taker to hold
    // more than the maker is willing to swap.
    let taker_original_balance = fund_taker(
        taker,
        bitcoind,
        4,
        Amount::from_btc(0.05).unwrap(),
        AddressType::P2TR,
    );
    fund_makers(
        &makers,
        bitcoind,
        4,
        Amount::from_btc(0.05).unwrap(),
        AddressType::P2TR,
    );

    info!("Starting Maker servers...");
    let maker_threads = makers
        .iter()
        .map(|maker| {
            let maker_clone = maker.clone();
            thread::spawn(move || {
                start_server(maker_clone).unwrap();
            })
        })
        .collect::<Vec<_>>();

    wait_for_makers_setup(&makers, 120);

    for maker in &makers {
        maker.wallet.write().unwrap().sync_and_save().unwrap();
    }

    let maker_spendable_balance = verify_maker_pre_swap_balances(&makers);
    generate_blocks(bitcoind, 1);

    // The maker advertises min = its `min_swap_amount`, max = its spendable
    // liquidity, so derive both bounds instead of hardcoding them.
    let maker_offer_max = makers[0]
        .wallet
        .read()
        .unwrap()
        .get_balances()
        .unwrap()
        .regular;
    let below_min = Amount::from_sat(5_000);
    let above_max = maker_offer_max + Amount::from_sat(100_000);
    info!(
        "Maker offer max: {}, testing below_min={} and above_max={}",
        maker_offer_max, below_min, above_max
    );

    let preferred: Vec<String> = makers
        .iter()
        .map(|m| format!("127.0.0.1:{}", m.config.network_port))
        .collect();

    // ---- 1. Below minimum, taker-side offerbook filter ----
    let err = taker
        .prepare_coinswap(
            SwapParams::new(ProtocolVersion::Taproot, below_min, 2)
                .with_tx_count(1)
                .with_required_confirms(1),
        )
        .expect_err("an amount under the maker's min_size must not be routable");
    assert!(
        matches!(err, TakerError::NotEnoughMakersInOfferBook),
        "Expected NotEnoughMakersInOfferBook for below-minimum amount, got: {:?}",
        err
    );
    info!("Below-minimum request rejected by the offerbook filter");

    // ---- 2. Above maximum, taker-side offerbook filter ----
    let err = taker
        .prepare_coinswap(
            SwapParams::new(ProtocolVersion::Taproot, above_max, 2)
                .with_tx_count(1)
                .with_required_confirms(1),
        )
        .expect_err("an amount over the maker's max_size must not be routable");
    assert!(
        matches!(err, TakerError::NotEnoughMakersInOfferBook),
        "Expected NotEnoughMakersInOfferBook for above-maximum amount, got: {:?}",
        err
    );
    info!("Above-maximum request rejected by the offerbook filter");

    // ---- 3. Below minimum, past the filter, caught at negotiation ----
    let err = taker
        .prepare_coinswap(
            SwapParams::new(ProtocolVersion::Taproot, below_min, 2)
                .with_tx_count(1)
                .with_required_confirms(1)
                .with_preferred_makers(preferred.clone()),
        )
        .expect_err("negotiation must refuse an amount under the maker's minimum");
    let msg = format!("{:?}", err);
    assert!(
        msg.contains(&format!(
            "Send amount ({} sats) is below maker 0 min_size",
            below_min.to_sat()
        )),
        "Expected the negotiation min_size guard, got: {}",
        msg
    );
    info!("Negotiation refused below-minimum request: {}", msg);

    // ---- 4. Above maximum, past the filter, caught at negotiation ----
    let err = taker
        .prepare_coinswap(
            SwapParams::new(ProtocolVersion::Taproot, above_max, 2)
                .with_tx_count(1)
                .with_required_confirms(1)
                .with_preferred_makers(preferred),
        )
        .expect_err("negotiation must refuse an amount over the maker's maximum");
    let msg = format!("{:?}", err);
    assert!(
        msg.contains(&format!(
            "Send amount ({} sats) exceeds maker 0 max_size",
            above_max.to_sat()
        )),
        "Expected the negotiation max_size guard, got: {}",
        msg
    );
    info!("Negotiation refused above-maximum request: {}", msg);

    // ---- 5. Taker aborts after maker selection, before negotiating ----
    taker.behavior = TakerBehavior::CloseEarly;
    let err = taker
        .prepare_coinswap(
            SwapParams::new(ProtocolVersion::Taproot, Amount::from_sat(500000), 2)
                .with_tx_count(1)
                .with_required_confirms(1),
        )
        .expect_err("CloseEarly must abort prepare_coinswap");
    info!("Taker closed early after maker selection: {:?}", err);
    taker.behavior = TakerBehavior::Normal;

    makers
        .iter()
        .for_each(|maker| maker.shutdown.store(true, Relaxed));
    maker_threads
        .into_iter()
        .for_each(|thread| thread.join().unwrap());

    let log_path = format!("{}/taker/debug.log", test_framework.temp_dir.display());
    test_framework.assert_log("closing early after maker selection", &log_path);

    // Nothing was funded, so nothing may have moved.
    taker.get_wallet().write().unwrap().sync_and_save().unwrap();
    let taker_balances = taker.get_wallet().read().unwrap().get_balances().unwrap();
    info!(
        "Taker balances: Regular: {}, Swap: {}, Contract: {}, Spendable: {}",
        taker_balances.regular,
        taker_balances.swap,
        taker_balances.contract,
        taker_balances.spendable,
    );
    assert_eq!(
        taker_balances.spendable, taker_original_balance,
        "Taker spendable balance must be untouched after rejected requests"
    );
    // 4 UTXOs of 0.05 BTC, none of them spent.
    assert_eq!(
        taker_balances.regular.to_sat(),
        20000000,
        "Taker regular balance mismatch"
    );
    assert_eq!(
        taker_balances.spendable.to_sat(),
        20000000,
        "Taker spendable balance mismatch"
    );
    assert_eq!(
        taker_balances.contract.to_sat(),
        0,
        "Taker must hold no contract funds"
    );
    assert_eq!(
        taker_balances.swap.to_sat(),
        0,
        "Taker must hold no swap funds"
    );
    assert_eq!(taker_balances.fidelity, Amount::ZERO);

    for (i, (maker, original)) in makers.iter().zip(maker_spendable_balance).enumerate() {
        maker.wallet.write().unwrap().sync_and_save().unwrap();
        let balances = maker.wallet.read().unwrap().get_balances().unwrap();
        info!(
            "Maker {} balances: Regular: {}, Swap: {}, Contract: {}, Spendable: {}",
            i, balances.regular, balances.swap, balances.contract, balances.spendable,
        );
        assert_eq!(
            balances.spendable, original,
            "Maker {} spendable balance must be untouched",
            i
        );
        // 4 UTXOs of 0.05 BTC minus the fidelity bond and its fee.
        assert_eq!(
            balances.regular.to_sat(),
            14999514,
            "Maker {} regular balance mismatch",
            i
        );
        assert_eq!(
            balances.spendable.to_sat(),
            14999514,
            "Maker {} spendable balance mismatch",
            i
        );
        assert_eq!(
            balances.swap.to_sat(),
            0,
            "Maker {} must hold no swap funds",
            i
        );
        assert_eq!(
            balances.contract.to_sat(),
            0,
            "Maker {} must hold no contract funds",
            i
        );
        assert_eq!(balances.fidelity, Amount::from_btc(0.05).unwrap());
    }

    info!("Maker SwapDetails rejection test completed successfully!");

    test_framework.stop();
    block_generation_handle.join().unwrap();
}
