//! Legacy malicious-maker test: reject sender contract data whose funding output
//! is not the advertised 2-of-2 multisig output.

use bitcoin::Amount;
use coinswap::{
    maker::{start_server, MakerBehavior},
    protocol::common_messages::ProtocolVersion,
    taker::{SwapParams, Taker, TakerBehavior},
    wallet::AddressType,
};

use super::test_framework::*;

use std::{sync::atomic::Ordering::Relaxed, thread};

#[test]
fn test_legacy_taker_rejects_malformed_maker_funding_output() {
    let makers_config_map = vec![
        (6102, Some(19051)),
        (16102, Some(19052)),
        (26102, Some(19053)),
    ];
    let taker_behavior = vec![TakerBehavior::Normal];
    // First maker returns Legacy sender contract data whose contract input points
    // at a real funding tx output, but not the advertised 2-of-2 multisig output.
    // The third maker is the honest replacement the second swap must pick.
    let maker_behaviors = vec![
        MakerBehavior::MalformedLegacyFundingOutput,
        MakerBehavior::Normal,
        MakerBehavior::Normal,
    ];

    let (test_framework, mut takers, makers, block_generation_handle) =
        TestFramework::init::<BitcoindBackend>(makers_config_map, taker_behavior, maker_behaviors);

    let bitcoind = &test_framework.bitcoind;
    // Owned, not borrowed: this taker gets dropped and re-inited mid-test.
    let mut taker = takers.remove(0);

    fund_taker(
        &taker,
        bitcoind,
        3,
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

    let maker_threads = makers
        .iter()
        .map(|maker| {
            let maker_clone = maker.clone();
            thread::spawn(move || start_server(maker_clone).unwrap())
        })
        .collect::<Vec<_>>();

    wait_for_makers_setup(&makers, 120);
    for maker in &makers {
        maker.wallet.write().unwrap().sync_and_save().unwrap();
    }

    // Pin the first route to the malicious maker. Left to auto-selection it
    // would sort into the spare slot and never be asked for a contract.
    let pinned = vec![
        format!("127.0.0.1:{}", makers[1].config.network_port),
        format!("127.0.0.1:{}", makers[0].config.network_port),
    ];
    let swap_params = SwapParams::new(ProtocolVersion::Legacy, Amount::from_sat(500_000), 2)
        .with_tx_count(3)
        .with_required_confirms(1)
        .with_preferred_makers(pinned);
    generate_blocks(bitcoind, 1);

    let summary = taker
        .prepare_coinswap(swap_params)
        .expect("prepare_coinswap should succeed");

    // The taker must reject before signing/finalizing; otherwise it can later
    // report success while the incoming sweep is unspendable.
    let result = taker.start_coinswap(&summary.swap_id);
    assert!(
        result.is_err(),
        "taker must reject malformed maker sender contract data"
    );

    let error = format!("{:?}", result.unwrap_err());
    assert!(
        error.contains("funding output does not pay to advertised multisig"),
        "unexpected taker error: {}",
        error
    );

    let log_path = format!("{}/taker/debug.log", test_framework.temp_dir.display());
    // Pin the operator-visible rejection, not just the returned Rust error.
    test_framework.assert_log(
        "funding output does not pay to advertised multisig",
        &log_path,
    );

    // Maker 0 sent the malformed contract. Makers 1 and 2 stayed honest.
    let cheat = format!("127.0.0.1:{}", makers[0].config.network_port);
    assert_eq!(
        taker.get_offerbook().unwrap().get_bad_makers(),
        vec![cheat.clone()]
    );

    // Restart against the same data dir. A ban that only lives in memory is no
    // ban at all. Dropping also stops the recovery loop, which would otherwise
    // fight the second swap over the wallet.
    drop(taker);
    let mut taker = Taker::init(test_framework.taker_init_config::<BitcoindBackend>(0))
        .expect("restarted taker should open the same wallet");
    assert_eq!(
        taker.get_offerbook().unwrap().get_bad_makers(),
        vec![cheat],
        "ban did not survive the restart"
    );

    // Second swap: selection and negotiation run again over the whole book, and
    // the route must come out as the two honest makers. Stopping before the
    // funding broadcast keeps the first swap's abandoned contracts out of it.
    let second_params = SwapParams::new(ProtocolVersion::Legacy, Amount::from_sat(500_000), 2)
        .with_tx_count(3)
        .with_required_confirms(1);
    let second = taker
        .prepare_coinswap(second_params)
        .expect("two honest makers are left, so a 2-hop route must still build");
    let route: Vec<String> = second.makers.iter().map(|m| m.address.clone()).collect();
    let honest: Vec<String> = makers[1..]
        .iter()
        .map(|m| format!("127.0.0.1:{}", m.config.network_port))
        .collect();
    assert_eq!(
        route, honest,
        "second swap must route through the honest makers only"
    );

    makers
        .iter()
        .for_each(|maker| maker.shutdown.store(true, Relaxed));
    maker_threads
        .into_iter()
        .for_each(|thread| thread.join().unwrap());

    test_framework.stop();
    block_generation_handle.join().unwrap();
}
