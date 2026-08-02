//! All Taker-related errors.
use crate::{
    error::NetError, protocol::error::ProtocolError, utill::TorError, wallet::WalletError,
    watch_tower::watcher_error::WatcherError,
};
use bitcoin::address::ParseError;

/// Represents errors that can occur during Taker operations.
///
/// This enum covers a range of errors related to I/O, wallet operations, network communication,
/// and other Taker-specific scenarios.
#[derive(Debug)]
pub enum TakerError {
    /// Standard input/output error.
    IO(std::io::Error),
    /// Error indicating contracts were broadcasted prematurely.
    /// Contains a list of the transaction IDs of the broadcasted contracts.
    ContractsBroadcasted(Vec<bitcoin::Txid>),
    /// Error indicating there are not enough makers available in the offer book.
    NotEnoughMakersInOfferBook,
    /// Error related to wallet operations.
    Wallet(WalletError),
    /// Error related to network operations.
    Net(NetError),
    /// Error indicating the send amount was not set for a transaction.
    SendAmountNotSet,
    /// Error deserializing data, typically related to CBOR-encoded data.
    Deserialize(String),
    /// Peer sent a valid message, but not the one the protocol expects here.
    /// Only an ill-behaved peer does this, so callers ban on it.
    MessageMismatch(String),
    /// Offer fields no honest build produces, like a fee that is not a number
    /// or a size range that accepts nothing.
    MalformedOffer(String),
    /// Error indicating an MPSC channel failure.
    ///
    /// This error occurs during internal thread communication.
    MPSC(String),
    /// Tor error
    TorError(TorError),
    /// Error relating to Bitcoin Address Parsing.
    AddressParseError(ParseError),
    /// General error with a custom message
    General(String),
    /// Watcher Service Error
    Watcher(WatcherError),
}

impl TakerError {
    /// True when the peer caused this: junk bytes, an oversized frame, a message
    /// the protocol does not allow here, or an offer no honest build produces.
    /// A dead link is not its fault.
    pub(crate) fn is_maker_at_fault(&self) -> bool {
        matches!(
            self,
            Self::Deserialize(_)
                | Self::MessageMismatch(_)
                | Self::MalformedOffer(_)
                | Self::Net(NetError::MessageTooLarge)
        )
    }
}

impl From<TorError> for TakerError {
    fn from(value: TorError) -> Self {
        Self::TorError(value)
    }
}

impl From<serde_cbor::Error> for TakerError {
    fn from(value: serde_cbor::Error) -> Self {
        Self::Deserialize(value.to_string())
    }
}

impl From<serde_json::Error> for TakerError {
    fn from(value: serde_json::Error) -> Self {
        Self::Deserialize(value.to_string())
    }
}

impl From<WalletError> for TakerError {
    fn from(value: WalletError) -> Self {
        Self::Wallet(value)
    }
}

impl From<std::io::Error> for TakerError {
    fn from(value: std::io::Error) -> Self {
        Self::IO(value)
    }
}

impl From<NetError> for TakerError {
    fn from(value: NetError) -> Self {
        Self::Net(value)
    }
}

impl From<ProtocolError> for TakerError {
    fn from(value: ProtocolError) -> Self {
        Self::Wallet(value.into())
    }
}

impl From<std::sync::mpsc::RecvError> for TakerError {
    fn from(value: std::sync::mpsc::RecvError) -> Self {
        Self::MPSC(value.to_string())
    }
}

impl From<std::sync::mpsc::RecvTimeoutError> for TakerError {
    fn from(value: std::sync::mpsc::RecvTimeoutError) -> Self {
        Self::MPSC(value.to_string())
    }
}

impl<T> From<std::sync::mpsc::SendError<T>> for TakerError {
    fn from(value: std::sync::mpsc::SendError<T>) -> Self {
        Self::MPSC(value.to_string())
    }
}

impl From<ParseError> for TakerError {
    fn from(value: ParseError) -> Self {
        Self::AddressParseError(value)
    }
}

impl From<WatcherError> for TakerError {
    fn from(value: WatcherError) -> Self {
        Self::Watcher(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_provable_faults_blame_the_maker() {
        assert!(TakerError::Deserialize("junk".into()).is_maker_at_fault());
        assert!(TakerError::MessageMismatch("wrong".into()).is_maker_at_fault());
        assert!(TakerError::Net(NetError::MessageTooLarge).is_maker_at_fault());

        // A dead link or our own failure never earns a ban.
        assert!(!TakerError::Net(NetError::ReachedEOF).is_maker_at_fault());
        assert!(!TakerError::Net(NetError::ConnectionTimedOut).is_maker_at_fault());
        assert!(!TakerError::General("ours".into()).is_maker_at_fault());
        assert!(!TakerError::SendAmountNotSet.is_maker_at_fault());
    }
}
