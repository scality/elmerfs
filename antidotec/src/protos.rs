use std::convert::TryFrom;

pub(crate) mod antidote;

use self::antidote::*;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AntidoteError {
    #[error("unknown error")]
    Unknown = 0,
    #[error("the request has timed out")]
    Timeout = 1,
    #[error("not enough permissions")]
    NoPermissions = 2,
    #[error("the request has been aborted")]
    Aborted = 3,
}

impl From<u32> for AntidoteError {
    fn from(code: u32) -> Self {
        match code {
            1 => Self::Timeout,
            2 => Self::NoPermissions,
            3 => Self::Aborted,
            _ => Self::Unknown,
        }
    }
}

macro_rules! apb_messages {
    ($($msg:ident = $code:literal,)*) => {
        #[derive(Debug, Copy, Clone, PartialEq, Eq)]
        pub enum ApbMessageCode {
            $(
                $msg = $code,
            )*
        }

        pub trait ApbMessage: prost::Message {
            fn code() -> ApbMessageCode;
        }

        #[derive(Debug, Error)]
        #[error("unknown message code: {0}")]
        pub struct MessageCodeError(u8);

        impl TryFrom<u8> for ApbMessageCode {
            type Error = MessageCodeError;

            fn try_from(code: u8) -> Result<ApbMessageCode, MessageCodeError> {
                match code {
                    $(
                        $code => Ok(ApbMessageCode::$msg),
                    )*
                    code => Err(MessageCodeError(code))
                }
            }
        }

        $(
            impl ApbMessage for $msg {
                fn code() -> ApbMessageCode {
                    ApbMessageCode::$msg
                }
            }
        )*
    };
}

apb_messages! {
    ApbErrorResp = 0,
    ApbRegUpdate = 107,
    ApbGetRegResp = 108,
    ApbCounterUpdate = 109,
    ApbGetCounterResp = 110,
    ApbOperationResp = 111,
    ApbSetUpdate = 112,
    ApbGetSetResp = 113,
    ApbTxnProperties = 114,
    ApbBoundObject = 115,
    ApbReadObjects = 116,
    ApbUpdateOp = 117,
    ApbUpdateObjects = 118,
    ApbStartTransaction = 119,
    ApbAbortTransaction = 120,
    ApbCommitTransaction = 121,
    ApbStaticUpdateObjects = 122,
    ApbStaticReadObjects = 123,
    ApbStartTransactionResp = 124,
    ApbReadObjectResp = 125,
    ApbReadObjectsResp = 126,
    ApbCommitResp = 127,
    ApbStaticReadObjectsResp = 128,
    ApbCreateDc = 129,
    ApbCreateDcResp = 130,
    ApbConnectToDCs = 131,
    ApbConnectToDCsResp = 132,
    ApbGetConnectionDescriptor = 133,
    ApbGetConnectionDescriptorResp = 134,
}
