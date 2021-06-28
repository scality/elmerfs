pub mod lwwreg {
    use prost::bytes::Bytes;

    use crate::connection::{lwwreg::*, RawIdent, UpdateQuery};
    use std::time::Duration;
    use std::{mem, u32, u64};

    pub fn read_u32(reg: &[u8]) -> u32 {
        assert_eq!(reg.len(), mem::size_of::<u32>());
        let mut bytes = [0_u8; 4];
        bytes.copy_from_slice(&reg);

        u32::from_le_bytes(bytes)
    }

    pub fn set_u16(key: impl Into<RawIdent>, x: u16) -> UpdateQuery {
        let bytes = &x.to_le_bytes()[..];
        set(key, Bytes::copy_from_slice(bytes))
    }

    pub fn read_u16(reg: &[u8]) -> u16 {
        assert_eq!(reg.len(), mem::size_of::<u16>());
        let mut bytes = [0_u8; 2];
        bytes.copy_from_slice(&reg);

        u16::from_le_bytes(bytes)
    }

    pub fn set_u32(key: impl Into<RawIdent>, x: u32) -> UpdateQuery {
        let bytes = &x.to_le_bytes()[..];
        set(key, Bytes::copy_from_slice(bytes))
    }

    pub fn set_u64(key: impl Into<RawIdent>, x: u64) -> UpdateQuery {
        let bytes = &x.to_le_bytes()[..];
        set(key, Bytes::copy_from_slice(bytes))
    }

    pub fn read_u64(reg: &[u8]) -> u64 {
        assert_eq!(reg.len(), mem::size_of::<u64>());
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&reg);

        u64::from_le_bytes(bytes)
    }

    pub fn set_u8(key: impl Into<RawIdent>, x: u8) -> UpdateQuery {
        set(key, Bytes::copy_from_slice(&[x]))
    }

    pub fn read_u8(reg: &[u8]) -> u8 {
        assert_eq!(reg.len(), 1);
        reg[0]
    }

    pub fn set_duration(key: impl Into<RawIdent>, duration: Duration) -> UpdateQuery {
        let mut buffer = Vec::with_capacity(mem::size_of::<u64>() + mem::size_of::<u32>());

        buffer.extend_from_slice(&duration.as_secs().to_le_bytes());
        buffer.extend_from_slice(&duration.subsec_nanos().to_le_bytes());

        set(key, Bytes::from(buffer))
    }

    pub fn read_duration(reg: &[u8]) -> Duration {
        assert_eq!(reg.len(), mem::size_of::<u64>() + mem::size_of::<u32>());

        let secs = read_u64(&reg[0..8]);
        let nanos = read_u32(&reg[8..]);

        Duration::new(secs, nanos)
    }
}

pub mod mvreg {
    use prost::bytes::Bytes;
    use crate::connection::{mvreg::*, RawIdent, UpdateQuery};

    pub fn set_u64(key: impl Into<RawIdent>, x: u64) -> UpdateQuery {
        let bytes = &(x.to_le_bytes())[..];
        set(key,  Bytes::copy_from_slice(bytes))
    }
}
