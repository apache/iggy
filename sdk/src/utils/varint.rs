use bytes::{Buf, BufMut, BytesMut};

pub struct IggyVarInt(u64);

impl From<u64> for IggyVarInt {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<u32> for IggyVarInt {
    fn from(value: u32) -> Self {
        Self(value as u64)
    }
}

impl From<u16> for IggyVarInt {
    fn from(value: u16) -> Self {
        Self(value as u64)
    }
}

impl IggyVarInt {
    #[inline]
    pub fn encode(&self, w: &mut BytesMut) {
        let x = self.0;
        if x < 2u64.pow(6) {
            w.put_u8(x as u8);
        } else if x < 2u64.pow(14) {
            w.put_u16(0b01 << 14 | x as u16);
        } else if x < 2u64.pow(30) {
            w.put_u32(0b10 << 30 | x as u32);
        } else if x < 2u64.pow(62) {
            w.put_u64(0b11 << 62 | x);
        } else {
            unreachable!("malformed VarInt")
        }
    }
     
    #[inline]
    pub fn decode(r: &[u8]) -> (usize, u64) {
        let tag: u8 = r[0] >> 6;
        let first_byte = r[0] & 0b0011_1111;

        match tag {
            0b00 => {
                (1, u64::from(first_byte))
            },
            0b01 => {
                let mut buf = [0u8; 2];
                buf[0] = first_byte;
                buf[1] = r[1];
                
                (2, u64::from(u16::from_be_bytes(buf)))
            },
            0b10 => {
                let mut buf = [0u8; 4];
                buf[0] = first_byte;
                buf[1..4].copy_from_slice(&r[1..4]);
                
                (4, u64::from(u32::from_be_bytes(buf)))
            },
            0b11 => {
                let mut buf = [0u8; 8];
                buf[0] = first_byte;
                buf[1..8].copy_from_slice(&r[1..8]);
                
                (8, u64::from_be_bytes(buf))
            },
            _ => unreachable!(),
        }
    }

}