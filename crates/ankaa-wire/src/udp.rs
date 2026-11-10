//! UDP receive with kernel/NIC timestamps (`SO_TIMESTAMPING`), Linux only.
//!
//! Each datagram is returned with [`RxStamp`]: the hardware timestamp (raw NIC clock, usually
//! the PHC) when the driver supplied one, and always the software receive timestamp. A caller
//! maps hardware stamps onto the CPU clock with [`crate::calib`] and must state which domain a
//! published number comes from.

use std::io;
use std::mem::{size_of, zeroed};
use std::net::{SocketAddr, UdpSocket};
use std::os::fd::AsRawFd;

/// Timestamps attached to one received datagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RxStamp {
    /// Software receive timestamp (kernel, `CLOCK_REALTIME` domain), ns since epoch.
    pub sw_ns: u64,
    /// Raw hardware timestamp from the NIC (its own clock domain), when available.
    pub hw_raw_ns: Option<u64>,
}

/// UDP socket with receive timestamping enabled.
#[derive(Debug)]
pub struct StampedUdp {
    sock: UdpSocket,
    hw_requested: bool,
}

const SOF_TIMESTAMPING_RX_SOFTWARE: u32 = 1 << 3;
const SOF_TIMESTAMPING_SOFTWARE: u32 = 1 << 4;
const SOF_TIMESTAMPING_RX_HARDWARE: u32 = 1 << 2;
const SOF_TIMESTAMPING_RAW_HARDWARE: u32 = 1 << 6;
const SO_TIMESTAMPING: libc::c_int = libc::SO_TIMESTAMPING;
const SCM_TIMESTAMPING: libc::c_int = libc::SCM_TIMESTAMPING;

/// Control-message buffer with the alignment `cmsghdr` requires.
#[repr(C, align(8))]
struct CmsgBuf([u8; 256]);

impl StampedUdp {
    /// Bind and request software (+ hardware when `hardware`) receive timestamps.
    /// Hardware stamps also need `ethtool -T`/`hwtstamp_config` on the interface (root).
    pub fn bind(addr: SocketAddr, hardware: bool) -> io::Result<Self> {
        let sock = UdpSocket::bind(addr)?;
        let mut flags = SOF_TIMESTAMPING_RX_SOFTWARE | SOF_TIMESTAMPING_SOFTWARE;
        if hardware {
            flags |= SOF_TIMESTAMPING_RX_HARDWARE | SOF_TIMESTAMPING_RAW_HARDWARE;
        }
        // SAFETY: setsockopt with a valid fd and a properly sized u32 option value.
        let rc = unsafe {
            libc::setsockopt(
                sock.as_raw_fd(),
                libc::SOL_SOCKET,
                SO_TIMESTAMPING,
                (&raw const flags).cast(),
                size_of::<u32>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            sock,
            hw_requested: hardware,
        })
    }

    /// Whether hardware stamps were requested.
    #[must_use]
    pub fn hardware_requested(&self) -> bool {
        self.hw_requested
    }

    /// Local address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()
    }

    /// Receive one datagram into `buf`; returns `(len, stamps)`.
    pub fn recv(&self, buf: &mut [u8]) -> io::Result<(usize, RxStamp)> {
        let mut iov = libc::iovec {
            iov_base: buf.as_mut_ptr().cast(),
            iov_len: buf.len(),
        };
        let mut control = CmsgBuf([0u8; 256]);
        // SAFETY: msghdr is plain data; all-zero is a valid value.
        let mut msg: libc::msghdr = unsafe { zeroed() };
        msg.msg_iov = &raw mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.0.as_mut_ptr().cast();
        msg.msg_controllen = control.0.len() as _;
        // SAFETY: fd is valid; iov/control point at live buffers of the stated lengths.
        let n = unsafe { libc::recvmsg(self.sock.as_raw_fd(), &raw mut msg, 0) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        if msg.msg_flags & libc::MSG_CTRUNC != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "control data truncated",
            ));
        }
        let mut stamp = RxStamp::default();
        let want = 3 * size_of::<libc::timespec>();
        // SAFETY: CMSG_* walk the control buffer the kernel just filled, bounded by
        // msg_controllen; the buffer is 8-byte aligned (CmsgBuf); each payload is read unaligned
        // only after its declared length has been checked.
        unsafe {
            let mut cmsg = libc::CMSG_FIRSTHDR(&raw const msg);
            while !cmsg.is_null() {
                let hdr = &*cmsg;
                let payload_len =
                    (hdr.cmsg_len as usize).saturating_sub(libc::CMSG_LEN(0) as usize);
                if hdr.cmsg_level == libc::SOL_SOCKET
                    && hdr.cmsg_type == SCM_TIMESTAMPING
                    && payload_len >= want
                {
                    let ts = libc::CMSG_DATA(cmsg).cast::<libc::timespec>();
                    // [0] software, [1] legacy/transformed, [2] raw hardware
                    let sw = std::ptr::read_unaligned(ts);
                    let hw = std::ptr::read_unaligned(ts.add(2));
                    stamp.sw_ns = to_ns(&sw);
                    let hw_ns = to_ns(&hw);
                    stamp.hw_raw_ns = (hw_ns != 0).then_some(hw_ns);
                }
                cmsg = libc::CMSG_NXTHDR(&raw const msg, cmsg);
            }
        }
        if stamp.sw_ns == 0 {
            stamp.sw_ns = crate::clock::realtime_ns();
        }
        Ok((n as usize, stamp))
    }
}

fn to_ns(ts: &libc::timespec) -> u64 {
    if ts.tv_sec < 0 || ts.tv_nsec < 0 {
        return 0;
    }
    u64::try_from(ts.tv_sec)
        .unwrap_or(0)
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::try_from(ts.tv_nsec).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_datagram_has_software_stamp() {
        let rx = StampedUdp::bind("127.0.0.1:0".parse().unwrap(), false).unwrap();
        let tx = UdpSocket::bind("127.0.0.1:0").unwrap();
        let before = crate::clock::realtime_ns();
        tx.send_to(b"tick", rx.local_addr().unwrap()).unwrap();
        let mut buf = [0u8; 64];
        let (n, stamp) = rx.recv(&mut buf).unwrap();
        let after = crate::clock::realtime_ns();
        assert_eq!(&buf[..n], b"tick");
        assert!(
            stamp.sw_ns >= before - 1_000_000 && stamp.sw_ns <= after + 1_000_000,
            "{stamp:?}"
        );
        assert!(stamp.hw_raw_ns.is_none(), "loopback has no NIC clock");
    }
}
