// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Segment writes that carry `RWF_DONTCACHE` (Linux >= 6.14): the kernel
//! kicks writeback as soon as the write lands and drops the pages from the
//! page cache once it completes, so a busy producer stops evicting the data
//! readers still want. compio does not expose `rw_flags`, hence our own
//! io_uring ops. Only ext4 and XFS honour the flag; every other filesystem
//! answers `EOPNOTSUPP`, which [`probe_uncached_write`] turns into a
//! boot-time error instead of a per-write surprise.

use std::env;
use std::fs::OpenOptions;
use std::io;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use compio::driver::{OpCode, OpEntry, SharedFd};
use compio_buf::{BufResult, IntoInner, IoBuf, IoVectoredBuf};
use io_uring::opcode;
use io_uring::types::Fd;
use nix::sys::statfs::{self, FsType};
use nix::sys::utsname::uname;

use crate::diagnostics::parse_kernel_version;

pub const RWF_DONTCACHE: i32 = libc::RWF_DONTCACHE;

/// First kernel that accepts `RWF_DONTCACHE` at all.
const RWF_DONTCACHE_MIN_KERNEL: (u32, u32) = (6, 14);
/// First kernel that runs the post-write writeback kick off the writer's
/// thread; before it the kick runs inline in the submitting shard.
const WRITEBACK_KICK_OFF_WRITER_KERNEL: (u32, u32) = (7, 2);
const PROBE_FILE_PREFIX: &str = ".iggy_uncached_probe.";
const PROBE_LEN: usize = 4096;
const UNKNOWN: &str = "unknown";
const REQUIRE_UNCACHED_IO_TESTS_ENV: &str = "IGGY_REQUIRE_UNCACHED_IO";
const FILESYSTEM_NAMES: [(FsType, &str); 7] = [
    (statfs::EXT4_SUPER_MAGIC, "ext4"),
    (statfs::XFS_SUPER_MAGIC, "xfs"),
    (statfs::BTRFS_SUPER_MAGIC, "btrfs"),
    (statfs::TMPFS_MAGIC, "tmpfs"),
    (statfs::OVERLAYFS_SUPER_MAGIC, "overlayfs"),
    (statfs::FUSE_SUPER_MAGIC, "fuse"),
    (statfs::NFS_SUPER_MAGIC, "nfs"),
];

#[derive(Debug, thiserror::Error)]
pub enum UncachedIoError {
    #[error("{}", unsupported_reason(.kernel_release, .filesystem))]
    Unsupported {
        kernel_release: String,
        filesystem: String,
    },
    #[error("uncached write probe failed: {0}")]
    Io(#[from] io::Error),
}

/// compio's `WriteAt` plus caller-chosen `rw_flags`.
pub struct WriteAtFlags<T: IoBuf, S> {
    fd: S,
    offset: u64,
    flags: i32,
    buffer: T,
}

impl<T: IoBuf, S> WriteAtFlags<T, S> {
    pub fn new(fd: S, offset: u64, flags: i32, buffer: T) -> Self {
        Self {
            fd,
            offset,
            flags,
            buffer,
        }
    }
}

impl<T: IoBuf, S> IntoInner for WriteAtFlags<T, S> {
    type Inner = T;

    fn into_inner(self) -> Self::Inner {
        self.buffer
    }
}

unsafe impl<T: IoBuf, S: AsFd> OpCode for WriteAtFlags<T, S> {
    type Control = ();

    fn create_entry(&mut self, _: &mut Self::Control) -> OpEntry {
        let slice = self.buffer.as_init();
        opcode::Write::new(
            Fd(self.fd.as_fd().as_raw_fd()),
            slice.as_ptr(),
            slice.len().try_into().unwrap_or(u32::MAX),
        )
        .offset(self.offset)
        .rw_flags(self.flags)
        .build()
        .into()
    }
}

/// compio's `WriteVectoredAt` plus caller-chosen `rw_flags`.
pub struct WriteVectoredAtFlags<T: IoVectoredBuf, S> {
    fd: S,
    offset: u64,
    flags: i32,
    buffer: T,
}

impl<T: IoVectoredBuf, S> WriteVectoredAtFlags<T, S> {
    pub fn new(fd: S, offset: u64, flags: i32, buffer: T) -> Self {
        Self {
            fd,
            offset,
            flags,
            buffer,
        }
    }
}

impl<T: IoVectoredBuf, S> IntoInner for WriteVectoredAtFlags<T, S> {
    type Inner = T;

    fn into_inner(self) -> Self::Inner {
        self.buffer
    }
}

/// The iovec array a `Writev` submission points at.
#[derive(Default)]
pub struct IovecControl {
    slices: Vec<libc::iovec>,
}

unsafe impl<T: IoVectoredBuf, S: AsFd> OpCode for WriteVectoredAtFlags<T, S> {
    type Control = IovecControl;

    // The driver calls `init` only after boxing the op, so the iovecs point
    // at memory that no longer moves. They stay valid until the completion
    // arrives even if the submitting future is dropped: the driver keeps the
    // boxed op (buffer, iovecs and fd clone) alive until then.
    unsafe fn init(&mut self, control: &mut Self::Control) {
        control.slices = self.buffer.iter_slice().map(iovec_from_slice).collect();
    }

    fn create_entry(&mut self, control: &mut Self::Control) -> OpEntry {
        opcode::Writev::new(
            Fd(self.fd.as_fd().as_raw_fd()),
            control.slices.as_ptr(),
            control.slices.len().try_into().unwrap_or(u32::MAX),
        )
        .offset(self.offset)
        .rw_flags(self.flags)
        .build()
        .into()
    }
}

/// Writes the whole buffer at `pos` with `RWF_DONTCACHE`, resubmitting the
/// tail after short writes like compio-io's `write_all_at`.
pub async fn write_all_at_uncached<T: IoBuf, S: AsFd + Clone + 'static>(
    fd: &S,
    buf: T,
    pos: u64,
) -> BufResult<(), T> {
    let len = buf.buf_len();
    write_all_with(buf, len, |buf, written| {
        let op = WriteAtFlags::new(
            fd.clone(),
            pos + written as u64,
            RWF_DONTCACHE,
            buf.slice(written..),
        );
        async move { compio::runtime::submit(op).await.into_inner().into_inner() }
    })
    .await
}

/// Vectored twin of [`write_all_at_uncached`].
pub async fn write_vectored_all_at_uncached<T: IoVectoredBuf, S: AsFd + Clone + 'static>(
    fd: &S,
    buf: T,
    pos: u64,
) -> BufResult<(), T> {
    let len = buf.total_len();
    write_all_with(buf, len, |buf, written| {
        let op = WriteVectoredAtFlags::new(
            fd.clone(),
            pos + written as u64,
            RWF_DONTCACHE,
            buf.slice(written),
        );
        async move { compio::runtime::submit(op).await.into_inner().into_inner() }
    })
    .await
}

/// Boot-time check that `dir` takes `RWF_DONTCACHE` writes: one page through
/// the very io_uring op the segment writers submit, on a private file that is
/// always removed again. A kernel and filesystem pair can accept the flag on
/// the `pwritev2` path and still refuse it on the io_uring path, so nothing
/// short of the real op proves the mode is usable.
///
/// Must be awaited inside a compio runtime; boot does that before any shard
/// exists.
pub async fn probe_uncached_write(dir: &Path) -> Result<(), UncachedIoError> {
    let path = dir.join(probe_file_name());
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)?;
    let _unlink = UnlinkOnDrop(&path);
    let fd = SharedFd::new(OwnedFd::from(file));
    let BufResult(result, _) = write_all_at_uncached(&fd, vec![0u8; PROBE_LEN], 0).await;
    result.map_err(|error| classify_probe_failure(error, dir))
}

/// Warns on kernels that accept `RWF_DONTCACHE` but still run the writeback
/// kick inline in the writer. `None` on 7.2+ or when the release string is
/// unparsable.
pub fn uncached_write_kernel_warning() -> Option<String> {
    kernel_warning_for(&kernel_release()?)
}

/// Test support: with `IGGY_REQUIRE_UNCACHED_IO=1` a test that would skip
/// itself for want of uncached I/O must fail instead, so CI cannot go green
/// on a box where the flag never reached a write.
pub fn require_uncached_io_tests() -> bool {
    env::var(REQUIRE_UNCACHED_IO_TESTS_ENV).is_ok_and(|value| value == "1")
}

/// Mirrors compio-io's `loop_write_all!`: resubmits the unwritten tail after
/// a short write, retries `Interrupted`, and reports a zero-length completion
/// as `WriteZero`.
async fn write_all_with<B, F, Fut>(mut buf: B, len: usize, mut write: F) -> BufResult<(), B>
where
    F: FnMut(B, usize) -> Fut,
    Fut: Future<Output = BufResult<usize, B>>,
{
    let mut written = 0;
    while written < len {
        match write(buf, written).await {
            BufResult(Ok(0), whole) => {
                return BufResult(
                    Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    )),
                    whole,
                );
            }
            BufResult(Ok(n), whole) => {
                written += n;
                buf = whole;
            }
            BufResult(Err(ref error), whole) if error.kind() == io::ErrorKind::Interrupted => {
                buf = whole;
            }
            BufResult(Err(error), whole) => return BufResult(Err(error), whole),
        }
    }
    BufResult(Ok(()), buf)
}

fn iovec_from_slice(slice: &[u8]) -> libc::iovec {
    libc::iovec {
        iov_base: slice.as_ptr().cast_mut().cast(),
        iov_len: slice.len(),
    }
}

/// The pid alone repeats across PID namespaces, so two servers sharing one
/// data directory would fight over a single probe file and misdiagnose each
/// other's writes.
fn probe_file_name() -> String {
    format!(
        "{PROBE_FILE_PREFIX}{}.{:016x}",
        std::process::id(),
        rand::random::<u64>()
    )
}

fn classify_probe_failure(error: io::Error, dir: &Path) -> UncachedIoError {
    if error.kind() == io::ErrorKind::WriteZero {
        return UncachedIoError::Io(io::Error::other(format!(
            "uncached probe write stopped short of {PROBE_LEN} bytes under {}; \
             RWF_DONTCACHE itself was accepted, so check free space and quotas",
            dir.display()
        )));
    }
    match error.raw_os_error() {
        Some(libc::EOPNOTSUPP | libc::EINVAL | libc::ENOSYS) => UncachedIoError::Unsupported {
            kernel_release: kernel_release().unwrap_or_else(|| UNKNOWN.to_owned()),
            filesystem: filesystem_name(dir),
        },
        _ => UncachedIoError::Io(error),
    }
}

fn unsupported_reason(kernel_release: &str, filesystem: &str) -> String {
    let failed = match parse_kernel_version(kernel_release) {
        Some(version) if version < RWF_DONTCACHE_MIN_KERNEL => {
            format!("kernel {kernel_release} predates RWF_DONTCACHE")
        }
        Some(_) => {
            format!("filesystem {filesystem} rejects RWF_DONTCACHE (kernel {kernel_release})")
        }
        None => format!(
            "the kernel version could not be determined from release {kernel_release}, and \
             filesystem {filesystem} rejected RWF_DONTCACHE"
        ),
    };
    format!(
        "{failed}: set write_io = \"buffered\", or keep segments on ext4/XFS under Linux >= 6.14"
    )
}

fn kernel_warning_for(release: &str) -> Option<String> {
    let version = parse_kernel_version(release)?;
    (RWF_DONTCACHE_MIN_KERNEL..WRITEBACK_KICK_OFF_WRITER_KERNEL)
        .contains(&version)
        .then(|| {
            format!(
                "Linux {release} runs the RWF_DONTCACHE writeback kick inline on the writing \
                 shard thread; Linux >= 7.2 moves it off the writer. Upgrade the kernel to keep \
                 uncached segment writes off the hot path."
            )
        })
}

fn kernel_release() -> Option<String> {
    uname()
        .ok()
        .map(|info| info.release().to_string_lossy().into_owned())
}

fn filesystem_name(dir: &Path) -> String {
    let Ok(stat) = statfs::statfs(dir) else {
        return UNKNOWN.to_owned();
    };
    let fs_type = stat.filesystem_type();
    FILESYSTEM_NAMES
        .iter()
        .find(|(magic, _)| *magic == fs_type)
        .map_or_else(
            || format!("{:#x}", fs_type.0),
            |(_, name)| (*name).to_owned(),
        )
}

struct UnlinkOnDrop<'a>(&'a Path);

impl Drop for UnlinkOnDrop<'_> {
    fn drop(&mut self) {
        // Best effort: whatever brought us here is the error worth reporting.
        let _ = std::fs::remove_file(self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::fs::File;
    use std::future::ready;
    use tempfile::{TempDir, tempdir_in};

    const SHM_DIR: &str = "/dev/shm";

    /// `/tmp` is tmpfs on many boxes and tmpfs rejects `RWF_DONTCACHE`, so
    /// fixtures live next to the test binary, on the build tree's filesystem.
    fn target_scratch_dir() -> TempDir {
        let exe = std::env::current_exe().expect("current_exe");
        let base = exe.parent().expect("test binary has a parent dir");
        tempdir_in(base).expect("scratch dir under target/")
    }

    /// Loud enough to spot in a scrolling CI log, and a hard failure wherever
    /// the uncached path is required to have run.
    fn skip_or_fail(reason: &str) {
        assert!(
            !require_uncached_io_tests(),
            "### {REQUIRE_UNCACHED_IO_TESTS_ENV}=1 forbids skipping: {reason} ###"
        );
        eprintln!("######## SKIPPING UNCACHED TEST: {reason} ########");
    }

    /// `None`, with the reason reported, when this box cannot do uncached
    /// writes at all (old kernel or unsupported build-tree filesystem).
    async fn supported_scratch_dir() -> Option<TempDir> {
        let dir = target_scratch_dir();
        match probe_uncached_write(dir.path()).await {
            Ok(()) => Some(dir),
            Err(error @ UncachedIoError::Unsupported { .. }) => {
                skip_or_fail(&error.to_string());
                None
            }
            Err(error) => panic!("probe hit an I/O error: {error}"),
        }
    }

    fn shm_tmpfs_dir() -> Option<TempDir> {
        let shm = Path::new(SHM_DIR);
        if filesystem_name(shm) != "tmpfs" {
            skip_or_fail(&format!("{SHM_DIR} is not tmpfs"));
            return None;
        }
        match tempdir_in(shm) {
            Ok(dir) => Some(dir),
            Err(error) => {
                skip_or_fail(&format!(
                    "cannot create a directory under {SHM_DIR}: {error}"
                ));
                None
            }
        }
    }

    fn open_new(dir: &TempDir) -> (SharedFd<OwnedFd>, std::path::PathBuf) {
        let path = dir.path().join("segment.log");
        let file = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("create segment file");
        (SharedFd::new(OwnedFd::from(file)), path)
    }

    /// Non-zero bytes that differ per offset, so a misplaced or repeated
    /// write cannot pass the byte-exact comparison.
    fn pattern(len: usize, seed: usize) -> Vec<u8> {
        (0..len).map(|i| ((i + seed) % 251 + 1) as u8).collect()
    }

    fn assert_no_probe_leftover(dir: &TempDir) {
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read scratch dir")
            .map(|entry| entry.expect("dir entry").file_name())
            .collect();
        assert!(leftovers.is_empty(), "probe left {leftovers:?} behind");
    }

    #[compio::test]
    async fn probe_accepts_the_build_tree_filesystem() {
        let dir = target_scratch_dir();
        match probe_uncached_write(dir.path()).await {
            Ok(()) => {}
            Err(error @ UncachedIoError::Unsupported { .. }) => skip_or_fail(&error.to_string()),
            Err(error) => panic!("probe hit an I/O error: {error}"),
        }
        assert_no_probe_leftover(&dir);
    }

    #[compio::test]
    async fn probe_reports_tmpfs_as_unsupported() {
        let Some(dir) = tmpfs_dir_that_rejects_the_flag() else {
            return;
        };
        let error = probe_uncached_write(dir.path())
            .await
            .expect_err("tmpfs never takes RWF_DONTCACHE");
        match &error {
            UncachedIoError::Unsupported { filesystem, .. } => assert_eq!(filesystem, "tmpfs"),
            other => panic!("expected Unsupported, got {other}"),
        }
        assert!(error.to_string().contains("tmpfs"), "{error}");
        assert_no_probe_leftover(&dir);
    }

    #[compio::test]
    async fn probe_reports_a_missing_directory_as_io_error() {
        let dir = target_scratch_dir();
        let error = probe_uncached_write(&dir.path().join("missing"))
            .await
            .expect_err("no such dir");
        match error {
            UncachedIoError::Io(error) => assert_eq!(error.kind(), io::ErrorKind::NotFound),
            other => panic!("expected Io, got {other}"),
        }
    }

    #[test]
    fn probe_file_names_never_repeat() {
        let names: HashSet<String> = (0..64).map(|_| probe_file_name()).collect();
        assert_eq!(names.len(), 64, "probe file name is not unique enough");
    }

    #[compio::test]
    async fn uncached_writes_round_trip_unaligned_buffers() {
        let Some(dir) = supported_scratch_dir().await else {
            return;
        };
        let (fd, path) = open_new(&dir);
        let mut expected = Vec::new();

        for len in [300, 4113, 1024 * 1024] {
            let buf = pattern(len, expected.len());
            let BufResult(result, buf) =
                write_all_at_uncached(&fd, buf, expected.len() as u64).await;
            result.expect("uncached write");
            expected.extend_from_slice(&buf);
        }

        let slices: Vec<Vec<u8>> = [7, 4096, 1, 9000, 33]
            .iter()
            .enumerate()
            .map(|(index, len)| pattern(*len, expected.len() + index))
            .collect();
        let BufResult(result, slices) =
            write_vectored_all_at_uncached(&fd, slices, expected.len() as u64).await;
        result.expect("uncached vectored write");
        for slice in &slices {
            expected.extend_from_slice(slice);
        }

        let on_disk = std::fs::read(&path).expect("read back");
        assert_eq!(on_disk.len(), expected.len());
        assert!(
            on_disk == expected,
            "file content differs from what was written"
        );
    }

    /// tmpfs rejecting the write is the only proof that `RWF_DONTCACHE`
    /// really rides on the submission, so the gate must not consult the ops
    /// under test: doing so would let a dropped `rw_flags` disable its own
    /// test. Kernel version plus filesystem decide it instead.
    fn tmpfs_dir_that_rejects_the_flag() -> Option<TempDir> {
        let release = kernel_release().unwrap_or_else(|| UNKNOWN.to_owned());
        match parse_kernel_version(&release) {
            Some(version) if version >= RWF_DONTCACHE_MIN_KERNEL => shm_tmpfs_dir(),
            _ => {
                skip_or_fail(&format!("kernel {release} predates RWF_DONTCACHE"));
                None
            }
        }
    }

    #[compio::test]
    async fn scalar_write_on_tmpfs_fails_with_eopnotsupp() {
        let Some(dir) = tmpfs_dir_that_rejects_the_flag() else {
            return;
        };
        let (fd, _path) = open_new(&dir);
        let BufResult(result, _) = write_all_at_uncached(&fd, pattern(300, 0), 0).await;
        let error = result.expect_err("tmpfs must reject RWF_DONTCACHE");
        assert_eq!(error.raw_os_error(), Some(libc::EOPNOTSUPP), "{error}");
    }

    #[compio::test]
    async fn vectored_write_on_tmpfs_fails_with_eopnotsupp() {
        let Some(dir) = tmpfs_dir_that_rejects_the_flag() else {
            return;
        };
        let (fd, _path) = open_new(&dir);
        let slices = vec![pattern(100, 0), pattern(200, 100)];
        let BufResult(result, _) = write_vectored_all_at_uncached(&fd, slices, 0).await;
        let error = result.expect_err("tmpfs must reject RWF_DONTCACHE");
        assert_eq!(error.raw_os_error(), Some(libc::EOPNOTSUPP), "{error}");
    }

    #[compio::test]
    async fn write_all_with_resubmits_the_tail_and_retries_interrupted() {
        let buf = pattern(10, 0);
        let mut calls = Vec::new();
        let mut outcomes = vec![
            Ok(3),
            Err(io::Error::from(io::ErrorKind::Interrupted)),
            Ok(7),
        ]
        .into_iter();
        let BufResult(result, buf) = write_all_with(buf, 10, |buf: Vec<u8>, written| {
            calls.push(written);
            ready(BufResult(outcomes.next().expect("scripted outcome"), buf))
        })
        .await;
        result.expect("all bytes written");
        assert_eq!(calls, [0, 3, 3]);
        assert_eq!(buf, pattern(10, 0));
    }

    /// A short vectored write must resume mid-iovec. Restarting the whole
    /// iovec array instead would duplicate bytes on disk and still report
    /// success.
    #[compio::test]
    async fn write_all_with_resumes_a_vectored_buffer_mid_iovec() {
        let slices = vec![pattern(7, 0), pattern(4096, 7), pattern(11, 4103)];
        let total = slices.iter().map(Vec::len).sum::<usize>();
        let mut handed = Vec::new();
        let mut outcomes = vec![Ok(7), Ok(4090), Ok(total - 7 - 4090)].into_iter();

        let BufResult(result, slices) =
            write_all_with(slices, total, |buf: Vec<Vec<u8>>, written| {
                let view = buf.slice(written);
                handed.push(view.iter_slice().map(<[u8]>::to_vec).collect::<Vec<_>>());
                let outcome = outcomes.next().expect("scripted outcome");
                ready(BufResult(outcome, view.into_inner()))
            })
            .await;
        result.expect("all bytes written");

        let expected = [
            vec![pattern(7, 0), pattern(4096, 7), pattern(11, 4103)],
            vec![pattern(4096, 7), pattern(11, 4103)],
            vec![pattern(4096, 7)[4090..].to_vec(), pattern(11, 4103)],
        ];
        assert_eq!(handed, expected);
        assert_eq!(
            slices,
            vec![pattern(7, 0), pattern(4096, 7), pattern(11, 4103)]
        );
    }

    #[compio::test]
    async fn write_all_with_reports_zero_length_completion_as_write_zero() {
        let BufResult(result, _) = write_all_with(pattern(4, 0), 4, |buf: Vec<u8>, _| {
            ready(BufResult(Ok(0), buf))
        })
        .await;
        let error = result.expect_err("zero-length completion");
        assert_eq!(error.kind(), io::ErrorKind::WriteZero);
    }

    #[test]
    fn kernel_warning_covers_only_the_inline_kick_range() {
        assert!(kernel_warning_for("6.13.9-arch1").is_none());
        assert!(kernel_warning_for("6.14.0-1-generic").is_some());
        assert!(kernel_warning_for("7.1.5").is_some());
        assert!(kernel_warning_for("7.2.0-1-cachyos").is_none());
        assert!(kernel_warning_for("not-a-version").is_none());
    }

    #[test]
    fn unsupported_message_names_the_failed_requirement_and_the_remedy() {
        let old_kernel = UncachedIoError::Unsupported {
            kernel_release: "6.8.0-45-generic".to_owned(),
            filesystem: "ext4".to_owned(),
        }
        .to_string();
        assert!(
            old_kernel.contains("kernel 6.8.0-45-generic predates"),
            "{old_kernel}"
        );

        let bad_filesystem = UncachedIoError::Unsupported {
            kernel_release: "7.2.0-1-cachyos".to_owned(),
            filesystem: "tmpfs".to_owned(),
        }
        .to_string();
        assert!(
            bad_filesystem.contains("filesystem tmpfs rejects"),
            "{bad_filesystem}"
        );

        let unknown_kernel = UncachedIoError::Unsupported {
            kernel_release: UNKNOWN.to_owned(),
            filesystem: "ext4".to_owned(),
        }
        .to_string();
        assert!(
            unknown_kernel.contains("kernel version could not be determined"),
            "{unknown_kernel}"
        );

        for message in [old_kernel, bad_filesystem, unknown_kernel] {
            assert!(message.contains("write_io = \"buffered\""), "{message}");
            assert!(message.contains("Linux >= 6.14"), "{message}");
        }
    }
}
