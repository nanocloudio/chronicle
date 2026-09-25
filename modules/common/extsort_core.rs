// Bounded, no_std, no-alloc EXTERNAL SORT core: records of any number sorted in a
// fixed work buffer, spilling sorted runs to an abstract run store and merging
// them back. Like the other `*_core.rs` files it carries no inner attributes and
// no test module; it is `include!`d by the modules that sort (sector's `relop`)
// and by the host harness. Needs `vm_core` in scope (`Value`, for key encoding).
//
// Order. A record carries a normalised KEY (order-preserving bytes, see
// `encode_key_part`), its FRAME (the chronicle typed record frame) and an arrival
// SEQUENCE number. Records order by (key, frame bytes, sequence): a total order in
// which identical frames are the only ties, so the emitted data depends on the
// input multiset alone — not on arrival order, the buffer size, or how many runs
// were spilled.
//
// Work. Every phase is incremental and runs in caller-owned memory (`mem`, passed
// to each call). `push` stores a record or reports the buffer full;
// `step(mem, store, work)` does at most `work` units (heap sifts, records moved)
// of sorting, spilling or merging; `next` hands out one record of the result. A
// store read may answer `Pending` ("ask again"): the core keeps the request and
// repeats it unchanged on the next call. Store writes are synchronous and bounded
// by the store's write chunk.
//
// Runs. A run is a sequence of BLOCKS: `[payload_len: u32 LE][crc32c: u32 LE]
// [payload]`, where the payload is whole records. When a block is loaded, before
// any of its records is compared or handed out, its length is bounded by what
// was read, its CRC is checked, and every record in it is checked to frame
// within it. A torn or corrupted spill therefore ends the sort with
// `ExtErr::Corrupt` instead of emitting a wrong answer — or indexing past a
// block, which on a device with no unwinder is a hang. The CRC detects accidental
// damage; it is not a defence against a store that forges its contents.
//
// Record layout (buffer and runs):
//   [rec_len: u32 LE][seq: u64 LE][gkey_len: u16 LE][key_len: u16 LE][key][frame]
// `gkey_len <= key_len` marks the leading key bytes that identify a GROUP (the
// rest orders records within a group, e.g. by an aggregate's value).

/// Record header bytes before the key.
pub const EXT_REC_HDR: usize = 16;
/// Block header bytes.
pub const EXT_BLOCK_HDR: usize = 8;
/// Largest merge fan-in: the reader table a sort holds, one reader (a pending
/// block request and a cursor) per input run of the merge in progress. Fixed so
/// a merge's state is bounded whatever the input, and a policy ceiling rather
/// than a derived one — the work buffer usually affords fewer readers than this
/// (`fanin`: one read block per run plus the write block), and a merge that
/// cannot take every run at once takes another pass.
pub const EXT_MAX_FANIN: usize = 16;
/// The number of runs one sort tracks at once; a merge pass folds them before
/// this fills, so input length is unbounded.
pub const EXT_MAX_RUNS: usize = 64;
/// Longest normalised key.
pub const EXT_KEY_MAX: usize = 1024;

// The relationships the gate checks (limit_register.md, Constraints).
const _: () = assert!(EXT_MAX_FANIN >= 2 && EXT_MAX_FANIN <= EXT_MAX_RUNS);
const _: () = assert!(EXT_KEY_MAX <= u16::MAX as usize);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ExtErr {
    /// A record larger than a block, or a key longer than `EXT_KEY_MAX`.
    TooLarge,
    /// The work buffer cannot hold one record and the merge blocks.
    BudgetTooSmall,
    /// More runs than `EXT_MAX_RUNS` would need tracking.
    TooManyRuns,
    /// A block failed its CRC or framing check.
    Corrupt,
    /// The run store refused (its errno).
    Store(i32),
    /// An operation not valid in the current phase.
    Phase,
}

/// A read the store may not answer yet.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Read {
    Done(usize),
    Pending,
    Err(i32),
}

/// Where runs live. Implemented by the consuming module over its storage
/// grant (sector's `relop`: `storage.object` on a scratch volume), and in
/// memory by the host tests. Run ids are small integers the core allocates.
pub trait RunStore {
    /// Largest single `write` the store accepts synchronously.
    fn write_chunk(&self) -> usize;
    fn create(&mut self, run: u32) -> Result<(), i32>;
    fn write(&mut self, run: u32, bytes: &[u8]) -> Result<(), i32>;
    fn commit(&mut self, run: u32) -> Result<(), i32>;
    /// Read up to `buf.len()` bytes at `offset` of committed run `run`.
    fn read_at(&mut self, run: u32, offset: u64, buf: &mut [u8]) -> Read;
    fn delete(&mut self, run: u32) -> Result<(), i32>;
}

// ---- normalised keys ------------------------------------------------------

const TAG_NULL: u8 = 0x01;
const TAG_BOOL: u8 = 0x02;
const TAG_NUM: u8 = 0x03;
const TAG_BYTES: u8 = 0x04;

/// Append `v` to `out[at..]` as an order-preserving key component: byte-wise
/// comparison of encodings orders the values (null < bool < numbers < bytes;
/// numbers by value across signed/unsigned; bytes lexicographically). `desc`
/// inverts the component. Returns the new end, or `None` if `out` is full.
///
/// `Double`, `Msg` and an unresolved `Scratch` have no encoding here and are
/// written as NULL, so they sort with nulls and compare equal to one another.
/// A caller keys on them only after resolving a scratch value to its bytes and
/// deciding what the others mean — as a grouping key, silently merging them
/// with nulls would be a wrong answer.
pub fn encode_key_part(v: Value<'_>, desc: bool, out: &mut [u8], at: usize) -> Option<usize> {
    let start = at;
    let mut p = at;
    let mut put = |b: u8, p: &mut usize| -> Option<()> {
        if *p >= out.len() {
            return None;
        }
        out[*p] = b;
        *p += 1;
        Some(())
    };
    match v {
        Value::Bool(b) => {
            put(TAG_BOOL, &mut p)?;
            put(u8::from(b), &mut p)?;
        }
        Value::Int(_) | Value::Uint(_) => {
            let n: i128 = match v {
                Value::Int(i) => i128::from(i),
                Value::Uint(u) => i128::from(u),
                _ => 0,
            };
            put(TAG_NUM, &mut p)?;
            let flipped = (n as u128) ^ (1u128 << 127);
            for b in flipped.to_be_bytes() {
                put(b, &mut p)?;
            }
        }
        Value::Bytes(_) | Value::Str(_) => {
            let bytes: &[u8] = match v {
                Value::Bytes(b) => b,
                Value::Str(s) => s.as_bytes(),
                _ => &[],
            };
            put(TAG_BYTES, &mut p)?;
            // 0x00 escapes as 0x00 0xFF; the terminator is 0x00 0x00, so a
            // prefix orders before any extension of it.
            for &b in bytes {
                put(b, &mut p)?;
                if b == 0 {
                    put(0xFF, &mut p)?;
                }
            }
            put(0, &mut p)?;
            put(0, &mut p)?;
        }
        _ => put(TAG_NULL, &mut p)?,
    }
    if desc {
        let mut i = start;
        while i < p {
            out[i] = !out[i];
            i += 1;
        }
    }
    Some(p)
}

// ---- CRC32C (Castagnoli), table-driven ---------------------------------------

const fn crc32c_table() -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0x82F6_3B78 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        t[i] = c;
        i += 1;
    }
    t
}

static CRC32C_TABLE: [u32; 256] = crc32c_table();

pub fn crc32c(data: &[u8]) -> u32 {
    let mut c = !0u32;
    for &b in data {
        c = CRC32C_TABLE[((c ^ u32::from(b)) & 0xFF) as usize] ^ (c >> 8);
    }
    !c
}

// ---- byte moves ----------------------------------------------------------------
//
// Plain loops rather than `copy_from_slice` / `copy_within`: their length and
// bounds panics carry formatting that a PIC module cannot link. Callers pass
// ranges they have already bounded.

/// `dst[..src.len()] = src` (up to `dst`'s length).
fn ext_copy(dst: &mut [u8], src: &[u8]) {
    let n = src.len().min(dst.len());
    let mut i = 0;
    while i < n {
        dst[i] = src[i];
        i += 1;
    }
}

/// Move `m[src..src + len]` to `m[dst..]`; the ranges may overlap.
fn ext_move(m: &mut [u8], src: usize, len: usize, dst: usize) {
    if dst <= src {
        let mut i = 0;
        while i < len {
            m[dst + i] = m[src + i];
            i += 1;
        }
    } else {
        let mut i = len;
        while i > 0 {
            i -= 1;
            m[dst + i] = m[src + i];
        }
    }
}

// ---- records ----------------------------------------------------------------

fn rd_u16(b: &[u8], at: usize) -> usize {
    usize::from(u16::from_le_bytes([b[at], b[at + 1]]))
}
fn rd_u32(b: &[u8], at: usize) -> usize {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]]) as usize
}
fn rd_u64(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes([
        b[at],
        b[at + 1],
        b[at + 2],
        b[at + 3],
        b[at + 4],
        b[at + 5],
        b[at + 6],
        b[at + 7],
    ])
}

/// Every record in a block payload frames within it: its length covers its
/// header and fits in what is left, its key fits after the header, and its group
/// key within its key. Checked once when a block loads, so everything that later
/// reads a record by its own lengths — comparison, emission, hand-out — reads
/// inside the block. No addition here can overflow: each bound is compared
/// against what remains, not summed.
fn block_frames(p: &[u8]) -> bool {
    let mut at = 0usize;
    while at < p.len() {
        let rest = p.len() - at;
        if rest < EXT_REC_HDR {
            return false;
        }
        let len = rd_u32(p, at);
        if len < EXT_REC_HDR || len > rest {
            return false;
        }
        let gk = rd_u16(p, at + 12);
        let k = rd_u16(p, at + 14);
        if gk > k || k > len - EXT_REC_HDR {
            return false;
        }
        at += len;
    }
    true
}

/// A record's parts: (sequence, group key, full key, frame).
pub fn rec_parts(rec: &[u8]) -> (u64, &[u8], &[u8], &[u8]) {
    let seq = rd_u64(rec, 4);
    let gk = rd_u16(rec, 12);
    let k = rd_u16(rec, 14);
    let key = &rec[EXT_REC_HDR..EXT_REC_HDR + k];
    (seq, &key[..gk], key, &rec[EXT_REC_HDR + k..])
}

/// Total order of two records (key, frame bytes, sequence).
pub fn rec_cmp(a: &[u8], b: &[u8]) -> core::cmp::Ordering {
    let (sa, _, ka, fa) = rec_parts(a);
    let (sb, _, kb, fb) = rec_parts(b);
    ka.cmp(kb)
        .then_with(|| fa.cmp(fb))
        .then_with(|| sa.cmp(&sb))
}

// ---- the sorter ---------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Phase {
    /// Accepting records into the buffer.
    Filling,
    /// Heap-sorting the buffer's index (then spilling or serving from memory).
    Sorting,
    /// Writing the sorted buffer as a run.
    Spilling,
    /// Merging runs into fewer runs (more runs than the fan-in).
    Reducing,
    /// Serving the result.
    Output,
    /// All records served.
    Done,
    Failed,
}

#[derive(Clone, Copy)]
struct RunInfo {
    id: u32,
    live: bool,
}

#[derive(Clone, Copy)]
struct Reader {
    run: u32,
    /// Offset of the next block to load.
    next_block: u64,
    /// Block buffer region in the work buffer.
    buf_at: usize,
    blen: usize,
    pos: usize,
    eof: bool,
    /// A load is outstanding (the store answered Pending).
    loading: bool,
}

const NO_READER: Reader = Reader {
    run: 0,
    next_block: 0,
    buf_at: 0,
    blen: 0,
    pos: 0,
    eof: true,
    loading: false,
};

/// Counters for reporting (never part of the data).
#[derive(Clone, Copy, Default, Debug)]
pub struct ExtStats {
    pub records: u64,
    pub runs_written: u32,
    pub bytes_spilled: u64,
    pub merge_passes: u32,
}

/// The sorter's state. Its work memory is the caller's: every call that
/// touches records takes the same `mem` (at least `cap` bytes), so the sorter
/// can live beside its buffer in a module's fixed state.
pub struct Sorter {
    cap: usize,
    block: usize,
    // Filling: records grow up from `front`, their u32 offsets down from `back`.
    front: usize,
    back: usize,
    nrec: usize,
    seq: u64,
    pub phase: Phase,
    input_done: bool,
    // incremental heapsort over the index
    hs_build: usize,
    hs_end: usize,
    // spill cursor (index position) and the write block being filled
    spill_i: usize,
    wlen: usize,
    wrun: u32,
    wrun_open: bool,
    wrun_bytes: u64,
    // runs
    runs: [RunInfo; EXT_MAX_RUNS],
    nruns: usize,
    next_id: u32,
    // merge
    readers: [Reader; EXT_MAX_FANIN],
    nreaders: usize,
    /// Output directly from memory (nothing spilled).
    from_memory: bool,
    out_i: usize,
    /// The final merge's inputs, kept for `rewind`.
    final_runs: [u32; EXT_MAX_FANIN],
    nfinal: usize,
    reduce_out: u32,
    /// Records the reduce pass in progress has written to `reduce_out`.
    reduce_emitted: u64,
    /// A reduce pass runs while input is still arriving; resume filling after.
    reduce_then_fill: bool,
    /// Only the first `limit` records of the result are wanted (0 = all).
    limit: u64,
    served: u64,
    pub stats: ExtStats,
    pub err: Option<ExtErr>,
}

// Index entry i (0-based, in sort order position) lives at back + 4*i.
fn idx(m: &[u8], back: usize, i: usize) -> usize {
    rd_u32(m, back + 4 * i)
}
fn set_idx(m: &mut [u8], back: usize, i: usize, v: usize) {
    let at = back + 4 * i;
    m[at..at + 4].copy_from_slice(&(v as u32).to_le_bytes());
}
fn rec_at(m: &[u8], off: usize) -> &[u8] {
    let len = rd_u32(m, off);
    &m[off..off + len]
}
fn less(m: &[u8], back: usize, i: usize, j: usize) -> bool {
    rec_cmp(rec_at(m, idx(m, back, i)), rec_at(m, idx(m, back, j))) == core::cmp::Ordering::Less
}
fn sift(m: &mut [u8], back: usize, mut root: usize, end: usize) {
    loop {
        let l = 2 * root + 1;
        if l >= end {
            return;
        }
        let mut c = l;
        if l + 1 < end && less(m, back, l, l + 1) {
            c = l + 1;
        }
        if less(m, back, root, c) {
            let (a, b) = (idx(m, back, root), idx(m, back, c));
            set_idx(m, back, root, b);
            set_idx(m, back, c, a);
            root = c;
        } else {
            return;
        }
    }
}
/// Heap-sort index entries 0..n by the offsets they hold.
fn sort_offsets(m: &mut [u8], back: usize, n: usize) {
    fn sift_off(m: &mut [u8], back: usize, mut root: usize, end: usize) {
        loop {
            let l = 2 * root + 1;
            if l >= end {
                return;
            }
            let mut c = l;
            if l + 1 < end && idx(m, back, l) < idx(m, back, l + 1) {
                c = l + 1;
            }
            let (a, b) = (idx(m, back, root), idx(m, back, c));
            if a >= b {
                return;
            }
            set_idx(m, back, root, b);
            set_idx(m, back, c, a);
            root = c;
        }
    }
    let mut b = n / 2;
    while b > 0 {
        b -= 1;
        sift_off(m, back, b, n);
    }
    let mut e = n;
    while e > 1 {
        e -= 1;
        let (a, z) = (idx(m, back, 0), idx(m, back, e));
        set_idx(m, back, 0, z);
        set_idx(m, back, e, a);
        sift_off(m, back, 0, e);
    }
}

fn reader_rec(m: &[u8], rd: Reader) -> &[u8] {
    let at = rd.buf_at + rd.pos;
    let len = rd_u32(m, at);
    &m[at..at + len]
}

impl Sorter {
    /// A sorter over `cap` bytes of work memory, spilling in blocks of
    /// `block` bytes.
    pub fn new(cap: usize, block: usize) -> Result<Self, ExtErr> {
        // One write block plus at least two merge blocks must fit.
        if block < 1024 || cap < 3 * block {
            return Err(ExtErr::BudgetTooSmall);
        }
        Ok(Sorter {
            cap,
            block,
            front: block,
            back: cap,
            nrec: 0,
            seq: 0,
            phase: Phase::Filling,
            input_done: false,
            hs_build: 0,
            hs_end: 0,
            spill_i: 0,
            wlen: 0,
            wrun: 0,
            wrun_open: false,
            wrun_bytes: 0,
            runs: [RunInfo { id: 0, live: false }; EXT_MAX_RUNS],
            nruns: 0,
            next_id: 0,
            readers: [NO_READER; EXT_MAX_FANIN],
            nreaders: 0,
            from_memory: false,
            out_i: 0,
            final_runs: [0; EXT_MAX_FANIN],
            nfinal: 0,
            reduce_out: 0,
            reduce_emitted: 0,
            reduce_then_fill: false,
            limit: 0,
            served: 0,
            stats: ExtStats::default(),
            err: None,
        })
    }

    /// The work memory the sorter was sized for, or `None` if `mem` is short.
    fn work<'m>(&self, mem: &'m mut [u8]) -> Option<&'m mut [u8]> {
        mem.get_mut(..self.cap)
    }

    /// Want only the first `n` records of the result (top-k). A full buffer
    /// then keeps its first `n` records and goes on filling, and every run —
    /// spilled from the buffer or produced by a merge — holds at most `n`, since
    /// only the first `n` of a sorted run can reach the result. Live spill is
    /// therefore bounded by `n` and the run table, not by the input. Set before
    /// the first `push`.
    pub fn set_limit(&mut self, n: u64) -> Result<(), ExtErr> {
        if self.phase != Phase::Filling || self.stats.records != 0 {
            return Err(ExtErr::Phase);
        }
        self.limit = n;
        Ok(())
    }

    /// Records of the current buffer a run takes (all, or the limit).
    fn keep(&self) -> usize {
        if self.limit == 0 {
            self.nrec
        } else {
            self.nrec.min(self.limit.min(usize::MAX as u64) as usize)
        }
    }

    /// Largest record (header + key + frame) a block can carry.
    pub fn max_record(&self) -> usize {
        self.block - EXT_BLOCK_HDR
    }

    /// Store one record. `Ok(false)` means the buffer is full: call `step`
    /// until the phase is `Filling` again, then push the same record.
    pub fn push(
        &mut self,
        mem: &mut [u8],
        key: &[u8],
        gkey_len: usize,
        frame: &[u8],
    ) -> Result<bool, ExtErr> {
        if self.phase != Phase::Filling || self.input_done {
            return Err(ExtErr::Phase);
        }
        let Some(m) = self.work(mem) else {
            return Err(ExtErr::BudgetTooSmall);
        };
        let len = EXT_REC_HDR + key.len() + frame.len();
        if key.len() > EXT_KEY_MAX || gkey_len > key.len() || len > self.max_record() {
            return Err(ExtErr::TooLarge);
        }
        if self.front + len + 4 > self.back {
            if self.nrec == 0 {
                return Err(ExtErr::BudgetTooSmall);
            }
            self.begin_sort();
            return Ok(false);
        }
        let at = self.front;
        m[at..at + 4].copy_from_slice(&(len as u32).to_le_bytes());
        m[at + 4..at + 12].copy_from_slice(&self.seq.to_le_bytes());
        m[at + 12..at + 14].copy_from_slice(&(gkey_len as u16).to_le_bytes());
        m[at + 14..at + 16].copy_from_slice(&(key.len() as u16).to_le_bytes());
        ext_copy(&mut m[at + 16..], key);
        ext_copy(&mut m[at + 16 + key.len()..], frame);
        self.back -= 4;
        let b = self.back;
        m[b..b + 4].copy_from_slice(&(at as u32).to_le_bytes());
        self.front += len;
        self.nrec += 1;
        self.seq += 1;
        self.stats.records += 1;
        Ok(true)
    }

    /// No more input.
    pub fn finish(&mut self) -> Result<(), ExtErr> {
        if self.phase != Phase::Filling {
            return Err(ExtErr::Phase);
        }
        self.input_done = true;
        self.begin_sort();
        Ok(())
    }

    fn begin_sort(&mut self) {
        self.phase = Phase::Sorting;
        self.hs_build = self.nrec / 2;
        self.hs_end = self.nrec;
    }

    /// Advance by at most `work` units. Returns the phase afterwards.
    pub fn step<S: RunStore>(&mut self, mem: &mut [u8], store: &mut S, work: u32) -> Phase {
        let Some(m) = self.work(mem) else {
            self.fail(store, ExtErr::BudgetTooSmall);
            return self.phase;
        };
        let mut budget = work.max(1);
        while budget > 0 {
            let r = match self.phase {
                Phase::Sorting => self.step_sort(m, &mut budget),
                Phase::Spilling => self.step_spill(m, store, &mut budget),
                Phase::Reducing => self.step_reduce(m, store, &mut budget),
                _ => return self.phase,
            };
            if let Err(e) = r {
                self.fail(store, e);
                return self.phase;
            }
            if matches!(self.phase, Phase::Filling | Phase::Output | Phase::Done) {
                return self.phase;
            }
        }
        self.phase
    }

    fn fail<S: RunStore>(&mut self, store: &mut S, e: ExtErr) {
        self.err = Some(e);
        self.phase = Phase::Failed;
        self.discard(store);
    }

    /// Delete every run still held (after output, on failure, on cancel).
    pub fn discard<S: RunStore>(&mut self, store: &mut S) {
        let mut i = 0;
        while i < self.nruns {
            if self.runs[i].live {
                let _ = store.delete(self.runs[i].id);
                self.runs[i].live = false;
            }
            i += 1;
        }
        self.nruns = 0;
        self.nfinal = 0;
    }

    fn step_sort(&mut self, m: &mut [u8], budget: &mut u32) -> Result<(), ExtErr> {
        let back = self.back;
        while *budget > 0 {
            *budget -= 1;
            if self.hs_build > 0 {
                self.hs_build -= 1;
                sift(m, back, self.hs_build, self.nrec);
                continue;
            }
            if self.hs_end > 1 {
                self.hs_end -= 1;
                let e = self.hs_end;
                let (a, z) = (idx(m, back, 0), idx(m, back, e));
                set_idx(m, back, 0, z);
                set_idx(m, back, e, a);
                sift(m, back, 0, e);
                continue;
            }
            // Sorted ascending in index positions 0..nrec.
            if !self.input_done && self.keep() < self.nrec {
                // Top-k: records past the first `limit` of a sorted buffer
                // cannot be in the result. Keep the rest and fill again if
                // that frees a quarter of the buffer; otherwise sort them
                // again (compaction leaves them in address order) and spill.
                self.truncate(m);
                if self.back - self.front >= self.cap / 4 {
                    self.phase = Phase::Filling;
                } else {
                    self.begin_sort();
                }
                return Ok(());
            }
            if self.input_done && self.nruns == 0 {
                self.from_memory = true;
                self.out_i = 0;
                self.phase = if self.nrec == 0 {
                    Phase::Done
                } else {
                    Phase::Output
                };
            } else {
                self.phase = Phase::Spilling;
                self.spill_i = 0;
                self.wlen = 0;
                self.wrun = 0;
            }
            return Ok(());
        }
        Ok(())
    }

    /// Keep the first `keep()` records of the sorted buffer, compacted to the
    /// front, with their index moved to the end of the buffer.
    fn truncate(&mut self, m: &mut [u8]) {
        let k = self.keep();
        let back = self.back;
        // Address order, so each record moves down over free or already-moved
        // bytes only.
        sort_offsets(m, back, k);
        let mut to = self.block;
        let mut i = 0;
        while i < k {
            let off = idx(m, back, i);
            let len = rd_u32(m, off);
            ext_move(m, off, len, to);
            set_idx(m, back, i, to);
            to += len;
            i += 1;
        }
        let new_back = self.cap - 4 * k;
        ext_move(m, back, 4 * k, new_back);
        self.front = to;
        self.back = new_back;
        self.nrec = k;
    }

    fn new_run<S: RunStore>(&mut self, store: &mut S) -> Result<u32, ExtErr> {
        if self.nruns >= EXT_MAX_RUNS {
            return Err(ExtErr::TooManyRuns);
        }
        let id = self.next_id;
        self.next_id += 1;
        store.create(id).map_err(ExtErr::Store)?;
        self.runs[self.nruns] = RunInfo { id, live: true };
        self.nruns += 1;
        self.stats.runs_written += 1;
        self.wrun_bytes = 0;
        Ok(id)
    }

    /// Append the record at `m[at..at + len]` (in the fill area or a reader
    /// block) to the write block at `m[0..block]`, flushing a full block first.
    fn emit<S: RunStore>(
        &mut self,
        m: &mut [u8],
        store: &mut S,
        run: u32,
        at: usize,
        len: usize,
    ) -> Result<(), ExtErr> {
        if EXT_BLOCK_HDR + self.wlen + len > self.block {
            self.flush_block(m, store, run)?;
        }
        let dst = EXT_BLOCK_HDR + self.wlen;
        ext_move(m, at, len, dst);
        self.wlen += len;
        Ok(())
    }

    fn flush_block<S: RunStore>(
        &mut self,
        m: &mut [u8],
        store: &mut S,
        run: u32,
    ) -> Result<(), ExtErr> {
        if self.wlen == 0 {
            return Ok(());
        }
        let wlen = self.wlen;
        let crc = crc32c(&m[EXT_BLOCK_HDR..EXT_BLOCK_HDR + wlen]);
        m[0..4].copy_from_slice(&(wlen as u32).to_le_bytes());
        m[4..8].copy_from_slice(&crc.to_le_bytes());
        let total = EXT_BLOCK_HDR + wlen;
        let chunk = store.write_chunk().max(1);
        let mut off = 0;
        while off < total {
            let n = chunk.min(total - off);
            store.write(run, &m[off..off + n]).map_err(ExtErr::Store)?;
            off += n;
        }
        self.stats.bytes_spilled += total as u64;
        self.wrun_bytes += total as u64;
        self.wlen = 0;
        Ok(())
    }

    fn step_spill<S: RunStore>(
        &mut self,
        m: &mut [u8],
        store: &mut S,
        budget: &mut u32,
    ) -> Result<(), ExtErr> {
        if !self.wrun_open && self.nrec > 0 {
            self.wrun = self.new_run(store)?;
            self.wrun_open = true;
        }
        let n = self.keep();
        while *budget > 0 && self.spill_i < n {
            *budget -= 1;
            let off = idx(m, self.back, self.spill_i);
            let len = rd_u32(m, off);
            let run = self.wrun;
            self.emit(m, store, run, off, len)?;
            self.spill_i += 1;
        }
        if self.spill_i < n {
            return Ok(());
        }
        let run = self.wrun;
        if self.wrun_open {
            self.flush_block(m, store, run)?;
            store.commit(run).map_err(ExtErr::Store)?;
            self.wrun_open = false;
        }
        // Buffer empty again.
        self.front = self.block;
        self.back = self.cap;
        self.nrec = 0;
        self.wrun_bytes = 0;
        if !self.input_done {
            // Keep the run count bounded however long the input is: once the
            // tracked runs near the limit, fold `F` of them into one before
            // accepting more records (the fill area is empty, so its memory
            // serves the merge).
            if self.nruns + 1 >= EXT_MAX_RUNS {
                self.reduce_then_fill = true;
                return self.begin_reduce(store);
            }
            self.phase = Phase::Filling;
            return Ok(());
        }
        self.begin_merges(store)
    }

    // ---- merging ----------------------------------------------------------------

    /// Fan-in the buffer affords: one write block plus `F` read blocks.
    fn fanin(&self) -> usize {
        ((self.cap / self.block.max(1)).saturating_sub(1)).clamp(2, EXT_MAX_FANIN)
    }

    fn live_runs(&self, out: &mut [u32; EXT_MAX_RUNS]) -> usize {
        let mut n = 0;
        let mut i = 0;
        while i < self.nruns {
            if self.runs[i].live {
                out[n] = self.runs[i].id;
                n += 1;
            }
            i += 1;
        }
        n
    }

    fn begin_merges<S: RunStore>(&mut self, store: &mut S) -> Result<(), ExtErr> {
        let mut live = [0u32; EXT_MAX_RUNS];
        let n = self.live_runs(&mut live);
        let f = self.fanin();
        if n <= f {
            self.nfinal = n;
            self.final_runs[..n].clone_from_slice(&live[..n]);
            self.open_readers(&live[..n]);
            self.phase = if n == 0 { Phase::Done } else { Phase::Output };
            return Ok(());
        }
        self.begin_reduce(store)
    }

    /// Merge the first `F` live runs into one new run.
    fn begin_reduce<S: RunStore>(&mut self, store: &mut S) -> Result<(), ExtErr> {
        let mut live = [0u32; EXT_MAX_RUNS];
        let n = self.live_runs(&mut live);
        let f = self.fanin().min(n);
        self.stats.merge_passes += 1;
        self.open_readers(&live[..f]);
        self.reduce_out = self.new_run(store)?;
        self.reduce_emitted = 0;
        self.wlen = 0;
        self.phase = Phase::Reducing;
        Ok(())
    }

    fn open_readers(&mut self, runs: &[u32]) {
        self.nreaders = runs.len();
        let mut i = 0;
        while i < runs.len() {
            self.readers[i] = Reader {
                run: runs[i],
                next_block: 0,
                buf_at: self.block * (i + 1),
                blen: 0,
                pos: 0,
                eof: false,
                loading: true,
            };
            i += 1;
        }
    }

    /// Make sure reader `r` holds a record at `pos` (loading its next block if
    /// needed). `Ok(true)`: ready; `Ok(false)`: pending; readers at end report
    /// `eof`.
    fn fill_reader<S: RunStore>(
        &mut self,
        m: &mut [u8],
        store: &mut S,
        r: usize,
    ) -> Result<bool, ExtErr> {
        let rd = self.readers[r];
        if rd.eof {
            return Ok(true);
        }
        if !rd.loading && rd.pos < rd.blen {
            return Ok(true);
        }
        let block = self.block;
        let buf = &mut m[rd.buf_at..rd.buf_at + block];
        match store.read_at(rd.run, rd.next_block, buf) {
            Read::Pending => {
                self.readers[r].loading = true;
                Ok(false)
            }
            Read::Err(e) => Err(ExtErr::Store(e)),
            Read::Done(0) => {
                self.readers[r].eof = true;
                self.readers[r].loading = false;
                Ok(true)
            }
            Read::Done(n) => {
                // `plen` comes off the medium, so it is compared against the
                // room left rather than added to the header size: on a 32-bit
                // core `EXT_BLOCK_HDR + plen` wraps for a torn length near
                // `u32::MAX` and would pass the very check meant to refuse it.
                let n = n.min(block);
                if n < EXT_BLOCK_HDR {
                    return Err(ExtErr::Corrupt);
                }
                let plen = rd_u32(buf, 0);
                let crc = rd_u32(buf, 4) as u32;
                if plen > n - EXT_BLOCK_HDR {
                    return Err(ExtErr::Corrupt);
                }
                let payload = &buf[EXT_BLOCK_HDR..EXT_BLOCK_HDR + plen];
                if crc32c(payload) != crc || !block_frames(payload) {
                    return Err(ExtErr::Corrupt);
                }
                let rd = &mut self.readers[r];
                rd.next_block += (EXT_BLOCK_HDR + plen) as u64;
                rd.blen = EXT_BLOCK_HDR + plen;
                rd.pos = EXT_BLOCK_HDR;
                rd.loading = false;
                Ok(true)
            }
        }
    }

    /// The reader holding the least record: `Ok(None)` when all are at end,
    /// `Ok(Some(usize::MAX))` when one is still loading.
    fn pick_min<S: RunStore>(
        &mut self,
        m: &mut [u8],
        store: &mut S,
    ) -> Result<Option<usize>, ExtErr> {
        let mut best: Option<usize> = None;
        let mut r = 0;
        while r < self.nreaders {
            if !self.fill_reader(m, store, r)? {
                return Ok(Some(usize::MAX));
            }
            let rd = self.readers[r];
            if !rd.eof {
                best = match best {
                    None => Some(r),
                    Some(b) => {
                        let a = reader_rec(m, rd);
                        let c = reader_rec(m, self.readers[b]);
                        if rec_cmp(a, c) == core::cmp::Ordering::Less {
                            Some(r)
                        } else {
                            Some(b)
                        }
                    }
                };
            }
            r += 1;
        }
        Ok(best)
    }

    fn advance(&mut self, m: &[u8], r: usize) -> Result<(), ExtErr> {
        let rd = self.readers[r];
        let len = rd_u32(m, rd.buf_at + rd.pos);
        if len < EXT_REC_HDR || len > rd.blen - rd.pos {
            return Err(ExtErr::Corrupt);
        }
        self.readers[r].pos += len;
        Ok(())
    }

    fn step_reduce<S: RunStore>(
        &mut self,
        m: &mut [u8],
        store: &mut S,
        budget: &mut u32,
    ) -> Result<(), ExtErr> {
        while *budget > 0 {
            *budget -= 1;
            // Under a limit, the merged run ends at the `limit`-th record: its
            // input is sorted, so nothing after that can reach the result. No
            // reader has a read outstanding here — the record just written was
            // picked with every reader filled — so the inputs can be deleted as
            // on a full merge.
            let next = if self.limit != 0 && self.reduce_emitted >= self.limit {
                None
            } else {
                self.pick_min(m, store)?
            };
            match next {
                Some(usize::MAX) => return Ok(()), // pending read
                Some(r) => {
                    let rd = self.readers[r];
                    let at = rd.buf_at + rd.pos;
                    let len = rd_u32(m, at);
                    let out = self.reduce_out;
                    self.emit(m, store, out, at, len)?;
                    self.advance(m, r)?;
                    self.reduce_emitted += 1;
                }
                None => {
                    let out = self.reduce_out;
                    self.flush_block(m, store, out)?;
                    store.commit(out).map_err(ExtErr::Store)?;
                    // The inputs are consumed.
                    let mut i = 0;
                    while i < self.nreaders {
                        let id = self.readers[i].run;
                        let _ = store.delete(id);
                        self.mark_dead(id);
                        i += 1;
                    }
                    self.nreaders = 0;
                    self.compact_runs();
                    if self.reduce_then_fill {
                        self.reduce_then_fill = false;
                        self.phase = Phase::Filling;
                        return Ok(());
                    }
                    return self.begin_merges(store);
                }
            }
        }
        Ok(())
    }

    fn mark_dead(&mut self, id: u32) {
        let mut i = 0;
        while i < self.nruns {
            if self.runs[i].id == id {
                self.runs[i].live = false;
            }
            i += 1;
        }
    }

    fn compact_runs(&mut self) {
        let mut w = 0;
        let mut r = 0;
        while r < self.nruns {
            if self.runs[r].live {
                self.runs[w] = self.runs[r];
                w += 1;
            }
            r += 1;
        }
        self.nruns = w;
    }

    // ---- output -------------------------------------------------------------

    /// Copy the next record of the result into `dst`.
    pub fn next<S: RunStore>(&mut self, mem: &mut [u8], store: &mut S, dst: &mut [u8]) -> Next {
        match self.phase {
            Phase::Output => {}
            Phase::Done => return Next::End,
            Phase::Failed => return Next::Err(self.err.unwrap_or(ExtErr::Phase)),
            _ => return Next::Busy,
        }
        if self.limit != 0 && self.served >= self.limit {
            self.phase = Phase::Done;
            return Next::End;
        }
        let Some(m) = self.work(mem) else {
            return Next::Err(ExtErr::BudgetTooSmall);
        };
        if self.from_memory {
            if self.out_i >= self.nrec {
                self.phase = Phase::Done;
                return Next::End;
            }
            let rec = rec_at(m, idx(m, self.back, self.out_i));
            if rec.len() > dst.len() {
                return Next::Err(ExtErr::TooLarge);
            }
            ext_copy(dst, rec);
            self.out_i += 1;
            self.served += 1;
            return Next::Rec(rec.len());
        }
        match self.pick_min(m, store) {
            Err(e) => {
                self.fail(store, e);
                Next::Err(e)
            }
            Ok(Some(usize::MAX)) => Next::Pending,
            Ok(None) => {
                self.phase = Phase::Done;
                Next::End
            }
            Ok(Some(r)) => {
                let rec = reader_rec(m, self.readers[r]);
                let n = rec.len();
                if n > dst.len() {
                    return Next::Err(ExtErr::TooLarge);
                }
                ext_copy(dst, rec);
                if let Err(e) = self.advance(m, r) {
                    self.fail(store, e);
                    return Next::Err(e);
                }
                self.served += 1;
                Next::Rec(n)
            }
        }
    }

    /// Serve the result again from its start (a second pass, e.g. for a
    /// quantile's rank or a join's inner side).
    pub fn rewind(&mut self) -> Result<(), ExtErr> {
        if !matches!(self.phase, Phase::Output | Phase::Done) {
            return Err(ExtErr::Phase);
        }
        self.served = 0;
        if self.from_memory {
            self.out_i = 0;
            self.phase = if self.nrec == 0 {
                Phase::Done
            } else {
                Phase::Output
            };
            return Ok(());
        }
        let n = self.nfinal;
        let mut runs = [0u32; EXT_MAX_FANIN];
        runs[..n].clone_from_slice(&self.final_runs[..n]);
        self.open_readers(&runs[..n]);
        self.phase = if n == 0 { Phase::Done } else { Phase::Output };
        Ok(())
    }

    /// Nothing was spilled.
    pub fn in_memory(&self) -> bool {
        self.from_memory
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Next {
    /// A record of this length was copied.
    Rec(usize),
    /// A store read is pending; call again.
    Pending,
    /// Still sorting/spilling/merging: call `step`.
    Busy,
    End,
    Err(ExtErr),
}
