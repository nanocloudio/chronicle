// Bounded, no_std, no-alloc AGGREGATE core: exact, checked accumulators for
// grouping over an ordered input. Like the other `*_core.rs` files it carries no
// inner attributes and no test module; it is `include!`d by the modules that
// group (sector's `relop`) and by the host harness. Needs `vm_core` in scope for
// `Value`.
//
// Exactness rules:
//   * `sum` / `avg` use checked i64 arithmetic; an overflow makes the value
//     absent and flags the accumulator (`overflowed`), never a wrapped number.
//   * `avg` rounds toward negative infinity (floor division), so the answer does
//     not depend on the sign convention of `/`.
//   * `min` / `max` take ints or bytes: ints order numerically, bytes
//     lexicographically, and any int orders before any bytes.
//   * `distinct(x)` is exact because its input arrives ordered by `x` within the
//     group: it counts runs of equal values, holding one value.
//   * `quantile(q, x)` is exact on the same ordered input given the group's
//     count, which the caller supplies from a first pass (`set_group_len`).
//   * Over no values, the counts (`count`, `distinct`) answer 0 and every other
//     aggregate answers absent, as SQL does.
// Values are borrowed only for the duration of `add`; bytes that an accumulator
// keeps (min / max / distinct / quantile) are copied into a bounded cell.

/// Longest bytes value an accumulator keeps (min, max, distinct, quantile).
///
/// A longer value is kept as this prefix and a truncation mark. The prefix
/// orders exactly against any value that differs within it or is no longer than
/// it; what cannot be decided is how two values BOTH longer than the cap and
/// sharing the prefix compare. `result` never answers from that: a truncated
/// value is reported absent rather than as a different string, and a `distinct`
/// count that met such a pair is reported absent rather than guessed.
pub const MONOID_VALUE_CAP: usize = 64;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AggKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Distinct,
    /// Nearest-rank quantile, `q` in thousandths (500 = median).
    Quantile(u16),
}

impl AggKind {
    /// The aggregate reads its value in order (the group's input must be
    /// sorted by that value).
    pub fn needs_order(self) -> bool {
        matches!(self, AggKind::Distinct | AggKind::Quantile(_))
    }
    /// The aggregate needs the group's length before it can answer.
    pub fn needs_count(self) -> bool {
        matches!(self, AggKind::Quantile(_))
    }
}

#[derive(Clone, Copy)]
enum Held {
    None,
    Int(i64),
    Bytes { len: u16, truncated: bool },
}

/// One accumulator. Plain data: zero it with `Acc::new(kind)` per group.
#[derive(Clone, Copy)]
pub struct Acc {
    pub kind: AggKind,
    n: u64,
    sum: i64,
    pub overflowed: bool,
    held: Held,
    buf: [u8; MONOID_VALUE_CAP],
    // quantile: rank wanted (1-based) and whether it has been captured.
    target: u64,
    // distinct: count of runs seen so far.
    runs: u64,
    pub type_error: bool,
    // distinct: two values past the cap shared the kept prefix, so whether they
    // were one run or two is unknown.
    ambiguous: bool,
}

impl Acc {
    pub const fn new(kind: AggKind) -> Self {
        Acc {
            kind,
            n: 0,
            sum: 0,
            overflowed: false,
            held: Held::None,
            buf: [0; MONOID_VALUE_CAP],
            target: 0,
            runs: 0,
            type_error: false,
            ambiguous: false,
        }
    }

    /// The group has `len` values (quantile's first pass; absent ones excluded).
    pub fn set_group_len(&mut self, len: u64) {
        if let AggKind::Quantile(q) = self.kind {
            if len == 0 {
                self.target = 0;
                return;
            }
            // Nearest rank: ceil(q/1000 * len), at least 1.
            let r = (u128::from(q) * u128::from(len)).div_ceil(1000) as u64;
            self.target = r.clamp(1, len);
        }
    }

    /// Fold one record's value. `count` counts records, absent values included,
    /// as SQL's `COUNT(*)` does; every other aggregate skips `Null` (an absent
    /// field), as SQL skips NULL — a sum or average is over the values present,
    /// a quantile ranks them alone.
    pub fn add(&mut self, v: Value<'_>) {
        if matches!(v, Value::Null) && self.kind != AggKind::Count {
            return;
        }
        self.n += 1;
        match self.kind {
            AggKind::Count => {}
            AggKind::Sum | AggKind::Avg => match monoid_int(v) {
                // An unsigned value past i64::MAX cannot be summed exactly.
                None if matches!(v, Value::Uint(_)) => self.overflowed = true,
                Some(i) => {
                    if !self.overflowed {
                        match self.sum.checked_add(i) {
                            Some(s) => self.sum = s,
                            None => self.overflowed = true,
                        }
                    }
                }
                None => self.type_error = true,
            },
            AggKind::Min | AggKind::Max => {
                let take = match self.held {
                    Held::None => true,
                    _ => {
                        let ord = self.compare_held(v);
                        match ord {
                            Some(o) => {
                                if self.kind == AggKind::Min {
                                    o == core::cmp::Ordering::Greater
                                } else {
                                    o == core::cmp::Ordering::Less
                                }
                            }
                            None => {
                                self.type_error = true;
                                false
                            }
                        }
                    }
                };
                if take {
                    self.hold(v);
                }
            }
            AggKind::Distinct => {
                if self.undecidable(v) {
                    self.ambiguous = true;
                }
                let new_run = match self.held {
                    Held::None => true,
                    _ => self.compare_held(v) != Some(core::cmp::Ordering::Equal),
                };
                if new_run {
                    self.runs += 1;
                    self.hold(v);
                }
            }
            AggKind::Quantile(_) => {
                if self.n == self.target {
                    self.hold(v);
                }
            }
        }
    }

    /// The group's answer. `None` when there is none to give: a value
    /// aggregate over no values, an overflow, a type error, a truncated bytes
    /// value, or a `distinct` count that could not be decided.
    pub fn result<'b>(&'b self) -> Option<Value<'b>> {
        if self.overflowed || self.type_error {
            return None;
        }
        match self.kind {
            AggKind::Count => Some(Value::Int(self.n as i64)),
            AggKind::Sum => {
                if self.n == 0 {
                    return None;
                }
                Some(Value::Int(self.sum))
            }
            AggKind::Avg => {
                if self.n == 0 {
                    return None;
                }
                let n = i64::try_from(self.n).ok()?;
                Some(Value::Int(self.sum.checked_div_euclid(n)?))
            }
            AggKind::Distinct => {
                if self.ambiguous {
                    return None;
                }
                Some(Value::Int(self.runs as i64))
            }
            AggKind::Min | AggKind::Max | AggKind::Quantile(_) => self.held_value(),
        }
    }

    fn held_value(&self) -> Option<Value<'_>> {
        match self.held {
            Held::None => None,
            Held::Int(i) => Some(Value::Int(i)),
            Held::Bytes { len, truncated } => {
                if truncated {
                    None
                } else {
                    Some(Value::Bytes(&self.buf[..len as usize]))
                }
            }
        }
    }

    fn hold(&mut self, v: Value<'_>) {
        if let Some(i) = monoid_int(v) {
            self.held = Held::Int(i);
            return;
        }
        match monoid_bytes(v) {
            Some(b) => {
                let n = b.len().min(MONOID_VALUE_CAP);
                let mut i = 0;
                while i < n {
                    self.buf[i] = b[i];
                    i += 1;
                }
                self.held = Held::Bytes {
                    len: n as u16,
                    truncated: b.len() > MONOID_VALUE_CAP,
                };
            }
            None => self.type_error = true,
        }
    }

    /// Whether `v` and the held value are both past the cap with the same kept
    /// prefix — the one pair whose order the kept bytes cannot settle.
    fn undecidable(&self, v: Value<'_>) -> bool {
        let Held::Bytes {
            len,
            truncated: true,
        } = self.held
        else {
            return false;
        };
        match monoid_bytes(v) {
            Some(b) => {
                b.len() > MONOID_VALUE_CAP && b[..MONOID_VALUE_CAP] == self.buf[..len as usize]
            }
            None => false,
        }
    }

    /// Order of the held value against `v` (held ? v). When both are past the
    /// cap and share the kept prefix the answer is a tie-break, not an order
    /// (see `undecidable`); callers that need it exact check that first.
    fn compare_held(&self, v: Value<'_>) -> Option<core::cmp::Ordering> {
        match self.held {
            Held::None => None,
            Held::Int(h) => match (monoid_int(v), monoid_bytes(v)) {
                (Some(i), _) => Some(h.cmp(&i)),
                (None, Some(_)) => Some(core::cmp::Ordering::Less),
                _ => None,
            },
            Held::Bytes { len, truncated } => {
                if let Some(b) = monoid_bytes(v) {
                    let held = &self.buf[..len as usize];
                    let cut = &b[..b.len().min(MONOID_VALUE_CAP)];
                    match held.cmp(cut) {
                        core::cmp::Ordering::Equal => {
                            // Equal kept prefixes: the longer original is greater.
                            let hl = if truncated { usize::MAX } else { held.len() };
                            Some(hl.cmp(&b.len()))
                        }
                        o => Some(o),
                    }
                } else if monoid_int(v).is_some() {
                    Some(core::cmp::Ordering::Greater)
                } else {
                    None
                }
            }
        }
    }
}

fn monoid_int(v: Value<'_>) -> Option<i64> {
    match v {
        Value::Int(i) => Some(i),
        Value::Uint(u) => i64::try_from(u).ok(),
        Value::Bool(b) => Some(i64::from(b)),
        _ => None,
    }
}

fn monoid_bytes<'v>(v: Value<'v>) -> Option<&'v [u8]> {
    match v {
        Value::Bytes(b) => Some(b),
        Value::Str(s) => Some(s.as_bytes()),
        _ => None,
    }
}
