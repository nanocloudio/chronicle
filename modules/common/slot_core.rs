// Reading and verifying a `graph_slot` OTA slot image, on device.
//
// BUILDING a slot image is fluxor's job and stays there: it compiles the graph's
// modules and self-supplies the ABI-surface pin, which only the target fluxor
// build can compute. Nothing here tries to construct one.
//
// INSPECTING one is a device concern, and a different question entirely. A node
// that has been handed an OTA image needs to know, BEFORE it writes anything to
// a slot, whether the image is intact and whether it was built for this runtime.
// Writing a corrupt or mismatched image and finding out at boot is how a device
// bricks itself — the check has to happen while there is still a working system
// to refuse with.
//
// Three things are checked, and they fail for different reasons:
//
//   * the header is well-formed and its blob extents lie inside the image;
//   * SHA-256 over `modules ++ config` matches what the header records — the
//     same gate the device applies at activate time, so a failure here is a
//     failure there;
//   * the ABI-surface pin matches this runtime's. An image built against a
//     different fluxor surface will load modules that disagree with the runtime
//     about the syscall ABI, which is not a recoverable error at boot.
//
// Reading only, with bounds-checked accessors throughout: a malformed image is
// exactly the input this is meant to survive, so nothing here may panic.
//
// Requires the fluxor SDK `sha256`.

/// Slot magic — ASCII "FXSL" (`GRAPH_SLOT_MAGIC`).
pub const SLOT_MAGIC: u32 = 0x4C53_5846;
/// Slot format version (`GRAPH_SLOT_VERSION`).
pub const SLOT_VERSION: u8 = 1;
/// Header size (`GRAPH_SLOT_HEADER_SIZE`).
pub const SLOT_HEADER_SIZE: usize = 256;
/// Offset of the ABI-surface digest pin (`GRAPH_SLOT_ABI_SURFACE_OFFSET`).
pub const ABI_SURFACE_OFFSET: usize = 64;
/// Total slot capacity (`GRAPH_SLOT_SIZE`, 512 KB).
pub const SLOT_SIZE: usize = 0x0008_0000;

/// Why a slot image was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotError {
    /// Shorter than the fixed header.
    TooShort,
    /// Not a slot image — wrong magic.
    BadMagic,
    /// A slot format this runtime does not know how to read.
    BadVersion,
    /// A blob's offset or size falls outside the image.
    BadExtent,
    /// Larger than the slot it is meant to be written into.
    TooLarge,
    /// The recorded SHA-256 does not match the payload: the image is corrupt.
    ShaMismatch,
    /// Built against a different fluxor ABI surface than this runtime exposes.
    AbiMismatch,
}

/// A decoded slot image: what it contains and what it was built for.
///
/// The blobs are SPANS into the caller's image rather than copies — a slot is up
/// to 512 KB and a device has nowhere to put a second one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotImage<'a> {
    /// Monotonic OTA epoch: which generation this image belongs to.
    pub epoch: u64,
    pub modules: &'a [u8],
    pub config: &'a [u8],
    /// The boot-selector pin the image was built against.
    pub abi_surface: [u8; 32],
}

fn rd_u32(b: &[u8], at: usize) -> Option<u32> {
    let s = b.get(at..at + 4)?;
    Some(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
}

fn rd_u64(b: &[u8], at: usize) -> Option<u64> {
    let s = b.get(at..at + 8)?;
    Some(u64::from_le_bytes([
        s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
    ]))
}

/// Decode a slot image's header and locate its blobs, WITHOUT checking
/// integrity.
///
/// Structure only: use [`slot_verify`] before trusting the contents. Separated
/// because inspecting an image you already know is broken is a legitimate thing
/// to want — reporting which epoch a corrupt image claims is more useful than
/// refusing to look at it.
pub fn slot_decode(bytes: &[u8]) -> Result<SlotImage<'_>, SlotError> {
    if bytes.len() < SLOT_HEADER_SIZE {
        return Err(SlotError::TooShort);
    }
    if rd_u32(bytes, 0).ok_or(SlotError::TooShort)? != SLOT_MAGIC {
        return Err(SlotError::BadMagic);
    }
    if bytes[4] != SLOT_VERSION {
        return Err(SlotError::BadVersion);
    }
    let epoch = rd_u64(bytes, 8).ok_or(SlotError::TooShort)?;

    // Extents come FROM the header, so any layout the device accepts is decoded
    // — but each is bounds-checked against the image rather than trusted.
    let mo = rd_u32(bytes, 16).ok_or(SlotError::TooShort)? as usize;
    let ms = rd_u32(bytes, 20).ok_or(SlotError::TooShort)? as usize;
    let co = rd_u32(bytes, 24).ok_or(SlotError::TooShort)? as usize;
    let cs = rd_u32(bytes, 28).ok_or(SlotError::TooShort)? as usize;

    let mend = mo.checked_add(ms).ok_or(SlotError::BadExtent)?;
    let cend = co.checked_add(cs).ok_or(SlotError::BadExtent)?;
    let modules = bytes.get(mo..mend).ok_or(SlotError::BadExtent)?;
    let config = bytes.get(co..cend).ok_or(SlotError::BadExtent)?;

    let mut abi = [0u8; 32];
    let a = bytes
        .get(ABI_SURFACE_OFFSET..ABI_SURFACE_OFFSET + 32)
        .ok_or(SlotError::TooShort)?;
    abi.copy_from_slice(a);

    Ok(SlotImage {
        epoch,
        modules,
        config,
        abi_surface: abi,
    })
}

/// Whether the payload matches the SHA-256 the header records.
///
/// This is the device's own activate gate, applied early: an image that fails
/// here would fail at activation, except that by then it has already been
/// written over a working slot.
pub fn slot_sha_ok(bytes: &[u8]) -> Result<bool, SlotError> {
    let img = slot_decode(bytes)?;
    let recorded = bytes.get(32..64).ok_or(SlotError::TooShort)?;
    // sha256 over `modules ++ config`. The SDK hashes one contiguous slice, and
    // the two blobs are adjacent in every image fluxor emits — but that is not
    // guaranteed by the format, so the concatenation is verified rather than
    // assumed: only when the config directly follows the modules can the two be
    // hashed in place.
    let mo = rd_u32(bytes, 16).ok_or(SlotError::TooShort)? as usize;
    let ms = rd_u32(bytes, 20).ok_or(SlotError::TooShort)? as usize;
    let co = rd_u32(bytes, 24).ok_or(SlotError::TooShort)? as usize;
    if co != mo + ms {
        return Err(SlotError::BadExtent);
    }
    let span = bytes
        .get(mo..mo + ms + img.config.len())
        .ok_or(SlotError::BadExtent)?;
    Ok(sha256(span).as_slice() == recorded)
}

/// Streaming verification, for images too large to hold.
///
/// A slot is 512 KB. It does not fit in a module's state, it does not fit
/// through argv, and a device that has just received one over the network has it
/// in the object store, not in memory. So the payload is verified in CHUNKS: the
/// header is read once, then the caller feeds the bytes that follow it in order
/// and finalizes.
///
/// The chunking is the caller's — it comes from whatever the transport or the
/// store hands back — and no chunk size is assumed. Only the total is checked,
/// so a short read is a `ShaMismatch` rather than a silent pass over a partial
/// image.
pub struct SlotVerifier {
    hasher: Sha256,
    /// Where the hashed payload starts, and how much of it is expected.
    payload_start: usize,
    expected: usize,
    fed: usize,
    recorded: [u8; 32],
    abi_surface: [u8; 32],
    epoch: u64,
}

impl SlotVerifier {
    /// Begin verification from the image's first `SLOT_HEADER_SIZE` bytes.
    ///
    /// The header alone settles structure, epoch and the ABI pin — so a mismatched
    /// image is refused after one small read, before the payload is fetched at all.
    pub fn begin(header: &[u8], abi_surface: &[u8; 32]) -> Result<Self, SlotError> {
        if header.len() < SLOT_HEADER_SIZE {
            return Err(SlotError::TooShort);
        }
        if rd_u32(header, 0).ok_or(SlotError::TooShort)? != SLOT_MAGIC {
            return Err(SlotError::BadMagic);
        }
        if header[4] != SLOT_VERSION {
            return Err(SlotError::BadVersion);
        }
        let epoch = rd_u64(header, 8).ok_or(SlotError::TooShort)?;
        let mo = rd_u32(header, 16).ok_or(SlotError::TooShort)? as usize;
        let ms = rd_u32(header, 20).ok_or(SlotError::TooShort)? as usize;
        let co = rd_u32(header, 24).ok_or(SlotError::TooShort)? as usize;
        let cs = rd_u32(header, 28).ok_or(SlotError::TooShort)? as usize;
        // The hash covers `modules ++ config`, so they must be adjacent for a
        // single streamed span to be the right bytes.
        if co != mo.checked_add(ms).ok_or(SlotError::BadExtent)? {
            return Err(SlotError::BadExtent);
        }
        let total = ms.checked_add(cs).ok_or(SlotError::BadExtent)?;
        if mo.checked_add(total).ok_or(SlotError::BadExtent)? > SLOT_SIZE {
            return Err(SlotError::TooLarge);
        }

        let mut recorded = [0u8; 32];
        recorded.copy_from_slice(header.get(32..64).ok_or(SlotError::TooShort)?);
        let mut abi = [0u8; 32];
        abi.copy_from_slice(
            header
                .get(ABI_SURFACE_OFFSET..ABI_SURFACE_OFFSET + 32)
                .ok_or(SlotError::TooShort)?,
        );
        // Checked here, not at finalize: hashing half a megabyte to then reject
        // the image for a reason known up front is wasted work on a device.
        if *abi_surface != [0u8; 32] && abi != *abi_surface {
            return Err(SlotError::AbiMismatch);
        }

        Ok(SlotVerifier {
            hasher: Sha256::new(),
            payload_start: mo,
            expected: total,
            fed: 0,
            recorded,
            abi_surface: abi,
            epoch,
        })
    }

    /// The byte offset the next chunk must start at, and how many bytes are
    /// still wanted — so a caller reading from a store knows what to ask for.
    pub fn next_range(&self) -> (usize, usize) {
        let at = self.payload_start + self.fed;
        (at, self.expected - self.fed)
    }

    /// Feed the next chunk of PAYLOAD, in order, starting at the offset
    /// [`next_range`] reported.
    ///
    /// Payload rather than whole-image bytes, because a caller reading from a
    /// store asks for exactly the window it wants — making it stream the header
    /// again only to have it skipped would be a read it never needed.
    ///
    /// Feeding more than the header promised is an error rather than a silent
    /// truncation: hashing extra bytes would produce a mismatch that reads like
    /// corruption when the real fault is the caller's loop.
    ///
    /// [`next_range`]: SlotVerifier::next_range
    pub fn feed_payload(&mut self, chunk: &[u8]) -> Result<(), SlotError> {
        if self.fed + chunk.len() > self.expected {
            return Err(SlotError::BadExtent);
        }
        self.hasher.update(chunk);
        self.fed += chunk.len();
        Ok(())
    }

    /// Finish: the payload must be complete AND match the recorded digest.
    ///
    /// Completeness is checked first and separately — a truncated stream that
    /// happened to hash to the right value is not a thing, but a truncated
    /// stream reported as a hash mismatch would send someone hunting for
    /// corruption that is really a short read.
    pub fn finish(self) -> Result<SlotSummary, SlotError> {
        if self.fed != self.expected {
            return Err(SlotError::TooShort);
        }
        if self.hasher.finalize().as_slice() != self.recorded {
            return Err(SlotError::ShaMismatch);
        }
        Ok(SlotSummary {
            epoch: self.epoch,
            payload_len: self.expected,
            abi_surface: self.abi_surface,
        })
    }
}

/// What a streamed verification establishes, without holding the image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotSummary {
    pub epoch: u64,
    pub payload_len: usize,
    pub abi_surface: [u8; 32],
}

/// Fully verify a slot image before it is written.
///
/// `abi_surface` is THIS runtime's pin. Pass an all-zero digest to skip that
/// check — meaningful only for offline inspection, since a node that does not
/// know its own surface cannot tell whether an image will boot.
pub fn slot_verify<'a>(
    bytes: &'a [u8],
    abi_surface: &[u8; 32],
) -> Result<SlotImage<'a>, SlotError> {
    let img = slot_decode(bytes)?;
    if bytes.len() > SLOT_SIZE {
        return Err(SlotError::TooLarge);
    }
    if !slot_sha_ok(bytes)? {
        return Err(SlotError::ShaMismatch);
    }
    if *abi_surface != [0u8; 32] && img.abi_surface != *abi_surface {
        return Err(SlotError::AbiMismatch);
    }
    Ok(img)
}
