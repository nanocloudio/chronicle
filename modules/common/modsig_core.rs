// Module signature verification, on device — what lets a node ACCEPT a module
// rather than only author one, instead of trusting whoever hands it bytes.
//
// Given the canonical protobuf of a Module and a set of trusted signer keys, it
// answers whether the module is genuinely what it claims to be and genuinely
// signed by someone trusted.
//
// TWO CHECKS, AND BOTH MATTER:
//
//   1. The digest is RECOMPUTED from the bytes and compared to the one the
//      module carries. Skipping this would make the whole exercise theatre — an
//      attacker who can set `content_digest` to any value can sign that value
//      and pass a signature check while the module's actual content says
//      something else entirely.
//   2. A signature over that digest verifies under a TRUSTED key. An untrusted
//      signer is rejected even when the signature is cryptographically perfect.
//
// Recomputation works by STRIPPING rather than re-encoding. The digest is
// defined over the module's canonical encoding with the header's `content_digest`
// (field 1.7) cleared and every detached signature (field 14) omitted. So the
// pre-digest bytes are recovered by copying every other field through
// byte-for-byte — a re-encode could perturb bytes it was only meant to pass on,
// and would silently disagree with the host on anything this file did not model.
// Only the header is rebuilt, because one field has to come out of the middle of
// it. `pb_differential.rs` pins the result against the host builder.
//
// Requires `pb_core` (the reader + writer) and the fluxor SDK `ed25519` +
// `sha256`. The crypto is the SDK's, not ours — one owner for the repo set.

/// Module field numbers (see `artefact_core::encode_module`).
const F_HEADER: u32 = 1;
const F_SIGNATURES: u32 = 14;
/// Header field number of the content digest (`artefact_core::pb_header`).
const F_HEADER_DIGEST: u32 = 7;
/// `Digest { algorithm = 1, value = 2 }`.
const F_DIGEST_ALGORITHM: u32 = 1;
const F_DIGEST_VALUE: u32 = 2;
/// The only content-digest algorithm accepted, checked rather than assumed.
const DIGEST_ALGORITHM: &[u8] = b"sha256";
/// `Signature { algorithm = 1, signature = 2, signer = 3 }`.
const F_SIG_ALGORITHM: u32 = 1;
const F_SIG_SIGNATURE: u32 = 2;
const F_SIG_SIGNER: u32 = 3;
/// `Capability { name = 1, optional = 2 }`, at Module field 11.
///
/// These live here rather than in each reader because BOTH `activation_core` and
/// `compat_core` walk the capability list, and two copies of a field number are
/// two chances to disagree about what field 11 means. Keeping them in the core
/// both already require also lets the two be mounted together — which they could
/// not be while each defined its own.
pub const F_CAPABILITIES: u32 = 11;
pub const F_CAP_NAME: u32 = 1;
pub const F_CAP_OPTIONAL: u32 = 2;

/// The only signature algorithm accepted. An unrecognised algorithm is not
/// "maybe fine" — it is a signature this node cannot check, so it does not count.
pub const ALG_ED25519: &[u8] = b"ed25519";

/// Why verification failed. Values, never panics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyError {
    /// The bytes are not a well-formed Module.
    Malformed,
    /// The module carries no `content_digest` to check against.
    NoDigest,
    /// The recomputed digest differs from the one the module carries — the
    /// content has been altered since it was sealed.
    DigestMismatch,
    /// No signature by a trusted signer verifies over the digest. Covers an
    /// untrusted signer, a corrupt signature, and no signatures at all: from the
    /// verifier's side these are the same answer.
    SignatureInvalid,
    /// A caller buffer was too small to rebuild the pre-digest encoding.
    TooLarge,
}

impl From<PbError> for VerifyError {
    fn from(e: PbError) -> Self {
        match e {
            PbError::Overflow => VerifyError::TooLarge,
            _ => VerifyError::Malformed,
        }
    }
}

/// Rebuild the PRE-DIGEST encoding of `module` into `out`, returning its length.
///
/// Every top-level field is copied through verbatim except the signatures, which
/// are dropped, and the header, which is re-emitted without its content digest.
pub fn module_predigest(module: &[u8], out: &mut [u8]) -> Result<usize, VerifyError> {
    let mut w = Pb::new(out);
    let mut r = PbR::new(module);
    while let Some(f) = r.next_field()? {
        match f.number {
            // Detached signatures sign the digest, so including them would make
            // identity unrecomputable.
            F_SIGNATURES => {}
            F_HEADER => {
                if f.wire != WT_LEN {
                    return Err(VerifyError::Malformed);
                }
                let m = w.open(F_HEADER)?;
                let mut hr = PbR::new(f.bytes);
                while let Some(hf) = hr.next_field()? {
                    if hf.number != F_HEADER_DIGEST {
                        w.raw(hf.raw)?;
                    }
                }
                w.close(m)?;
            }
            _ => w.raw(f.raw)?,
        }
    }
    Ok(w.len())
}

/// The digest `module` CLAIMS: its header's `content_digest.value`.
pub fn module_claimed_digest(module: &[u8]) -> Result<[u8; 32], VerifyError> {
    let mut r = PbR::new(module);
    while let Some(f) = r.next_field()? {
        if f.number != F_HEADER || f.wire != WT_LEN {
            continue;
        }
        let mut hr = PbR::new(f.bytes);
        while let Some(hf) = hr.next_field()? {
            if hf.number != F_HEADER_DIGEST || hf.wire != WT_LEN {
                continue;
            }
            let mut dr = PbR::new(hf.bytes);
            let mut value: Option<[u8; 32]> = None;
            let mut algorithm: &[u8] = b"";
            while let Some(df) = dr.next_field()? {
                match df.number {
                    F_DIGEST_ALGORITHM if df.wire == WT_LEN => algorithm = df.bytes,
                    F_DIGEST_VALUE if df.wire == WT_LEN => {
                        if df.bytes.len() != 32 {
                            return Err(VerifyError::Malformed);
                        }
                        let mut d = [0u8; 32];
                        d.copy_from_slice(df.bytes);
                        value = Some(d);
                    }
                    _ => {}
                }
            }
            // The algorithm label is INSIDE the content-digest field, which
            // recomputation strips — so nothing else authenticates it, and a
            // module whose label had been altered would otherwise verify
            // cleanly. Pinning it here is what makes every byte of a sealed
            // module either signed or checked. Found by the property test that
            // perturbs each byte in turn.
            if algorithm != DIGEST_ALGORITHM {
                return Err(VerifyError::Malformed);
            }
            if let Some(d) = value {
                return Ok(d);
            }
        }
    }
    Err(VerifyError::NoDigest)
}

/// Recompute the digest of `module` from its own bytes. `scratch` holds the
/// pre-digest encoding and must be at least as large as `module`.
pub fn module_recomputed_digest(
    module: &[u8],
    scratch: &mut [u8],
) -> Result<[u8; 32], VerifyError> {
    let n = module_predigest(module, scratch)?;
    Ok(sha256(&scratch[..n]))
}

/// Verify `module`: recompute its digest, check it against the one carried, then
/// require a signature over it by one of `trusted`.
///
/// Returns the index into `trusted` of the signer that verified — a caller
/// usually wants to know WHICH key accepted, not merely that one did.
///
/// An empty `trusted` set rejects everything, which is the correct behaviour: a
/// node that trusts nobody accepts nothing.
pub fn module_verify(
    module: &[u8],
    trusted: &[[u8; 32]],
    scratch: &mut [u8],
) -> Result<usize, VerifyError> {
    let claimed = module_claimed_digest(module)?;
    let actual = module_recomputed_digest(module, scratch)?;
    if claimed != actual {
        return Err(VerifyError::DigestMismatch);
    }

    let mut r = PbR::new(module);
    while let Some(f) = r.next_field()? {
        if f.number != F_SIGNATURES || f.wire != WT_LEN {
            continue;
        }
        let mut alg: &[u8] = &[];
        let mut sig = [0u8; 64];
        let mut signer = [0u8; 32];
        let (mut have_sig, mut have_signer) = (false, false);
        let mut sr = PbR::new(f.bytes);
        while let Some(sf) = sr.next_field()? {
            match sf.number {
                F_SIG_ALGORITHM => alg = sf.bytes,
                F_SIG_SIGNATURE if sf.bytes.len() == 64 => {
                    sig.copy_from_slice(sf.bytes);
                    have_sig = true;
                }
                F_SIG_SIGNER if sf.bytes.len() == 32 => {
                    signer.copy_from_slice(sf.bytes);
                    have_signer = true;
                }
                _ => {}
            }
        }
        if alg != ALG_ED25519 || !have_sig || !have_signer {
            continue;
        }
        // Trust is checked BEFORE the cryptography: a valid signature from a key
        // this node does not trust must not be able to influence the outcome.
        let Some(k) = trusted.iter().position(|t| *t == signer) else {
            continue;
        };
        if ed25519_verify(&signer, &actual, &sig) {
            return Ok(k);
        }
    }
    Err(VerifyError::SignatureInvalid)
}
