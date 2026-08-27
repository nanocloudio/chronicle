//! The chronicle toolchain CLI as a fluxor cli-applet fmod
//! (rfc_cli_execution.md): ONE PIC module dispatches chronicle subcommands over
//! the cli host surface. The `cli` stack injects cli_in/cli_out; this module
//! reads argv (NUL-separated, from `args`), routes on the subcommand, writes
//! output to `stdout` (→ cli_out.bytes_in), latches an exit code on `exit`
//! (→ cli_out.exit_in), and returns Done so the run-to-completion CLI exits.
//!
//!   chronicle compile <schema> <params> <src>  CEL → checked-IR hex (no-alloc front end)
//!   chronicle stages <ir_hex>...            wrap stage IRs into an ir_stages container
//!   chronicle decision <when> <out>… <def>  first-hit Decision container
//!   chronicle agg <win> <late> <lanes> <step> <horizon> <key> <time> <emit> [k:sel]…
//!   chronicle check <ir_hex>                validate + lower an ir_stages container
//!   chronicle eval <ir_hex> <record_hex>    dry-run a record through the stages
//!   chronicle seal <schema> <params> <src> …   seal an Expression artefact
//!   chronicle seal-tf …                     seal a Transformation artefact
//!   chronicle seal-module <pkg> <sym> …     seal a Module (the deployment unit)
//!   chronicle digest <hex>                  sha256 content digest (version identity)
//!   chronicle help
//!
//! The `seal*` commands close the self-hosting loop: an artefact's identity is
//! the sha256 of its canonical protobuf encoding, and `modules/common/
//! {pb_core,artefact_core}.rs` produce that with no allocator and no `prost` —
//! digest-identical to the host toolchain, so a device can author and publish
//! artefacts without a Linux build host.
//!
//! With `compile` the ENTIRE authoring loop runs on device — compile → stages →
//! check → eval — via `modules/common/celc_core.rs`, which a differential test
//! proves byte-identical to the host compiler. Schemas arrive as text
//! (`Name{f:ty@N,...};ENUM=n`), params as `name:Type,...` (order = index).
//!
//! `check` and `eval` run the IDENTICAL include!'d cores the pipeline .fmod runs
//! at load (`lower_stages` re-deriving each stage's cost, `run_stages` executing
//! them) — so a green `check` here is exactly the proof a device performs before
//! accepting a param, and `eval`'s output frame is byte-for-byte what the
//! deployed engine would emit. `digest` is the sha256 whose prefix identifies a
//! version in release tables (chronicle-authoring::release).

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    unreachable_patterns,
    reason = "the fluxor SDK + shared cores are include!'d wholesale; each module consumes only a subset"
)]

use core::ffi::c_void;

#[allow(
    unused_imports,
    dead_code,
    reason = "shared SDK surface across modules"
)]
#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");

// Evaluator + staged executor + hex codec + IR lowerer — identical source to
// the on-device engines, all in ONE module so cross-references resolve.
mod tc {
    // `blobstore_core` talks to a provider, so unlike the pure-compute cores it
    // needs the ABI surface.
    use super::abi::SyscallTable;

    include!("../../common/vm_core.rs");
    include!("../../common/pipeline_core.rs");
    include!("../../common/hex_core.rs");
    include!("../../common/lower_core.rs");
    include!("../../common/celc_core.rs");
    include!("../../common/pb_core.rs");
    include!("../../common/artefact_core.rs");
    include!("../../common/blobstore_core.rs");
    include!("../../common/oci_core.rs");
    include!("../../common/ckptstore_core.rs");
    include!("../../common/uproc_core.rs");
    include!("../../common/uproc_lower_core.rs");
    include!("../../common/modsig_core.rs");
    include!("../../common/activation_core.rs");
    include!("../../common/barrier_core.rs");
    // The deployment half of authoring: pack a compiled artefact into the param
    // container a module loads, resolve a document's pipeline into plan stages,
    // and lower those to the graph YAML a runtime boots. With these the whole
    // chain — source to runnable graph — happens inside fluxor.
    include!("../../common/pack_core.rs");
    include!("../../common/graph_core.rs");
    include!("../../common/plan_core.rs");
    include!("../../common/registry_core.rs");
    // The release control plane: author a versions param and the hot-reload
    // messages that converge a running fleet onto it.
    include!("../../common/version_core.rs");
    include!("../../common/release_core.rs");
    // OTA slot images: a node must be able to check one BEFORE writing it over
    // a working slot. Building images stays fluxor's job.
    include!("../../common/slot_core.rs");
    // fluxor SDK crypto — the repo set's single crypto owner. Include ORDER is
    // load-bearing: ed25519 needs the SHA-512 that lives in sha384.rs.
    include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
    include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha384.rs");
    include!("../../../target/fluxor/fluxor-abi/sdk/crypto/hmac.rs");
    // p256 carries the shared U256 arithmetic ed25519 builds on — the SDK's
    // documented include set, not an unused import.
    include!("../../../target/fluxor/fluxor-abi/sdk/crypto/p256.rs");
    include!("../../../target/fluxor/fluxor-abi/sdk/crypto/ed25519.rs");

    // Authoring orchestration + shared CLI helpers, mounted last so it
    // sees every core it composes (celc/lower/uproc/artefact).
    include!("../../common/author_core.rs");
}
use tc::{
    add_version_msg,
    agg_from_argv,
    append,
    append_hex,
    append_release_reason,
    append_slot_reason,
    append_u32,
    append_uproc_reason,
    author_document,
    blob_get,
    blob_put,
    blob_range,
    // The release control plane.
    build_versions_param,
    celc_compile_auto,
    celc_err_name,
    ckpt_load_latest,
    ckpt_save,
    compile_decision,
    compile_failed,
    compile_ir,
    compile_one,
    // The dynamic ("POST source") compile path.
    compile_pipeline_ir,
    compile_to_code_buf,
    emit_hex_buf,
    emit_sealed_buf,
    graph_document,
    hex_decode,
    hex_encode,
    kind_from_name,
    lower_arg_buf,
    lower_flat,
    lower_pipeline_with,
    lower_stages,
    module_verify,
    oci_fetch,
    oci_init,
    oci_push,
    oci_resolve_tag,
    param_message_name,
    parse_i64,
    plan_activation,
    print_digest,
    put_ir_prog,
    put_prog,
    release_from_argv,
    remove_version_msg,
    run_stages,
    seal_aggregation,
    seal_decision,
    seal_expression,
    seal_module,
    seal_pipeline,
    seal_transformation,
    set_default_msg,
    sha256,
    // OTA slot inspection.
    slot_decode,
    slot_verify,
    split_argv,
    split_csv,
    split_digests,
    split_qname,
    split_qualified,
    stage_at,
    stage_count,
    uproc_agg_emit_params,
    uproc_agg_emit_schema,
    uproc_line_col,
    uproc_params_text,
    uproc_parse,
    uproc_schema_text,
    version_digest,
    ActivationError,
    AggregationSpec,
    BindingSpec,
    BlobError,
    CkptError,
    DecisionSpec,
    Doc,
    EntrySpec,
    ExpressionSpec,
    HeaderSpec,
    Layer,
    ManifestRef,
    ModuleRef,
    ModuleSpec,
    NodeState,
    OciError,
    OperatorSpec,
    PipelineSpec,
    PlanStage,
    PushBufs,
    RegError,
    ReleaseError,
    RuleSource,
    RuleSpec,
    SlotError,
    SlotVerifier,
    Stage,
    StageSource,
    StageSpec as ArtStageSpec,
    TargetProfile,
    TransformationSpec,
    UprocArena,
    UprocErrorKind,
    VerifyError,
    VersionRef,
    VersionSpec,
    BIN_BUF,
    KIND_AGGREGATION,
    KIND_DECISION,
    KIND_EXPRESSION,
    KIND_MODULE,
    KIND_PIPELINE,
    KIND_SCHEMA,
    KIND_TRANSFORMATION,
    MAX_ARGV,
    MAX_ITEMS,
    MAX_SET,
    MAX_VERSIONS,
    MEDIA_TYPE_MODULE,
    OP_AVG,
    OP_COUNT,
    OP_DISTINCT,
    OP_MAX,
    OP_MIN,
    OP_QUANTILE,
    OP_SUM,
    OP_TOPK,
    PROVENANCE_LOCAL_BUILD,
    ROUTE_NONE,
    // The deployment half: document -> plan -> graph, all on device.
    STAGE_EFFECT,
    VERSION_DIGEST_LEN,
};

const PORT_INPUT: u8 = 0;
const PORT_OUTPUT: u8 = 1;
const STEP_DONE: i32 = 1;

const MAX_STAGES: usize = 8;
// Sized as 2 x UPROC_BUF: a `.uproc` travels as ONE hex argument, so the
// argv record is twice the document plus the subcommand. The two constants
// move TOGETHER or an over-bound document stops being a compile error and
// becomes an argv record the channel never delivers.
const ARGV_BUF: usize = 65536;
/// Largest `.uproc` document this CLI will author, in bytes of source.
///
/// Raised 16384 -> 32768 when the identity provider grew the code-exchange
/// and authorization operations: the introspect + token dispatch alone was
/// 13.3 KB, and each further OIDC junction is another message set, decision
/// and pipeline. The 2x `ARGV_BUF` relationship is preserved.
///
/// Separate from [`BIN_BUF`], which also sizes stack arrays — work buffers
/// deliberately live in module state, not on the PIC stack, so the document
/// bound must not drag `2 * BIN_BUF` stack allocations up with it.
///
/// It was `BIN_BUF`, and `examples/oci_registry/registry.uproc` is **1967
/// bytes** — 96% of it. An OCI registry is a far simpler protocol surface
/// than the identity provider `C13`/`C14` call for, so the bound was
/// already the binding constraint on what a `.uproc` could express, and the
/// next real application would have hit it immediately.
///
/// Raised rather than worked around, because both alternatives are worse.
/// Splitting the document breaks the four-file application shape the
/// examples establish. Pushing configuration out into referenced params
/// helps and is not enough on its own: even a dispatch-only IdP `.uproc`
/// has a decision per protocol junction — authorize, redeem, token,
/// refresh, revoke, introspect, userinfo, logout, consent — where the
/// registry has a handful.
///
/// The cost is module state on a device that has plenty: `chronicle_cli`,
/// `pipeline` and `decision` are all `hardware_targets = ["bcm2712"]`, so
/// there is no constrained target paying for this.
const UPROC_BUF: usize = 32768;
const OUT_BUF: usize = 4096;
/// Steps to wait for the argv record before defaulting to `help` (cli_in emits
/// it early; an empty argv — no `--` — never arrives, so we fall through).
const ARGV_WAIT: u32 = 2000;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    /// argv channel (input port 0 ← cli_in.args_out).
    args_chan: i32,
    /// stdout channel (output port 0 → cli_out.bytes_in).
    out_chan: i32,
    /// exit-code channel (output port 1 → cli_out.exit_in).
    exit_chan: i32,
    waited: u32,
    done: u8,
    /// 1 once the command has run and produced (`out`, exit code). The command is
    /// side-effecting (`put`/`get`, OCI writes), so it runs EXACTLY once; delivery
    /// of its output and exit status then retries across steps without re-running it.
    result_ready: u8,
    /// The produced stdout length and how much of it has been delivered.
    res_olen: u16,
    out_off: u16,
    /// The produced exit code and whether it has been delivered.
    res_code: i32,
    exit_done: u8,
    // Work buffers live in module state, not the PIC stack.
    arec: [u8; ARGV_BUF],
    /// Hex-decoded input: a `.uproc` document for `author`/`parse`, or a
    /// smaller artefact for the other subcommands.
    ir: [u8; UPROC_BUF],
    /// Compiled program, or the schema text `author` generates from a
    /// document — which scales with the document.
    prog: [u8; UPROC_BUF],
    record: [u8; 512],
    buf_a: [u8; 512],
    buf_b: [u8; 512],
    /// Container assembly + per-program lowering scratch (`decision`/`agg`).
    cont: [u8; BIN_BUF],
    code: [u8; BIN_BUF],
    /// Digest-free artefact encoding, for the two-pass seal.
    scratch: [u8; BIN_BUF],
    frame: [u8; 512],
    out: [u8; OUT_BUF],
    // `.uproc` declaration arena. In module state, not on the stack: the arrays
    // total several KiB and a PIC frame cannot hold them. Table sizes were
    // raised when the reference IdP grew introspect + token + authorization-code
    // + exchange into one document: 80 message fields across 13 messages, 33
    // decision rules across 8 junctions — the examples that set the originals
    // had a handful of each.
    // total several KiB and a PIC frame cannot hold them alongside the compile
    // buffers each artefact kind needs.
    u_messages: [tc::MessageDecl; 24],
    u_fields: [tc::FieldDecl; 128],
    // 32, not 16. A `decision` matches on a TYPED DISCRIMINANT another module
    // produced, so a document that routes a protocol names every value of
    // every discriminant it routes on — kagi's `verify_err` alone is twelve.
    // 16 was not sized for a workload; it fit the examples that existed.
    u_enums: [tc::EnumDecl; 48],
    u_expressions: [tc::FnDecl; 16],
    u_transformations: [tc::FnDecl; 24],
    u_decisions: [tc::DecisionDecl; 16],
    u_rules: [tc::RuleDecl; 48],
    u_resources: [tc::ResourceDecl; 8],
    u_pipelines: [tc::PipelineDecl; 16],
    u_stages: [tc::StageDecl; 48],
    u_args: [tc::Span; 32],
    u_aggregations: [tc::AggregationDecl; 8],
    u_operators: [tc::OperatorDecl; 16],
    u_entries: [tc::EntryDecl; 8],
}

/// Parse a decimal integer argument (optionally negative).
/// Split the NUL-separated argv record into (start, end) spans.
fn cmd_help(out: &mut [u8]) -> usize {
    append(
        out,
        0,
        b"chronicle - the deterministic-processing toolchain applet\n\
          \x20 chronicle compile <schema> <params> <source>   CEL -> checked-IR hex\n\
          \x20 chronicle stages <ir_hex>...           wrap stage IRs into an ir_stages container\n\
          \x20 chronicle decision <when_ir> <out_ir>... <default_ir>   first-hit decision container\n\
          \x20 chronicle agg <win> <late> <lanes> <step> <horizon> <key_ir> <time_ir> <emit_ir> [<kind>:<sel_ir>]...\n\
          \x20 chronicle check <ir_hex>               validate + lower an ir_stages container\n\
          \x20 chronicle eval <ir_hex> <record_hex>   dry-run a record through the stages\n\
          \x20 chronicle seal <schema> <params> <src> <pkg> <sym> <param> <param_msg> <result_ty>\n\
          \x20                                        -> content digest + sealed Expression artefact\n\
          \x20 chronicle seal-tf <schema> <params> <src> <pkg> <sym> <in_ty> <out_ty>\n\
          \x20                                        -> a sealed Transformation artefact\n\
          \x20 chronicle seal-module <pkg> <sym> <rev> <toolchain> [<kind> <pkg.sym> <digest>]...\n\
          \x20                                        -> a sealed Module (the deployment unit)\n\
          \x20 chronicle put <hex>                    store a blob under its own sha256\n\
          \x20 chronicle get <digest_hex>             read it back, verifying the content address\n\
          \x20 chronicle digest <hex>                 sha256 content digest (version identity)\n\
          \x20 chronicle author <uproc_hex>          compile a .uproc document to sealed artefacts\n\
          \x20 chronicle parse <uproc_hex>           parse a .uproc module document\n\
          \x20 chronicle graph <uproc_hex> <pipeline> [target]\n\
          \x20                                        lower a pipeline to runnable graph YAML\n\
          \x20 chronicle compile-source <schema> <param> <type> <source>\n\
          \x20                                        type-check source at runtime -> ir hex\n\
          \x20 chronicle compile-stages <schema> <param> <type> <source>...\n\
          \x20                                        ...as a chain -> ir_stages hex\n\
          \x20 chronicle release <default_tag> <tag>:<prog_hex>...\n\
          \x20                                        build the multi-version `versions` param\n\
          \x20 chronicle release-ctl add|default|remove <tag> [prog_hex]\n\
          \x20                                        hot-reload control message for a live instance\n\
          \x20 chronicle slot-verify <image_digest_hex> [abi_hex]\n\
          \x20                                        stream-check a stored OTA slot image\n\
          \x20 chronicle ckpt-save <hex>              persist a checkpoint, move `latest` to it\n\
          \x20 chronicle ckpt-load                    read back the latest checkpoint (recovery)\n\
          \x20 chronicle activate <module_hex> <key_hex> <caps> <artefacts> <modules> <bindings>\n\
          \x20                                        full activation sequence ('-' for an empty list)\n\
          \x20 chronicle verify <module_hex> <trusted_pubkey_hex>...\n\
          \x20                                        recompute the digest + check a trusted signature\n\
          \x20 chronicle oci-init                     create an OCI image layout in the store\n\
          \x20 chronicle oci-push <hex> <tag>         publish a bundle, print its manifest digest\n\
          \x20 chronicle oci-resolve <tag>            the digest a tag points at\n\
          \x20 chronicle oci-fetch <digest_hex>       fetch by digest, verifying every layer\n\
          \x20 chronicle help\n\
          schema: 'Name{f:ty@N,...};ENUM=n;...'  params: 'name:Type,...' (order = index)\n\
          ty: int|uint|double|bool|str|bytes|MessageName. All commands run the same\n\
          include!'d cores the on-device engines run - compile is differentially\n\
          proven byte-identical to the host compiler.\n",
    )
}

/// `compile <schema> <params> <source>`: the no-alloc CEL front end. Emits the
/// flat checked IR (RET appended for a scalar result — the host `compile`
/// convention), ready for `stages`/`check`/`eval` or an `ir`/`ir_stages` param.
fn cmd_compile(s: &mut State, schema: &[u8], params: &[u8], src: &[u8]) -> (usize, i32) {
    let mut irbuf = [0u8; BIN_BUF];
    match celc_compile_auto(schema, params, src, &mut irbuf) {
        Ok(n) => {
            let mut hexed = [0u8; 2 * BIN_BUF];
            let Some(hl) = hex_encode(&irbuf[..n], &mut hexed) else {
                return (append(&mut s.out, 0, b"error: IR too large to print\n"), 1);
            };
            let mut p = append(&mut s.out, 0, &hexed[..hl]);
            p = append(&mut s.out, p, b"\n");
            (p, 0)
        }
        Err(e) => {
            let mut p = append(&mut s.out, 0, b"error: compile failed: ");
            p = append(&mut s.out, p, celc_err_name(e));
            p = append(&mut s.out, p, b"\n");
            (p, 1)
        }
    }
}

/// `stages <ir_hex>...`: assemble stage IRs into the `ir_stages` container the
/// pipeline module loads: `[nstages:u8]` then per stage `[len:u16 LE][ir]`.
fn cmd_stages(s: &mut State, arec: &[u8], argv: &[(usize, usize)], argc: usize) -> (usize, i32) {
    let n = argc - 1;
    if n == 0 || n > MAX_STAGES {
        return (
            append(&mut s.out, 0, b"error: stages needs 1..8 <ir_hex> args\n"),
            1,
        );
    }
    let mut cont = [0u8; BIN_BUF];
    cont[0] = n as u8;
    let mut w = 1usize;
    for (a, b) in argv[1..=n].iter() {
        let mut irbuf = [0u8; BIN_BUF];
        let Some(ilen) = hex_decode(&arec[*a..*b], &mut irbuf) else {
            return (
                append(&mut s.out, 0, b"error: stage IR is not valid hex\n"),
                1,
            );
        };
        if w + 3 + ilen > cont.len() {
            return (append(&mut s.out, 0, b"error: container too large\n"), 1);
        }
        // Failure route: `stages` builds an unrouted chain. A routed table is
        // authored, not assembled from bare stage IRs on a command line.
        cont[w] = ROUTE_NONE;
        w += 1;
        cont[w..w + 2].copy_from_slice(&(ilen as u16).to_le_bytes());
        w += 2;
        cont[w..w + ilen].copy_from_slice(&irbuf[..ilen]);
        w += ilen;
    }
    let mut hexed = [0u8; 2 * BIN_BUF];
    let Some(hl) = hex_encode(&cont[..w], &mut hexed) else {
        return (
            append(&mut s.out, 0, b"error: container too large to print\n"),
            1,
        );
    };
    let mut p = append(&mut s.out, 0, &hexed[..hl]);
    p = append(&mut s.out, p, b"\n");
    (p, 0)
}

/// Decode one IR-hex argument into `s.ir` and lower it to bytecode in `s.code`,
/// returning `(code_len, max_cost)` — the same `lower_flat` a device runs at
/// load, so the cost bound is derived here exactly as it would be there.
/// Append one `[max_cost:u32 LE][code_len:u16 LE][code]` program — the encoding
/// both the decision container and the pipeline stage table use.
/// Append one `[ir_len:u16 LE][flat_ir]` program — the aggregation IR-def form
/// (no cost: `lower_def` re-derives it on device).
/// Hex-encode `cont[..w]` into the output buffer as the command's answer.
/// `decision <when_ir> <outcome_ir> ... <default_ir>`: assemble the first-hit
/// Decision container `run_decision` consumes — `[nrules:u8]` then per rule
/// `[when prog][outcome prog]`, then the default outcome's prog. Each IR is
/// lowered HERE with the device lowerer, so every embedded cost bound is the
/// one the device would derive; a rule that will not lower fails the whole
/// command rather than shipping a container the engine would reject.
fn cmd_decision(s: &mut State, arec: &[u8], argv: &[(usize, usize)], argc: usize) -> (usize, i32) {
    let n = argc - 1;
    // 2 programs per rule + 1 default: an odd, non-zero count.
    if n == 0 || n.is_multiple_of(2) || n > 2 * MAX_STAGES + 1 {
        return (
            append(
                &mut s.out,
                0,
                b"error: decision needs <when_ir> <outcome_ir> ... <default_ir> \
                  (pairs plus one default)\n",
            ),
            1,
        );
    }
    let mut w = 1usize;
    s.cont[0] = ((n - 1) / 2) as u8;
    for k in 0..n {
        let (a, b) = argv[1 + k];
        let Some((clen, cost)) = lower_arg_buf(&arec[a..b], &mut s.ir, &mut s.code) else {
            let mut p = append(&mut s.out, 0, b"error: program ");
            p = append_u32(&mut s.out, p, k as u32);
            p = append(&mut s.out, p, b" is not valid IR hex or failed to lower\n");
            return (p, 1);
        };
        let mut code = [0u8; BIN_BUF];
        code[..clen].copy_from_slice(&s.code[..clen]);
        if !put_prog(&mut s.cont, &mut w, &code[..clen], cost) {
            return (append(&mut s.out, 0, b"error: container too large\n"), 1);
        }
    }
    emit_hex_buf(&s.cont, w, &mut s.out)
}

/// `agg <window> <lateness> <lanes> <step> <horizon> <key_ir> <time_ir>
/// <emit_ir> [<kind>:<selector_ir>]...`: assemble the aggregation IR-def
/// container the engine lowers at load (`lower_def`). Programs stay as IR here
/// — unlike `decision`, the aggregation module re-derives every cost itself.
/// An operator with no selector (e.g. COUNT) takes an empty selector: `0:`.
fn cmd_agg(s: &mut State, arec: &[u8], argv: &[(usize, usize)], argc: usize) -> (usize, i32) {
    agg_from_argv(arec, argv, argc, &mut s.cont, &mut s.out)
}

/// `seal <schema> <params> <src> <pkg> <sym> <param> <param_msg> <result_ty>`:
/// compile the source, build the canonical Expression artefact, and print its
/// CONTENT DIGEST followed by the sealed artefact bytes as hex.
///
/// This is artefact identity produced entirely on device — the digest is the
/// sha256 of the canonical protobuf encoding with the digest field cleared, and
/// `tests/harness/tests/chronicle_cli.rs (corpus suite)` pins it equal to what the host
/// `build_expression` yields. A node that can do this can author a
/// content-addressed artefact without a Linux toolchain.
#[allow(clippy::too_many_arguments, reason = "one argument per artefact field")]
fn cmd_seal(
    s: &mut State,
    schema: &[u8],
    params: &[u8],
    src: &[u8],
    pkg: &[u8],
    sym: &[u8],
    param: &[u8],
    param_msg: &[u8],
    result_ty: &[u8],
) -> (usize, i32) {
    // A PIC module's stack is small; an expression's bytecode is hundreds of
    // bytes, not thousands. Everything larger lives in State (the module's arena).
    let mut code = [0u8; 512];
    let (clen, cost) = match compile_to_code_buf(schema, params, src, &mut code, &mut s.out) {
        Ok(v) => v,
        Err(p) => return (p, 1),
    };

    let header = HeaderSpec {
        package: pkg,
        symbol: sym,
        kind: KIND_EXPRESSION,
        capability: b"expression.cel.strict.v1",
    };
    let spec = ExpressionSpec {
        param_name: param,
        param_message: param_msg,
        result_type: result_ty,
        source: src,
        bytecode: &code[..clen],
        max_cost: cost,
    };
    // Disjoint field borrows: `cont` receives the sealed artefact, `scratch`
    // holds the digest-free pass.
    match seal_expression(&mut s.cont, &mut s.scratch, &header, &spec) {
        Ok((n, d)) => emit_sealed_buf(&s.cont, n, &d, &mut s.out),
        Err(_) => (append(&mut s.out, 0, b"error: artefact too large\n"), 1),
    }
}

/// Compile `src` to bytecode in `code`, returning `(len, cost)` or emitting the
/// structured error. Shared by the seal commands.
/// Emit `<digest hex>\n<sealed artefact hex>\n`.
/// `seal-tf <schema> <params> <src> <pkg> <sym> <in_ty> <out_ty>`: seal a
/// Transformation — the artefact a pipeline stage is made of.
#[allow(clippy::too_many_arguments, reason = "one argument per artefact field")]
fn cmd_seal_tf(
    s: &mut State,
    schema: &[u8],
    params: &[u8],
    src: &[u8],
    pkg: &[u8],
    sym: &[u8],
    in_ty: &[u8],
    out_ty: &[u8],
) -> (usize, i32) {
    let mut code = [0u8; 512];
    let (clen, cost) = match compile_to_code_buf(schema, params, src, &mut code, &mut s.out) {
        Ok(v) => v,
        Err(p) => return (p, 1),
    };
    let header = HeaderSpec {
        package: pkg,
        symbol: sym,
        kind: KIND_TRANSFORMATION,
        capability: b"transformation.cel.v1",
    };
    let spec = TransformationSpec {
        input_type: in_ty,
        output_type: out_ty,
        source: src,
        bytecode: &code[..clen],
        max_cost: cost,
    };
    match seal_transformation(&mut s.cont, &mut s.scratch, &header, &spec) {
        Ok((n, d)) => emit_sealed_buf(&s.cont, n, &d, &mut s.out),
        Err(_) => (append(&mut s.out, 0, b"error: artefact too large\n"), 1),
    }
}

/// The `ArtefactKind` a `seal-module` ref argument names.
/// Split `pkg.sym` at its LAST dot — packages are dotted, the symbol is not.
/// `seal-module <pkg> <sym> <rev> <toolchain> [<kind> <pkg.sym> <digest_hex>]…`
///
/// Seals the DEPLOYMENT UNIT: the artefact that names what it contains by
/// digest and carries provenance. A node that can do this can publish work it
/// authored itself, with an identity every consumer resolves to the same bytes.
fn cmd_seal_module(
    s: &mut State,
    arec: &[u8],
    argv: &[(usize, usize)],
    argc: usize,
) -> (usize, i32) {
    if argc < 5 || !(argc - 5).is_multiple_of(3) {
        return (
            append(
                &mut s.out,
                0,
                b"error: seal-module needs <pkg> <sym> <rev> <toolchain> \
                  [<kind> <pkg.sym> <digest_hex>]...\n",
            ),
            1,
        );
    }
    const MAX_REFS: usize = 6;
    let nrefs = (argc - 5) / 3;
    if nrefs > MAX_REFS {
        return (append(&mut s.out, 0, b"error: too many refs\n"), 1);
    }

    // Two passes: decode every digest FIRST, then build the refs. A ref borrows
    // its digest out of `dbuf`, so decoding and borrowing cannot interleave.
    let mut dbuf = [[0u8; 32]; MAX_REFS];
    let mut kinds = [0i32; MAX_REFS];
    for k in 0..nrefs {
        let (ks, ke) = argv[5 + k * 3];
        let (ds, de) = argv[7 + k * 3];
        match kind_from_name(&arec[ks..ke]) {
            Some(kind) => kinds[k] = kind,
            None => return (append(&mut s.out, 0, b"error: unknown artefact kind\n"), 1),
        }
        if hex_decode(&arec[ds..de], &mut dbuf[k]) != Some(32) {
            return (
                append(&mut s.out, 0, b"error: a ref digest is not 32 hex bytes\n"),
                1,
            );
        }
    }
    let mut refs = [ModuleRef {
        package: &[],
        symbol: &[],
        kind: 0,
        digest: &[],
    }; MAX_REFS];
    for (k, (r, d)) in refs.iter_mut().zip(dbuf.iter()).enumerate().take(nrefs) {
        let (ns, ne) = argv[6 + k * 3];
        let (pkg, sym) = split_qualified(&arec[ns..ne]);
        *r = ModuleRef {
            package: pkg,
            symbol: sym,
            kind: kinds[k],
            digest: d,
        };
    }

    let (p1, p2) = (argv[1], argv[2]);
    let (r1, t1) = (argv[3], argv[4]);
    let header = HeaderSpec {
        package: &arec[p1.0..p1.1],
        symbol: &arec[p2.0..p2.1],
        kind: KIND_MODULE,
        capability: b"",
    };
    let spec = ModuleSpec {
        source_revision: &arec[r1.0..r1.1],
        build_toolchain: &arec[t1.0..t1.1],
        provenance_class: PROVENANCE_LOCAL_BUILD,
    };
    match seal_module(
        &mut s.cont,
        &mut s.scratch,
        &header,
        &refs[..nrefs],
        &[],
        &[],
        &spec,
        &[],
    ) {
        Ok((n, d)) => emit_sealed_buf(&s.cont, n, &d, &mut s.out),
        Err(_) => (append(&mut s.out, 0, b"error: artefact too large\n"), 1),
    }
}

/// `author <uproc_hex>`: compile a whole `.uproc` document to SEALED artefacts,
/// printing one `<name> <digest>` line per artefact.
///
/// This is authoring end to end on device: parse the document, assemble the type
/// environment from its own declarations, compile each body, and seal it. No
/// host is involved at any step, and the digests are the same ones the host
/// toolchain would produce for the same source.
fn cmd_author(s: &mut State, hex: &[u8]) -> (usize, i32) {
    let Some(n) = hex_decode(hex, &mut s.ir) else {
        return (append(&mut s.out, 0, b"error: not valid hex\n"), 1);
    };
    let mut arena = UprocArena {
        messages: &mut s.u_messages,
        fields: &mut s.u_fields,
        enums: &mut s.u_enums,
        expressions: &mut s.u_expressions,
        transformations: &mut s.u_transformations,
        decisions: &mut s.u_decisions,
        rules: &mut s.u_rules,
        resources: &mut s.u_resources,
        pipelines: &mut s.u_pipelines,
        stages: &mut s.u_stages,
        args: &mut s.u_args,
        aggregations: &mut s.u_aggregations,
        operators: &mut s.u_operators,
        entries: &mut s.u_entries,
    };
    author_document(
        &s.ir[..n],
        &mut arena,
        &mut s.prog,
        &mut s.code,
        &mut s.cont,
        &mut s.scratch,
        &mut s.out,
    )
}

/// Map a `.uproc` operator kind to the canonical `OperatorKind` enum value.
///
/// The two vocabularies are NOT the same numbering — the DSL orders them by
/// declaration, the proto by its own history — so this translates explicitly
/// rather than casting.
/// Compile one body to bytecode: checked IR via `celc_core`, then lowered.
/// `ir` is scratch; `code` receives the bytecode.
/// The artefact kind a pipeline stage's target refers to, resolved from the
/// document's own declarations. A stage names an artefact; only the document
/// knows which kind that name was declared as.
/// `slot-verify <image_digest_hex> [abi_surface_hex]`: check an OTA slot image
/// in the object store before it is written.
///
/// A device handed an image needs to know it is intact and built for THIS
/// runtime while there is still a working system to refuse with — writing it
/// first and discovering the problem at boot is how a node bricks itself.
///
/// Without an `abi_surface` the pin is reported but not enforced, which is
/// inspection rather than a go/no-go.
fn cmd_slot_verify(s: &mut State, arg: &[u8], abi_hex: &[u8]) -> (usize, i32) {
    let mut abi = [0u8; 32];
    if !abi_hex.is_empty() {
        let mut buf = [0u8; 32];
        match hex_decode(abi_hex, &mut buf) {
            Some(32) => abi = buf,
            _ => {
                return (
                    append(
                        &mut s.out,
                        0,
                        b"error: abi surface must be 32 bytes of hex\n",
                    ),
                    1,
                )
            }
        }
    }

    // A slot is 512 KB: it cannot arrive through argv and cannot be held in
    // module state. So the argument is the image's CONTENT DIGEST and the bytes
    // are streamed from the object store in BIN_BUF chunks — which is also where
    // an image actually is after an OTA transfer.
    let mut digest = [0u8; 32];
    if hex_decode(arg, &mut digest) != Some(32) {
        return (
            append(
                &mut s.out,
                0,
                b"error: slot-verify takes the image's 32-byte store digest\n",
            ),
            1,
        );
    }

    // The header settles structure, epoch and the ABI pin after one small read,
    // so a mismatched image is refused before its payload is fetched at all.
    // SAFETY: `s.syscalls` is the live table installed by module_new.
    let head = match unsafe { blob_range(&*s.syscalls, &digest, 0, &mut s.cont) } {
        Ok(n) => n,
        Err(_) => return (append(&mut s.out, 0, b"error: image not in the store\n"), 1),
    };
    let mut v = match SlotVerifier::begin(&s.cont[..head], &abi) {
        Ok(v) => v,
        Err(e) => {
            let mut p = append(&mut s.out, 0, b"error: ");
            p = append_slot_reason(&mut s.out, p, e);
            p = append(&mut s.out, p, b"\n");
            return (p, 1);
        }
    };

    // Stream the payload. `next_range` says exactly what is still wanted, so the
    // loop reads no more of a half-megabyte image than it has to.
    loop {
        let (at, want) = v.next_range();
        if want == 0 {
            break;
        }
        let n = match unsafe { blob_range(&*s.syscalls, &digest, at as u64, &mut s.cont) } {
            Ok(n) => n,
            Err(_) => {
                return (
                    append(&mut s.out, 0, b"error: short read from the store\n"),
                    1,
                )
            }
        };
        if n == 0 {
            // No progress: the store has less than the header promised.
            return (
                append(
                    &mut s.out,
                    0,
                    b"error: image is shorter than its header claims\n",
                ),
                1,
            );
        }
        let take = if n > want { want } else { n };
        if let Err(e) = v.feed_payload(&s.cont[..take]) {
            let mut p = append(&mut s.out, 0, b"error: ");
            p = append_slot_reason(&mut s.out, p, e);
            p = append(&mut s.out, p, b"\n");
            return (p, 1);
        }
    }

    match v.finish() {
        Ok(sum) => {
            let mut p = append(&mut s.out, 0, b"ok epoch ");
            p = append_u32(&mut s.out, p, sum.epoch as u32);
            p = append(&mut s.out, p, b" payload ");
            p = append_u32(&mut s.out, p, sum.payload_len as u32);
            p = append(&mut s.out, p, b" abi ");
            let mut dhex = [0u8; 64];
            if let Some(dl) = hex_encode(&sum.abi_surface, &mut dhex) {
                p = append(&mut s.out, p, &dhex[..dl]);
            }
            p = append(&mut s.out, p, b"\n");
            (p, 0)
        }
        Err(e) => {
            let mut p = append(&mut s.out, 0, b"error: ");
            p = append_slot_reason(&mut s.out, p, e);
            p = append(&mut s.out, p, b"\n");
            (p, 1)
        }
    }
}

/// Render why a slot image was refused. Appends inside each arm rather than
/// returning `&'static [u8]`: static pointer tables do not relocate in PIC.
/// The content digest identifying a version: the sha256 prefix of its program.
///
/// A tag is a human label; the DIGEST is the identity. That is what makes a
/// mixed-version fleet consistent — the same tag resolves to the same bytecode
/// on every instance, because the entry carries the content address rather than
/// a name someone could repoint.
/// `release <default_tag> <tag>:<prog_hex>...`: build the `versions` param a
/// pipeline module loads to serve several versions at once.
///
/// The blue-green / canary substrate: one param carries every version the
/// instance can serve plus which one unselected traffic gets. Validated before
/// it is emitted, so a manifest that could not be represented on device fails
/// here rather than as a truncated param after rollout.
fn cmd_release(s: &mut State, arec: &[u8], argv: &[(usize, usize)], argc: usize) -> (usize, i32) {
    release_from_argv(
        arec,
        argv,
        argc,
        &mut s.prog,
        &mut s.cont,
        &mut s.scratch,
        &mut s.out,
    )
}

/// Render why a manifest was refused.
///
/// Deliberately not `fn(ReleaseError) -> &'static [u8]`: a match returning
/// static references compiles to a pointer table, and static pointer tables do
/// not relocate in a PIC module.
/// `release-ctl add|default|remove <tag> [prog_hex]`: the hot-reload control
/// messages that converge a RUNNING instance onto a new release.
///
/// `add` loads a version; `default` is the blue-green flip; `remove` reclaims a
/// drained slot. Ordering is the caller's responsibility and it matters: a flip
/// to a version the instance has not loaded fails closed until it arrives.
fn cmd_release_ctl(
    s: &mut State,
    arec: &[u8],
    argv: &[(usize, usize)],
    argc: usize,
) -> (usize, i32) {
    if argc < 3 {
        return (
            append(
                &mut s.out,
                0,
                b"error: release-ctl needs add|default|remove <tag> [prog_hex]\n",
            ),
            1,
        );
    }
    let (a, b) = argv[1];
    let (c, d) = argv[2];
    let (op, tag) = (&arec[a..b], &arec[c..d]);

    let r = if op == b"add" {
        if argc < 4 {
            return (append(&mut s.out, 0, b"error: add needs <prog_hex>\n"), 1);
        }
        let (e, f) = argv[3];
        let Some(pn) = hex_decode(&arec[e..f], &mut s.cont) else {
            return (
                append(&mut s.out, 0, b"error: program is not valid hex\n"),
                1,
            );
        };
        let program = &s.cont[..pn];
        let digest = version_digest(program);
        add_version_msg(&mut s.scratch, tag, program, &digest)
    } else if op == b"default" {
        set_default_msg(&mut s.scratch, tag)
    } else if op == b"remove" {
        remove_version_msg(&mut s.scratch, tag)
    } else {
        return (
            append(&mut s.out, 0, b"error: op must be add|default|remove\n"),
            1,
        );
    };

    match r {
        Ok(n) => {
            let Some(hn) = hex_encode(&s.scratch[..n], &mut s.prog) else {
                return (append(&mut s.out, 0, b"error: output too large\n"), 1);
            };
            let mut p = append(&mut s.out, 0, &s.prog[..hn]);
            p = append(&mut s.out, p, b"\n");
            (p, 0)
        }
        Err(e) => {
            let mut p = append(&mut s.out, 0, b"error: ");
            p = append_release_reason(&mut s.out, p, e);
            p = append(&mut s.out, p, b"\n");
            (p, 1)
        }
    }
}

/// `compile-source <schema> <param> <type> <source>`: type-check source handed
/// over at RUNTIME and emit the `ir` param a module loads.
///
/// The dynamic delivery path — "POST source, no build step" — as opposed to
/// compiling a `.uproc` document. A node holds a schema and checks source it has
/// never seen against it, emitting the same shippable checked IR the modules
/// lower at load. The target then proves it can run the result by lowering it,
/// so there is no opaque bytecode and no out-of-band agreement on the opcode set.
///
/// Plain text, not hex: a schema and an expression are both printable, and the
/// point of this command is that a caller can hand over source directly.
fn cmd_compile_source(
    s: &mut State,
    schema: &[u8],
    param: &[u8],
    input_type: &[u8],
    source: &[u8],
) -> (usize, i32) {
    match compile_ir(schema, source, param, input_type, &mut s.cont) {
        Ok((n, is_msg)) => {
            let Some(hn) = hex_encode(&s.cont[..n], &mut s.prog) else {
                return (append(&mut s.out, 0, b"error: output too large\n"), 1);
            };
            let mut p = append(&mut s.out, 0, &s.prog[..hn]);
            // The result type decides where the IR can be used: only a
            // message-constructing program is a valid pipeline stage, so it is
            // reported rather than left for the caller to rediscover.
            p = append(
                &mut s.out,
                p,
                if is_msg {
                    b"\nmessage\n"
                } else {
                    b"\nscalar\n"
                },
            );
            (p, 0)
        }
        Err(_) => (
            append(&mut s.out, 0, b"error: source did not type-check\n"),
            1,
        ),
    }
}

/// `compile-stages <schema> <param> <type> <source>...`: type-check a chain of
/// stage sources and emit the `ir_stages` param the pipeline module lowers.
///
/// Every stage must construct a message — the executor hands one stage's output
/// frame to the next, so a scalar has nothing to pass on. Each stage after the
/// first reads the previous stage's output, so they share the parameter binding.
fn cmd_compile_stages(
    s: &mut State,
    arec: &[u8],
    argv: &[(usize, usize)],
    argc: usize,
) -> (usize, i32) {
    if argc < 5 {
        return (
            append(
                &mut s.out,
                0,
                b"error: compile-stages needs <schema> <param> <type> <source>...\n",
            ),
            1,
        );
    }
    let (a, b) = argv[1];
    let (c, d) = argv[2];
    let (e, f) = argv[3];
    let (schema, param, ity) = (&arec[a..b], &arec[c..d], &arec[e..f]);

    // Bounded by MAX_ITEMS, but argv is the tighter limit in practice: a record
    // carries at most MAX_ARGV fields, so at most MAX_ARGV-4 sources can arrive.
    // Checking the real bound keeps this from being a branch that never runs.
    let n = argc - 4;
    if n > MAX_ITEMS || n > MAX_ARGV - 4 {
        return (append(&mut s.out, 0, b"error: too many stages\n"), 1);
    }
    let mut stages = [StageSource {
        source: b"",
        param_name: b"",
        input_type: b"",
    }; MAX_ITEMS];
    for (i, (x, y)) in argv[4..argc].iter().enumerate() {
        stages[i] = StageSource {
            source: &arec[*x..*y],
            param_name: param,
            input_type: ity,
        };
    }
    match compile_pipeline_ir(schema, &stages[..n], &mut s.cont, &mut s.code) {
        Ok(cn) => {
            let Some(hn) = hex_encode(&s.cont[..cn], &mut s.prog) else {
                return (append(&mut s.out, 0, b"error: output too large\n"), 1);
            };
            let mut p = append(&mut s.out, 0, &s.prog[..hn]);
            p = append(&mut s.out, p, b"\n");
            (p, 0)
        }
        Err(RegError::NotAMessage) => (
            append(
                &mut s.out,
                0,
                b"error: every stage must construct a message\n",
            ),
            1,
        ),
        Err(_) => (
            append(&mut s.out, 0, b"error: a stage did not type-check\n"),
            1,
        ),
    }
}

/// `graph <uproc_hex> <pipeline> [target]`: lower a document's pipeline all the
/// way to the fluxor graph YAML a runtime boots.
///
/// This is the last hop of authoring: parse the document, compile each stage's
/// body against the document's own schema, pack the results into the params a
/// module loads, and emit the graph. Everything upstream (`parse`, `author`) already ran here; with
/// this, source to runnable graph is one device.
///
/// `target` defaults to the EMBEDDED profile, because a node authoring a graph is
/// almost always authoring it for a node — the host-`cli` bracketing would make
/// the result un-embeddable. Pass `linux` for the host profile.
///
/// An `Effect` stage is refused rather than guessed: a connector binding names an
/// endpoint and credentials that exist nowhere in the document, and inventing a
/// default would emit a graph that looks runnable and points at nothing.
fn cmd_graph(s: &mut State, hex: &[u8], pipeline: &[u8], target: &[u8]) -> (usize, i32) {
    let Some(n) = hex_decode(hex, &mut s.ir) else {
        return (append(&mut s.out, 0, b"error: not valid hex\n"), 1);
    };
    let mut arena = UprocArena {
        messages: &mut s.u_messages,
        fields: &mut s.u_fields,
        enums: &mut s.u_enums,
        expressions: &mut s.u_expressions,
        transformations: &mut s.u_transformations,
        decisions: &mut s.u_decisions,
        rules: &mut s.u_rules,
        resources: &mut s.u_resources,
        pipelines: &mut s.u_pipelines,
        stages: &mut s.u_stages,
        args: &mut s.u_args,
        aggregations: &mut s.u_aggregations,
        operators: &mut s.u_operators,
        entries: &mut s.u_entries,
    };
    graph_document(
        &s.ir[..n],
        &mut arena,
        &mut s.prog,
        &mut s.code,
        &mut s.cont,
        &mut s.scratch,
        &mut s.out,
        pipeline,
        target,
    )
}

/// `parse <uproc_hex>`: parse a `.uproc` module document and summarise it.
///
/// Hex-encoded because argv is NUL-separated and a document contains newlines
/// and quotes; the CLI's job here is to prove a device can READ a module
/// document, which is the last thing that needed a Linux host.
fn cmd_parse(s: &mut State, hex: &[u8]) -> (usize, i32) {
    let Some(n) = hex_decode(hex, &mut s.ir) else {
        return (append(&mut s.out, 0, b"error: not valid hex\n"), 1);
    };
    // Declaration arrays live in module state, not on the PIC stack — the
    // same arena `author`/`graph` use, so `parse` validates exactly the
    // documents they accept rather than a smaller subset.
    let mut arena = UprocArena {
        messages: &mut s.u_messages,
        fields: &mut s.u_fields,
        enums: &mut s.u_enums,
        expressions: &mut s.u_expressions,
        transformations: &mut s.u_transformations,
        decisions: &mut s.u_decisions,
        rules: &mut s.u_rules,
        resources: &mut s.u_resources,
        pipelines: &mut s.u_pipelines,
        stages: &mut s.u_stages,
        args: &mut s.u_args,
        aggregations: &mut s.u_aggregations,
        operators: &mut s.u_operators,
        entries: &mut s.u_entries,
    };
    match uproc_parse(&s.ir[..n], &mut arena) {
        Ok(doc) => {
            let mut p = append(&mut s.out, 0, b"module ");
            p = append(&mut s.out, p, doc.module.of(&s.ir[..n]));
            p = append(&mut s.out, p, b" messages=");
            p = append_u32(&mut s.out, p, doc.n_messages as u32);
            p = append(&mut s.out, p, b" expressions=");
            p = append_u32(&mut s.out, p, doc.n_expressions as u32);
            p = append(&mut s.out, p, b" transformations=");
            p = append_u32(&mut s.out, p, doc.n_transformations as u32);
            p = append(&mut s.out, p, b" decisions=");
            p = append_u32(&mut s.out, p, doc.n_decisions as u32);
            p = append(&mut s.out, p, b" pipelines=");
            p = append_u32(&mut s.out, p, doc.n_pipelines as u32);
            p = append(&mut s.out, p, b" aggregations=");
            p = append_u32(&mut s.out, p, doc.n_aggregations as u32);
            p = append(&mut s.out, p, b"\n");
            (p, 0)
        }
        Err(e) => {
            let (line, col) = uproc_line_col(&s.ir[..n], e.offset);
            let mut p = append(&mut s.out, 0, b"error: ");
            p = append_uproc_reason(&mut s.out, p, e.kind);
            p = append(&mut s.out, p, b" at line ");
            p = append_u32(&mut s.out, p, line as u32);
            p = append(&mut s.out, p, b" column ");
            p = append_u32(&mut s.out, p, col as u32);
            p = append(&mut s.out, p, b"\n");
            (p, 1)
        }
    }
}

/// Append the reason for a parse failure directly into `out`.
///
/// Deliberately NOT `fn(kind) -> &'static [u8]`. A match returning static
/// references compiles to a table of (pointer, len) pairs, and static pointer
/// tables do not relocate in a PIC module — the lookup segfaults at runtime
/// while compiling and linking cleanly. Appending inside each arm keeps every
/// literal a direct reference from code, which does relocate.
/// `ckpt-save <hex>`: persist a checkpoint and move `latest` to it.
fn cmd_ckpt_save(s: &mut State, hex: &[u8]) -> (usize, i32) {
    let Some(n) = hex_decode(hex, &mut s.ir) else {
        return (append(&mut s.out, 0, b"error: not valid hex\n"), 1);
    };
    // SAFETY: `s.syscalls` is the live table installed by module_new.
    match unsafe { ckpt_save(&*s.syscalls, &s.ir[..n]) } {
        Ok(d) => print_digest(&mut s.out, &d),
        Err(e) => (ckpt_err(&mut s.out, e), 1),
    }
}

/// `ckpt-load`: read back whatever `latest` points at — the recovery path a
/// node takes on restart, when nobody is around to tell it a digest.
fn cmd_ckpt_load(s: &mut State) -> (usize, i32) {
    // SAFETY: `s.syscalls` is the live table installed by module_new.
    match unsafe { ckpt_load_latest(&*s.syscalls, &mut s.scratch) } {
        Ok((n, _)) => {
            let mut hbuf = [0u8; 2 * BIN_BUF];
            let Some(hl) = hex_encode(&s.scratch[..n], &mut hbuf) else {
                return (append(&mut s.out, 0, b"error: encode\n"), 1);
            };
            let mut p = append(&mut s.out, 0, &hbuf[..hl]);
            p = append(&mut s.out, p, b"\n");
            (p, 0)
        }
        Err(e) => (ckpt_err(&mut s.out, e), 1),
    }
}

/// Append a checkpoint-store failure reason. Appends inside the match for the
/// same PIC-relocation reason as [`oci_err`].
fn ckpt_err(out: &mut [u8], e: CkptError) -> usize {
    match e {
        CkptError::Store => append(
            out,
            0,
            b"error: store write failed (is a storage.object provider wired?)\n",
        ),
        CkptError::NotFound => append(out, 0, b"error: no checkpoint saved\n"),
        CkptError::TooLarge => append(out, 0, b"error: checkpoint too large for the buffers\n"),
        CkptError::DigestMismatch => append(
            out,
            0,
            b"error: checkpoint failed its content-address check\n",
        ),
        CkptError::CorruptPointer => append(out, 0, b"error: the latest pointer is corrupt\n"),
    }
}

/// `activate <module_hex> <trusted_key_hex> <caps> <artefacts> <modules> <bindings>`
///
/// Runs the full activation sequence: verify, then resolve dependencies,
/// artefacts, capabilities and bindings against what this node holds. The four
/// list arguments are comma-separated (`-` for none); digests are 64-char hex.
fn cmd_activate(s: &mut State, arec: &[u8], argv: &[(usize, usize)], argc: usize) -> (usize, i32) {
    if argc < 7 {
        return (
            append(
                &mut s.out,
                0,
                b"error: activate needs <module_hex> <key_hex> <caps> <artefacts> <modules> <bindings> ('-' for none)\n",
            ),
            1,
        );
    }
    let (a, b) = argv[1];
    let Some(mlen) = hex_decode(&arec[a..b], &mut s.ir) else {
        return (
            append(&mut s.out, 0, b"error: module is not valid hex\n"),
            1,
        );
    };
    let (ka, kb) = argv[2];
    let mut key = [0u8; 32];
    if kb - ka != 64 || hex_decode(&arec[ka..kb], &mut key) != Some(32) {
        return (
            append(&mut s.out, 0, b"error: trusted key is not 64 hex chars\n"),
            1,
        );
    }

    let mut caps = [b"".as_slice(); MAX_SET];
    let (ca, cb) = argv[3];
    let ncaps = split_csv(&arec[ca..cb], &mut caps);

    let mut arts = [[0u8; 32]; MAX_SET];
    let (aa, ab) = argv[4];
    let Some(narts) = split_digests(&arec[aa..ab], &mut arts) else {
        return (append(&mut s.out, 0, b"error: bad artefact digest\n"), 1);
    };

    let mut mods = [[0u8; 32]; MAX_SET];
    let (ma, mb) = argv[5];
    let Some(nmods) = split_digests(&arec[ma..mb], &mut mods) else {
        return (append(&mut s.out, 0, b"error: bad module digest\n"), 1);
    };

    let mut binds = [b"".as_slice(); MAX_SET];
    let (ba, bb) = argv[6];
    let nbinds = split_csv(&arec[ba..bb], &mut binds);

    let node = NodeState {
        capabilities: &caps[..ncaps],
        artefacts: &arts[..narts],
        modules: &mods[..nmods],
        bound_contracts: &binds[..nbinds],
    };
    match plan_activation(&s.ir[..mlen], &[key], &node, &mut s.scratch) {
        Ok(p) => {
            let mut o = append(&mut s.out, 0, b"ok: activate deps=");
            o = append_u32(&mut s.out, o, p.dependencies as u32);
            o = append(&mut s.out, o, b" artefacts=");
            o = append_u32(&mut s.out, o, p.artefacts as u32);
            o = append(&mut s.out, o, b" caps=");
            o = append_u32(&mut s.out, o, p.capabilities_satisfied as u32);
            o = append(&mut s.out, o, b" bindings=");
            o = append_u32(&mut s.out, o, p.bindings as u32);
            o = append(&mut s.out, o, b"\n");
            (o, 0)
        }
        Err(e) => {
            let msg: &[u8] = match e {
                ActivationError::NotVerified(_) => b"error: not verified (digest or signature)\n",
                ActivationError::MissingDependency => b"error: missing dependency\n",
                ActivationError::UnresolvedArtefact { .. } => b"error: unresolved artefact\n",
                ActivationError::CapabilityUnsupported => b"error: capability unsupported\n",
                ActivationError::MissingBinding => b"error: missing binding\n",
                ActivationError::Malformed => b"error: not a well-formed Module\n",
            };
            (append(&mut s.out, 0, msg), 1)
        }
    }
}

/// `verify <module_hex> <trusted_pubkey_hex>...`: verify a sealed Module.
///
/// Recomputes the module's digest from its own bytes, checks it against the one
/// carried, then requires an ed25519 signature over it by one of the trusted
/// keys. Prints which key accepted, or why it was refused.
fn cmd_verify(s: &mut State, arec: &[u8], argv: &[(usize, usize)], argc: usize) -> (usize, i32) {
    if argc < 3 {
        return (
            append(
                &mut s.out,
                0,
                b"error: verify needs <module_hex> <trusted_pubkey_hex>...\n",
            ),
            1,
        );
    }
    let (a, b) = argv[1];
    let Some(mlen) = hex_decode(&arec[a..b], &mut s.ir) else {
        return (
            append(&mut s.out, 0, b"error: module is not valid hex\n"),
            1,
        );
    };
    // Trusted keys, decoded into a bounded table.
    const MAX_TRUSTED: usize = 8;
    let mut trusted = [[0u8; 32]; MAX_TRUSTED];
    let n = (argc - 2).min(MAX_TRUSTED);
    for k in 0..n {
        let (c, d) = argv[2 + k];
        let mut kb = [0u8; 32];
        if d - c != 64 || hex_decode(&arec[c..d], &mut kb) != Some(32) {
            return (
                append(&mut s.out, 0, b"error: a trusted key is not 64 hex chars\n"),
                1,
            );
        }
        trusted[k] = kb;
    }
    match module_verify(&s.ir[..mlen], &trusted[..n], &mut s.scratch) {
        Ok(k) => {
            let mut p = append(&mut s.out, 0, b"ok: signed by trusted key ");
            p = append_u32(&mut s.out, p, k as u32);
            p = append(&mut s.out, p, b"\n");
            (p, 0)
        }
        Err(e) => {
            let msg: &[u8] = match e {
                VerifyError::Malformed => b"error: not a well-formed Module\n",
                VerifyError::NoDigest => b"error: module carries no content digest\n",
                VerifyError::DigestMismatch => {
                    b"error: digest mismatch (content altered since sealing)\n"
                }
                VerifyError::SignatureInvalid => b"error: no valid signature by a trusted signer\n",
                VerifyError::TooLarge => b"error: module too large for the buffers\n",
            };
            (append(&mut s.out, 0, msg), 1)
        }
    }
}

/// `oci-init`: create the image layout (`oci-layout` + an empty `index.json`)
/// if it is not already there. Idempotent.
fn cmd_oci_init(s: &mut State) -> (usize, i32) {
    // SAFETY: `s.syscalls` is the live table installed by module_new.
    match unsafe { oci_init(&*s.syscalls, &mut s.scratch) } {
        Ok(()) => (append(&mut s.out, 0, b"ok\n"), 0),
        Err(e) => (oci_err(&mut s.out, e), 1),
    }
}

/// `oci-push <hex> <tag>`: publish `hex` as a single module layer under `tag`,
/// printing the bundle reference (the OCI manifest digest).
fn cmd_oci_push(s: &mut State, hex: &[u8], tag: &[u8]) -> (usize, i32) {
    let Some(blen) = hex_decode(hex, &mut s.ir) else {
        return (append(&mut s.out, 0, b"error: not valid hex\n"), 1);
    };
    // Disjoint field borrows: the body is read out of `ir` while the manifest,
    // index and read buffers are written — none of them alias.
    let layers = [Layer {
        media_type: MEDIA_TYPE_MODULE,
        bytes: &s.ir[..blen],
        name: b"",
    }];
    // SAFETY: `s.syscalls` is the live table installed by module_new.
    let r = unsafe {
        oci_push(
            &*s.syscalls,
            &layers,
            tag,
            b"local-build",
            b"",
            PushBufs {
                manifest: &mut s.cont,
                index: &mut s.code,
                read: &mut s.scratch,
            },
        )
    };
    match r {
        Ok(d) => print_digest(&mut s.out, &d),
        Err(e) => (oci_err(&mut s.out, e), 1),
    }
}

/// `oci-resolve <tag>`: print the bundle digest a tag currently points at.
fn cmd_oci_resolve(s: &mut State, tag: &[u8]) -> (usize, i32) {
    // SAFETY: `s.syscalls` is the live table installed by module_new.
    match unsafe { oci_resolve_tag(&*s.syscalls, tag, &mut s.scratch) } {
        Ok(d) => print_digest(&mut s.out, &d),
        Err(e) => (oci_err(&mut s.out, e), 1),
    }
}

/// `oci-fetch <digest_hex>`: fetch a bundle by digest, printing one hex line per
/// layer. Every layer is content-address verified on the way out.
fn cmd_oci_fetch(s: &mut State, hex: &[u8]) -> (usize, i32) {
    let mut d = [0u8; 32];
    if hex.len() != 64 || hex_decode(hex, &mut d) != Some(32) {
        return (append(&mut s.out, 0, b"error: need a 64-char digest\n"), 1);
    }
    let mut p = 0usize;
    // Set if any layer did not fit the output buffer. `append` clamps, so
    // without this a truncated line would read as a complete one.
    let mut overflowed = false;
    let out = &mut s.out;
    // SAFETY: `s.syscalls` is the live table installed by module_new.
    let r = unsafe {
        oci_fetch(
            &*s.syscalls,
            &d,
            &mut s.cont,
            &mut s.code,
            |_media, _name, bytes| {
                // Encode straight into `out`: a temp buffer would cap the layer
                // size at its own length and silently drop anything larger.
                if p + bytes.len() * 2 + 1 > out.len() {
                    overflowed = true;
                    return;
                }
                p = append_hex(out, p, bytes);
                p = append(out, p, b"\n");
            },
        )
    };
    match r {
        Ok(_) if overflowed => (
            append(out, 0, b"error: a layer is larger than the output buffer\n"),
            1,
        ),
        Ok(_) => (p, 0),
        Err(e) => (oci_err(out, e), 1),
    }
}

/// Append an OCI failure reason. Written as appends inside the match rather than
/// `match -> &'static [u8]`: a match yielding static references compiles to a
/// pointer table, and static pointer tables do not relocate in a PIC module.
fn oci_err(out: &mut [u8], e: OciError) -> usize {
    match e {
        OciError::Store => append(
            out,
            0,
            b"error: store write failed (is a storage.object provider wired?)\n",
        ),
        OciError::NotFound => append(out, 0, b"error: not found\n"),
        OciError::TooLarge => append(out, 0, b"error: too large for the bounded buffers\n"),
        OciError::Malformed => append(out, 0, b"error: malformed index or manifest json\n"),
        OciError::DigestMismatch => append(
            out,
            0,
            b"error: content address mismatch (blob does not hash to its key)\n",
        ),
        OciError::TooMany => append(out, 0, b"error: too many layers or index entries\n"),
    }
}

/// `put <hex>`: store a blob under its own sha256 and print that digest.
///
/// Completes the self-hosting loop — a node that seals an artefact can also
/// KEEP it, content-addressed, without a Linux toolchain. The key layout is
/// `blobs/sha256/<hex>`, what an OCI image directory uses, so a store written
/// here is one a registry can serve.
fn cmd_put(s: &mut State, hex: &[u8]) -> (usize, i32) {
    let Some(blen) = hex_decode(hex, &mut s.ir) else {
        return (append(&mut s.out, 0, b"error: not valid hex\n"), 1);
    };
    // SAFETY: `s.syscalls` is the live table installed by module_new.
    let r = unsafe { blob_put(&*s.syscalls, &s.ir[..blen]) };
    match r {
        Ok(d) => print_digest(&mut s.out, &d),
        Err(_) => (oci_err(&mut s.out, OciError::Store), 1),
    }
}

/// `get <digest_hex>`: read the blob back, VERIFYING the content address, and
/// print it as hex. A digest mismatch is reported rather than returned — bytes
/// that no longer hash to their key are not the artefact anyone pinned.
fn cmd_get(s: &mut State, hex: &[u8]) -> (usize, i32) {
    let mut digest = [0u8; 32];
    if hex_decode(hex, &mut digest) != Some(32) {
        return (
            append(&mut s.out, 0, b"error: digest must be 32 hex bytes\n"),
            1,
        );
    }
    // SAFETY: `s.syscalls` is the live table installed by module_new.
    let r = unsafe { blob_get(&*s.syscalls, &digest, &mut s.cont) };
    match r {
        Ok(n) => {
            let mut p = append_hex(&mut s.out, 0, &s.cont[..n]);
            p = append(&mut s.out, p, b"\n");
            (p, 0)
        }
        Err(BlobError::DigestMismatch) => (
            append(
                &mut s.out,
                0,
                b"error: stored bytes do not hash to their key (store corrupt)\n",
            ),
            1,
        ),
        Err(_) => (append(&mut s.out, 0, b"error: not found\n"), 1),
    }
}

/// `check <ir_hex>`: decode, lower every stage (the exact load-time transcode a
/// pipeline .fmod performs), report per-stage cost — or the structured error.
fn cmd_check(s: &mut State, ir_hex: &[u8]) -> (usize, i32) {
    let Some(ilen) = hex_decode(ir_hex, &mut s.ir) else {
        let p = append(&mut s.out, 0, b"error: ir_hex is not valid hex\n");
        return (p, 1);
    };
    let ir = {
        let (head, _) = s.ir.split_at(ilen);
        // copy so `prog` can borrow s mutably below
        head
    };
    let mut irc = [0u8; BIN_BUF];
    irc[..ilen].copy_from_slice(ir);
    match lower_stages(&irc[..ilen], &mut s.prog) {
        Ok(plen) => {
            let n = stage_count(&s.prog[..plen]);
            let mut p = append(&mut s.out, 0, b"ok: ");
            p = append_u32(&mut s.out, p, n as u32);
            p = append(&mut s.out, p, b" stage(s), ");
            p = append_u32(&mut s.out, p, plen as u32);
            p = append(&mut s.out, p, b" bytecode byte(s)\n");
            for i in 0..n.min(MAX_STAGES) {
                if let Some(st) = stage_at(&s.prog[..plen], i) {
                    p = append(&mut s.out, p, b"  stage ");
                    p = append_u32(&mut s.out, p, i as u32);
                    p = append(&mut s.out, p, b": cost=");
                    p = append_u32(&mut s.out, p, st.max_cost as u32);
                    p = append(&mut s.out, p, b" len=");
                    p = append_u32(&mut s.out, p, st.code.len() as u32);
                    p = append(&mut s.out, p, b"\n");
                }
            }
            (p, 0)
        }
        Err(_) => {
            let p = append(
                &mut s.out,
                0,
                b"error: ir_stages container failed to lower (a device would reject this param)\n",
            );
            (p, 1)
        }
    }
}

/// `eval <ir_hex> <record_hex>`: lower, execute over the record frame, print
/// the output frame as hex — byte-for-byte what the deployed engine emits.
fn cmd_eval(s: &mut State, ir_hex: &[u8], rec_hex: &[u8]) -> (usize, i32) {
    let Some(ilen) = hex_decode(ir_hex, &mut s.ir) else {
        let p = append(&mut s.out, 0, b"error: ir_hex is not valid hex\n");
        return (p, 1);
    };
    let Some(rlen) = hex_decode(rec_hex, &mut s.record) else {
        let p = append(&mut s.out, 0, b"error: record_hex is not valid hex\n");
        return (p, 1);
    };
    let mut irc = [0u8; BIN_BUF];
    irc[..ilen].copy_from_slice(&s.ir[..ilen]);
    let plen = match lower_stages(&irc[..ilen], &mut s.prog) {
        Ok(l) => l,
        Err(_) => {
            let p = append(
                &mut s.out,
                0,
                b"error: ir_stages container failed to lower\n",
            );
            return (p, 1);
        }
    };
    let mut stages = [Stage {
        code: &[],
        max_cost: 0,
        on_failure: None,
    }; MAX_STAGES];
    let n = stage_count(&s.prog[..plen]).min(MAX_STAGES);
    let prog = &s.prog[..plen];
    for (i, slot) in stages.iter_mut().enumerate().take(n) {
        match stage_at(prog, i) {
            Some(st) => *slot = st,
            None => {
                let p = append(&mut s.out, 0, b"error: stage table is truncated\n");
                return (p, 1);
            }
        }
    }
    let mut record = [0u8; 512];
    record[..rlen].copy_from_slice(&s.record[..rlen]);
    match run_stages(
        &stages[..n],
        &record[..rlen],
        &mut s.buf_a,
        &mut s.buf_b,
        &mut s.frame,
    ) {
        Ok(flen) => {
            let mut hexed = [0u8; 1024];
            let Some(hl) = hex_encode(&s.frame[..flen], &mut hexed) else {
                let p = append(&mut s.out, 0, b"error: output frame too large to print\n");
                return (p, 1);
            };
            let mut p = append(&mut s.out, 0, &hexed[..hl]);
            p = append(&mut s.out, p, b"\n");
            (p, 0)
        }
        Err(e) => {
            // Name the structured error. On device a failed record only bumps
            // a counter, so this command is the diagnosis path — flattening
            // the error kind here would leave "records vanish" with no way to
            // tell a type error from an arena overflow from a missing
            // builtin. (The VERSION_UNAVAILABLE lesson, applied.)
            let mut p = append(&mut s.out, 0, b"error: execution failed: ");
            p = append(&mut s.out, p, pipe_err_name(e));
            p = append(&mut s.out, p, b"\n");
            (p, 1)
        }
    }
}

/// Render a `PipeError` (and its inner `EvalError`) as a stable diagnostic
/// name. Names, not Debug formatting: the device CLI has no `format!`.
fn pipe_err_name(e: tc::PipeError) -> &'static [u8] {
    use tc::{EvalError, PipeError};
    match e {
        PipeError::NotConstructed => b"stage did not construct a message",
        PipeError::RouteLoop => b"failure-routing loop",
        PipeError::BadFrame => b"malformed record frame",
        PipeError::Encode => b"output frame encoding failed",
        PipeError::StageEval(ev) => match ev {
            EvalError::Truncated => b"stage eval: truncated program",
            EvalError::BadOpcode(_) => b"stage eval: bad opcode",
            EvalError::StackOverflow => b"stage eval: stack overflow",
            EvalError::StackUnderflow => b"stage eval: stack underflow",
            EvalError::BadParam(_) => b"stage eval: bad parameter index",
            EvalError::NotAMessage => b"stage eval: selected into a non-message",
            EvalError::CostExceeded => b"stage eval: cost ceiling exceeded",
            EvalError::BadResultArity => b"stage eval: bad result arity",
            EvalError::TypeError => b"stage eval: type error",
            EvalError::BuildOverflow => b"stage eval: too many constructed fields",
            EvalError::BadBuiltin(_) => b"stage eval: builtin not in this build",
            EvalError::ScratchOverflow => b"stage eval: scratch arena overflow (STAGE_SCRATCH_CAP)",
            EvalError::BadLocal(_) => b"stage eval: bad cel.bind local slot",
        },
    }
}

/// `digest <hex>`: full sha256 of the decoded bytes. Release tables identify a
/// version by this digest's prefix; the OCI store addresses blobs by all of it.
fn cmd_digest(s: &mut State, hex: &[u8]) -> (usize, i32) {
    let Some(blen) = hex_decode(hex, &mut s.ir) else {
        let p = append(&mut s.out, 0, b"error: not valid hex\n");
        return (p, 1);
    };
    let d = sha256(&s.ir[..blen]);
    print_digest(&mut s.out, &d)
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<State>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_new"]
#[allow(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "the fluxor module ABI entry point: the runtime owns these pointers and \
              their validity is the ABI's contract, and the signature is fixed by that \
              contract rather than chosen here"
)]
pub extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    _params: *const u8,
    _params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    unsafe {
        if syscalls.is_null() || state.is_null() {
            return -1;
        }
        if state_size < core::mem::size_of::<State>() {
            return -2;
        }
        let s = &mut *(state as *mut State);
        s.syscalls = syscalls as *const SyscallTable;
        s.args_chan = in_chan; // input port 0 ← cli_in.args_out
        s.out_chan = out_chan; // output port 0 → cli_out.bytes_in
        s.exit_chan = -1; // output port 1 → cli_out.exit_in (resolved lazily)
        s.waited = 0;
        s.done = 0;
        s.result_ready = 0;
        s.res_olen = 0;
        s.out_off = 0;
        s.res_code = 0;
        s.exit_done = 0;
        0
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        if state.is_null() {
            return STEP_DONE;
        }
        let s = &mut *(state as *mut State);
        if s.syscalls.is_null() {
            return STEP_DONE;
        }
        let sys = &*s.syscalls;
        if s.done != 0 {
            return STEP_DONE;
        }
        if s.exit_chan < 0 {
            s.exit_chan = dev_channel_port(sys, PORT_OUTPUT, 1);
        }

        if s.result_ready == 0 {
            // Read the argv record (one NUL-separated record from cli_in). Retry a
            // bounded number of steps; an empty argv (no `--`) never arrives, so we
            // fall through to `help`.
            let an = if s.args_chan >= 0 {
                (sys.channel_read)(s.args_chan, s.arec.as_mut_ptr(), s.arec.len())
            } else {
                0
            };
            if an <= 0 {
                s.waited += 1;
                if s.waited < ARGV_WAIT {
                    return 0; // keep waiting for argv
                }
            }
            let alen = if an > 0 { an as usize } else { 0 };

            let mut argv = [(0usize, 0usize); MAX_ARGV];
            let mut arec = [0u8; ARGV_BUF];
            arec[..alen].copy_from_slice(&s.arec[..alen]);
            let argc = split_argv(&arec[..alen], &mut argv);

            let (olen, code): (usize, i32) = if argc == 0 {
                (cmd_help(&mut s.out), 0)
            } else {
                let (s0, e0) = argv[0];
                let sub = &arec[s0..e0];
                if sub == b"compile" {
                    if argc >= 4 {
                        let (a, b) = argv[1];
                        let (c, d) = argv[2];
                        let (e, f) = argv[3];
                        cmd_compile(s, &arec[a..b], &arec[c..d], &arec[e..f])
                    } else {
                        (
                            append(
                                &mut s.out,
                                0,
                                b"error: compile needs <schema> <params> <source>\n",
                            ),
                            1,
                        )
                    }
                } else if sub == b"stages" {
                    cmd_stages(s, &arec, &argv, argc)
                } else if sub == b"seal" {
                    if argc >= 9 {
                        let a = |k: usize| {
                            let (x, y) = argv[k];
                            (x, y)
                        };
                        let (s1, e1) = a(1);
                        let (s2, e2) = a(2);
                        let (s3, e3) = a(3);
                        let (s4, e4) = a(4);
                        let (s5, e5) = a(5);
                        let (s6, e6) = a(6);
                        let (s7, e7) = a(7);
                        let (s8, e8) = a(8);
                        cmd_seal(
                            s,
                            &arec[s1..e1],
                            &arec[s2..e2],
                            &arec[s3..e3],
                            &arec[s4..e4],
                            &arec[s5..e5],
                            &arec[s6..e6],
                            &arec[s7..e7],
                            &arec[s8..e8],
                        )
                    } else {
                        (
                        append(
                            &mut s.out,
                            0,
                            b"error: seal needs <schema> <params> <src> <pkg> <sym> <param> <param_msg> <result_ty>\n",
                        ),
                        1,
                    )
                    }
                } else if sub == b"put" {
                    if argc >= 2 {
                        let (a, b) = argv[1];
                        cmd_put(s, &arec[a..b])
                    } else {
                        (append(&mut s.out, 0, b"error: put needs <hex>\n"), 1)
                    }
                } else if sub == b"get" {
                    if argc >= 2 {
                        let (a, b) = argv[1];
                        cmd_get(s, &arec[a..b])
                    } else {
                        (append(&mut s.out, 0, b"error: get needs <digest_hex>\n"), 1)
                    }
                } else if sub == b"seal-tf" {
                    if argc >= 8 {
                        let g = |k: usize| argv[k];
                        let (a1, b1) = g(1);
                        let (a2, b2) = g(2);
                        let (a3, b3) = g(3);
                        let (a4, b4) = g(4);
                        let (a5, b5) = g(5);
                        let (a6, b6) = g(6);
                        let (a7, b7) = g(7);
                        cmd_seal_tf(
                            s,
                            &arec[a1..b1],
                            &arec[a2..b2],
                            &arec[a3..b3],
                            &arec[a4..b4],
                            &arec[a5..b5],
                            &arec[a6..b6],
                            &arec[a7..b7],
                        )
                    } else {
                        (
                        append(
                            &mut s.out,
                            0,
                            b"error: seal-tf needs <schema> <params> <src> <pkg> <sym> <in_ty> <out_ty>\n",
                        ),
                        1,
                    )
                    }
                } else if sub == b"seal-module" {
                    cmd_seal_module(s, &arec, &argv, argc)
                } else if sub == b"decision" {
                    cmd_decision(s, &arec, &argv, argc)
                } else if sub == b"agg" {
                    cmd_agg(s, &arec, &argv, argc)
                } else if sub == b"check" {
                    if argc >= 2 {
                        let (a, b) = argv[1];
                        cmd_check(s, &arec[a..b])
                    } else {
                        (append(&mut s.out, 0, b"error: check needs <ir_hex>\n"), 1)
                    }
                } else if sub == b"eval" {
                    if argc >= 3 {
                        let (a, b) = argv[1];
                        let (c, d) = argv[2];
                        cmd_eval(s, &arec[a..b], &arec[c..d])
                    } else {
                        (
                            append(&mut s.out, 0, b"error: eval needs <ir_hex> <record_hex>\n"),
                            1,
                        )
                    }
                } else if sub == b"digest" {
                    if argc >= 2 {
                        let (a, b) = argv[1];
                        cmd_digest(s, &arec[a..b])
                    } else {
                        (append(&mut s.out, 0, b"error: digest needs <hex>\n"), 1)
                    }
                } else if sub == b"author" {
                    if argc >= 2 {
                        let (a, b) = argv[1];
                        cmd_author(s, &arec[a..b])
                    } else {
                        (
                            append(&mut s.out, 0, b"error: author needs <uproc_hex>\n"),
                            1,
                        )
                    }
                } else if sub == b"slot-verify" {
                    if argc >= 2 {
                        let (a, b) = argv[1];
                        let abi = if argc >= 3 {
                            let (c, d) = argv[2];
                            &arec[c..d]
                        } else {
                            b"".as_slice()
                        };
                        cmd_slot_verify(s, &arec[a..b], abi)
                    } else {
                        (
                            append(
                                &mut s.out,
                                0,
                                b"error: slot-verify needs <image_digest_hex> [abi_surface_hex]\n",
                            ),
                            1,
                        )
                    }
                } else if sub == b"release" {
                    cmd_release(s, &arec, &argv, argc)
                } else if sub == b"release-ctl" {
                    cmd_release_ctl(s, &arec, &argv, argc)
                } else if sub == b"compile-source" {
                    if argc >= 5 {
                        let (a, b) = argv[1];
                        let (c, d) = argv[2];
                        let (e, f) = argv[3];
                        let (g, h) = argv[4];
                        cmd_compile_source(s, &arec[a..b], &arec[c..d], &arec[e..f], &arec[g..h])
                    } else {
                        (
                            append(
                                &mut s.out,
                                0,
                                b"error: compile-source needs <schema> <param> <type> <source>\n",
                            ),
                            1,
                        )
                    }
                } else if sub == b"compile-stages" {
                    cmd_compile_stages(s, &arec, &argv, argc)
                } else if sub == b"graph" {
                    if argc >= 3 {
                        let (a, b) = argv[1];
                        let (c, d) = argv[2];
                        // Target is optional; embedded `bcm2712` is the default,
                        // since a node authoring a graph authors it for a node.
                        let target = if argc >= 4 {
                            let (e, f) = argv[3];
                            &arec[e..f]
                        } else {
                            b"bcm2712".as_slice()
                        };
                        cmd_graph(s, &arec[a..b], &arec[c..d], target)
                    } else {
                        (
                            append(
                                &mut s.out,
                                0,
                                b"error: graph needs <uproc_hex> <pipeline> [target]\n",
                            ),
                            1,
                        )
                    }
                } else if sub == b"parse" {
                    if argc >= 2 {
                        let (a, b) = argv[1];
                        cmd_parse(s, &arec[a..b])
                    } else {
                        (
                            append(&mut s.out, 0, b"error: parse needs <uproc_hex>\n"),
                            1,
                        )
                    }
                } else if sub == b"ckpt-save" {
                    if argc >= 2 {
                        let (a, b) = argv[1];
                        cmd_ckpt_save(s, &arec[a..b])
                    } else {
                        (append(&mut s.out, 0, b"error: ckpt-save needs <hex>\n"), 1)
                    }
                } else if sub == b"ckpt-load" {
                    cmd_ckpt_load(s)
                } else if sub == b"activate" {
                    cmd_activate(s, &arec, &argv, argc)
                } else if sub == b"verify" {
                    cmd_verify(s, &arec, &argv, argc)
                } else if sub == b"oci-init" {
                    cmd_oci_init(s)
                } else if sub == b"oci-push" {
                    if argc >= 3 {
                        let (a, b) = argv[1];
                        let (c, d) = argv[2];
                        cmd_oci_push(s, &arec[a..b], &arec[c..d])
                    } else {
                        (
                            append(&mut s.out, 0, b"error: oci-push needs <hex> <tag>\n"),
                            1,
                        )
                    }
                } else if sub == b"oci-resolve" {
                    if argc >= 2 {
                        let (a, b) = argv[1];
                        cmd_oci_resolve(s, &arec[a..b])
                    } else {
                        (
                            append(&mut s.out, 0, b"error: oci-resolve needs <tag>\n"),
                            1,
                        )
                    }
                } else if sub == b"oci-fetch" {
                    if argc >= 2 {
                        let (a, b) = argv[1];
                        cmd_oci_fetch(s, &arec[a..b])
                    } else {
                        (
                            append(&mut s.out, 0, b"error: oci-fetch needs <digest_hex>\n"),
                            1,
                        )
                    }
                } else if sub == b"help" {
                    (cmd_help(&mut s.out), 0)
                } else {
                    let mut p = append(&mut s.out, 0, b"error: unknown command '");
                    p = append(&mut s.out, p, sub);
                    p = append(&mut s.out, p, b"' (try `help`)\n");
                    (p, 1)
                }
            };

            s.res_olen = olen as u16;
            s.res_code = code;
            s.out_off = 0;
            s.exit_done = 0;
            s.result_ready = 1;
        }

        // Deliver the produced output and exit status, retrying across steps on a
        // full ring — the command already ran, so nothing re-executes. The result is
        // complete only when BOTH stdout and exit are accepted; a terminal
        // channel fault gives up rather than spinning.
        if s.out_chan >= 0 && s.out_off < s.res_olen {
            let remaining = (s.res_olen - s.out_off) as usize;
            let w = (sys.channel_write)(
                s.out_chan,
                s.out.as_ptr().add(s.out_off as usize),
                remaining,
            );
            if w > 0 {
                s.out_off += w as u16;
            } else if w == 0 {
                return 0;
            } else {
                s.done = 1;
                return STEP_DONE;
            }
            if s.out_off < s.res_olen {
                return 0;
            }
        }
        if s.exit_chan >= 0 && s.exit_done == 0 {
            let c = s.res_code.to_le_bytes();
            let w = (sys.channel_write)(s.exit_chan, c.as_ptr(), c.len());
            if w == c.len() as i32 {
                s.exit_done = 1;
            } else if w == 0 {
                return 0;
            } else {
                s.done = 1;
                return STEP_DONE;
            }
        }
        s.done = 1;
        STEP_DONE
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
