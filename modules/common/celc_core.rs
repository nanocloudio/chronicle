// No-alloc CEL front end: parse + type-check + checked-IR emission in ONE pass.
//
// This is THE compiler — the only implementation. The host crate
// (`chronicle-canonical::cel`) is a thin std wrapper that `include!`s this same
// source: its `TypeEnv` builder renders to the textual environment below and
// its string errors are sliced from the spans this core reports. The core
// exploits the fact that the flat checked-IR encoding (`lower_core.rs` `ir`) is
// a POST-ORDER token stream — operands precede operators — so a recursive
// descent over the source can emit IR bytes directly as it reduces, carry each
// subexpression's type in its return value, and never allocate. The
// differential test (`chronicle-canonical/tests/celc_differential.rs`)
// proves the wrapper's rendered environment is faithful to the hand-written
// text form the device receives.
//
// Grammar and semantics:
//   expr := or ; or := and ('||' and)* ; and := cmp ('&&' cmp)*
//   cmp := add (CMPOP add)? ; add := mul (('+'|'-') mul)*
//   mul := unary ('*' unary)* ; unary := '!' unary | '-' INT | primary
//   primary := INT | STR | 'true' | 'false' | '(' expr ')'
//            | DOTTED ('{' name ':' expr, … '}')?
// Comparisons are untyped (any operands), arithmetic requires int/uint,
// logical ops and `!` require bool, field values are NOT type-checked against
// their declaration, and a construct may not appear as a field value.
//
// Deliberate bounds (all fail closed): identifiers are ASCII-only, expression
// nesting depth is capped (`CELC_MAX_DEPTH` — a PIC stack cannot recurse
// unboundedly), and on inputs with several independent errors the streaming
// pass surfaces a value's inner error before the enclosing field-name check.
//
// The schema/param environment arrives as TEXT (no builder API on device):
//   schema : defs separated by ';'. A message def is `Name{f:ty@N,…}` where
//            ty ∈ int|uint|double|bool|str|bytes or a (dotted) message name;
//            an enum def is `NAME=INT`. Message names may be dotted.
//   params : `name:Type,…` in declaration order (order IS the runtime index).
// Lookups scan the text on demand — no tables, no interning, no allocation.

/// Nesting-depth cap for expressions and the schema-free recursion below.
pub const CELC_MAX_DEPTH: usize = 16;
/// Maximum `.`-separated segments in one dotted name.
pub const CELC_MAX_SEGS: usize = 8;

/// Which text buffer a span points into.
pub const BUF_SRC: u8 = 0;
pub const BUF_SCHEMA: u8 = 1;
pub const BUF_PARAMS: u8 = 2;

/// Structured, deterministic compile errors. Codes plus SPANS, never strings —
/// a std wrapper (the host crate) renders names by slicing the referenced
/// buffer; the device CLI renders the code alone. `Parse`/`BadSchema` carry the
/// byte offset that stopped the scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CelcErr {
    Empty,
    Parse(u16),
    BadInteger,
    Trailing,
    /// Bare identifier that is neither a parameter nor an enum (src span).
    UnknownName(u16, u16),
    /// Dotted path whose root is not a parameter (src span of the root).
    UnknownParam(u16, u16),
    /// No such field: message-name span (in `buf`) + field span (in src).
    UnknownField {
        buf: u8,
        msg_s: u16,
        msg_e: u16,
        field_s: u16,
        field_e: u16,
    },
    /// Message name that resolves to nothing: span in `buf` (src for a
    /// construct's type name; schema/params for a path's current type).
    UnknownMessageType {
        buf: u8,
        s: u16,
        e: u16,
    },
    /// Selected into a scalar: src span of the path SO FAR + the scalar's CTy
    /// discriminant (for rendering the type name).
    NotAMessage {
        path_s: u16,
        path_e: u16,
        scalar: u8,
    },
    NotBool,
    NotInteger,
    NestedConstruction,
    Depth,
    Capacity,
    BadSchema(u16),
    /// A call to a name that is not in the pinned builtin table (src span of
    /// the function name).
    UnknownFunction(u16, u16),
    /// A builtin exists but the argument count or types do not match any of
    /// its pinned overloads (src span of the function name).
    BadCallArgs(u16, u16),
    /// `cel.bind` nesting exceeded the VM's local-slot file (`MAX_LOCALS`).
    LocalDepth,
}

/// A subexpression's type. Message identity is a span into the schema (canonical
/// def-site name) or the params text (a message the schema never defines — only
/// an error if it is ever selected into).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CTy {
    Int,
    Uint,
    Double,
    Bool,
    Str,
    Bytes,
    /// (in_params, start, end) — name span in schema (false) or params (true).
    Msg(bool, u16, u16),
}

fn cty_is_int(t: &CTy) -> bool {
    matches!(t, CTy::Int | CTy::Uint)
}

/// Discriminant for a scalar `CTy` (carried in `NotAMessage` for rendering).
pub fn cty_scalar_code(t: &CTy) -> u8 {
    match t {
        CTy::Int => 0,
        CTy::Uint => 1,
        CTy::Double => 2,
        CTy::Bool => 3,
        CTy::Str => 4,
        CTy::Bytes => 5,
        CTy::Msg(..) => 6,
    }
}

/// Compile `src` against the textual environment into flat checked IR written to
/// `out`. Returns `(len, result_type)`. NO `RET` terminator is appended — see
/// [`celc_compile_auto`] for the host-`compile` convention.
pub fn celc_compile_ty(
    schema: &[u8],
    params: &[u8],
    src: &[u8],
    out: &mut [u8],
) -> Result<(usize, CTy), CelcErr> {
    let mut p = Celc {
        schema,
        params,
        src,
        pos: 0,
        out,
        w: 0,
        depth: 0,
        locals: [None; CELC_MAX_LOCALS],
        nlocals: 0,
    };
    p.skip_ws();
    if p.pos >= p.src.len() {
        return Err(CelcErr::Empty);
    }
    let (ty, _) = p.expr()?;
    p.skip_ws();
    if p.pos != p.src.len() {
        return Err(CelcErr::Trailing);
    }
    Ok((p.w, ty))
}

/// [`celc_compile_ty`] reduced to `(len, is_message_result)`.
pub fn celc_compile(
    schema: &[u8],
    params: &[u8],
    src: &[u8],
    out: &mut [u8],
) -> Result<(usize, bool), CelcErr> {
    let (n, ty) = celc_compile_ty(schema, params, src, out)?;
    Ok((n, matches!(ty, CTy::Msg(..))))
}

/// [`celc_compile`] plus the host `compile`/`encode_ir` scalar convention: a
/// non-message result gets the `ir::RET` terminator appended.
pub fn celc_compile_auto(
    schema: &[u8],
    params: &[u8],
    src: &[u8],
    out: &mut [u8],
) -> Result<usize, CelcErr> {
    let (mut n, is_msg) = celc_compile(schema, params, src, out)?;
    if !is_msg {
        if n >= out.len() {
            return Err(CelcErr::Capacity);
        }
        out[n] = ir::RET;
        n += 1;
    }
    Ok(n)
}

/// One dotted-name segment: a span into the source.
#[derive(Clone, Copy)]
struct Seg {
    s: u16,
    e: u16,
}

/// One live `cel.bind` binding: the bound name's src span, its VM local slot,
/// and the init expression's type.
#[derive(Clone, Copy)]
struct Binding {
    name: Seg,
    slot: u8,
    ty: CTy,
}

/// Local-slot budget mirrored from the VM (`vm_core::MAX_LOCALS`).
const CELC_MAX_LOCALS: usize = 8;

struct Celc<'a> {
    schema: &'a [u8],
    params: &'a [u8],
    src: &'a [u8],
    pos: usize,
    out: &'a mut [u8],
    w: usize,
    depth: usize,
    /// Live `cel.bind` bindings, innermost last (shadowing = last match wins).
    locals: [Option<Binding>; CELC_MAX_LOCALS],
    nlocals: usize,
}

impl Celc<'_> {
    // ---- emission -------------------------------------------------------

    fn put(&mut self, b: &[u8]) -> Result<(), CelcErr> {
        if self.w + b.len() > self.out.len() {
            return Err(CelcErr::Capacity);
        }
        self.out[self.w..self.w + b.len()].copy_from_slice(b);
        self.w += b.len();
        Ok(())
    }
    fn put1(&mut self, b: u8) -> Result<(), CelcErr> {
        self.put(&[b])
    }

    // ---- source scanning ------------------------------------------------

    fn skip_ws(&mut self) {
        while self.pos < self.src.len() && self.src[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }
    fn peek(&mut self) -> Option<u8> {
        self.skip_ws();
        self.src.get(self.pos).copied()
    }
    /// Consume `lit` (post-whitespace) if present.
    fn eat(&mut self, lit: &[u8]) -> bool {
        self.skip_ws();
        if self.src[self.pos..].starts_with(lit) {
            self.pos += lit.len();
            true
        } else {
            false
        }
    }
    fn err_here(&self) -> CelcErr {
        CelcErr::Parse(self.pos as u16)
    }

    fn ident(&mut self) -> Option<Seg> {
        self.skip_ws();
        let s = self.pos;
        let first = *self.src.get(self.pos)?;
        if !(first.is_ascii_alphabetic() || first == b'_') {
            return None;
        }
        while self
            .src
            .get(self.pos)
            .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_')
        {
            self.pos += 1;
        }
        Some(Seg {
            s: s as u16,
            e: self.pos as u16,
        })
    }

    /// Parse an unsigned decimal literal into i64 (host `str::parse` semantics:
    /// overflow is `BadInteger`). `neg` folds the unary-minus literal case.
    fn int_lit(&mut self, neg: bool) -> Result<i64, CelcErr> {
        self.skip_ws();
        if !self.src.get(self.pos).is_some_and(u8::is_ascii_digit) {
            return Err(self.err_here());
        }
        let mut v: i64 = 0;
        while let Some(c) = self.src.get(self.pos).filter(|c| c.is_ascii_digit()) {
            v = v
                .checked_mul(10)
                .and_then(|v| v.checked_add((c - b'0') as i64))
                .ok_or(CelcErr::BadInteger)?;
            self.pos += 1;
        }
        Ok(if neg { -v } else { v })
    }

    // ---- expression grammar (each level emits post-order + returns type) --
    // The bool in the return is "the root of this subexpression is a message
    // construction" — parens are transparent to it, exactly like the host's
    // `matches!(value, Ast::Construct { .. })` on the parsed tree.

    fn expr(&mut self) -> Result<(CTy, bool), CelcErr> {
        if self.depth >= CELC_MAX_DEPTH {
            return Err(CelcErr::Depth);
        }
        self.depth += 1;
        let r = self.or_level();
        self.depth -= 1;
        r
    }

    fn or_level(&mut self) -> Result<(CTy, bool), CelcErr> {
        let (mut lt, mut root) = self.and_level()?;
        while self.eat(b"||") {
            let (rt, _) = self.and_level()?;
            if lt != CTy::Bool || rt != CTy::Bool {
                return Err(CelcErr::NotBool);
            }
            self.put1(ir::OR)?;
            lt = CTy::Bool;
            root = false;
        }
        Ok((lt, root))
    }

    fn and_level(&mut self) -> Result<(CTy, bool), CelcErr> {
        let (mut lt, mut root) = self.cmp_level()?;
        loop {
            // Careful: `&&` only — a single `&` is a parse error, and `&&` must
            // not be consumed as two failed singles.
            self.skip_ws();
            if !self.src[self.pos..].starts_with(b"&&") {
                break;
            }
            self.pos += 2;
            let (rt, _) = self.cmp_level()?;
            if lt != CTy::Bool || rt != CTy::Bool {
                return Err(CelcErr::NotBool);
            }
            self.put1(ir::AND)?;
            lt = CTy::Bool;
            root = false;
        }
        Ok((lt, root))
    }

    fn cmp_level(&mut self) -> Result<(CTy, bool), CelcErr> {
        let (lt, root) = self.add_level()?;
        self.skip_ws();
        let rest = &self.src[self.pos..];
        // Two-char forms first; lone `=` is a parse error (host parity). A
        // `!` here would be `!=` only — bare `!` cannot follow an operand.
        let (tag, len) = if rest.starts_with(b">=") {
            (ir::CMP_GE, 2)
        } else if rest.starts_with(b"<=") {
            (ir::CMP_LE, 2)
        } else if rest.starts_with(b"==") {
            (ir::CMP_EQ, 2)
        } else if rest.starts_with(b"!=") {
            (ir::CMP_NE, 2)
        } else if rest.starts_with(b"=") {
            return Err(self.err_here());
        } else if rest.starts_with(b">") {
            (ir::CMP_GT, 1)
        } else if rest.starts_with(b"<") {
            (ir::CMP_LT, 1)
        } else {
            return Ok((lt, root));
        };
        self.pos += len;
        let (_rt, _) = self.add_level()?;
        // Host `check_ast` compares ANY operand types — no check here either.
        self.put1(tag)?;
        Ok((CTy::Bool, false))
    }

    fn add_level(&mut self) -> Result<(CTy, bool), CelcErr> {
        let (mut lt, mut root) = self.mul_level()?;
        loop {
            self.skip_ws();
            let tag = match self.src.get(self.pos) {
                Some(b'+') => ir::ADD,
                Some(b'-') => ir::SUB,
                _ => break,
            };
            self.pos += 1;
            let (rt, _) = self.mul_level()?;
            if !cty_is_int(&lt) || !cty_is_int(&rt) {
                return Err(CelcErr::NotInteger);
            }
            self.put1(tag)?;
            lt = CTy::Int;
            root = false;
        }
        Ok((lt, root))
    }

    fn mul_level(&mut self) -> Result<(CTy, bool), CelcErr> {
        let (mut lt, mut root) = self.unary()?;
        while self.eat(b"*") {
            let (rt, _) = self.unary()?;
            if !cty_is_int(&lt) || !cty_is_int(&rt) {
                return Err(CelcErr::NotInteger);
            }
            self.put1(ir::MUL)?;
            lt = CTy::Int;
            root = false;
        }
        Ok((lt, root))
    }

    fn unary(&mut self) -> Result<(CTy, bool), CelcErr> {
        if self.depth >= CELC_MAX_DEPTH {
            return Err(CelcErr::Depth);
        }
        self.skip_ws();
        match self.src.get(self.pos) {
            Some(b'!') if self.src.get(self.pos + 1) != Some(&b'=') => {
                self.pos += 1;
                self.depth += 1;
                let r = self.unary();
                self.depth -= 1;
                let (t, _) = r?;
                if t != CTy::Bool {
                    return Err(CelcErr::NotBool);
                }
                self.put1(ir::NOT)?;
                Ok((CTy::Bool, false))
            }
            Some(b'-') => {
                // Negative INTEGER LITERAL only, as in the host subset.
                self.pos += 1;
                let v = self.int_lit(true)?;
                self.put1(ir::INT)?;
                self.put(&v.to_le_bytes())?;
                Ok((CTy::Int, false))
            }
            _ => self.primary(),
        }
    }

    fn primary(&mut self) -> Result<(CTy, bool), CelcErr> {
        let (ty, root) = match self.peek().ok_or(CelcErr::Parse(self.pos as u16))? {
            b'0'..=b'9' => {
                let v = self.int_lit(false)?;
                self.put1(ir::INT)?;
                self.put(&v.to_le_bytes())?;
                Ok((CTy::Int, false))
            }
            b'"' => self.str_lit(),
            b'(' => {
                self.pos += 1;
                let (t, root) = self.expr()?;
                if !self.eat(b")") {
                    return Err(self.err_here());
                }
                // Parens are transparent, including for the construct-root flag.
                Ok((t, root))
            }
            c if c.is_ascii_alphabetic() || c == b'_' => self.dotted_or_construct(),
            _ => Err(self.err_here()),
        }?;
        // Postfix method calls on any completed primary: `"a b".size()`,
        // `(x.name).trim().reverse()`. Dotted paths never leave a trailing
        // `.` behind (their own loop consumes `.ident(` as a method), so
        // this only fires on literal/paren/call receivers.
        self.postfix_chain(ty, root)
    }

    /// Zero or more `.method(args)` postfix calls on an already-emitted value.
    /// A `.ident` NOT followed by `(` is a parse error here — outside a field
    /// path, a bare selection has no meaning in this subset.
    fn postfix_chain(&mut self, mut ty: CTy, mut root: bool) -> Result<(CTy, bool), CelcErr> {
        loop {
            self.skip_ws();
            if self.src.get(self.pos) != Some(&b'.') {
                return Ok((ty, root));
            }
            self.pos += 1;
            let name = self.ident().ok_or(CelcErr::Parse(self.pos as u16))?;
            self.skip_ws();
            if self.src.get(self.pos) != Some(&b'(') {
                return Err(self.err_here());
            }
            self.pos += 1;
            let rty = self.call_and_emit(NS_METHOD, name, Some(ty))?;
            ty = rty;
            root = false;
        }
    }

    /// String literal with the host's escape set: `\n` `\r` `\t`, anything else
    /// escaped maps to itself. Emits `STR [len:u16][bytes]`.
    fn str_lit(&mut self) -> Result<(CTy, bool), CelcErr> {
        self.pos += 1; // opening quote
        self.put1(ir::STR)?;
        let len_at = self.w;
        self.put(&[0, 0])?; // length backfilled below
        loop {
            let c = *self
                .src
                .get(self.pos)
                .ok_or(CelcErr::Parse(self.pos as u16))?;
            self.pos += 1;
            match c {
                b'"' => break,
                b'\\' => {
                    let esc = *self
                        .src
                        .get(self.pos)
                        .ok_or(CelcErr::Parse(self.pos as u16))?;
                    self.pos += 1;
                    self.put1(match esc {
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        other => other,
                    })?;
                }
                other => self.put1(other)?,
            }
        }
        let n = self.w - len_at - 2;
        let nb = (n as u16).to_le_bytes();
        self.out[len_at] = nb[0];
        self.out[len_at + 1] = nb[1];
        Ok((CTy::Str, false))
    }

    fn dotted_or_construct(&mut self) -> Result<(CTy, bool), CelcErr> {
        let mut segs = [Seg { s: 0, e: 0 }; CELC_MAX_SEGS];
        let first = self.ident().ok_or(CelcErr::Parse(self.pos as u16))?;

        // Global function form: `ident(` — today only `size(x)`.
        self.skip_ws();
        if self.src.get(self.pos) == Some(&b'(') {
            self.pos += 1;
            let rty = self.call_and_emit(NS_GLOBAL, first, None)?;
            return Ok((rty, false));
        }

        segs[0] = first;
        let mut nsegs = 1usize;
        while self.eat(b".") {
            let seg = self.ident().ok_or(CelcErr::Parse(self.pos as u16))?;
            // `.ident(` terminates the path: `seg` is a call, not a field.
            self.skip_ws();
            if self.src.get(self.pos) == Some(&b'(') {
                self.pos += 1;
                // Namespace calls hang off a single reserved root segment;
                // reserved before parameter lookup, so `math`/`base64`/`cel`
                // are not usable as parameter names in call position.
                if nsegs == 1 {
                    match self.seg_bytes(segs[0]) {
                        b"math" => {
                            let rty = self.call_and_emit(NS_MATH, seg, None)?;
                            return Ok((rty, false));
                        }
                        b"base64" => {
                            let rty = self.call_and_emit(NS_B64, seg, None)?;
                            return Ok((rty, false));
                        }
                        b"cel" => return self.cel_macro(seg),
                        _ => {}
                    }
                }
                // Method call: emit the receiver path, then the call.
                let (recv_ty, _) = self.emit_name(&segs[..nsegs])?;
                let rty = self.call_and_emit(NS_METHOD, seg, Some(recv_ty))?;
                return Ok((rty, false));
            }
            if nsegs >= CELC_MAX_SEGS {
                return Err(CelcErr::Capacity);
            }
            segs[nsegs] = seg;
            nsegs += 1;
        }

        if self.eat(b"{") {
            return self.construct(&segs[..nsegs]);
        }

        self.emit_name(&segs[..nsegs])
    }

    /// Emit a bare identifier (keyword literal, `cel.bind` local, parameter,
    /// enum constant) or a field path — the non-call tail of
    /// [`dotted_or_construct`], factored so method receivers reuse it.
    fn emit_name(&mut self, segs: &[Seg]) -> Result<(CTy, bool), CelcErr> {
        let nsegs = segs.len();

        // Keyword literals.
        if nsegs == 1 {
            let is_true = self.seg_bytes(segs[0]) == b"true";
            if is_true || self.seg_bytes(segs[0]) == b"false" {
                self.put1(ir::BOOL)?;
                self.put1(u8::from(is_true))?;
                return Ok((CTy::Bool, false));
            }
        }

        // Bare identifier: `cel.bind` local (innermost first — shadowing),
        // then parameter, then enum constant.
        if nsegs == 1 {
            let name = segs[0];
            let nb = self.seg_bytes(name);
            let mut i = self.nlocals;
            while i > 0 {
                i -= 1;
                if let Some(b) = self.locals[i] {
                    if self.seg_bytes(b.name) == nb {
                        self.put1(ir::LOAD_LOCAL)?;
                        self.put1(b.slot)?;
                        return Ok((b.ty, false));
                    }
                }
            }
            if let Some((idx, ty)) = self.lookup_param(name) {
                self.put1(ir::LOADPARAM)?;
                self.put1(idx)?;
                return Ok((ty?, false));
            }
            if let Some(v) = self.lookup_enum(name) {
                self.put1(ir::INT)?;
                self.put(&v.to_le_bytes())?;
                return Ok((CTy::Int, false));
            }
            return Err(CelcErr::UnknownName(name.s, name.e));
        }

        // Field path: root must be a message-typed parameter.
        let (idx, ty) = self
            .lookup_param(segs[0])
            .ok_or(CelcErr::UnknownParam(segs[0].s, segs[0].e))?;
        let mut current = ty?;
        let mut numbers = [0u32; CELC_MAX_SEGS];
        // `path_e` tracks the end of the path SO FAR (host `at` rendering).
        let mut path_e = segs[0].e;
        for (i, seg) in segs[1..nsegs].iter().enumerate() {
            let CTy::Msg(in_params, ms, me) = current else {
                return Err(CelcErr::NotAMessage {
                    path_s: segs[0].s,
                    path_e,
                    scalar: cty_scalar_code(&current),
                });
            };
            let mbuf = if in_params { BUF_PARAMS } else { BUF_SCHEMA };
            let mname_buf = if in_params { self.params } else { self.schema };
            let body = find_message(self.schema, &mname_buf[ms as usize..me as usize]).ok_or(
                CelcErr::UnknownMessageType {
                    buf: mbuf,
                    s: ms,
                    e: me,
                },
            )?;
            let (number, fty) = find_field(self.schema, body, self.seg_bytes(*seg))
                .map_err(CelcErr::BadSchema)?
                .ok_or(CelcErr::UnknownField {
                    buf: mbuf,
                    msg_s: ms,
                    msg_e: me,
                    field_s: seg.s,
                    field_e: seg.e,
                })?;
            numbers[i] = number;
            current = fty;
            path_e = seg.e;
        }
        self.put1(ir::PATH)?;
        self.put1(idx)?;
        self.put1((nsegs - 1) as u8)?;
        for n in &numbers[..nsegs - 1] {
            self.put(&n.to_le_bytes())?;
        }
        Ok((current, false))
    }

    // ---- builtin calls (the pinned CEL extension surface) ----------------

    /// Parse `args…)` (the `(` is already consumed), resolve the overload
    /// against the pinned table, emit `CALL`, and return the result type.
    /// Argument expressions are emitted in order — post-order stack layout,
    /// receiver (if any) already emitted deepest.
    fn call_and_emit(&mut self, ns: u8, name: Seg, recv: Option<CTy>) -> Result<CTy, CelcErr> {
        let mut args = [CTy::Int; 2];
        let mut nargs = 0usize;
        self.skip_ws();
        if self.src.get(self.pos) == Some(&b')') {
            self.pos += 1;
        } else {
            loop {
                let (aty, aroot) = self.expr()?;
                if aroot {
                    return Err(CelcErr::NestedConstruction);
                }
                if nargs >= 2 {
                    return Err(CelcErr::BadCallArgs(name.s, name.e));
                }
                args[nargs] = aty;
                nargs += 1;
                if self.eat(b",") {
                    continue;
                }
                if !self.eat(b")") {
                    return Err(self.err_here());
                }
                break;
            }
        }
        let (id, rty) = resolve_builtin(ns, self.seg_bytes(name), recv.as_ref(), &args[..nargs])
            .map_err(|bad_args| {
                if bad_args {
                    CelcErr::BadCallArgs(name.s, name.e)
                } else {
                    CelcErr::UnknownFunction(name.s, name.e)
                }
            })?;
        self.put1(ir::CALL)?;
        self.put(&id.to_le_bytes())?;
        Ok(rty)
    }

    /// `cel.<name>(…)` macros — today only `cel.bind(x, init, result)`,
    /// which stores `init` in a VM local and makes `x` name it inside
    /// `result`. Purely compiler machinery plus two opcodes; the binding is
    /// lexically scoped and shadowing is innermost-wins.
    fn cel_macro(&mut self, name: Seg) -> Result<(CTy, bool), CelcErr> {
        if self.seg_bytes(name) != b"bind" {
            return Err(CelcErr::UnknownFunction(name.s, name.e));
        }
        let var = self.ident().ok_or(CelcErr::Parse(self.pos as u16))?;
        if !self.eat(b",") {
            return Err(self.err_here());
        }
        let (ity, iroot) = self.expr()?;
        if iroot {
            return Err(CelcErr::NestedConstruction);
        }
        if self.nlocals >= CELC_MAX_LOCALS {
            return Err(CelcErr::LocalDepth);
        }
        let slot = self.nlocals as u8;
        self.put1(ir::STORE_LOCAL)?;
        self.put1(slot)?;
        self.locals[self.nlocals] = Some(Binding {
            name: var,
            slot,
            ty: ity,
        });
        self.nlocals += 1;
        if !self.eat(b",") {
            return Err(self.err_here());
        }
        let r = self.expr();
        // Unwind the binding on every path so an error inside `result`
        // cannot leak the local into a sibling expression.
        self.nlocals -= 1;
        self.locals[self.nlocals] = None;
        let (rty, rroot) = r?;
        if rroot {
            return Err(CelcErr::NestedConstruction);
        }
        if !self.eat(b")") {
            return Err(self.err_here());
        }
        Ok((rty, false))
    }

    fn construct(&mut self, type_segs: &[Seg]) -> Result<(CTy, bool), CelcErr> {
        // The message must exist before any field is examined (host order).
        let src_span = (type_segs[0].s, type_segs[type_segs.len() - 1].e);
        let not_found = CelcErr::UnknownMessageType {
            buf: BUF_SRC,
            s: src_span.0,
            e: src_span.1,
        };
        let name_span = self.find_message_by_segs(type_segs).ok_or(not_found)?;
        let body = find_message(
            self.schema,
            &self.schema[name_span.0 as usize..name_span.1 as usize],
        )
        .ok_or(not_found)?;

        if !self.eat(b"}") {
            loop {
                let fname = self.ident().ok_or(CelcErr::Parse(self.pos as u16))?;
                if !self.eat(b":") {
                    return Err(self.err_here());
                }
                let (_vty, was_construct) = self.expr()?;
                if was_construct {
                    return Err(CelcErr::NestedConstruction);
                }
                let (number, _fty) = find_field(self.schema, body, self.seg_bytes(fname))
                    .map_err(CelcErr::BadSchema)?
                    .ok_or(CelcErr::UnknownField {
                        buf: BUF_SCHEMA,
                        msg_s: name_span.0,
                        msg_e: name_span.1,
                        field_s: fname.s,
                        field_e: fname.e,
                    })?;
                // Field value types are NOT checked against the declaration —
                // host parity (`check_construct` ignores the value's type).
                self.put1(ir::SETFIELD)?;
                self.put(&number.to_le_bytes())?;
                if self.eat(b",") {
                    continue;
                }
                if !self.eat(b"}") {
                    return Err(self.err_here());
                }
                break;
            }
        }
        self.put1(ir::FINISHMSG)?;
        Ok((CTy::Msg(false, name_span.0, name_span.1), true))
    }

    // ---- environment lookups (scans over the raw text, zero-copy) --------

    fn seg_bytes(&self, seg: Seg) -> &[u8] {
        &self.src[seg.s as usize..seg.e as usize]
    }

    /// Does the dotted source name (segments) equal `candidate` (schema bytes,
    /// dots included)? Compared segment-wise so whitespace around source dots
    /// cannot matter.
    fn segs_eq_name(&self, segs: &[Seg], candidate: &[u8]) -> bool {
        let mut c = 0usize;
        for (i, seg) in segs.iter().enumerate() {
            if i > 0 {
                if candidate.get(c) != Some(&b'.') {
                    return false;
                }
                c += 1;
            }
            let sb = self.seg_bytes(*seg);
            if candidate.len() < c + sb.len() || &candidate[c..c + sb.len()] != sb {
                return false;
            }
            c += sb.len();
        }
        c == candidate.len()
    }

    fn find_message_by_segs(&self, segs: &[Seg]) -> Option<(u16, u16)> {
        let mut iter = SchemaDefs::new(self.schema);
        while let Some(def) = iter.next_def() {
            if let SchemaDef::Message { name_s, name_e, .. } = def {
                if self.segs_eq_name(segs, &self.schema[name_s..name_e]) {
                    return Some((name_s as u16, name_e as u16));
                }
            }
        }
        None
    }

    /// Parameter lookup: `name:Type,…` — position is the runtime index. The
    /// inner Result defers a malformed-params error until the param is used.
    #[allow(
        clippy::type_complexity,
        reason = "the inner Result defers a malformed-params error to the use site"
    )]
    fn lookup_param(&self, name: Seg) -> Option<(u8, Result<CTy, CelcErr>)> {
        let nb = self.seg_bytes(name);
        let mut pos = 0usize;
        let mut idx = 0u8;
        loop {
            skip_ws_at(self.params, &mut pos);
            if pos >= self.params.len() {
                return None;
            }
            let ps = pos;
            let pe = scan_ident(self.params, &mut pos)?;
            skip_ws_at(self.params, &mut pos);
            if self.params.get(pos) != Some(&b':') {
                return None;
            }
            pos += 1;
            skip_ws_at(self.params, &mut pos);
            let ts = pos;
            let te = scan_dotted(self.params, &mut pos)?;
            let matched = &self.params[ps..pe] == nb;
            if matched {
                let ty = self.resolve_type_name(true, ts as u16, te as u16);
                return Some((idx, Ok(ty)));
            }
            skip_ws_at(self.params, &mut pos);
            if self.params.get(pos) == Some(&b',') {
                pos += 1;
                idx = idx.saturating_add(1);
                continue;
            }
            return None;
        }
    }

    /// Map a type NAME span to a `CTy`. Scalars by keyword; anything else is a
    /// message — canonicalized to its schema def-site span when defined there.
    fn resolve_type_name(&self, in_params: bool, s: u16, e: u16) -> CTy {
        let buf = if in_params { self.params } else { self.schema };
        match &buf[s as usize..e as usize] {
            b"int" => CTy::Int,
            b"uint" => CTy::Uint,
            b"double" => CTy::Double,
            b"bool" => CTy::Bool,
            b"str" => CTy::Str,
            b"bytes" => CTy::Bytes,
            name => {
                let mut iter = SchemaDefs::new(self.schema);
                while let Some(def) = iter.next_def() {
                    if let SchemaDef::Message { name_s, name_e, .. } = def {
                        if &self.schema[name_s..name_e] == name {
                            return CTy::Msg(false, name_s as u16, name_e as u16);
                        }
                    }
                }
                CTy::Msg(in_params, s, e)
            }
        }
    }

    fn lookup_enum(&self, name: Seg) -> Option<i64> {
        let nb = self.seg_bytes(name);
        let mut iter = SchemaDefs::new(self.schema);
        while let Some(def) = iter.next_def() {
            if let SchemaDef::Enum {
                name_s,
                name_e,
                value,
            } = def
            {
                if &self.schema[name_s..name_e] == nb {
                    return Some(value);
                }
            }
        }
        None
    }
}

// ---- builtin signature table ----------------------------------------------
//
// The compiler face of the pinned surface: name + call form + static types →
// (builtin id, result type). Overloads resolve HERE, statically — `reverse`
// on `str` and on `bytes` are different ids, so the runtime never sniffs
// content. The compiler accepts the FULL pinned surface unconditionally; the
// engine variants gate what a target's runtime carries, and a program calling
// a builtin its engine lacks fails closed at runtime (`BadBuiltin`), exactly
// like any other artifact/instance version mismatch.

/// Call forms for [`resolve_builtin`].
const NS_METHOD: u8 = 0; // recv.name(args)
const NS_GLOBAL: u8 = 1; // name(args)         — `size`
const NS_MATH: u8 = 2; // math.name(args)
const NS_B64: u8 = 3; // base64.name(args)

fn cty_stringish(t: &CTy) -> bool {
    matches!(t, CTy::Str | CTy::Bytes)
}

/// Resolve one call. `Err(false)` = unknown name (in this form); `Err(true)` =
/// known name, wrong arity or argument types.
#[allow(
    clippy::result_unit_err,
    reason = "the bool distinguishes unknown-name from bad-args; the caller maps both to spanned errors"
)]
fn resolve_builtin(
    ns: u8,
    name: &[u8],
    recv: Option<&CTy>,
    args: &[CTy],
) -> Result<(u16, CTy), bool> {
    use builtin as b;
    // (want_stringish per arg is the common case; ints are the exception.)
    let all_str = |n: usize| -> Result<(), bool> {
        if args.len() != n {
            return Err(true);
        }
        if args.iter().all(cty_stringish) {
            Ok(())
        } else {
            Err(true)
        }
    };
    let all_int = |n: usize| -> Result<(), bool> {
        if args.len() != n {
            return Err(true);
        }
        if args.iter().all(cty_is_int) {
            Ok(())
        } else {
            Err(true)
        }
    };

    match ns {
        NS_METHOD => {
            let r = recv.ok_or(false)?;
            if !cty_stringish(r) {
                // Every method today is string-ish; a known name on a wrong
                // receiver is bad-args, an unknown name is unknown.
                return match name {
                    b"size" | b"contains" | b"startsWith" | b"endsWith" | b"indexOf"
                    | b"lastIndexOf" | b"charAt" | b"substring" | b"trim" | b"reverse"
                    | b"lowerAscii" | b"upperAscii" | b"replace" => Err(true),
                    _ => Err(false),
                };
            }
            match name {
                b"size" => {
                    all_str(0)?;
                    Ok((b::SIZE, CTy::Int))
                }
                b"contains" => {
                    all_str(1)?;
                    Ok((b::CONTAINS, CTy::Bool))
                }
                b"startsWith" => {
                    all_str(1)?;
                    Ok((b::STARTS_WITH, CTy::Bool))
                }
                b"endsWith" => {
                    all_str(1)?;
                    Ok((b::ENDS_WITH, CTy::Bool))
                }
                b"indexOf" => {
                    all_str(1)?;
                    Ok((b::INDEX_OF, CTy::Int))
                }
                b"lastIndexOf" => {
                    all_str(1)?;
                    Ok((b::LAST_INDEX_OF, CTy::Int))
                }
                b"charAt" => {
                    all_int(1)?;
                    Ok((b::CHAR_AT, CTy::Str))
                }
                b"substring" => match args.len() {
                    1 => {
                        all_int(1)?;
                        Ok((b::SUBSTRING, *r))
                    }
                    2 => {
                        all_int(2)?;
                        Ok((b::SUBSTRING_RANGE, *r))
                    }
                    _ => Err(true),
                },
                b"trim" => {
                    all_str(0)?;
                    Ok((b::TRIM, *r))
                }
                b"reverse" => {
                    all_str(0)?;
                    // The static-type overload: code-point reverse for str,
                    // byte reverse for bytes.
                    Ok(match r {
                        CTy::Bytes => (b::REVERSE_BYTES, CTy::Bytes),
                        _ => (b::REVERSE_STR, CTy::Str),
                    })
                }
                b"lowerAscii" => {
                    all_str(0)?;
                    Ok((b::LOWER_ASCII, *r))
                }
                b"upperAscii" => {
                    all_str(0)?;
                    Ok((b::UPPER_ASCII, *r))
                }
                b"replace" => {
                    all_str(2)?;
                    Ok((b::REPLACE, *r))
                }
                _ => Err(false),
            }
        }
        NS_GLOBAL => match name {
            b"size" => {
                all_str(1)?;
                Ok((b::SIZE, CTy::Int))
            }
            _ => Err(false),
        },
        NS_MATH => match name {
            b"greatest" => {
                all_int(2)?;
                Ok((b::MATH_GREATEST, CTy::Int))
            }
            b"least" => {
                all_int(2)?;
                Ok((b::MATH_LEAST, CTy::Int))
            }
            b"abs" => {
                all_int(1)?;
                Ok((b::MATH_ABS, CTy::Int))
            }
            b"sign" => {
                all_int(1)?;
                Ok((b::MATH_SIGN, CTy::Int))
            }
            b"bitAnd" => {
                all_int(2)?;
                Ok((b::BIT_AND, CTy::Int))
            }
            b"bitOr" => {
                all_int(2)?;
                Ok((b::BIT_OR, CTy::Int))
            }
            b"bitXor" => {
                all_int(2)?;
                Ok((b::BIT_XOR, CTy::Int))
            }
            b"bitShiftLeft" => {
                all_int(2)?;
                Ok((b::BIT_SHL, CTy::Int))
            }
            b"bitShiftRight" => {
                all_int(2)?;
                Ok((b::BIT_SHR, CTy::Int))
            }
            _ => Err(false),
        },
        _ => match name {
            // NS_B64
            b"encode" => {
                all_str(1)?;
                Ok((b::B64_ENCODE, CTy::Str))
            }
            b"decode" => {
                all_str(1)?;
                Ok((b::B64_DECODE, CTy::Bytes))
            }
            _ => Err(false),
        },
    }
}

// ---- schema text scanning ------------------------------------------------

fn skip_ws_at(buf: &[u8], pos: &mut usize) {
    while buf.get(*pos).is_some_and(u8::is_ascii_whitespace) {
        *pos += 1;
    }
}

/// Scan one identifier `[A-Za-z_][A-Za-z0-9_]*`; returns its end.
fn scan_ident(buf: &[u8], pos: &mut usize) -> Option<usize> {
    let first = *buf.get(*pos)?;
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return None;
    }
    while buf
        .get(*pos)
        .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_')
    {
        *pos += 1;
    }
    Some(*pos)
}

/// Scan a dotted name `ident(.ident)*`; returns its end.
fn scan_dotted(buf: &[u8], pos: &mut usize) -> Option<usize> {
    let mut end = scan_ident(buf, pos)?;
    while buf.get(*pos) == Some(&b'.') {
        *pos += 1;
        end = scan_ident(buf, pos)?;
    }
    Some(end)
}

enum SchemaDef {
    /// `Name{…}` — body is the byte range INSIDE the braces.
    Message {
        name_s: usize,
        name_e: usize,
        body_s: usize,
        body_e: usize,
    },
    /// `NAME=INT`.
    Enum {
        name_s: usize,
        name_e: usize,
        value: i64,
    },
}

/// Iterator over `;`-separated schema defs. Malformed text simply ends the
/// iteration — a def that never parses can never be found, and every "not
/// found" is a structured error at the use site.
struct SchemaDefs<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> SchemaDefs<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn next_def(&mut self) -> Option<SchemaDef> {
        loop {
            skip_ws_at(self.buf, &mut self.pos);
            if self.buf.get(self.pos) == Some(&b';') {
                self.pos += 1;
                continue;
            }
            break;
        }
        if self.pos >= self.buf.len() {
            return None;
        }
        let name_s = self.pos;
        let name_e = scan_dotted(self.buf, &mut self.pos)?;
        skip_ws_at(self.buf, &mut self.pos);
        match self.buf.get(self.pos) {
            Some(&b'{') => {
                self.pos += 1;
                let body_s = self.pos;
                while self.buf.get(self.pos).is_some_and(|c| *c != b'}') {
                    self.pos += 1;
                }
                let body_e = self.pos;
                if self.buf.get(self.pos) != Some(&b'}') {
                    return None; // unterminated — stop iterating
                }
                self.pos += 1;
                Some(SchemaDef::Message {
                    name_s,
                    name_e,
                    body_s,
                    body_e,
                })
            }
            Some(&b'=') => {
                self.pos += 1;
                skip_ws_at(self.buf, &mut self.pos);
                let neg = self.buf.get(self.pos) == Some(&b'-');
                if neg {
                    self.pos += 1;
                }
                let mut v: i64 = 0;
                let mut any = false;
                while let Some(c) = self.buf.get(self.pos).filter(|c| c.is_ascii_digit()) {
                    v = v.checked_mul(10)?.checked_add((c - b'0') as i64)?;
                    self.pos += 1;
                    any = true;
                }
                if !any {
                    return None;
                }
                Some(SchemaDef::Enum {
                    name_s,
                    name_e,
                    value: if neg { -v } else { v },
                })
            }
            _ => None,
        }
    }
}

/// Find a message def by exact name; returns the body span inside the braces.
fn find_message(schema: &[u8], name: &[u8]) -> Option<(usize, usize)> {
    let mut iter = SchemaDefs::new(schema);
    while let Some(def) = iter.next_def() {
        if let SchemaDef::Message {
            name_s,
            name_e,
            body_s,
            body_e,
        } = def
        {
            if &schema[name_s..name_e] == name {
                return Some((body_s, body_e));
            }
        }
    }
    None
}

/// Find `field:ty@N` in a message body. `Ok(None)` = no such field;
/// `Err(offset)` = the body text itself is malformed at `offset`.
fn find_field(schema: &[u8], body: (usize, usize), name: &[u8]) -> Result<Option<(u32, CTy)>, u16> {
    let mut pos = body.0;
    loop {
        skip_ws_at(schema, &mut pos);
        if pos >= body.1 {
            return Ok(None);
        }
        let fs = pos;
        let fe = scan_ident(schema, &mut pos).ok_or(pos as u16)?;
        skip_ws_at(schema, &mut pos);
        if schema.get(pos) != Some(&b':') {
            return Err(pos as u16);
        }
        pos += 1;
        skip_ws_at(schema, &mut pos);
        let ts = pos;
        let te = scan_dotted(schema, &mut pos).ok_or(pos as u16)?;
        skip_ws_at(schema, &mut pos);
        if schema.get(pos) != Some(&b'@') {
            return Err(pos as u16);
        }
        pos += 1;
        skip_ws_at(schema, &mut pos);
        let mut number: u32 = 0;
        let mut any = false;
        while let Some(c) = schema.get(pos).filter(|c| c.is_ascii_digit()) {
            number = number
                .checked_mul(10)
                .and_then(|n| n.checked_add((c - b'0') as u32))
                .ok_or(pos as u16)?;
            pos += 1;
            any = true;
        }
        if !any {
            return Err(pos as u16);
        }
        if &schema[fs..fe] == name {
            let ty = resolve_schema_type(schema, ts, te);
            return Ok(Some((number, ty)));
        }
        skip_ws_at(schema, &mut pos);
        if schema.get(pos) == Some(&b',') {
            pos += 1;
            continue;
        }
        if pos >= body.1 {
            return Ok(None);
        }
        return Err(pos as u16);
    }
}

/// Field-type resolution: scalar keywords, else a message named by this span.
fn resolve_schema_type(schema: &[u8], s: usize, e: usize) -> CTy {
    match &schema[s..e] {
        b"int" => CTy::Int,
        b"uint" => CTy::Uint,
        b"double" => CTy::Double,
        b"bool" => CTy::Bool,
        b"str" => CTy::Str,
        b"bytes" => CTy::Bytes,
        _ => CTy::Msg(false, s as u16, e as u16),
    }
}
