// The `.uproc` authoring DSL document parser, on device.
//
// The last thing that required a Linux host to author. A device could already
// compile CEL (`celc_core`), seal artefacts (`artefact_core`), store, publish,
// verify and activate them — but it could not read a MODULE DOCUMENT, so
// authoring on device meant assembling artefacts one CLI call at a time. This
// closes that: a `.uproc` file goes in, a structured document comes out.
//
// Like the host parser this is deliberately THIN. It recognizes document
// structure and captures every expression body as a verbatim span; the spans go
// to `celc_core` unchanged, so the DSL reuses the proven CEL grammar,
// type-checker and lowerer rather than growing a second one.
//
// SPANS, NOT STRINGS. With no allocator there is nothing to own a `String`, and
// nothing needs one: every name, type and body is a byte range in the caller's
// source buffer. That is not a compromise — it is strictly better, because a
// span also carries the position an error should point at.
//
// The caller supplies the declaration arrays (`UprocArena`), so this core has no
// ceiling of its own and a node parses whatever its memory allows. Overflowing
// an array is reported as `TooMany` rather than truncated: a document parsed in
// part is a document silently missing artefacts.
//
// Grammar (identical to the host's — see chronicle-authoring/src/parse.rs):
//   document      := 'module' QNAME '{' decl* '}'
//   message       := 'message' QNAME '{' field* '}'
//   field         := IDENT ':' type '=' INT ';'
//   enumconst     := 'enum' IDENT '=' INT ';'
//   expression    := 'expression' IDENT '(' IDENT ':' type ')' '->' type BODY
//   transformation:= 'transformation' IDENT '(' IDENT ':' type ')' '->' type BODY
//   decision      := 'decision' IDENT '(' IDENT ':' type ')' '->' type '{' rule* '}'
//   rule          := 'when' EXPR '->' EXPR ';' | 'default' '->' EXPR ';'
//   resource      := 'resource' QNAME ['required'] ';'
//   pipeline      := 'pipeline' IDENT '(' IDENT ':' type ')' '->' type '{' stage* '}'
//   stage         := 'call' IDENT '=' QNAME '(' args ')' ';'
//                  | 'effect' IDENT '=' '@' IDENT ['.' IDENT] '(' IDENT ')' ';'
//                  | 'commit' 'after' IDENT ';' | 'return' IDENT ';'
//   aggregation   := 'aggregation' IDENT '(' IDENT ':' type ')' '->' type '{' aclause* '}'
//   aclause       := 'key' EXPR ';' | 'event_time' EXPR ';'
//                  | 'window' ('tumbling' INT | 'sliding' INT INT) ';'
//                  | 'lateness' INT ';' | 'guard' INT ';' | 'lanes' INT ';'
//                  | 'operator' IDENT '=' opkind ';' | 'emit' EXPR ';'
//   opkind        := 'count' | ('sum'|'avg'|'min'|'max'|'distinct') '(' EXPR ')'
//                  | ('topk'|'quantile') '(' INT ',' EXPR ')'
//   entry         := 'entry' IDENT '=' IDENT ';'
//   provenance    := 'provenance' 'revision' STRING 'toolchain' STRING ';'
//
// Scanning is byte-level, which is safe for UTF-8: braces, quotes, backslash and
// `->` are all ASCII, and continuation bytes (>= 0x80) never collide with them,
// so a string body containing `{` or `}` is skipped correctly.

/// A byte range in the source document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Span {
    pub start: u32,
    pub len: u32,
}

impl Span {
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    /// The bytes this span names, given the document it came from.
    pub fn of<'a>(&self, src: &'a [u8]) -> &'a [u8] {
        let s = self.start as usize;
        let e = s + self.len as usize;
        if e <= src.len() {
            &src[s..e]
        } else {
            &[]
        }
    }
}

/// Why parsing failed, and where. The offset maps back to a line/column for
/// display by whatever has the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UprocError {
    pub kind: UprocErrorKind,
    pub offset: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UprocErrorKind {
    ExpectedModule,
    ExpectedIdent,
    ExpectedByte(u8),
    ExpectedType,
    ExpectedInt,
    ExpectedString,
    ExpectedArrow,
    UnknownDeclaration,
    UnknownOperator,
    UnterminatedBody,
    /// Content after the module's closing brace.
    TrailingInput,
    MissingClause,
    /// A declaration array in the arena is full.
    TooMany,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FieldDecl {
    pub name: Span,
    pub ty: Span,
    pub number: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MessageDecl {
    pub name: Span,
    pub first_field: u16,
    pub n_fields: u16,
}

/// Shared by `expression` and `transformation` — identical shape, different
/// keyword and different downstream artefact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FnDecl {
    pub name: Span,
    pub param_name: Span,
    pub param_type: Span,
    pub result_type: Span,
    pub body: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RuleDecl {
    pub when: Span,
    pub outcome: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DecisionDecl {
    pub name: Span,
    pub param_name: Span,
    pub input_type: Span,
    pub output_type: Span,
    pub first_rule: u16,
    pub n_rules: u16,
    pub default: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResourceDecl {
    pub name: Span,
    pub required: bool,
}

/// Stage kinds. `Call` uses `artefact` + the arg range; `Effect` uses
/// `resource`, `operation` and `arg0`.
pub const STAGE_CALL: u8 = 0;
pub const STAGE_EFFECT: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StageDecl {
    pub kind: u8,
    pub name: Span,
    /// Call: the artefact QNAME. Effect: the resource identifier.
    pub target: Span,
    /// Effect only: the operation after `.` (empty when absent).
    pub operation: Span,
    /// Effect only: the single argument.
    pub arg0: Span,
    /// Call only: range into the arena's `args`.
    pub first_arg: u16,
    pub n_args: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PipelineDecl {
    pub name: Span,
    pub port_name: Span,
    pub input_type: Span,
    pub output_type: Span,
    pub first_stage: u16,
    pub n_stages: u16,
    /// Empty when there is no `commit after`.
    pub commit_after: Span,
    pub return_stage: Span,
}

/// Operator kinds, matching the host's `AggOpKind` declaration order.
pub const AGG_SUM: u8 = 0;
pub const AGG_COUNT: u8 = 1;
pub const AGG_AVG: u8 = 2;
pub const AGG_MIN: u8 = 3;
pub const AGG_MAX: u8 = 4;
pub const AGG_DISTINCT: u8 = 5;
pub const AGG_TOPK: u8 = 6;
pub const AGG_QUANTILE: u8 = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OperatorDecl {
    pub name: Span,
    pub kind: u8,
    /// `k` for TopK, permille for Quantile; 0 otherwise.
    pub param: u32,
    /// Value selector (empty for Count).
    pub selector: Span,
}

pub const WINDOW_TUMBLING: u8 = 0;
pub const WINDOW_SLIDING: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AggregationDecl {
    pub name: Span,
    pub param_name: Span,
    pub input_type: Span,
    pub output_type: Span,
    pub key: Span,
    pub event_time: Span,
    pub window_kind: u8,
    pub window_size_ms: i64,
    pub window_step_ms: i64,
    pub lateness_ms: i64,
    pub guard_ms: i64,
    pub max_lanes: u32,
    pub first_op: u16,
    pub n_ops: u16,
    pub emit: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EnumDecl {
    pub name: Span,
    pub value: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EntryDecl {
    pub name: Span,
    pub pipeline: Span,
}

/// Caller-provided storage for the parsed declarations.
pub struct UprocArena<'a> {
    pub messages: &'a mut [MessageDecl],
    pub fields: &'a mut [FieldDecl],
    pub enums: &'a mut [EnumDecl],
    pub expressions: &'a mut [FnDecl],
    pub transformations: &'a mut [FnDecl],
    pub decisions: &'a mut [DecisionDecl],
    pub rules: &'a mut [RuleDecl],
    pub resources: &'a mut [ResourceDecl],
    pub pipelines: &'a mut [PipelineDecl],
    pub stages: &'a mut [StageDecl],
    pub args: &'a mut [Span],
    pub aggregations: &'a mut [AggregationDecl],
    pub operators: &'a mut [OperatorDecl],
    pub entries: &'a mut [EntryDecl],
}

/// How much of each arena array the parse filled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Doc {
    pub module: Span,
    pub n_messages: usize,
    pub n_fields: usize,
    pub n_enums: usize,
    pub n_expressions: usize,
    pub n_transformations: usize,
    pub n_decisions: usize,
    pub n_rules: usize,
    pub n_resources: usize,
    pub n_pipelines: usize,
    pub n_stages: usize,
    pub n_args: usize,
    pub n_aggregations: usize,
    pub n_operators: usize,
    pub n_entries: usize,
    /// `provenance revision "..." toolchain "..."`; empty when absent.
    pub provenance_revision: Span,
    pub provenance_toolchain: Span,
}

// ------------------------------------------------------------------ scanner

struct P<'a> {
    s: &'a [u8],
    i: usize,
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}
fn is_ident_continue(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

impl<'a> P<'a> {
    fn new(s: &'a [u8]) -> Self {
        P { s, i: 0 }
    }

    fn err<T>(&self, kind: UprocErrorKind) -> Result<T, UprocError> {
        Err(UprocError {
            kind,
            offset: self.i,
        })
    }

    /// Skip whitespace and `//` line comments.
    fn ws(&mut self) {
        loop {
            while self.i < self.s.len() && self.s[self.i].is_ascii_whitespace() {
                self.i += 1;
            }
            if self.i + 1 < self.s.len() && self.s[self.i] == b'/' && self.s[self.i + 1] == b'/' {
                while self.i < self.s.len() && self.s[self.i] != b'\n' {
                    self.i += 1;
                }
                continue;
            }
            break;
        }
    }

    fn peek(&mut self) -> Option<u8> {
        self.ws();
        self.s.get(self.i).copied()
    }

    /// Match `kw` on a word boundary, consuming it on success.
    fn keyword(&mut self, kw: &[u8]) -> bool {
        self.ws();
        if self.i < self.s.len() && self.s[self.i..].starts_with(kw) {
            let after = self.i + kw.len();
            if after >= self.s.len() || !is_ident_continue(self.s[after]) {
                self.i = after;
                return true;
            }
        }
        false
    }

    fn expect_byte(&mut self, b: u8) -> Result<(), UprocError> {
        self.ws();
        if self.s.get(self.i) == Some(&b) {
            self.i += 1;
            Ok(())
        } else {
            self.err(UprocErrorKind::ExpectedByte(b))
        }
    }

    fn eat_byte(&mut self, b: u8) -> bool {
        self.ws();
        if self.s.get(self.i) == Some(&b) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn arrow(&mut self) -> Result<(), UprocError> {
        self.ws();
        if self.i + 1 < self.s.len() && self.s[self.i] == b'-' && self.s[self.i + 1] == b'>' {
            self.i += 2;
            Ok(())
        } else {
            self.err(UprocErrorKind::ExpectedArrow)
        }
    }

    fn ident(&mut self) -> Result<Span, UprocError> {
        self.ws();
        let start = self.i;
        if self.i >= self.s.len() || !is_ident_start(self.s[self.i]) {
            return self.err(UprocErrorKind::ExpectedIdent);
        }
        self.i += 1;
        while self.i < self.s.len() && is_ident_continue(self.s[self.i]) {
            self.i += 1;
        }
        Ok(Span {
            start: start as u32,
            len: (self.i - start) as u32,
        })
    }

    /// A dotted qualified name: `a.b.c`.
    fn qname(&mut self) -> Result<Span, UprocError> {
        self.ws();
        let start = self.i;
        self.ident()?;
        loop {
            let save = self.i;
            if self.i < self.s.len() && self.s[self.i] == b'.' {
                self.i += 1;
                if self.ident().is_err() {
                    self.i = save;
                    break;
                }
            } else {
                break;
            }
        }
        Ok(Span {
            start: start as u32,
            len: (self.i - start) as u32,
        })
    }

    /// A type: a builtin keyword or a qualified message name. Either way the
    /// span is the verbatim text, which is what `celc_core` consumes.
    fn ty(&mut self) -> Result<Span, UprocError> {
        self.ws();
        if self.i >= self.s.len() || !is_ident_start(self.s[self.i]) {
            return self.err(UprocErrorKind::ExpectedType);
        }
        self.qname()
    }

    fn int(&mut self) -> Result<i64, UprocError> {
        self.ws();
        let neg = if self.i < self.s.len() && self.s[self.i] == b'-' {
            self.i += 1;
            true
        } else {
            false
        };
        let start = self.i;
        while self.i < self.s.len() && self.s[self.i].is_ascii_digit() {
            self.i += 1;
        }
        if self.i == start {
            return self.err(UprocErrorKind::ExpectedInt);
        }
        let mut v: i64 = 0;
        for k in start..self.i {
            v = match v
                .checked_mul(10)
                .and_then(|x| x.checked_add((self.s[k] - b'0') as i64))
            {
                Some(x) => x,
                None => return self.err(UprocErrorKind::ExpectedInt),
            };
        }
        Ok(if neg { -v } else { v })
    }

    /// A double-quoted string; the span excludes the quotes.
    fn string(&mut self) -> Result<Span, UprocError> {
        self.ws();
        if self.s.get(self.i) != Some(&b'"') {
            return self.err(UprocErrorKind::ExpectedString);
        }
        self.i += 1;
        let start = self.i;
        while self.i < self.s.len() && self.s[self.i] != b'"' {
            if self.s[self.i] == b'\\' {
                self.i += 1;
            }
            self.i += 1;
        }
        if self.i >= self.s.len() {
            return self.err(UprocErrorKind::ExpectedString);
        }
        let span = Span {
            start: start as u32,
            len: (self.i - start) as u32,
        };
        self.i += 1;
        Ok(span)
    }

    /// A `{ … }` body captured verbatim between the braces, with surrounding
    /// whitespace TRIMMED. Tracks nesting and skips string literals so a `}`
    /// inside a string does not close it.
    fn body(&mut self) -> Result<Span, UprocError> {
        self.expect_byte(b'{')?;
        let start = self.i;
        let mut depth = 1usize;
        while self.i < self.s.len() {
            match self.s[self.i] {
                b'"' => {
                    self.i += 1;
                    while self.i < self.s.len() && self.s[self.i] != b'"' {
                        if self.s[self.i] == b'\\' {
                            self.i += 1;
                        }
                        self.i += 1;
                    }
                }
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        // TRIM both ends. The body is sealed into the artefact as
                        // its `source`, so leading/trailing layout would change
                        // the content digest — two documents differing only in
                        // indentation would produce different identities.
                        let mut b = start;
                        let mut e = self.i;
                        while b < e && self.s[b].is_ascii_whitespace() {
                            b += 1;
                        }
                        while e > b && self.s[e - 1].is_ascii_whitespace() {
                            e -= 1;
                        }
                        let span = Span {
                            start: b as u32,
                            len: (e - b) as u32,
                        };
                        self.i += 1;
                        return Ok(span);
                    }
                }
                _ => {}
            }
            self.i += 1;
        }
        self.err(UprocErrorKind::UnterminatedBody)
    }

    /// An expression running to the next `->` at depth 0, captured verbatim.
    ///
    /// A `when` expression cannot simply stop at `>`: a comparison like
    /// `units >= 1000` contains one, and stopping there would truncate the
    /// predicate to `units` — which still parses as CEL, so the mistake would
    /// survive as a silently wrong rule rather than an error.
    fn expr_until_arrow(&mut self) -> Result<Span, UprocError> {
        self.ws();
        let start = self.i;
        let mut depth = 0usize;
        while self.i < self.s.len() {
            match self.s[self.i] {
                b'"' => {
                    self.i += 1;
                    while self.i < self.s.len() && self.s[self.i] != b'"' {
                        if self.s[self.i] == b'\\' {
                            self.i += 1;
                        }
                        self.i += 1;
                    }
                }
                b'{' | b'(' | b'[' => depth += 1,
                b'}' | b')' | b']' => {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                }
                b'-' if depth == 0 && self.i + 1 < self.s.len() && self.s[self.i + 1] == b'>' => {
                    break
                }
                _ => {}
            }
            self.i += 1;
        }
        let mut end = self.i;
        while end > start && self.s[end - 1].is_ascii_whitespace() {
            end -= 1;
        }
        Ok(Span {
            start: start as u32,
            len: (end - start) as u32,
        })
    }

    /// An expression running to `terminator` at depth 0, captured verbatim.
    /// Used for rule/clause expressions, which are not brace-delimited but may
    /// CONTAIN braces (a message construction).
    fn expr_until(&mut self, terminator: u8) -> Result<Span, UprocError> {
        self.ws();
        let start = self.i;
        let mut depth = 0usize;
        while self.i < self.s.len() {
            let b = self.s[self.i];
            match b {
                b'"' => {
                    self.i += 1;
                    while self.i < self.s.len() && self.s[self.i] != b'"' {
                        if self.s[self.i] == b'\\' {
                            self.i += 1;
                        }
                        self.i += 1;
                    }
                }
                b'{' | b'(' | b'[' => depth += 1,
                b'}' | b')' | b']' => {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                }
                _ if b == terminator && depth == 0 => break,
                _ => {}
            }
            self.i += 1;
        }
        let mut end = self.i;
        while end > start && self.s[end - 1].is_ascii_whitespace() {
            end -= 1;
        }
        Ok(Span {
            start: start as u32,
            len: (end - start) as u32,
        })
    }
}

/// Push into an arena array, reporting `TooMany` rather than truncating.
macro_rules! push {
    ($p:expr, $arr:expr, $n:expr, $val:expr) => {{
        if $n >= $arr.len() {
            return $p.err(UprocErrorKind::TooMany);
        }
        $arr[$n] = $val;
        $n += 1;
    }};
}

/// Parse a `.uproc` document into `arena`.
pub fn uproc_parse(src: &[u8], arena: &mut UprocArena) -> Result<Doc, UprocError> {
    let mut p = P::new(src);
    let mut doc = Doc::default();

    if !p.keyword(b"module") {
        return p.err(UprocErrorKind::ExpectedModule);
    }
    doc.module = p.qname()?;
    p.expect_byte(b'{')?;

    loop {
        if p.eat_byte(b'}') {
            break;
        }
        if p.peek().is_none() {
            return p.err(UprocErrorKind::ExpectedByte(b'}'));
        }

        if p.keyword(b"message") {
            let name = p.qname()?;
            p.expect_byte(b'{')?;
            let first = doc.n_fields;
            while !p.eat_byte(b'}') {
                let fname = p.ident()?;
                p.expect_byte(b':')?;
                let fty = p.ty()?;
                p.expect_byte(b'=')?;
                let num = p.int()? as u32;
                p.expect_byte(b';')?;
                push!(
                    p,
                    arena.fields,
                    doc.n_fields,
                    FieldDecl {
                        name: fname,
                        ty: fty,
                        number: num,
                    }
                );
            }
            push!(
                p,
                arena.messages,
                doc.n_messages,
                MessageDecl {
                    name,
                    first_field: first as u16,
                    n_fields: (doc.n_fields - first) as u16,
                }
            );
        } else if p.keyword(b"enum") {
            let name = p.ident()?;
            p.expect_byte(b'=')?;
            let value = p.int()?;
            p.expect_byte(b';')?;
            push!(p, arena.enums, doc.n_enums, EnumDecl { name, value });
        } else if p.keyword(b"expression") {
            let d = parse_fn(&mut p)?;
            push!(p, arena.expressions, doc.n_expressions, d);
        } else if p.keyword(b"transformation") {
            let d = parse_fn(&mut p)?;
            push!(p, arena.transformations, doc.n_transformations, d);
        } else if p.keyword(b"decision") {
            let name = p.ident()?;
            p.expect_byte(b'(')?;
            let param_name = p.ident()?;
            p.expect_byte(b':')?;
            let input_type = p.ty()?;
            p.expect_byte(b')')?;
            p.arrow()?;
            let output_type = p.ty()?;
            p.expect_byte(b'{')?;
            let first = doc.n_rules;
            let mut default = Span::default();
            while !p.eat_byte(b'}') {
                if p.keyword(b"when") {
                    let when = p.expr_until_arrow()?;
                    p.arrow()?;
                    let outcome = p.expr_until(b';')?;
                    p.expect_byte(b';')?;
                    push!(p, arena.rules, doc.n_rules, RuleDecl { when, outcome });
                } else if p.keyword(b"default") {
                    p.arrow()?;
                    default = p.expr_until(b';')?;
                    p.expect_byte(b';')?;
                } else {
                    return p.err(UprocErrorKind::UnknownDeclaration);
                }
            }
            if default.is_empty() {
                return p.err(UprocErrorKind::MissingClause);
            }
            push!(
                p,
                arena.decisions,
                doc.n_decisions,
                DecisionDecl {
                    name,
                    param_name,
                    input_type,
                    output_type,
                    first_rule: first as u16,
                    n_rules: (doc.n_rules - first) as u16,
                    default,
                }
            );
        } else if p.keyword(b"resource") {
            let name = p.qname()?;
            let required = p.keyword(b"required");
            p.expect_byte(b';')?;
            push!(
                p,
                arena.resources,
                doc.n_resources,
                ResourceDecl { name, required }
            );
        } else if p.keyword(b"pipeline") {
            let d = parse_pipeline(&mut p, arena, &mut doc)?;
            push!(p, arena.pipelines, doc.n_pipelines, d);
        } else if p.keyword(b"aggregation") {
            let d = parse_aggregation(&mut p, arena, &mut doc)?;
            push!(p, arena.aggregations, doc.n_aggregations, d);
        } else if p.keyword(b"entry") {
            let name = p.ident()?;
            p.expect_byte(b'=')?;
            let pipeline = p.ident()?;
            p.expect_byte(b';')?;
            push!(
                p,
                arena.entries,
                doc.n_entries,
                EntryDecl { name, pipeline }
            );
        } else if p.keyword(b"provenance") {
            if !p.keyword(b"revision") {
                return p.err(UprocErrorKind::UnknownDeclaration);
            }
            doc.provenance_revision = p.string()?;
            if !p.keyword(b"toolchain") {
                return p.err(UprocErrorKind::UnknownDeclaration);
            }
            doc.provenance_toolchain = p.string()?;
            p.expect_byte(b';')?;
        } else {
            return p.err(UprocErrorKind::UnknownDeclaration);
        }
    }
    // Nothing may follow the module's closing brace. Without this, a document
    // with junk appended parses as though the junk were not there — so a file
    // and that file plus arbitrary trailing bytes would seal to the SAME
    // digests, and a content digest would no longer identify one source. Found
    // by the generated-document differential; the host has always refused it.
    p.ws();
    if p.peek().is_some() {
        return p.err(UprocErrorKind::TrailingInput);
    }
    Ok(doc)
}

/// `NAME '(' PARAM ':' TYPE ')' '->' TYPE BODY` — expression and transformation.
fn parse_fn(p: &mut P) -> Result<FnDecl, UprocError> {
    let name = p.ident()?;
    p.expect_byte(b'(')?;
    let param_name = p.ident()?;
    p.expect_byte(b':')?;
    let param_type = p.ty()?;
    p.expect_byte(b')')?;
    p.arrow()?;
    let result_type = p.ty()?;
    let body = p.body()?;
    Ok(FnDecl {
        name,
        param_name,
        param_type,
        result_type,
        body,
    })
}

fn parse_pipeline(
    p: &mut P,
    arena: &mut UprocArena,
    doc: &mut Doc,
) -> Result<PipelineDecl, UprocError> {
    let name = p.ident()?;
    p.expect_byte(b'(')?;
    let port_name = p.ident()?;
    p.expect_byte(b':')?;
    let input_type = p.ty()?;
    p.expect_byte(b')')?;
    p.arrow()?;
    let output_type = p.ty()?;
    p.expect_byte(b'{')?;

    let first = doc.n_stages;
    let mut commit_after = Span::default();
    let mut return_stage = Span::default();
    while !p.eat_byte(b'}') {
        if p.keyword(b"call") {
            let sname = p.ident()?;
            p.expect_byte(b'=')?;
            let artefact = p.qname()?;
            p.expect_byte(b'(')?;
            let arg_first = doc.n_args;
            if !p.eat_byte(b')') {
                loop {
                    let a = p.ident()?;
                    push!(p, arena.args, doc.n_args, a);
                    if p.eat_byte(b',') {
                        continue;
                    }
                    p.expect_byte(b')')?;
                    break;
                }
            }
            p.expect_byte(b';')?;
            push!(
                p,
                arena.stages,
                doc.n_stages,
                StageDecl {
                    kind: STAGE_CALL,
                    name: sname,
                    target: artefact,
                    operation: Span::default(),
                    arg0: Span::default(),
                    first_arg: arg_first as u16,
                    n_args: (doc.n_args - arg_first) as u16,
                }
            );
        } else if p.keyword(b"effect") {
            let sname = p.ident()?;
            p.expect_byte(b'=')?;
            p.expect_byte(b'@')?;
            let resource = p.ident()?;
            let operation = if p.eat_byte(b'.') {
                p.ident()?
            } else {
                Span::default()
            };
            p.expect_byte(b'(')?;
            let arg = p.ident()?;
            p.expect_byte(b')')?;
            p.expect_byte(b';')?;
            push!(
                p,
                arena.stages,
                doc.n_stages,
                StageDecl {
                    kind: STAGE_EFFECT,
                    name: sname,
                    target: resource,
                    operation,
                    arg0: arg,
                    first_arg: 0,
                    n_args: 0,
                }
            );
        } else if p.keyword(b"commit") {
            if !p.keyword(b"after") {
                return p.err(UprocErrorKind::UnknownDeclaration);
            }
            commit_after = p.ident()?;
            p.expect_byte(b';')?;
        } else if p.keyword(b"return") {
            return_stage = p.ident()?;
            p.expect_byte(b';')?;
        } else {
            return p.err(UprocErrorKind::UnknownDeclaration);
        }
    }
    if return_stage.is_empty() {
        return p.err(UprocErrorKind::MissingClause);
    }
    Ok(PipelineDecl {
        name,
        port_name,
        input_type,
        output_type,
        first_stage: first as u16,
        n_stages: (doc.n_stages - first) as u16,
        commit_after,
        return_stage,
    })
}

fn parse_aggregation(
    p: &mut P,
    arena: &mut UprocArena,
    doc: &mut Doc,
) -> Result<AggregationDecl, UprocError> {
    let name = p.ident()?;
    p.expect_byte(b'(')?;
    let param_name = p.ident()?;
    p.expect_byte(b':')?;
    let input_type = p.ty()?;
    p.expect_byte(b')')?;
    p.arrow()?;
    let output_type = p.ty()?;
    p.expect_byte(b'{')?;

    let first = doc.n_operators;
    let mut d = AggregationDecl {
        name,
        param_name,
        input_type,
        output_type,
        first_op: first as u16,
        ..Default::default()
    };
    let mut seen_window = false;
    while !p.eat_byte(b'}') {
        if p.keyword(b"key") {
            d.key = p.expr_until(b';')?;
            p.expect_byte(b';')?;
        } else if p.keyword(b"event_time") {
            d.event_time = p.expr_until(b';')?;
            p.expect_byte(b';')?;
        } else if p.keyword(b"window") {
            if p.keyword(b"tumbling") {
                d.window_kind = WINDOW_TUMBLING;
                d.window_size_ms = p.int()?;
            } else if p.keyword(b"sliding") {
                d.window_kind = WINDOW_SLIDING;
                d.window_size_ms = p.int()?;
                d.window_step_ms = p.int()?;
            } else {
                return p.err(UprocErrorKind::UnknownDeclaration);
            }
            p.expect_byte(b';')?;
            seen_window = true;
        } else if p.keyword(b"lateness") {
            d.lateness_ms = p.int()?;
            p.expect_byte(b';')?;
        } else if p.keyword(b"guard") {
            d.guard_ms = p.int()?;
            p.expect_byte(b';')?;
        } else if p.keyword(b"lanes") {
            d.max_lanes = p.int()? as u32;
            p.expect_byte(b';')?;
        } else if p.keyword(b"operator") {
            let oname = p.ident()?;
            p.expect_byte(b'=')?;
            let op = parse_opkind(p)?;
            p.expect_byte(b';')?;
            push!(
                p,
                arena.operators,
                doc.n_operators,
                OperatorDecl {
                    name: oname,
                    kind: op.0,
                    param: op.1,
                    selector: op.2,
                }
            );
        } else if p.keyword(b"emit") {
            d.emit = p.expr_until(b';')?;
            p.expect_byte(b';')?;
        } else {
            return p.err(UprocErrorKind::UnknownDeclaration);
        }
    }
    if !seen_window || d.key.is_empty() || d.event_time.is_empty() || d.emit.is_empty() {
        return p.err(UprocErrorKind::MissingClause);
    }
    d.n_ops = (doc.n_operators - first) as u16;
    Ok(d)
}

/// `(kind, param, selector)`.
fn parse_opkind(p: &mut P) -> Result<(u8, u32, Span), UprocError> {
    if p.keyword(b"count") {
        return Ok((AGG_COUNT, 0, Span::default()));
    }
    for (kw, kind) in [
        (b"sum".as_slice(), AGG_SUM),
        (b"avg".as_slice(), AGG_AVG),
        (b"min".as_slice(), AGG_MIN),
        (b"max".as_slice(), AGG_MAX),
        (b"distinct".as_slice(), AGG_DISTINCT),
    ] {
        if p.keyword(kw) {
            p.expect_byte(b'(')?;
            let sel = p.expr_until(b')')?;
            p.expect_byte(b')')?;
            return Ok((kind, 0, sel));
        }
    }
    for (kw, kind) in [
        (b"topk".as_slice(), AGG_TOPK),
        (b"quantile".as_slice(), AGG_QUANTILE),
    ] {
        if p.keyword(kw) {
            p.expect_byte(b'(')?;
            let n = p.int()? as u32;
            p.expect_byte(b',')?;
            let sel = p.expr_until(b')')?;
            p.expect_byte(b')')?;
            return Ok((kind, n, sel));
        }
    }
    p.err(UprocErrorKind::UnknownOperator)
}

/// Map a byte offset to a 1-based (line, column) for error display.
pub fn uproc_line_col(src: &[u8], offset: usize) -> (usize, usize) {
    let mut line = 1usize;
    let mut col = 1usize;
    let end = offset.min(src.len());
    let mut i = 0;
    while i < end {
        if src[i] == b'\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
        i += 1;
    }
    (line, col)
}
