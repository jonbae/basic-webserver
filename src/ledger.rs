//! PROTOTYPE, throwaway: basic-webserver's binding to the effect-ledger
//! library. Codegen would emit the hosted half of this from `Host.roc`.
//!
//! A server process handles many requests: `init!` once, `respond!` per HTTP
//! request (concurrently, on worker threads), `shutdown!` once. Each is its own
//! request in the ledger. Replaying a `respond!` re-runs `init!` first, because
//! the context it hands every handler is computed from `init!`'s crossings,
//! and design.md rules out any other state shared between handlers.

use std::io;
use std::sync::OnceLock;

use effect_ledger::{iso_utc, Boundary, Echo, Entry, Hosted};

use crate::abi::{roc_host, ServerHeader, ServerRequest, ServerResponse, ServerResponseTag, ServerShutdownReason};
use crate::roc_platform_abi::*;

pub static BOUNDARY: Boundary = Boundary {
    platform: "basic-webserver 0.18.0",
    hosted: &[
        Hosted { symbol: "hosted_stdout_line", echo: Echo::Stdout, show_request: show_line, show_response: show_unit },
        Hosted { symbol: "hosted_stderr_line", echo: Echo::Stderr, show_request: show_line, show_response: show_unit },
        Hosted { symbol: "hosted_unix_time_now", echo: Echo::Nothing, show_request: |_| String::new(), show_response: show_unix_time },
        Hosted { symbol: "hosted_sleep_millis", echo: Echo::Nothing, show_request: |r| u64::from_le_bytes(r.try_into().unwrap()).to_string(), show_response: |_| "{}".into() },
        Hosted { symbol: "hosted_file_read_utf8", echo: Echo::Nothing, show_request: show_raw_path, show_response: show_str_result },
    ],
    entries: &[
        Entry { name: "init!", after: &[], replay: replay_init, ok: |r| r[0] == 0, show_request: |_| String::new(), show_response: show_exit_result },
        Entry { name: "respond!", after: &["init!"], replay: replay_respond, ok: response_ok, show_request: show_request, show_response: show_response },
        Entry { name: "shutdown!", after: &["init!"], replay: replay_shutdown, ok: |r| r[0] == 0, show_request: show_shutdown_reason, show_response: show_exit_result },
    ],
};

// ---------------------------------------------------------------- bytes

#[derive(Default)]
struct Out(Vec<u8>);

impl Out {
    fn u8(mut self, v: u8) -> Self {
        self.0.push(v);
        self
    }
    fn u16(mut self, v: u16) -> Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    fn u64(mut self, v: u64) -> Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    fn bytes(mut self, b: &[u8]) -> Self {
        self.0.extend_from_slice(&(b.len() as u32).to_le_bytes());
        self.0.extend_from_slice(b);
        self
    }
    fn str(self, s: &str) -> Self {
        self.bytes(s.as_bytes())
    }
    fn headers(self, headers: &[ServerHeader]) -> Self {
        let mut out = self.u64(headers.len() as u64);
        for h in headers {
            out = out.str(h.name.as_str()).str(h.value.as_str());
        }
        out
    }
}

struct In<'a>(&'a [u8]);

impl In<'_> {
    fn take(&mut self, n: usize) -> &[u8] {
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        head
    }
    fn u8(&mut self) -> u8 {
        self.take(1)[0]
    }
    fn u16(&mut self) -> u16 {
        u16::from_le_bytes(self.take(2).try_into().unwrap())
    }
    fn u64(&mut self) -> u64 {
        u64::from_le_bytes(self.take(8).try_into().unwrap())
    }
    fn bytes(&mut self) -> Vec<u8> {
        let n = u32::from_le_bytes(self.take(4).try_into().unwrap()) as usize;
        self.take(n).to_vec()
    }
    fn str(&mut self) -> String {
        String::from_utf8(self.bytes()).unwrap()
    }
    fn headers(&mut self) -> Vec<(String, String)> {
        (0..self.u64()).map(|_| (self.str(), self.str())).collect()
    }
}

// ---------------------------------------------------------------- hosted codecs

// The IOErr tags io_err_from_io names, in a fixed order: the byte is the index.
// Every other kind is `Other(Str)` carrying `error.to_string()`.
const IO_KINDS: [io::ErrorKind; 6] = [
    io::ErrorKind::AlreadyExists,
    io::ErrorKind::BrokenPipe,
    io::ErrorKind::Interrupted,
    io::ErrorKind::NotFound,
    io::ErrorKind::OutOfMemory,
    io::ErrorKind::PermissionDenied,
];
const IO_UNSUPPORTED: u8 = 6;
const IO_OTHER: u8 = 255;

fn encode_io_err(error: &io::Error) -> Vec<u8> {
    match IO_KINDS.iter().position(|kind| *kind == error.kind()) {
        Some(index) => vec![index as u8],
        None if error.kind() == io::ErrorKind::Unsupported => vec![IO_UNSUPPORTED],
        None => [&[IO_OTHER][..], error.to_string().as_bytes()].concat(),
    }
}

fn decode_io_err(bytes: &[u8]) -> io::Error {
    match bytes[0] {
        IO_OTHER => io::Error::other(String::from_utf8_lossy(&bytes[1..]).into_owned()),
        IO_UNSUPPORTED => io::Error::from(io::ErrorKind::Unsupported),
        index => io::Error::from(IO_KINDS[index as usize]),
    }
}

/// `hosted_stdout_line`/`hosted_stderr_line` response: 0 | 1 + IOErr.
pub fn encode_unit(result: &io::Result<()>) -> Vec<u8> {
    match result {
        Ok(()) => vec![0],
        Err(e) => [&[1u8][..], &encode_io_err(e)].concat(),
    }
}

pub fn decode_unit(bytes: &[u8]) -> io::Result<()> {
    match bytes[0] {
        0 => Ok(()),
        _ => Err(decode_io_err(&bytes[1..])),
    }
}

/// `hosted_file_read_utf8` response: 0 + content | 1 + IOErr.
pub fn encode_str_result(result: &io::Result<String>) -> Vec<u8> {
    match result {
        Ok(content) => [&[0u8][..], content.as_bytes()].concat(),
        Err(e) => [&[1u8][..], &encode_io_err(e)].concat(),
    }
}

pub fn decode_str_result(bytes: &[u8]) -> io::Result<String> {
    match bytes[0] {
        0 => Ok(String::from_utf8(bytes[1..].to_vec()).unwrap()),
        _ => Err(decode_io_err(&bytes[1..])),
    }
}

/// A `RawPath` argument as Roc holds it: is_windows, unix bytes, windows u16s.
pub fn encode_raw_path(path: &HostFileReadUtf8Args) -> Vec<u8> {
    let units: Vec<u8> = path.windows_u16s.as_slice().iter().flat_map(|u| u.to_le_bytes()).collect();
    Out::default().u8(path.is_windows as u8).bytes(path.unix_bytes.as_slice()).bytes(&units).0
}

// ---------------------------------------------------------------- entries

/// The context `init!` returned, kept for the `respond!` or `shutdown!` that
/// replay runs next in the same child.
static REPLAY_CONTEXT: OnceLock<usize> = OnceLock::new();

fn replay_context() -> RocBox {
    let context = *REPLAY_CONTEXT.get().expect("replay ran respond! without init!") as RocBox;
    // Each call consumes one reference, as RocContext::retain_for_request arranges live.
    unsafe { incref_box(context, 1) };
    context
}

/// `init!` response: 0 (Ok; the config and context aren't comparable bytes) | 1 + exit code.
pub fn encode_init(result: &InitForHostResult) -> Vec<u8> {
    match result.tag {
        InitForHostResultTag::Ok => vec![0],
        InitForHostResultTag::Err => Out::default().u8(1).u64(unsafe { *result.borrow_payload_err_unchecked() } as u64).0,
    }
}

fn replay_init(_: &[u8]) -> Vec<u8> {
    let mut result = unsafe { roc_init_for_host() };
    let response = encode_init(&result);
    if let InitForHostResultTag::Ok = result.tag {
        // The replay child exits after its requests, so the config is leaked.
        let ok = unsafe { result.take_payload_ok_unchecked() };
        let _ = REPLAY_CONTEXT.set(ok.context as usize);
    }
    response
}

/// `respond!` request: every field of `Server.Request` except the body
/// capability, a heap address that differs per run. Body reads are crossings.
pub fn encode_request(r: &ServerRequest) -> Vec<u8> {
    Out::default()
        .u8(r.method)
        .str(r.method_ext.as_str())
        .headers(r.headers.as_slice())
        .u8(r.target_tag)
        .str(r.target_path.as_str())
        .str(r.target_query.as_str())
        .u8(r.target_query_present as u8)
        .str(r.target_authority_host.as_str())
        .u16(r.target_authority_port)
        .u8(r.target_authority_port_present as u8)
        .u8(r.authority_present as u8)
        .str(r.authority_host.as_str())
        .u16(r.authority_port)
        .u8(r.authority_port_present as u8)
        .u64(r.content_length)
        .u8(r.content_length_known as u8)
        .u64(r.body_limit_bytes)
        .0
}

fn decode_request(bytes: &[u8]) -> ServerRequest {
    let host = roc_host();
    let s = |s: String| RocStr::from_str(&s, host);
    let mut i = In(bytes);
    let method = i.u8();
    let method_ext = s(i.str());
    let header_values = i.headers();
    let headers = unsafe { RocList::<ServerHeader>::allocate(header_values.len(), host) };
    for (n, (name, value)) in header_values.into_iter().enumerate() {
        unsafe { headers.elements.add(n).write(ServerHeader { name: s(name), value: s(value) }) };
    }
    // A fresh box stands in for the body capability; Roc only releases it.
    let body_handle = unsafe { allocate_box(8, 8, false, host) } as *mut u64;
    unsafe { body_handle.write(0) };
    ServerRequest {
        method,
        method_ext,
        headers,
        target_tag: i.u8(),
        target_path: s(i.str()),
        target_query: s(i.str()),
        target_query_present: i.u8() != 0,
        target_authority_host: s(i.str()),
        target_authority_port: i.u16(),
        target_authority_port_present: i.u8() != 0,
        authority_present: i.u8() != 0,
        authority_host: s(i.str()),
        authority_port: i.u16(),
        authority_port_present: i.u8() != 0,
        content_length: i.u64(),
        content_length_known: i.u8() != 0,
        body_limit_bytes: i.u64(),
        body_handle,
    }
}

/// `respond!` response: tag, then for `Ordinary` the status, headers, body,
/// stop flag and exit code; for `File` the root id and relative path. A
/// `Stream` holds a Roc closure, so only its tag is recorded.
pub fn encode_response(r: &ServerResponse) -> Vec<u8> {
    match r.tag {
        ServerResponseTag::Ordinary => {
            let o = unsafe { r.borrow_payload_ordinary_unchecked() };
            Out::default().u8(1).u16(o.status).headers(o.headers.as_slice()).bytes(o.body.as_slice()).u8(o.stop as u8).u64(o.exit_code as u64).0
        }
        ServerResponseTag::File => {
            let f = unsafe { r.borrow_payload_file_unchecked() };
            Out::default().u8(0).str(f.file_root_id.as_str()).str(f.file_relative.as_str()).0
        }
        ServerResponseTag::Stream => vec![2],
    }
}

fn replay_respond(request: &[u8]) -> Vec<u8> {
    let response = unsafe { roc_respond_for_host(decode_request(request), replay_context()) };
    encode_response(&response)
}

fn encode_shutdown_reason(tag: u8, detail: &str) -> Vec<u8> {
    Out::default().u8(tag).str(detail).0
}

pub fn encode_shutdown_request(reason: &ServerShutdownReason) -> Vec<u8> {
    encode_shutdown_reason(reason.tag, reason.detail.as_str())
}

pub fn encode_shutdown_result(result: &ShutdownForHostResult) -> Vec<u8> {
    match result.tag {
        ShutdownForHostResultTag::Ok => vec![0],
        ShutdownForHostResultTag::Err => Out::default().u8(1).u64(unsafe { *result.borrow_payload_err_unchecked() } as u64).0,
    }
}

fn replay_shutdown(request: &[u8]) -> Vec<u8> {
    let mut i = In(request);
    let reason = ServerShutdownReason { tag: i.u8(), detail: RocStr::from_str(&i.str(), roc_host()) };
    let result = unsafe { roc_shutdown_for_host(reason, replay_context()) };
    encode_shutdown_result(&result)
}

// ---------------------------------------------------------------- display

fn show_line(request: &[u8]) -> String {
    format!("{:?}", String::from_utf8_lossy(request))
}

fn show_io_err(error: &io::Error) -> String {
    match error.kind() {
        io::ErrorKind::Other => format!("Other({:?})", error.to_string()),
        kind => format!("{kind:?}"),
    }
}

fn show_unit(response: &[u8]) -> String {
    match decode_unit(response) {
        Ok(()) => "Ok({})".into(),
        Err(e) => format!("Err({})", show_io_err(&e)),
    }
}

fn show_unix_time(response: &[u8]) -> String {
    let nanos = i128::from_le_bytes(response.try_into().unwrap());
    format!("{nanos}  # {}", iso_utc(nanos.max(0) as u128))
}

fn show_raw_path(request: &[u8]) -> String {
    let mut i = In(request);
    let is_windows = i.u8() != 0;
    let unix = i.bytes();
    let windows = i.bytes();
    if is_windows {
        let units: Vec<u16> = windows.chunks_exact(2).map(|b| u16::from_le_bytes([b[0], b[1]])).collect();
        format!("{{ windows: {:?} }}", String::from_utf16_lossy(&units))
    } else {
        format!("{{ unix: {:?} }}", String::from_utf8_lossy(&unix))
    }
}

fn show_str_result(response: &[u8]) -> String {
    match decode_str_result(response) {
        Ok(content) => format!("Ok({content:?})"),
        Err(e) => format!("Err(FileErr({}))", show_io_err(&e)),
    }
}

fn show_exit_result(response: &[u8]) -> String {
    match response[0] {
        0 => "Ok".into(),
        _ => format!("Err(Exit({}))", In(&response[1..]).u64() as i64),
    }
}

const METHODS: [&str; 11] = ["CONNECT", "DELETE", "", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT", "TRACE", "QUERY"];

fn show_request(request: &[u8]) -> String {
    let mut i = In(request);
    let method = i.u8();
    let ext = i.str();
    let headers = i.headers();
    let _target_tag = i.u8();
    let path = i.str();
    let query = i.str();
    let query_present = i.u8() != 0;
    let method = if method == 2 { ext } else { METHODS[method as usize].to_string() };
    let query = if query_present { format!("?{query}") } else { String::new() };
    format!("{method} {path}{query}, {} headers", headers.len())
}

fn response_ok(response: &[u8]) -> bool {
    response[0] != 1 || In(&response[1..]).u16() < 500
}

fn show_response(response: &[u8]) -> String {
    let mut i = In(response);
    match i.u8() {
        1 => {
            let status = i.u16();
            let headers = i.headers();
            let body = i.bytes();
            let stop = i.u8() != 0;
            let stop = if stop { ", then stop" } else { "" };
            let headers: Vec<String> = headers.iter().map(|(n, v)| format!("{n}: {v}")).collect();
            format!("{status} [{}] {:?}{stop}", headers.join(", "), String::from_utf8_lossy(&body))
        }
        0 => format!("File({:?}, {:?})", i.str(), i.str()),
        _ => "Stream".into(),
    }
}

fn show_shutdown_reason(request: &[u8]) -> String {
    let mut i = In(request);
    let tag = i.u8();
    let detail = i.str();
    match tag {
        0 => "ApplicationRequested".into(),
        1 => "Interrupt".into(),
        2 => "Terminate".into(),
        _ => format!("Failed({detail:?})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_errs_keep_what_roc_sees() {
        for kind in IO_KINDS.into_iter().chain([io::ErrorKind::Unsupported]) {
            assert_eq!(decode_io_err(&encode_io_err(&io::Error::from(kind))).kind(), kind);
        }
        let other = decode_io_err(&encode_io_err(&io::Error::from_raw_os_error(libc::EIO)));
        assert_eq!(other.to_string(), "Input/output error (os error 5)");
    }

    #[test]
    fn shutdown_reason_shows_its_tag() {
        assert_eq!(show_shutdown_reason(&encode_shutdown_reason(2, "")), "Terminate");
        assert_eq!(show_shutdown_reason(&encode_shutdown_reason(4, "bind failed")), r#"Failed("bind failed")"#);
    }

    #[test]
    fn ordinary_response_shows_status_headers_and_body() {
        let bytes = Out::default().u8(1).u16(200).u64(1).str("Content-Type").str("text/plain").bytes(b"hi").u8(0).u64(0).0;
        assert_eq!(show_response(&bytes), r#"200 [Content-Type: text/plain] "hi""#);
        assert!(response_ok(&bytes));
    }
}
