//! In-process self-update for the `zydecodb` binary.
//!
//! Speaks the same GitHub Release asset contract as `scripts/install.sh`:
//! `zydecodb-${tag}-${target}.tar.gz` + `zydecodb-${tag}-${target}.sha256`.

use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{self, BufRead, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

const REPO: &str = "dataparade/zydecodb";
const DEFAULT_API: &str = "https://api.github.com/repos/dataparade/zydecodb/releases";
const DEFAULT_DOWNLOAD: &str = "https://github.com/dataparade/zydecodb/releases/download";

#[derive(Debug, Clone)]
pub struct UpdateOptions {
    pub check: bool,
    pub version: Option<String>,
    pub force: bool,
    pub yes: bool,
    /// Override GitHub API base (…/releases). Tests inject a local server.
    pub api_base: Option<String>,
    /// Override download base (…/releases/download).
    pub download_base: Option<String>,
    /// Override path to replace (default: `current_exe()`).
    pub install_path: Option<PathBuf>,
    /// Current package version (default: `CARGO_PKG_VERSION`).
    pub current_version: Option<String>,
}

impl Default for UpdateOptions {
    fn default() -> Self {
        Self {
            check: false,
            version: None,
            force: false,
            yes: false,
            api_base: None,
            download_base: None,
            install_path: None,
            current_version: None,
        }
    }
}

/// Result of a successful `run` (errors use `Err`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateOutcome {
    /// Binary was replaced.
    Updated,
    /// Already on the requested version (no download).
    AlreadyCurrent,
    /// `--check`: installed version matches available.
    CheckCurrent,
    /// `--check`: a newer (or different requested) version is available.
    CheckUpdateAvailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemVer {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
    /// Pre-release label without leading `-` (e.g. `beta.1`), if any.
    pub pre: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum UpdateError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Http(#[from] ureq::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

type Result<T> = std::result::Result<T, UpdateError>;

fn err(msg: impl Into<String>) -> UpdateError {
    UpdateError::Message(msg.into())
}

/// Map OS/arch strings (as from `uname`) to a release target triple.
pub fn target_from_uname(os: &str, arch: &str) -> Result<String> {
    let arch = match arch {
        "x86_64" | "amd64" => "x86_64",
        "aarch64" | "arm64" => "aarch64",
        other => {
            return Err(err(format!(
                "unsupported architecture: {other} (supported: x86_64, aarch64/arm64)"
            )));
        }
    };
    match os {
        "Linux" => Ok(format!("{arch}-unknown-linux-musl")),
        "Darwin" => Ok(format!("{arch}-apple-darwin")),
        other => Err(err(format!(
            "unsupported OS: {other} (ZydecoDB runs on Linux and macOS; Windows is not supported)"
        ))),
    }
}

pub fn detect_target() -> Result<String> {
    target_from_uname(consts_compat::os_name(), consts_compat::arch_name())
}

/// Normalize a tag or version string to always start with `v`.
pub fn normalize_tag(raw: &str) -> String {
    let t = raw.trim();
    if t.starts_with('v') || t.starts_with('V') {
        format!("v{}", &t[1..])
    } else {
        format!("v{t}")
    }
}

/// Parse `0.10.0`, `v0.10.0`, `0.10.0-beta.1`, etc.
pub fn parse_semver(raw: &str) -> Result<SemVer> {
    let s = raw.trim().trim_start_matches(['v', 'V']);
    let (core, pre) = match s.split_once('-') {
        Some((c, p)) => (c, Some(p.to_string())),
        None => (s, None),
    };
    let mut parts = core.split('.');
    let major = parts
        .next()
        .ok_or_else(|| err(format!("invalid version: {raw}")))?
        .parse()
        .map_err(|_| err(format!("invalid version: {raw}")))?;
    let minor = parts
        .next()
        .ok_or_else(|| err(format!("invalid version: {raw}")))?
        .parse()
        .map_err(|_| err(format!("invalid version: {raw}")))?;
    let patch = parts
        .next()
        .ok_or_else(|| err(format!("invalid version: {raw}")))?
        .parse()
        .map_err(|_| err(format!("invalid version: {raw}")))?;
    if parts.next().is_some() {
        return Err(err(format!("invalid version: {raw}")));
    }
    Ok(SemVer {
        major,
        minor,
        patch,
        pre,
    })
}

/// True when `remote` is a different major than `current` (requires `--force`).
pub fn is_major_bump(current: &SemVer, remote: &SemVer) -> bool {
    remote.major != current.major
}

/// True when versions differ (any component / pre-release).
pub fn versions_differ(current: &SemVer, remote: &SemVer) -> bool {
    current != remote
}

fn agent() -> String {
    format!("zydecodb-update/{}", env!("CARGO_PKG_VERSION"))
}

fn http_get(url: &str) -> Result<ureq::Response> {
    Ok(ureq::get(url)
        .set("User-Agent", &agent())
        .set("Accept", "application/vnd.github+json")
        .call()?)
}

fn http_get_bytes(url: &str) -> Result<Vec<u8>> {
    let resp = http_get(url)?;
    let mut buf = Vec::new();
    resp.into_reader().read_to_end(&mut buf)?;
    Ok(buf)
}

fn http_get_string(url: &str) -> Result<String> {
    Ok(String::from_utf8(http_get_bytes(url)?).map_err(|e| err(e.to_string()))?)
}

/// Resolve the release tag: explicit `--version`, else `/latest`, else newest.
pub fn resolve_tag(api_base: &str, explicit: Option<&str>) -> Result<String> {
    if let Some(v) = explicit {
        return Ok(normalize_tag(v));
    }
    let latest_url = format!("{api_base}/latest");
    if let Ok(body) = http_get_string(&latest_url) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
            if let Some(tag) = v.get("tag_name").and_then(|t| t.as_str()) {
                if !tag.is_empty() {
                    return Ok(normalize_tag(tag));
                }
            }
        }
    }
    let list_url = format!("{api_base}?per_page=1");
    let body = http_get_string(&list_url)?;
    let arr: Vec<serde_json::Value> = serde_json::from_str(&body)?;
    let tag = arr
        .first()
        .and_then(|v| v.get("tag_name"))
        .and_then(|t| t.as_str())
        .ok_or_else(|| err(format!("could not determine the latest release of {REPO}")))?;
    Ok(normalize_tag(tag))
}

/// Verify `archive_path` against a `sha256sum -c` style sidecar.
pub fn verify_sha256(archive_path: &Path, checksum_text: &str) -> Result<()> {
    let expected = parse_sha256_sidecar(checksum_text, archive_path)?;
    let mut file = File::open(archive_path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let got = hasher.finalize();
    let got_hex = hex_encode(&got);
    if got_hex != expected {
        return Err(err(format!(
            "sha256 verification FAILED for {} — aborting",
            archive_path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("archive")
        )));
    }
    Ok(())
}

/// Parse the first `hex  filename` line; filename may be basename-only.
pub fn parse_sha256_sidecar(text: &str, archive_path: &Path) -> Result<String> {
    let want_name = archive_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| err("archive path has no file name"))?;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let hex = parts
            .next()
            .ok_or_else(|| err("checksum file empty"))?;
        let name = parts.next().unwrap_or(want_name);
        let name = name.trim_start_matches('*');
        if name == want_name || Path::new(name).file_name().and_then(|s| s.to_str()) == Some(want_name)
        {
            if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(err("invalid sha256 hex in checksum file"));
            }
            return Ok(hex.to_ascii_lowercase());
        }
    }
    // Some sidecars list only the hex and a different archive stem; accept a
    // single-line hex + any name when only one entry exists.
    let mut lines = text.lines().filter(|l| {
        let t = l.trim();
        !t.is_empty() && !t.starts_with('#')
    });
    if let Some(line) = lines.next() {
        if lines.next().is_none() {
            let hex = line
                .split_whitespace()
                .next()
                .ok_or_else(|| err("checksum file empty"))?;
            if hex.len() == 64 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
                return Ok(hex.to_ascii_lowercase());
            }
        }
    }
    Err(err(format!(
        "checksum file does not mention {want_name}"
    )))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

/// Extract the `zydecodb` member from a `.tar.gz` into `dest_dir`.
pub fn extract_binary(archive_path: &Path, dest_dir: &Path) -> Result<PathBuf> {
    let file = File::open(archive_path)?;
    let dec = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(dec);
    let mut found: Option<PathBuf> = None;
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("");
        if name != "zydecodb" {
            continue;
        }
        let out = dest_dir.join("zydecodb");
        {
            let mut out_file = File::create(&out)?;
            io::copy(&mut entry, &mut out_file)?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&out, fs::Permissions::from_mode(0o755))?;
        }
        found = Some(out);
        break;
    }
    found.ok_or_else(|| err("archive did not contain the zydecodb binary"))
}

/// Atomically replace `install_path` with `new_bin`.
pub fn atomic_replace(install_path: &Path, new_bin: &Path) -> Result<()> {
    let parent = install_path
        .parent()
        .ok_or_else(|| err("install path has no parent directory"))?;
    let staging = parent.join("zydecodb.new");
    fs::copy(new_bin, &staging)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&staging, fs::Permissions::from_mode(0o755))?;
    }
    match fs::rename(&staging, install_path) {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = fs::remove_file(&staging);
            let kind = e.kind();
            let busy = kind == io::ErrorKind::ExecutableFileBusy
                || e.raw_os_error() == Some(26) // ETXTBSY on Linux
                || e.to_string().contains("Text file busy");
            if busy {
                return Err(err(
                    "cannot replace the running binary (text file busy). \
                     Stop `zydecodb serve` and retry `zydecodb update`",
                ));
            }
            // Fallback: move current aside, then place new.
            let bak = parent.join("zydecodb.old");
            let _ = fs::remove_file(&bak);
            if let Err(e2) = fs::rename(install_path, &bak) {
                return Err(err(format!(
                    "failed to replace binary at {}: {e} (also: {e2}). \
                     Stop `zydecodb serve` if it is running and retry",
                    install_path.display()
                )));
            }
            fs::copy(new_bin, install_path).map_err(|e3| {
                // Best-effort restore.
                let _ = fs::rename(&bak, install_path);
                err(format!(
                    "failed to install new binary at {}: {e3}",
                    install_path.display()
                ))
            })?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = fs::set_permissions(install_path, fs::Permissions::from_mode(0o755));
            }
            let _ = fs::remove_file(&bak);
            Ok(())
        }
    }
}

fn maybe_attest(archive_path: &Path) {
    let Some(gh) = which("gh") else {
        return;
    };
    // Older gh builds lack `attestation`; skip quietly.
    let probe = Command::new(&gh)
        .args(["attestation", "--help"])
        .output();
    if !matches!(probe, Ok(ref o) if o.status.success()) {
        return;
    }
    match Command::new(gh)
        .args([
            "attestation",
            "verify",
            archive_path.to_str().unwrap_or(""),
            "--repo",
            REPO,
        ])
        .output()
    {
        Ok(out) if out.status.success() => {
            eprintln!("Attestation verified.");
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            eprintln!(
                "warning: gh attestation verify failed (continuing after sha256): {}",
                stderr.trim()
            );
        }
        Err(e) => {
            eprintln!("warning: could not run gh attestation verify: {e}");
        }
    }
}

fn which(bin: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(bin);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

fn confirm_tty(prompt: &str) -> Result<bool> {
    if !io::stdin().is_terminal() {
        return Err(err(
            "refusing to update without --yes when stdin is not a TTY",
        ));
    }
    eprint!("{prompt} [y/N] ");
    let _ = io::stderr().flush();
    let mut line = String::new();
    io::stdin().lock().read_line(&mut line)?;
    let t = line.trim().to_ascii_lowercase();
    Ok(t == "y" || t == "yes")
}

trait IsTerminal {
    fn is_terminal(&self) -> bool;
}

impl IsTerminal for io::Stdin {
    fn is_terminal(&self) -> bool {
        // std::io::IsTerminal is stable on our MSRV.
        std::io::IsTerminal::is_terminal(self)
    }
}

/// Compat shims: `std::env::consts::{OS, ARCH}` use different names than uname.
mod consts_compat {
    pub fn os_name() -> &'static str {
        match std::env::consts::OS {
            "linux" => "Linux",
            "macos" => "Darwin",
            other => other,
        }
    }
    pub fn arch_name() -> &'static str {
        match std::env::consts::ARCH {
            "x86_64" => "x86_64",
            "aarch64" => "aarch64",
            other => other,
        }
    }
}

pub fn run(opts: UpdateOptions) -> Result<UpdateOutcome> {
    let api_base = opts.api_base.as_deref().unwrap_or(DEFAULT_API);
    let download_base = opts.download_base.as_deref().unwrap_or(DEFAULT_DOWNLOAD);
    let current_raw = opts
        .current_version
        .as_deref()
        .unwrap_or(env!("CARGO_PKG_VERSION"));
    let current = parse_semver(current_raw)?;
    let target = detect_target()?;
    let tag = resolve_tag(api_base, opts.version.as_deref())?;
    let remote = parse_semver(&tag)?;

    println!("current:  {current_raw}");
    println!("available: {}", tag.trim_start_matches('v'));

    if opts.check {
        if versions_differ(&current, &remote) {
            println!("update available");
            return Ok(UpdateOutcome::CheckUpdateAvailable);
        }
        println!("already up to date");
        return Ok(UpdateOutcome::CheckCurrent);
    }

    if !opts.force && !versions_differ(&current, &remote) {
        println!("already up to date");
        return Ok(UpdateOutcome::AlreadyCurrent);
    }

    if is_major_bump(&current, &remote) && !opts.force {
        return Err(err(format!(
            "refusing major version jump {} → {} without --force",
            current_raw,
            tag.trim_start_matches('v')
        )));
    }

    if !opts.yes {
        let prompt = format!("Install zydecodb {} ({target})?", tag.trim_start_matches('v'));
        if !confirm_tty(&prompt)? {
            return Err(err("update cancelled"));
        }
    }

    let archive_name = format!("zydecodb-{tag}-{target}.tar.gz");
    let checksum_name = format!("zydecodb-{tag}-{target}.sha256");
    let archive_url = format!("{download_base}/{tag}/{archive_name}");
    let checksum_url = format!("{download_base}/{tag}/{checksum_name}");

    eprintln!("Downloading {archive_url}");
    let tmp = tempfile::tempdir()?;
    let archive_path = tmp.path().join(&archive_name);
    let checksum_path = tmp.path().join(&checksum_name);

    {
        let bytes = http_get_bytes(&archive_url).map_err(|e| {
            err(format!(
                "download failed — no {target} build for {tag}? ({e})"
            ))
        })?;
        fs::write(&archive_path, bytes)?;
    }
    {
        let text = http_get_string(&checksum_url)
            .map_err(|_| err(format!("checksum sidecar missing for {checksum_name}")))?;
        fs::write(&checksum_path, &text)?;
        verify_sha256(&archive_path, &text)?;
    }
    eprintln!("Checksum verified.");
    maybe_attest(&archive_path);

    let extracted = extract_binary(&archive_path, tmp.path())?;
    let install_path = match opts.install_path {
        Some(p) => p,
        None => std::env::current_exe().map_err(UpdateError::Io)?,
    };
    // Resolve symlinks so we replace the real binary.
    let install_path = fs::canonicalize(&install_path).unwrap_or(install_path);

    atomic_replace(&install_path, &extracted)?;

    println!(
        "Updated zydecodb {} → {} at {}.",
        current_raw,
        tag.trim_start_matches('v'),
        install_path.display()
    );
    println!("Restart any running `serve` process to use the new binary.");
    println!("Drivers are not updated (use pip/npm/go get).");
    Ok(UpdateOutcome::Updated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn target_mapping_matches_install_sh() {
        assert_eq!(
            target_from_uname("Linux", "x86_64").unwrap(),
            "x86_64-unknown-linux-musl"
        );
        assert_eq!(
            target_from_uname("Linux", "aarch64").unwrap(),
            "aarch64-unknown-linux-musl"
        );
        assert_eq!(
            target_from_uname("Darwin", "arm64").unwrap(),
            "aarch64-apple-darwin"
        );
        assert!(target_from_uname("Windows", "x86_64").is_err());
    }

    #[test]
    fn tag_and_semver_parse() {
        assert_eq!(normalize_tag("0.10.0"), "v0.10.0");
        assert_eq!(normalize_tag("v0.10.0"), "v0.10.0");
        let v = parse_semver("v0.10.0-beta.1").unwrap();
        assert_eq!(v.major, 0);
        assert_eq!(v.minor, 10);
        assert_eq!(v.patch, 0);
        assert_eq!(v.pre.as_deref(), Some("beta.1"));
    }

    #[test]
    fn major_bump_gate() {
        let a = parse_semver("0.10.0").unwrap();
        let b = parse_semver("1.0.0").unwrap();
        assert!(is_major_bump(&a, &b));
        assert!(!is_major_bump(&a, &parse_semver("0.11.0").unwrap()));
        assert!(versions_differ(&a, &b));
        assert!(!versions_differ(&a, &parse_semver("0.10.0").unwrap()));
    }

    #[test]
    fn sha256_sidecar_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let archive = dir.path().join("zydecodb-v0.10.0-x86_64-unknown-linux-musl.tar.gz");
        let payload = b"hello-zydecodb-archive";
        fs::write(&archive, payload).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(payload);
        let hex = hex_encode(&hasher.finalize());
        let sidecar = format!(
            "{hex}  zydecodb-v0.10.0-x86_64-unknown-linux-musl.tar.gz\n"
        );
        verify_sha256(&archive, &sidecar).unwrap();
        // Single-entry sidecar with a different filename still verifies.
        verify_sha256(&archive, &format!("{hex}  other.tar.gz\n")).unwrap();
        assert!(verify_sha256(&archive, "not-a-checksum\n").is_err());
    }

    #[test]
    fn extract_and_atomic_replace() {
        let dir = tempfile::tempdir().unwrap();
        let bin_dir = dir.path().join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let install = bin_dir.join("zydecodb");
        fs::write(&install, b"old").unwrap();

        // Build a tiny gzip tar with a zydecodb member.
        let archive_path = dir.path().join("pkg.tar.gz");
        {
            let file = File::create(&archive_path).unwrap();
            let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
            let mut builder = tar::Builder::new(enc);
            let mut header = tar::Header::new_gnu();
            let data = b"#!/bin/sh\necho new\n";
            header.set_size(data.len() as u64);
            header.set_mode(0o755);
            header.set_cksum();
            builder.append_data(&mut header, "zydecodb", &data[..]).unwrap();
            builder.finish().unwrap();
        }

        let mut hasher = Sha256::new();
        hasher.update(fs::read(&archive_path).unwrap());
        let hex = hex_encode(&hasher.finalize());
        let name = archive_path.file_name().unwrap().to_str().unwrap();
        verify_sha256(&archive_path, &format!("{hex}  {name}\n")).unwrap();

        let extracted = extract_binary(&archive_path, dir.path()).unwrap();
        atomic_replace(&install, &extracted).unwrap();
        assert_eq!(fs::read(&install).unwrap(), b"#!/bin/sh\necho new\n");
    }

    #[test]
    fn fixture_http_download_replace() {
        let dir = tempfile::tempdir().unwrap();
        let target = detect_target().unwrap();
        let archive_name = format!("zydecodb-v9.9.9-{target}.tar.gz");
        let checksum_name = format!("zydecodb-v9.9.9-{target}.sha256");

        let archive_path = dir.path().join(&archive_name);
        {
            let file = File::create(&archive_path).unwrap();
            let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
            let mut builder = tar::Builder::new(enc);
            let mut header = tar::Header::new_gnu();
            let data = b"new-binary-bytes";
            header.set_size(data.len() as u64);
            header.set_mode(0o755);
            header.set_cksum();
            builder
                .append_data(&mut header, "zydecodb", &data[..])
                .unwrap();
            builder.finish().unwrap();
        }
        let archive_bytes = fs::read(&archive_path).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(&archive_bytes);
        let hex = hex_encode(&hasher.finalize());
        let checksum_bytes = format!("{hex}  {archive_name}\n").into_bytes();

        let latest_json = r#"{"tag_name":"v9.9.9"}"#;
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_ip().unwrap();
        let base = format!("http://{addr}");
        let api_base = format!("{base}/releases");
        let download_base = format!("{base}/download");

        let archive_bytes2 = archive_bytes.clone();
        let checksum_bytes2 = checksum_bytes.clone();
        let archive_name2 = archive_name.clone();
        let checksum_name2 = checksum_name.clone();
        let _handle = thread::spawn(move || {
            for _ in 0..8 {
                let Ok(req) = server.recv() else {
                    break;
                };
                let url = req.url().to_string();
                let (status, body, ctype) = if url.ends_with("/releases/latest") {
                    (200, latest_json.as_bytes().to_vec(), "application/json")
                } else if url.contains(&archive_name2) {
                    (200, archive_bytes2.clone(), "application/gzip")
                } else if url.contains(&checksum_name2) {
                    (200, checksum_bytes2.clone(), "text/plain")
                } else {
                    (404, b"missing".to_vec(), "text/plain")
                };
                let header =
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], ctype.as_bytes()).unwrap();
                let _ = req.respond(
                    tiny_http::Response::from_data(body)
                        .with_status_code(status)
                        .with_header(header),
                );
            }
        });

        let install_dir = dir.path().join("install");
        fs::create_dir_all(&install_dir).unwrap();
        let install = install_dir.join("zydecodb");
        fs::write(&install, b"old-binary").unwrap();

        let outcome = run(UpdateOptions {
            check: false,
            version: None,
            force: true,
            yes: true,
            api_base: Some(api_base.clone()),
            download_base: Some(download_base.clone()),
            install_path: Some(install.clone()),
            current_version: Some("0.10.0".into()),
        })
        .unwrap();
        assert_eq!(outcome, UpdateOutcome::Updated);
        assert_eq!(fs::read(&install).unwrap(), b"new-binary-bytes");

        let check = run(UpdateOptions {
            check: true,
            version: Some("v9.9.9".into()),
            force: false,
            yes: true,
            api_base: Some(api_base),
            download_base: Some(download_base),
            install_path: Some(install),
            current_version: Some("9.9.9".into()),
        })
        .unwrap();
        assert_eq!(check, UpdateOutcome::CheckCurrent);
    }
}
