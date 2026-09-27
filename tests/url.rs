//! `Engine::open_url` accepts http/https URLs and rejects anything else
//! (Electron `shell.openExternal` parity — no `file://`, `javascript:` etc).
//! URL schemes are case-insensitive per RFC 3986 §3.1, so `HTTPS://…` must
//! take the same path as `https://…`.
use kosmos_gpui_kit::engine::open_url;

fn invalid_url_error(url: &str) -> bool {
    matches!(open_url(url), Err(e) if e == "Недопустимый URL")
}

#[test]
fn open_url_rejects_non_http_schemes() {
    assert!(invalid_url_error("file:///etc/passwd"));
    assert!(invalid_url_error("javascript:alert(1)"));
    assert!(invalid_url_error("example.com"));
    assert!(invalid_url_error(""));
}

#[test]
fn open_url_accepts_any_case_http_scheme() {
    // An uppercase scheme is a valid URL — it must not take the
    // "Недопустимый URL" branch. (Whether xdg-open actually launches is
    // environment-dependent; only the scheme check is asserted.)
    for url in [
        "HTTPS://example.com",
        "HTTP://example.com",
        "HtTpS://example.com",
    ] {
        assert!(
            !invalid_url_error(url),
            "valid https/http URL rejected: {url}"
        );
    }
}
