//! Integration probes for `Engine::subscribe` / `EventStream::next_event`
//! against a real `tungstenite` server stub — the same protocol shape as
//! `runtime/src/ws_server` (hello handshake → `{"event": ...}` frames).
use kosmos_gpui_kit::engine::Engine;
use serde_json::{json, Value};
use std::net::TcpListener;
use std::path::PathBuf;

/// Write a lock file pointing `ws_port` at the stub and return the Engine.
fn engine_with_ws_lock(ws_port: u16) -> (Engine, PathBuf) {
    let dir = std::env::temp_dir().join(format!("kgk-ws-{}-{}", std::process::id(), ws_port));
    std::fs::create_dir_all(&dir).unwrap();
    let lock = json!({
        "format_version": 1,
        "api_version": {"major": 1},
        "http_port": 1,
        "ws_port": ws_port,
        "auth_token": "b".repeat(64),
    });
    std::fs::write(dir.join("engine.lock.json"), lock.to_string()).unwrap();
    (
        Engine {
            data_dir: Some(dir.clone()),
        },
        dir,
    )
}

/// Spawn a WS stub: accept one upgrade, read the hello frame, then run
/// `script` against the connected socket. Returns `(port, handle)` — the
/// caller joins the handle so assertion failures inside the stub propagate.
fn ws_stub(
    script: impl FnOnce(tungstenite::WebSocket<std::net::TcpStream>, Value) + Send + 'static,
) -> (u16, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut socket = tungstenite::accept(stream).unwrap();
        let hello = loop {
            if let tungstenite::Message::Text(text) = socket.read().unwrap() {
                break serde_json::from_str(&text).unwrap();
            }
        };
        script(socket, hello);
    });
    (port, handle)
}

fn assert_hello(hello: &Value) {
    assert_eq!(hello["kind"].as_str().unwrap(), "hello");
    assert_eq!(hello["apiVersion"].as_str().unwrap(), "1.0.0");
    assert_eq!(
        hello["token"].as_str().unwrap(),
        "b".repeat(64),
        "hello must carry the lock-file auth token"
    );
    assert!(
        hello["pid"].as_u64().is_some(),
        "hello must carry the client pid"
    );
}

#[test]
fn subscribe_delivers_events_after_hello_ok() {
    let (port, stub) = ws_stub(|mut socket, hello| {
        assert_hello(&hello);
        socket
            .send(tungstenite::Message::text(
                r#"{"kind":"hello_ok","apiVersion":"1.0.0","compatibility":"exact"}"#,
            ))
            .unwrap();
        socket
            .send(tungstenite::Message::text(
                r#"{"event":"state_changed","scope":"focus"}"#,
            ))
            .unwrap();
        socket
            .send(tungstenite::Message::text(
                r#"{"event":"dictation_toggle_trigger","vk":192}"#,
            ))
            .unwrap();
        // Keep the connection open briefly so the client reads both frames
        // before the peer disappears.
        std::thread::sleep(std::time::Duration::from_millis(200));
    });
    let (engine, dir) = engine_with_ws_lock(port);
    let mut stream = engine.subscribe().expect("handshake must succeed");
    let first = stream.next_event().expect("first event");
    assert_eq!(first["event"].as_str().unwrap(), "state_changed");
    let second = stream.next_event().expect("second event");
    assert_eq!(
        second["event"].as_str().unwrap(),
        "dictation_toggle_trigger"
    );
    stub.join().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn subscribe_surfaces_hello_error_message() {
    let (port, stub) = ws_stub(|mut socket, hello| {
        assert_hello(&hello);
        socket
            .send(tungstenite::Message::text(
                r#"{"kind":"hello_error","code":"INVALID_TOKEN","message":"auth token does not match"}"#,
            ))
            .unwrap();
    });
    let (engine, dir) = engine_with_ws_lock(port);
    let error = match engine.subscribe() {
        Err(error) => error,
        Ok(_) => panic!("subscribe must fail on hello_error"),
    };
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(error, "Engine отклонил подписку: auth token does not match");
    stub.join().unwrap();
}

#[test]
fn next_event_skips_malformed_frames_and_ends_on_close() {
    let (port, stub) = ws_stub(|mut socket, hello| {
        assert_hello(&hello);
        socket
            .send(tungstenite::Message::text(
                r#"{"kind":"hello_ok","apiVersion":"1.0.0","compatibility":"exact"}"#,
            ))
            .unwrap();
        socket
            .send(tungstenite::Message::text("not json at all"))
            .unwrap();
        socket
            .send(tungstenite::Message::text(r#"{"event":"real"}"#))
            .unwrap();
        socket.send(tungstenite::Message::Close(None)).unwrap();
        let _ = socket.flush();
        // RFC 6455 §5.5.1: the client must answer the close frame with its
        // own close frame — not just drop TCP. tungstenite queues that reply
        // inside `read`; the client still has to flush it.
        socket.get_mut().set_nonblocking(true).unwrap();
        let mut saw_close = false;
        for _ in 0..50 {
            match socket.read() {
                Ok(tungstenite::Message::Close(_)) => {
                    saw_close = true;
                    break;
                }
                Err(tungstenite::Error::Io(ref e))
                    if e.kind() == std::io::ErrorKind::WouldBlock =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                _ => break,
            }
        }
        assert!(
            saw_close,
            "client dropped the connection without answering the close frame"
        );
    });
    let (engine, dir) = engine_with_ws_lock(port);
    let mut stream = engine.subscribe().unwrap();
    let event = stream
        .next_event()
        .expect("malformed frame must be skipped");
    assert_eq!(event["event"].as_str().unwrap(), "real");
    assert!(
        stream.next_event().is_none(),
        "stream must end after the close frame"
    );
    stub.join().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn subscribe_rejects_ws_port_zero() {
    let (engine, dir) = engine_with_ws_lock(0);
    let error = match engine.subscribe() {
        Err(error) => error,
        Ok(_) => panic!("subscribe must fail when ws_port is 0"),
    };
    let _ = std::fs::remove_dir_all(&dir);
    assert!(error.contains("не поддерживает"), "unexpected: {error}");
}

/// A Ping between events must not break the stream — tungstenite queues
/// the Pong and `next_event` flushes it so the server keeps reading.
#[test]
fn next_event_survives_server_ping() {
    let (port, stub) = ws_stub(|mut socket, hello| {
        assert_hello(&hello);
        socket
            .send(tungstenite::Message::text(
                r#"{"kind":"hello_ok","apiVersion":"1.0.0","compatibility":"exact"}"#,
            ))
            .unwrap();
        socket
            .send(tungstenite::Message::Ping(vec![1, 2, 3]))
            .unwrap();
        socket
            .send(tungstenite::Message::text(r#"{"event":"after_ping"}"#))
            .unwrap();
        // The client should answer the ping; give it a moment, then read.
        std::thread::sleep(std::time::Duration::from_millis(100));
        socket.get_mut().set_nonblocking(true).unwrap();
        let mut saw_pong = false;
        for _ in 0..10 {
            match socket.read() {
                Ok(tungstenite::Message::Pong(_)) => {
                    saw_pong = true;
                    break;
                }
                Err(tungstenite::Error::Io(ref e))
                    if e.kind() == std::io::ErrorKind::WouldBlock =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                _ => break,
            }
        }
        assert!(saw_pong, "client must answer the server ping with a pong");
    });
    let (engine, dir) = engine_with_ws_lock(port);
    let mut stream = engine.subscribe().unwrap();
    let event = stream.next_event().unwrap();
    assert_eq!(event["event"].as_str().unwrap(), "after_ping");
    stub.join().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
