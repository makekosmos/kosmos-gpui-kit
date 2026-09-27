//! Engine transport: discovery via engine.lock.json, POST /v1/rpc and the
//! GET /v1/status surfaces the Vue Manager reaches through Electron IPC.
//! Engine is the only owner of state — this client never opens databases.
use serde::Deserialize;
use serde_json::{json, Value};
use std::{path::PathBuf, time::Duration};

pub(crate) const CLIENT_CLASS: &str = "manager-gpui";

#[derive(Deserialize)]
pub(crate) struct Lock {
    format_version: u32,
    api_version: Version,
    pub http_port: u16,
    /// Broadcast/event socket the runtime exposes next to the HTTP API
    /// (`ws_server`). Absent in pre-WS locks — serde default keeps them
    /// readable; `subscribe` rejects `0`.
    #[serde(default)]
    pub ws_port: u16,
    pub auth_token: String,
}

#[derive(Deserialize)]
struct Version {
    major: u32,
}

#[derive(Clone, Default)]
pub struct Engine {
    pub data_dir: Option<PathBuf>,
}

impl Engine {
    /// POST /v1/rpc. Params merge with `operation`/`_req_id` like the
    /// Electron engine-client does.
    pub fn rpc(&self, operation: &str, mut params: Value) -> Result<Value, String> {
        let lock = self.lock()?;
        if !params.is_object() {
            return Err("Некорректный запрос Engine".into());
        }
        params["operation"] = json!(operation);
        params["_req_id"] = json!(uuid::Uuid::new_v4().to_string());
        let response = or_status_body(
            agent()
                .post(&format!("http://127.0.0.1:{}/v1/rpc", lock.http_port))
                .set("Authorization", &format!("Bearer {}", lock.auth_token))
                .set("X-Kosmos-Api-Version", "1.0.0")
                .set("X-Kosmos-Client-Class", CLIENT_CLASS)
                .set("X-Kosmos-Client-Version", env!("CARGO_PKG_VERSION"))
                .set("X-Kosmos-Client-Pid", &std::process::id().to_string())
                .send_json(params),
        )
        .ok_or_else(|| {
            "Нет подтверждения от Engine. Обновите список перед повтором.".to_string()
        })?;
        decode(response)
    }

    /// GET /v1/health or /v1/info — plain status surfaces the Manager header
    /// uses for Engine reachability.
    pub fn status(&self, path: &str) -> Result<Value, String> {
        let lock = self.lock()?;
        let response = or_status_body(
            agent()
                .get(&format!("http://127.0.0.1:{}/v1/{path}", lock.http_port))
                .set("Authorization", &format!("Bearer {}", lock.auth_token))
                .set("X-Kosmos-Api-Version", "1.0.0")
                .set("X-Kosmos-Client-Class", CLIENT_CLASS)
                .set("X-Kosmos-Client-Version", env!("CARGO_PKG_VERSION"))
                .set("X-Kosmos-Client-Pid", &std::process::id().to_string())
                .call(),
        )
        .ok_or_else(|| "Нет подтверждения от Engine. Обновите список.".to_string())?;
        // Status endpoints answer `{"ok":true,...}` without a `data` envelope.
        let value: Value = response
            .into_json()
            .map_err(|_| "Некорректный ответ Engine")?;
        if value["ok"] != true {
            return Err("Engine отклонил запрос состояния".into());
        }
        Ok(value)
    }

    /// Engine discovery read on every call: Engine may have restarted.
    pub(crate) fn lock(&self) -> Result<Lock, String> {
        let directory = self.data_dir.clone().map(Ok).unwrap_or_else(data_dir)?;
        let bytes = std::fs::read(directory.join("engine.lock.json"))
            .map_err(|_| "Engine не запущен. Запустите Kosmos и обновите список.".to_string())?;
        let lock: Lock =
            serde_json::from_slice(&bytes).map_err(|_| "Некорректный файл состояния Engine")?;
        if lock.format_version != 1
            || lock.api_version.major != 1
            || lock.http_port == 0
            || lock.auth_token.len() != 64
            || !lock.auth_token.bytes().all(|v| v.is_ascii_hexdigit())
        {
            return Err("Несовместимое состояние Engine. Обновите Kosmos.".into());
        }
        Ok(lock)
    }
}

/// A non-2xx status still carries Engine's `{"ok":false,"error":...}` body —
/// keep the response so `decode` surfaces the real rejection instead of
/// reporting the Engine as unreachable. `None` is a transport-level failure.
fn or_status_body(result: Result<ureq::Response, ureq::Error>) -> Option<ureq::Response> {
    match result {
        Ok(response) | Err(ureq::Error::Status(_, response)) => Some(response),
        Err(_) => None,
    }
}

fn decode(response: ureq::Response) -> Result<Value, String> {
    let value: Value = response
        .into_json()
        .map_err(|_| "Некорректный ответ Engine")?;
    if value["ok"] != true {
        let detail = value
            .get("error")
            .map(|e| match e {
                Value::String(s) => s.clone(),
                other => other
                    .get("message")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| other.to_string()),
            })
            .unwrap_or_else(|| "неизвестная ошибка".into());
        return Err(format!("Engine отклонил операцию: {detail}"));
    }
    Ok(value.get("data").cloned().unwrap_or(Value::Null))
}

fn agent() -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(15))
        .redirects(0)
        .build()
}

pub fn data_dir() -> Result<PathBuf, String> {
    if let Some(path) = std::env::var_os("KOSMOS_DATA_DIR").filter(|v| !v.is_empty()) {
        return Ok(PathBuf::from(path));
    }
    #[cfg(target_os = "windows")]
    let base = std::env::var_os("APPDATA").map(PathBuf::from);
    #[cfg(target_os = "macos")]
    let base =
        std::env::var_os("HOME").map(|v| PathBuf::from(v).join("Library/Application Support"));
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|v| PathBuf::from(v).join(".config")));
    base.map(|v| v.join("Kosmos"))
        .ok_or("Не найдена папка данных Kosmos".into())
}

/// Electron userData of the Vue Manager build ("Kosmos Manager" productName)
/// — the browser.json persistence flag lives there.
pub fn host_user_data() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    let base = std::env::var_os("APPDATA").map(PathBuf::from);
    #[cfg(target_os = "macos")]
    let base =
        std::env::var_os("HOME").map(|v| PathBuf::from(v).join("Library/Application Support"));
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|v| PathBuf::from(v).join(".config")));
    base.map(|v| v.join("Kosmos Manager"))
}

/// Packaged Agenda GPUI lives next to this exe as
/// `resources/components/agenda/Kosmos Agenda.exe` (KOS-137).
/// `KOSMOS_AGENDA_EXECUTABLE` overrides for dev/local runs.
pub fn agenda_executable() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("KOSMOS_AGENDA_EXECUTABLE").filter(|v| !v.is_empty()) {
        let candidate = PathBuf::from(path);
        return candidate.is_file().then_some(candidate);
    }
    let exe = std::env::current_exe().ok()?;
    let candidate = exe
        .parent()?
        .parent()?
        .join("agenda")
        .join("Kosmos Agenda.exe");
    candidate.is_file().then_some(candidate)
}

/// Launch the sibling Agenda component; the child inherits this process env
/// (the shell sets KOSMOS_DATA_DIR at spawn). `data_dir` re-pins the same
/// Engine lock when Manager itself was started directly.
pub fn open_agenda(data_dir: Option<&std::path::Path>) -> Result<(), String> {
    let exe = agenda_executable().ok_or("Agenda не входит в эту сборку Kosmos.")?;
    let mut command = std::process::Command::new(exe);
    if let Some(dir) = data_dir {
        command.env("KOSMOS_DATA_DIR", dir);
    }
    command
        .spawn()
        .map(|_| ())
        .map_err(|e| format!("Не удалось запустить Agenda: {e}"))
}

/// Open a file/folder with the OS handler (Electron shell.openPath parity).
pub fn open_path(path: &std::path::Path) -> Result<(), String> {
    #[cfg(target_os = "windows")]
    let cmd = "explorer";
    #[cfg(target_os = "macos")]
    let cmd = "open";
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    let cmd = "xdg-open";
    std::process::Command::new(cmd)
        .arg(path)
        .spawn()
        .map(|_| ())
        .map_err(|e| format!("Не удалось открыть путь: {e}"))
}

/// Open an https/http URL in the system browser (shell.openExternal parity).
/// URL schemes are case-insensitive (RFC 3986 §3.1) — `HTTPS://…` is valid.
pub fn open_url(url: &str) -> Result<(), String> {
    let http = url
        .get(..8)
        .is_some_and(|p| p.eq_ignore_ascii_case("https://"))
        || url
            .get(..7)
            .is_some_and(|p| p.eq_ignore_ascii_case("http://"));
    if !http {
        return Err("Недопустимый URL".into());
    }
    open_path(std::path::Path::new(url))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    fn engine_with_lock(dir: &std::path::Path, http_port: u16) -> Engine {
        std::fs::create_dir_all(dir).unwrap();
        let lock = json!({
            "format_version": 1,
            "api_version": {"major": 1},
            "http_port": http_port,
            "auth_token": "a".repeat(64),
        });
        std::fs::write(dir.join("engine.lock.json"), lock.to_string()).unwrap();
        Engine {
            data_dir: Some(dir.to_path_buf()),
        }
    }

    /// One-shot HTTP stub: accepts a single request, answers `status` +
    /// `body`. Returns the bound port for the lock file.
    fn serve_once(status: &str, body: &'static str) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let status = status.to_string();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 8192];
            let _ = stream.read(&mut buf);
            let response = format!(
                "{status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
        });
        port
    }

    /// Engine rejecting an operation with a non-2xx status still answers
    /// `{"ok":false,"error":...}` — the client must surface that rejection,
    /// not report the Engine as unreachable.
    #[test]
    fn rpc_decodes_engine_error_on_http_error_status() {
        let dir = std::env::temp_dir().join(format!("kgk-rpc-{}", std::process::id()));
        let port = serve_once(
            "HTTP/1.1 401 Unauthorized",
            r#"{"ok":false,"error":"bad token"}"#,
        );
        let engine = engine_with_lock(&dir, port);
        let error = engine.rpc("demo.op", json!({})).unwrap_err();
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(error, "Engine отклонил операцию: bad token");
    }

    #[test]
    fn status_decodes_engine_error_on_http_error_status() {
        let dir = std::env::temp_dir().join(format!("kgk-status-{}", std::process::id()));
        let port = serve_once(
            "HTTP/1.1 503 Service Unavailable",
            r#"{"ok":false,"error":"booting"}"#,
        );
        let engine = engine_with_lock(&dir, port);
        let error = engine.status("health").unwrap_err();
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(error, "Engine отклонил запрос состояния");
    }
}
