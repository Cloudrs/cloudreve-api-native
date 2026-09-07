use napi::{Env, JsFunction, JsObject, threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode}};
use napi_derive::napi;
use std::{fs, future::Future, io::{Read, Seek, SeekFrom}, sync::{Mutex, OnceLock}};
use base64::Engine;
use cloudreve_api::{
    ApiVersion, CloudreveAPI, Error as ApiError, LoginResponse,
    api::v3::models::{
        Aria2CreateRequest, DeleteObjectRequest, MoveObjectRequest,
        CopyObjectRequest, RenameObjectRequest, SourceItems,
        UploadFileRequest, CreateFileRequest,
    },
    api::v4::{
        ApiV4Client,
        models::{
            FileType as V4FileType,
            ApiResponse as V4ApiResponse,
            CreateUploadSessionRequest, CreateDownloadRequest, CreateDownloadUrlRequest,
            TaskStatus, TaskType, TaskListResponse,
            CreateShareLinkRequest, PermissionSetting,
            CreateArchiveRequest, ExtractArchiveRequest,
            RefreshTokenRequest,
            MoveFileRequest as V4MoveFileRequest,
            CreateFileRequest as V4CreateFileRequest, CreateFileType as V4CreateFileType,
            DeleteFileRequest as V4DeleteFileRequest, UnlockFilesRequest,
            ListFilesRequest as V4ListFilesRequest, File as V4File,
        },
        uri::{path_to_uri as v4_path_to_uri, search_uri as v4_search_uri},
    },
    cloudreve_api::{SiteConfigValue, FileList, FileListAll, DeleteResult, ItemFailure, TransferResult},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

// ---- v3-compatible DirectoryInfo structs for ArkTS ----

#[derive(Serialize)]
struct ApiObjectInfo {
    id: String,
    name: String,
    path: String,
    thumb: bool,
    size: i64,
    #[serde(rename = "type")]
    object_type: &'static str,
    date: String,
    create_date: String,
    source_enabled: bool,
}

#[derive(Serialize)]
struct ApiPolicy {
    id: String,
    name: String,
    #[serde(rename = "type")]
    policy_type: String,
    max_size: i64,
    file_type: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct ApiDirectoryInfo {
    parent: String,
    objects: Vec<ApiObjectInfo>,
    policy: ApiPolicy,
}

#[derive(Serialize)]
struct ApiUserSetting {
    uid: i64,
    authn: Vec<String>,
    homepage: bool,
    prefer_theme: String,
    themes: String,
    two_factor: bool,
}

#[derive(Serialize)]
struct ApiObjectDetail {
    created_at: String,
    updated_at: String,
    policy: String,
    size: i64,
    child_folder_num: i64,
    child_file_num: i64,
    path: String,
    query_date: String,
}

#[derive(Debug, Deserialize)]
struct V4ObjectFolderSummary {
    #[serde(default)]
    size: i64,
    #[serde(default)]
    files: i64,
    #[serde(default)]
    folders: i64,
}

#[derive(Debug, Deserialize)]
struct V4ObjectStoragePolicy {
    #[serde(default)]
    name: String,
}

#[derive(Debug, Deserialize)]
struct V4ObjectExtendedInfo {
    #[serde(default)]
    storage_policy: Option<V4ObjectStoragePolicy>,
}

#[derive(Debug, Deserialize)]
struct V4ObjectDetailResponse {
    created_at: String,
    updated_at: String,
    #[serde(default)]
    size: i64,
    path: String,
    #[serde(default)]
    folder_summary: Option<V4ObjectFolderSummary>,
    #[serde(default)]
    extended_info: Option<V4ObjectExtendedInfo>,
}

// Decode percent-encoded URI path and strip the cloudreve URI prefix.
// Handles both "cloudreve://my/..." and "cloudreve://{user}@my/..." formats.
// In the @my format, the path component uses %2F as the path separator,
// e.g. cloudreve://KZHZ@my/%2Fpackages → /packages
fn v4_uri_to_unix(uri: &str) -> String {
    let rest = uri.trim_start_matches("cloudreve://");
    // Find where the path starts: after "@my" or after "my"
    let encoded = if let Some(idx) = rest.find("@my") {
        &rest[idx + 3..] // skip "@my"
    } else if rest.starts_with("my") {
        &rest[2..] // skip "my"
    } else {
        rest
    };
    let src = encoded.as_bytes();
    let mut bytes: Vec<u8> = Vec::with_capacity(src.len());
    let mut i = 0;
    while i < src.len() {
        if src[i] == b'%' && i + 2 < src.len() {
            if let Ok(hex) = std::str::from_utf8(&src[i + 1..i + 3]) {
                if let Ok(b) = u8::from_str_radix(hex, 16) {
                    bytes.push(b);
                    i += 3;
                    continue;
                }
            }
        }
        bytes.push(src[i]);
        i += 1;
    }
    let decoded = String::from_utf8_lossy(&bytes).into_owned();
    // The @my format produces "//path" after decoding (leading / + decoded %2F = //)
    // Collapse leading double-slash to single slash
    if decoded.starts_with("//") {
        decoded[1..].to_string()
    } else if decoded.is_empty() {
        "/".to_string()
    } else {
        decoded
    }
}

fn supports_thumbnail(name: &str) -> bool {
    let ext = name.rsplit('.').next().unwrap_or("").to_lowercase();
    matches!(
        ext.as_str(),
        "jpg"
            | "jpeg"
            | "png"
            | "gif"
            | "webp"
            | "bmp"
            | "heic"
            | "heif"
            | "tiff"
            | "tif"
            | "avif"
            | "mp4"
            | "mkv"
            | "mov"
            | "wmv"
            | "flv"
            | "avi"
            | "rmvb"
            | "mpg"
            | "mpeg"
            | "m4v"
            | "webm"
            | "3gp"
    )
}

fn encode_query_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => {
                encoded.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    encoded
}

fn share_next_page_token(data: &serde_json::Value) -> Option<String> {
    data.get("pagination")
        .and_then(|pagination| {
            pagination.get("next_token")
                .or_else(|| pagination.get("next_page_token"))
        })
        .and_then(|token| token.as_str())
        .filter(|token| !token.is_empty())
        .map(|token| token.to_string())
}

async fn enrich_share_source_uri(v4: &ApiV4Client, share: &mut serde_json::Value) {
    let has_source_uri = share
        .get("source_uri")
        .and_then(|value| value.as_str())
        .map(|value| !value.is_empty())
        .unwrap_or(false);
    if has_source_uri {
        return;
    }

    let share_id = match share.get("id").and_then(|value| value.as_str()) {
        Some(id) if !id.is_empty() => id,
        _ => return,
    };
    let password = share
        .get("password")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty());

    let mut endpoint = format!(
        "/share/info/{}?owner_extended=true&count_views=false",
        encode_query_component(share_id)
    );
    if let Some(password) = password {
        endpoint.push_str("&password=");
        endpoint.push_str(&encode_query_component(password));
    }

    let detail: Result<V4ApiResponse<serde_json::Value>, _> = v4.get(&endpoint).await;
    if let Ok(resp) = detail {
        if let Some(detail_data) = resp.data {
            if let (Some(share_obj), Some(detail_obj)) = (share.as_object_mut(), detail_data.as_object()) {
                for (key, value) in detail_obj {
                    share_obj.insert(key.clone(), value.clone());
                }
            }
        }
    }
}

// Get parent directory from a unix-style path (e.g. "/videos" → "/").
fn unix_parent(full_path: &str) -> String {
    match full_path.rfind('/') {
        None | Some(0) => "/".to_string(),
        Some(idx) => full_path[..idx].to_string(),
    }
}

fn remote_parent(full_path: &str) -> String {
    match full_path.rfind('/') {
        None | Some(0) => "/".to_string(),
        Some(idx) => full_path[..idx].to_string(),
    }
}

async fn ensure_remote_directory(api: &CloudreveAPI, dir: &str) -> Result<(), ApiError> {
    if dir.is_empty() || dir == "/" {
        return Ok(());
    }

    let mut current = String::new();
    for part in dir.split('/').filter(|p| !p.is_empty()) {
        current.push('/');
        current.push_str(part);
        if api.list_files(&current, None, None).await.is_ok() {
            continue;
        }

        api.create_directory(&current).await?;
    }

    Ok(())
}

async fn resolve_upload_policy_id(api: &CloudreveAPI, dir: &str) -> Option<String> {
    match api.list_files(dir, None, None).await {
        Ok(FileList::V4(v4)) => v4.storage_policy.map(|policy| policy.id),
        _ => None,
    }
}

/// Extract a numeric size from a serde_json props object, trying multiple keys and both int/float.
fn extract_size(props: Option<&serde_json::Value>) -> i64 {
    let Some(p) = props else { return 0 };
    for key in &["size", "total", "total_size", "file_size", "length"] {
        if let Some(v) = p.get(key) {
            if let Some(n) = json_value_as_i64(v) { return n; }
        }
    }
    0
}

fn json_value_as_i64(value: &serde_json::Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().map(|n| n as i64))
        .or_else(|| value.as_f64().map(|n| n as i64))
        .or_else(|| {
            value
                .as_str()
                .and_then(|s| s.trim().parse::<f64>().ok())
                .map(|n| n as i64)
        })
}

fn json_number_for_keys(value: &serde_json::Value, keys: &[&str]) -> Option<i64> {
    if let Some(number) = json_value_as_i64(value) {
        return Some(number);
    }

    if let serde_json::Value::Object(map) = value {
        for key in keys {
            if let Some(value) = map.get(*key) {
                if let Some(found) = json_value_as_i64(value) {
                    return Some(found);
                }
            }
        }

        for value in map.values() {
            if let Some(found) = json_number_for_keys(value, keys) {
                return Some(found);
            }
        }
    } else if let serde_json::Value::Array(values) = value {
        for value in values {
            if let Some(found) = json_number_for_keys(value, keys) {
                return Some(found);
            }
        }
    }
    None
}

fn json_value_first_string(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() || trimmed == "." {
                None
            } else if (trimmed.starts_with('[') && trimmed.ends_with(']')) ||
                (trimmed.starts_with('{') && trimmed.ends_with('}')) {
                serde_json::from_str::<serde_json::Value>(trimmed)
                    .ok()
                    .and_then(|parsed| json_value_first_string(&parsed))
                    .or_else(|| Some(trimmed.to_string()))
            } else {
                Some(trimmed.to_string())
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                if let Some(found) = json_value_first_string(value) {
                    return Some(found);
                }
            }
            None
        }
        serde_json::Value::Object(map) => {
            for key in &["name", "src", "url", "src_str", "url_str", "source", "source_url", "uri", "urls"] {
                if let Some(value) = map.get(*key) {
                    if let Some(found) = json_value_first_string(value) {
                        return Some(found);
                    }
                }
            }
            for value in map.values() {
                if let Some(found) = json_value_first_string(value) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}

fn json_value_string_for_keys(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    if let serde_json::Value::Object(map) = value {
        for key in keys {
            if let Some(value) = map.get(*key) {
                if let Some(found) = json_value_first_string(value) {
                    return Some(found);
                }
            }
        }

        for value in map.values() {
            if let Some(found) = json_value_string_for_keys(value, keys) {
                return Some(found);
            }
        }
    } else if let serde_json::Value::Array(values) = value {
        for value in values {
            if let Some(found) = json_value_string_for_keys(value, keys) {
                return Some(found);
            }
        }
    }
    None
}

/// Extract filename from a URL (strip query string and take last path segment).
fn filename_from_url_or_str(s: &str) -> String {
    let without_query = s.split('?').next().unwrap_or(s).trim();
    let file_name = without_query
        .rsplit('/')
        .next()
        .filter(|f| !f.is_empty() && *f != ".")
        .unwrap_or(without_query);
    file_name.trim().trim_matches('"').to_string()
}

fn task_name_from_props(props: Option<&serde_json::Value>, dl: Option<&serde_json::Value>, task_id: &str) -> String {
    let name_keys = [
        "name",
        "file_name",
        "filename",
        "src",
        "src_str",
        "url",
        "url_str",
        "urls",
        "source",
        "source_url",
        "uri",
    ];
    let raw_name = dl
        .and_then(|d| json_value_string_for_keys(d, &name_keys))
        .or_else(|| props.and_then(|p| json_value_string_for_keys(p, &name_keys)))
        .unwrap_or_else(|| task_id.to_string());
    let name = filename_from_url_or_str(&raw_name);
    if name.is_empty() || name == "." {
        task_id.to_string()
    } else {
        name
    }
}

fn task_error_from_props(props: Option<&serde_json::Value>, fallback: Option<&str>) -> String {
    for key in &["error", "error_message", "message", "msg", "task_error"] {
        if let Some(value) = props.and_then(|p| p.get(*key)) {
            if let Some(found) = json_value_first_string(value) {
                return found;
            }
        }
    }
    fallback.unwrap_or("").to_string()
}

fn decode_v4_prop_path(val: &str) -> String {
    if val.starts_with("cloudreve://") {
        v4_uri_to_unix(val)
    } else {
        val.to_string()
    }
}

fn v4_task_status_to_i32(status: &TaskStatus) -> i32 {
    match status {
        TaskStatus::Queued => 0,
        TaskStatus::Processing | TaskStatus::Suspending => 1,
        TaskStatus::Error => -1,
        TaskStatus::Canceled => 2,
        TaskStatus::Completed => 4,
    }
}

fn v4_task_type_to_i32(task_type: &TaskType) -> i32 {
    match task_type {
        TaskType::RemoteDownload => 2,
        TaskType::Relocate => 4,
        _ => 3,
    }
}

// ---- Global state ----

static CLIENT: OnceLock<Mutex<Option<CloudreveAPI>>> = OnceLock::new();
static V4_REFRESH_TOKEN: OnceLock<Mutex<Option<String>>> = OnceLock::new();
/// Tokens minted by a silent 401 refresh, waiting for ETS to pick them up.
/// (access_token, refresh_token, refresh_expires)
static REFRESHED_SESSION: OnceLock<Mutex<Option<(String, String, String)>>> = OnceLock::new();
static HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

/// 共享的 reqwest 客户端。
///
/// 分片上传原先每片 `Client::new()`，连接池随客户端一起丢弃，于是每个分片都要重做
/// 一次 TCP + TLS 握手。相册备份 5 并发时这部分 TLS 计算相当可观，在手机上会和渲染
/// 抢核心。`Client` 内部是 Arc，clone 很便宜，复用即可共享连接池。
fn http_client() -> reqwest::Client {
    HTTP_CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                // 分片上传的 body 是流，reqwest 无法重放，所以拿到一条服务端已经关掉的
                // 空闲连接时不会自动重试，会直接报传输错误。取一个明显短于常见服务端
                // keep-alive（nginx 默认 75s）的空闲上限，既保留连接复用省下的 TLS 握手，
                // 又基本不会复用到已经失效的连接。
                .pool_idle_timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new())
        })
        .clone()
}

/// reqwest::Error 的 Display 只有一句概括（"error sending request for url (...)"），
/// 真正的原因在 source 链里。上传失败要靠这条信息定位，展开成一行返回给 ETS。
fn describe_error<E: std::error::Error>(err: E) -> String {
    let mut text = err.to_string();
    let mut cause = err.source();
    while let Some(inner) = cause {
        text.push_str(&format!(": {}", inner));
        cause = inner.source();
    }
    text
}

fn state() -> &'static Mutex<Option<CloudreveAPI>> {
    CLIENT.get_or_init(|| Mutex::new(None))
}

fn refresh_state() -> &'static Mutex<Option<String>> {
    V4_REFRESH_TOKEN.get_or_init(|| Mutex::new(None))
}

fn get_client() -> napi::Result<CloudreveAPI> {
    state()
        .lock()
        .unwrap()
        .clone()
        .ok_or_else(|| napi::Error::from_reason("not initialized: call init() first"))
}

fn set_client(api: CloudreveAPI) {
    *state().lock().unwrap() = Some(api);
}

fn get_v4_refresh() -> Option<String> {
    refresh_state().lock().unwrap().clone()
}

fn set_v4_refresh(token: Option<String>) {
    *refresh_state().lock().unwrap() = token;
}

fn refreshed_session_state() -> &'static Mutex<Option<(String, String, String)>> {
    REFRESHED_SESSION.get_or_init(|| Mutex::new(None))
}

/// Hand ETS the tokens produced by the most recent silent refresh, exactly once.
/// Returns [access_token, refresh_token, refresh_expires], or an empty array when
/// nothing has been refreshed since the last call.
///
/// A silent refresh only ever updated the in-process client, so a restart fell back
/// to whatever ETS had persisted — stale by then. ETS polls this after every API
/// call and writes the result to its database.
#[napi(js_name = "takeRefreshedSession")]
pub fn take_refreshed_session() -> Vec<String> {
    match refreshed_session_state().lock().unwrap().take() {
        Some((access, refresh, expires)) => vec![access, refresh, expires],
        None => Vec::new(),
    }
}

static V4_POLICY_ID: OnceLock<Mutex<Option<String>>> = OnceLock::new();

fn policy_state() -> &'static Mutex<Option<String>> {
    V4_POLICY_ID.get_or_init(|| Mutex::new(None))
}

fn get_v4_policy_id() -> Option<String> {
    policy_state().lock().unwrap().clone()
}

fn set_v4_policy_id(id: Option<String>) {
    *policy_state().lock().unwrap() = id;
}

/// Use the stored refresh_token to get new access/refresh tokens, update client in-place.
async fn do_v4_refresh() -> napi::Result<()> {
    let refresh_tok = get_v4_refresh()
        .ok_or_else(|| napi::Error::from_reason("Unauthorized: no refresh token stored"))?;

    let base_url = get_client()?.base_url().to_string();

    // Use a fresh unauthenticated client so the expired access_token doesn't interfere
    let temp = CloudreveAPI::with_version(&base_url, ApiVersion::V4)
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;

    let new_tok = temp.inner().as_v4()
        .ok_or_else(|| napi::Error::from_reason("Not a v4 client"))?
        .refresh_token(&cloudreve_api::api::v4::models::RefreshTokenRequest {
            refresh_token: &refresh_tok,
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("Token refresh failed: {}", e)))?;

    log::info!("V4 access token refreshed (len={})", new_tok.access_token.len());

    // Apply new tokens to stored client
    let mut api = get_client()?;
    if let Some(v4) = api.inner_mut().as_v4_mut() {
        v4.set_token(new_tok.access_token.clone());
        v4.set_refresh_token(new_tok.refresh_token.clone());
    }
    set_client(api);
    set_v4_refresh(Some(new_tok.refresh_token.clone()));
    *refreshed_session_state().lock().unwrap() = Some((
        new_tok.access_token,
        new_tok.refresh_token,
        new_tok.refresh_expires,
    ));
    Ok(())
}

fn to_napi_error(error: ApiError) -> napi::Error {
    napi::Error::from_reason(error.to_string())
}

async fn run_api_with_v4_refresh<T, F, Fut>(operation: F) -> napi::Result<T>
where
    F: Fn(CloudreveAPI) -> Fut,
    Fut: Future<Output = Result<T, ApiError>>,
{
    match operation(get_client()?).await {
        Ok(value) => Ok(value),
        Err(ApiError::Unauthorized(error)) => {
            if !get_client()?.inner().is_v4() {
                return Err(napi::Error::from_reason(
                    ApiError::Unauthorized(error).to_string(),
                ));
            }
            do_v4_refresh().await?;
            operation(get_client()?).await.map_err(to_napi_error)
        }
        Err(error) => Err(to_napi_error(error)),
    }
}

// ---- Init / session ----

/// Connect to a Cloudreve server, auto-detect v3/v4. Returns "v3" or "v4".
#[napi]
pub async fn init(base_url: String) -> napi::Result<String> {
    let api = CloudreveAPI::new(&base_url)
        .await
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let version = api.api_version().as_str().to_string();
    set_client(api);
    Ok(version)
}

/// Return [apiVersion, serverVersion] for the current connected server.
/// apiVersion is "v3" or "v4"; serverVersion comes from /site/ping and may be
/// "unknown" if the server doesn't expose it (or the call times out). The
/// ping is capped at 5s so the About page never stalls on a flaky server.
#[napi]
pub async fn get_api_version_info() -> napi::Result<Vec<String>> {
    let api = get_client()?;
    let api_version = api.api_version().as_str().to_string();
    let server_version = if let Some(v3) = api.inner().as_v3() {
        ping_with_timeout(v3.ping()).await
    } else if let Some(v4) = api.inner().as_v4() {
        ping_with_timeout(v4.ping()).await
    } else {
        "unknown".to_string()
    };
    Ok(vec![api_version, server_version])
}

async fn ping_with_timeout<F>(fut: F) -> String
where
    F: std::future::Future<Output = Result<String, cloudreve_api::Error>>,
{
    match tokio::time::timeout(std::time::Duration::from_secs(5), fut).await {
        Ok(Ok(version)) => version,
        _ => "unknown".to_string(),
    }
}

/// Restore a saved session without re-authenticating.
/// For v3: `access_token` is the raw session cookie value (or "cloudreve-session=VALUE").
/// For v4: `access_token` is the JWT access token, `refresh_token` is the refresh token.
#[napi]
pub fn restore_session(base_url: String, access_token: String, refresh_token: String, is_v3: bool) -> napi::Result<()> {
    // Drop anything the previous account's silent refresh left behind: once we switch
    // accounts those tokens must never reach the incoming account's stored credentials.
    refreshed_session_state().lock().unwrap().take();
    let version = if is_v3 { ApiVersion::V3 } else { ApiVersion::V4 };
    let mut api = CloudreveAPI::with_version(&base_url, version)
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    if is_v3 {
        if let Some(v3) = api.inner_mut().as_v3_mut() {
            let val = access_token
                .strip_prefix("cloudreve-session=")
                .unwrap_or(&access_token)
                .to_string();
            v3.set_session_cookie(val);
        }
    } else {
        let token = access_token
            .strip_prefix("v4:")
            .unwrap_or(&access_token)
            .to_string();
        api.set_token(&token)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        if !refresh_token.is_empty() {
            if let Some(v4) = api.inner_mut().as_v4_mut() {
                v4.set_refresh_token(refresh_token.clone());
            }
            set_v4_refresh(Some(refresh_token));
        }
    }
    set_client(api);
    Ok(())
}

// ---- Auth ----

/// (access_token, refresh_token, refresh_expires). The expiry is what the server
/// actually grants the refresh token; without it the client has to guess, and a
/// guessed lifetime makes the stored credential look valid long after it is not.
/// v3 has no refresh token, so it yields an empty expiry and the client falls back.
fn extract_v4_tokens(response: &LoginResponse) -> (String, String, String) {
    match response {
        LoginResponse::V4(r) => (
            r.token.access_token.clone(),
            r.token.refresh_token.clone(),
            r.token.refresh_expires.clone(),
        ),
        _ => (String::new(), String::new(), String::new()),
    }
}

/// Login with a v4 refresh token. Returns [userJson, access_token, refresh_token, "v4", refresh_expires].
#[napi(js_name = "loginWithRefreshToken")]
pub async fn login_with_refresh_token(base_url: String, refresh_token: String) -> napi::Result<Vec<String>> {
    let mut api = CloudreveAPI::with_version(&base_url, ApiVersion::V4)
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;

    let token = api
        .inner()
        .as_v4()
        .ok_or_else(|| napi::Error::from_reason("Not a v4 client"))?
        .refresh_token(&RefreshTokenRequest {
            refresh_token: &refresh_token,
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("Token refresh failed: {}", e)))?;

    let access_token = token.access_token.clone();
    let next_refresh_token = token.refresh_token.clone();
    let refresh_expires = token.refresh_expires.clone();

    if let Some(v4) = api.inner_mut().as_v4_mut() {
        v4.set_token(access_token.clone());
        v4.set_refresh_token(next_refresh_token.clone());
    }
    set_v4_refresh(Some(next_refresh_token.clone()));

    let user_json = match api.get_site_config(None).await {
        Ok(SiteConfigValue::V4(cfg)) => match cfg.user {
            Some(user) => serde_json::to_string(&user).unwrap_or_else(|_| json!({}).to_string()),
            None => json!({}).to_string(),
        },
        Ok(_) => json!({}).to_string(),
        Err(err) => {
            log::warn!("load user info after refresh login failed: {}", err);
            json!({}).to_string()
        }
    };

    set_client(api);
    Ok(vec![
        user_json,
        access_token,
        next_refresh_token,
        "v4".to_string(),
        refresh_expires,
    ])
}

/// Login. Returns [userJson, access_token, refresh_token, "v3"/"v4", refresh_expires].
/// When 2FA is required, returns ["2fa_required", "", "", "v3"/"v4", ""].
#[napi]
pub async fn login(username: String, password: String) -> napi::Result<Vec<String>> {
    let mut api = get_client()?;
    match api.login(&username, &password).await {
        Ok(response) => {
            let v3 = api.inner().is_v3();
            let (access_token, refresh_token, refresh_expires) = if v3 {
                (
                    api.get_session_cookie().unwrap_or_default(),
                    String::new(),
                    String::new(),
                )
            } else {
                let tokens = extract_v4_tokens(&response);
                set_v4_refresh(Some(tokens.1.clone()));
                tokens
            };
            let user_json = match &response {
                LoginResponse::V3(r) => serde_json::to_string(&r.user),
                LoginResponse::V4(r) => serde_json::to_string(&r.user),
            }
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
            set_client(api);
            Ok(vec![
                user_json,
                access_token,
                refresh_token,
                if v3 { "v3" } else { "v4" }.to_string(),
                refresh_expires,
            ])
        }
        Err(ApiError::TwoFactorRequired(_)) => {
            let v3 = api.inner().is_v3();
            set_client(api);
            Ok(vec![
                "2fa_required".to_string(),
                String::new(),
                String::new(),
                if v3 { "v3" } else { "v4" }.to_string(),
                String::new(),
            ])
        }
        Err(e) => Err(napi::Error::from_reason(e.to_string())),
    }
}

/// Submit 2FA OTP code. Returns [userJson, access_token, refresh_token, "v3"/"v4", refresh_expires].
#[napi(js_name = "login2fa")]
pub async fn login_2fa(code: String) -> napi::Result<Vec<String>> {
    let mut api = get_client()?;
    let response = api
        .login_2fa(&code)
        .await
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let v3 = api.inner().is_v3();
    let (access_token, refresh_token, refresh_expires) = if v3 {
        (
            api.get_session_cookie().unwrap_or_default(),
            String::new(),
            String::new(),
        )
    } else {
        let tokens = extract_v4_tokens(&response);
        set_v4_refresh(Some(tokens.1.clone()));
        tokens
    };
    let user_json = match &response {
        LoginResponse::V3(r) => serde_json::to_string(&r.user),
        LoginResponse::V4(r) => serde_json::to_string(&r.user),
    }
    .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    set_client(api);
    Ok(vec![
        user_json,
        access_token,
        refresh_token,
        if v3 { "v3" } else { "v4" }.to_string(),
        refresh_expires,
    ])
}

// ---- Site ----

#[napi]
pub async fn get_site_config() -> napi::Result<String> {
    let api = get_client()?;
    let cfg = api
        .get_site_config(None)
        .await
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    match cfg {
        SiteConfigValue::V3(c) => serde_json::to_string(&c),
        SiteConfigValue::V4(c) => serde_json::to_string(&*c),
    }
    .map_err(|e| napi::Error::from_reason(e.to_string()))
}

// ---- User ----

#[napi]
pub async fn get_user_storage() -> napi::Result<String> {
    let api = get_client()?;
    let quota = api
        .get_storage_quota()
        .await
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let j = json!({ "used": quota.used, "total": quota.total, "free": quota.free });
    Ok(j.to_string())
}

#[napi]
pub async fn get_user_setting() -> napi::Result<String> {
    let api = get_client()?;
    if let Some(v3) = api.inner().as_v3() {
        // 早先 crate 把 /user/setting 解成 /user/storage 的 StorageInfo，
        // 每次进「我的」页都稳定报 missing field `used`。现在类型对了，
        // 这里统一映射成和下面 V4 分支一样的形状，ETS 侧只认这一种。
        let setting = v3
            .get_user_settings()
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let mapped = ApiUserSetting {
            uid: setting.uid,
            authn: setting
                .authn
                .iter()
                .map(|credential| credential.id.clone())
                .collect(),
            homepage: setting.homepage,
            prefer_theme: setting.prefer_theme,
            themes: setting.themes,
            two_factor: setting.two_factor,
        };
        serde_json::to_string(&mapped).map_err(|e| napi::Error::from_reason(e.to_string()))
    } else {
        let v4 = api.inner().as_v4()
            .ok_or_else(|| napi::Error::from_reason("not a v4 client"))?;
        let settings = v4
            .get_user_setting()
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let mapped = ApiUserSetting {
            uid: 0,
            authn: settings.passkeys.map(|p| p.iter().map(|k| k.id.clone()).collect()).unwrap_or_default(),
            homepage: false,
            prefer_theme: String::new(),
            themes: String::new(),
            two_factor: settings.two_fa_enabled,
        };
        serde_json::to_string(&mapped).map_err(|e| napi::Error::from_reason(e.to_string()))
    }
}

/// 头像同样要能在 access token 过期后自愈：它不走 CloudreveAPI 的封装
/// （要的是原始字节，不是 JSON），所以 run_api_with_v4_refresh 套不上，
/// 这里照 get_download_uri 的写法手动刷新重试一次。
/// 返回 Uint8Array 而不是 Vec<u8>：napi-rs v2 把 Vec<u8> 映射成 JS 的普通数组，
/// ArkTS 侧 image.createImageSource 拿到它会直接返回 undefined。
#[napi]
pub async fn get_user_avatar(user_id: String) -> napi::Result<napi::bindgen_prelude::Uint8Array> {
    let api = get_client()?;
    let result = fetch_user_avatar(&api, &user_id).await;
    match result {
        Err(_) if api.inner().is_v4() => {
            if do_v4_refresh().await.is_ok() {
                let api2 = get_client()?;
                fetch_user_avatar(&api2, &user_id).await
            } else {
                result
            }
        }
        other => other,
    }
}

async fn fetch_user_avatar(
    api: &CloudreveAPI,
    user_id: &str,
) -> napi::Result<napi::bindgen_prelude::Uint8Array> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;

    let request = if let Some(v3) = api.inner().as_v3() {
        let url = format!(
            "{}/api/v3/user/avatar/{}/l",
            v3.base_url.trim_end_matches('/'),
            user_id
        );
        let cookie = api.get_session_cookie().unwrap_or_default();
        let cookie_header = if cookie.starts_with("cloudreve-session=") {
            cookie
        } else {
            format!("cloudreve-session={}", cookie)
        };
        client.get(url).header(reqwest::header::COOKIE, cookie_header)
    } else {
        let v4 = api.inner().as_v4()
            .ok_or_else(|| napi::Error::from_reason("not a v4 client"))?;
        let url = format!(
            "{}/api/v4/user/avatar/{}?nocache=true",
            v4.base_url.trim_end_matches('/'),
            user_id
        );
        let mut request = client.get(url);
        if let Some(token) = &v4.token {
            request = request.bearer_auth(token);
        }
        request
    };

    let response = request
        .send()
        .await
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let status = response.status();
    if !status.is_success() {
        return Err(napi::Error::from_reason(format!(
            "avatar request failed: {}",
            status
        )));
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    Ok(napi::bindgen_prelude::Uint8Array::new(bytes.to_vec()))
}

// ---- Directory / Files ----

#[napi]
pub async fn get_directory(path: String) -> napi::Result<String> {
    let files = run_api_with_v4_refresh(|api| {
        let path = path.clone();
        async move { api.list_files_all(&path, None).await }
    }).await?;
    match files {
        FileListAll::V3(dir) => serde_json::to_string(&dir),
        FileListAll::V4(v4) => {
            // Cache the policy id for use in upload
            if let Some(policy) = &v4.storage_policy {
                set_v4_policy_id(Some(policy.id.clone()));
            }

            let objects: Vec<ApiObjectInfo> = v4.files.iter().map(|f| {
                let is_dir = matches!(f.r#type, V4FileType::Folder);
                let unix_path = v4_uri_to_unix(&f.path);
                let parent_path = unix_parent(&unix_path);
                ApiObjectInfo {
                    id: unix_path.clone(),   // full path — used by delete/move/copy/rename/download
                    name: f.name.clone(),
                    path: parent_path,       // parent dir — matches V3 convention
                    // Cloudreve v4 can generate thumbnails for both images and
                    // videos. The API does not expose a per-file availability
                    // flag, so mark supported media as candidates and let the
                    // thumbnail request fall back to the file-type icon when the
                    // server cannot generate one.
                    thumb: !is_dir && supports_thumbnail(&f.name),
                    size: f.size,
                    object_type: if is_dir { "dir" } else { "file" },
                    date: f.updated_at.clone(),
                    create_date: f.created_at.clone(),
                    source_enabled: false,
                }
            }).collect();

            let policy = v4.storage_policy.as_ref()
                .map(|p| ApiPolicy {
                    id: p.id.clone(),
                    name: p.name.clone(),
                    policy_type: p.type_.clone(),
                    max_size: p.max_size as i64,
                    file_type: None,
                })
                .unwrap_or_else(|| ApiPolicy {
                    id: String::new(),
                    name: String::new(),
                    policy_type: String::new(),
                    max_size: 0,
                    file_type: None,
                });

            let parent_unix = v4_uri_to_unix(&v4.parent.path);
            let dir_info = ApiDirectoryInfo { parent: parent_unix, objects, policy };
            serde_json::to_string(&dir_info)
        }
    }
    .map_err(|e| napi::Error::from_reason(e.to_string()))
}

#[napi]
pub async fn get_object_detail(id: String, is_folder: bool) -> napi::Result<String> {
    let api = get_client()?;
    if let Some(v3) = api.inner().as_v3() {
        let prop = v3
            .get_object_property(&id, Some(is_folder), Some(false))
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        serde_json::to_string(&prop).map_err(|e| napi::Error::from_reason(e.to_string()))
    } else {
        // id is the unix path (e.g., /videos/movie.mp4)
        let file = run_api_with_v4_refresh(|api| {
            let id = id.clone();
            async move {
                let uri = encode_query_component(&v4_path_to_uri(&id));
                let endpoint = if is_folder {
                    format!("/file/info?uri={}&extended=true&folder_summary=true", uri)
                } else {
                    format!("/file/info?uri={}&extended=true", uri)
                };
                let response: V4ApiResponse<V4ObjectDetailResponse> = api.inner()
                    .as_v4()
                    .expect("v4 client")
                    .get(&endpoint)
                    .await?;
                let response_debug = format!("{:?}", response);
                response.data.ok_or_else(|| {
                    ApiError::InvalidResponse(format!(
                        "API returned no data for get_object_detail request: {}",
                        response_debug
                    ))
                })
            }
        }).await?;
        let folder_summary = file.folder_summary.as_ref();
        let policy = file
            .extended_info
            .as_ref()
            .and_then(|info| info.storage_policy.as_ref())
            .map(|policy| policy.name.clone())
            .unwrap_or_default();
        let detail = ApiObjectDetail {
            created_at: file.created_at.clone(),
            updated_at: file.updated_at.clone(),
            policy,
            size: folder_summary.map(|summary| summary.size).unwrap_or(file.size),
            child_folder_num: folder_summary.map(|summary| summary.folders).unwrap_or(0),
            child_file_num: folder_summary.map(|summary| summary.files).unwrap_or(0),
            path: v4_uri_to_unix(&file.path),
            query_date: file.updated_at.clone(),
        };
        serde_json::to_string(&detail).map_err(|e| napi::Error::from_reason(e.to_string()))
    }
}

// ---- Object operations ----

/// V4 下 items/dirs 里装的是完整路径（见 ApiObjectInfo.id），正好是 crate
/// 批量接口要的形状，锁冲突剥离和 aggregated_error 分账都由它负责。
async fn v4_do_delete(api: &CloudreveAPI, items: &[String], dirs: &[String]) -> Result<(), ApiError> {
    let paths: Vec<&str> = items.iter().chain(dirs.iter()).map(String::as_str).collect();
    if paths.is_empty() {
        return Ok(());
    }
    let result = api.batch_delete(&paths).await?;
    if batch_auth_expired(&result.errors) {
        return Err(ApiError::Unauthorized("Login required".to_string()));
    }
    match result.errors.first() {
        None => Ok(()),
        Some(failure) => Err(ApiError::Api {
            code: failure.code.unwrap_or(-1),
            message: failure.message.clone(),
        }),
    }
}

async fn v4_create_object(v4: &ApiV4Client, path: &str, object_type: &str) -> Result<(), ApiError> {
    let request = V4CreateFileRequest {
        uri: path,
        r#type: if object_type == "folder" {
            V4CreateFileType::Folder
        } else {
            V4CreateFileType::File
        },
        metadata: None,
        // 重名时直接报错，交给上层去问用户，而不是静默返回既有对象
        err_on_conflict: Some(true),
    };
    v4.create_file(&request).await.map(|_| ())
}

// dst is always a destination *directory*; call the raw /file/move endpoint directly.
// The crate's high-level move_file/copy_file treat a dst whose parent equals the
// source parent as a rename, which breaks moving into a sibling folder.
async fn v4_do_move_or_copy(api: &CloudreveAPI, items: &[String], dirs: &[String], dst: &str, is_copy: bool) -> Result<(), ApiError> {
    let v4 = api.inner().as_v4().ok_or_else(|| {
        ApiError::UnsupportedFeature("move/copy".to_string(), "non-v4".to_string())
    })?;
    let uris: Vec<String> = items.iter().chain(dirs.iter())
        .map(|p| v4_path_to_uri(p))
        .collect();
    if uris.is_empty() {
        return Ok(());
    }
    let dst_uri = v4_path_to_uri(dst);
    let request = V4MoveFileRequest {
        uris: uris.iter().map(String::as_str).collect(),
        dst: &dst_uri,
        copy: if is_copy { Some(true) } else { None },
    };
    v4.move_file(&request).await
}

async fn v4_do_move(api: &CloudreveAPI, items: &[String], dirs: &[String], dst: &str) -> Result<(), ApiError> {
    v4_do_move_or_copy(api, items, dirs, dst, false).await
}

async fn v4_do_copy(api: &CloudreveAPI, items: &[String], dirs: &[String], dst: &str) -> Result<(), ApiError> {
    v4_do_move_or_copy(api, items, dirs, dst, true).await
}

#[napi]
pub async fn delete_objects(items: Vec<String>, dirs: Vec<String>) -> napi::Result<()> {
    let api = get_client()?;
    if let Some(v3) = api.inner().as_v3() {
        let req = DeleteObjectRequest {
            items: items.iter().map(String::as_str).collect(),
            dirs: dirs.iter().map(String::as_str).collect(),
            force: false,
            unlink: false,
        };
        v3.delete_object(&req)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    } else {
        run_api_with_v4_refresh(|api| {
            let items = items.clone();
            let dirs = dirs.clone();
            async move { v4_do_delete(&api, &items, &dirs).await }
        }).await
    }
}

// ---- 锁感知删除 / 解锁 / metadata 点查 / 搜索 ----
//
// 这四件事以前留在 ArkTS 侧裸发 HTTP，因为 crate 的封装满足不了：
// delete_file 只返回 Result<(), Error>，而 Error::Api 只带 code+message，
// 40073 响应体里的锁冲突列表（data）在那一层就丢了，拿不到就没法"解除占用"。
// 结果是这条链路绕开了 run_api_with_v4_refresh，access token 一过期就永久失败。
// 现在统一挪回 native，全部套刷新重试。

/// V4 在登录态失效时的表现并不一致：GET /file/info 这类返回 code=401，
/// 而 DELETE /api/v4/file 会先去解析 uri，失败后返回 40081 + "Login required"。
/// 两种都得认成失效，否则 run_api_with_v4_refresh 不会刷新 token。
fn v4_auth_expired(code: i32, msg: &str) -> bool {
    code == 401 || msg.to_ascii_lowercase().contains("login required")
}

/// crate 只把 code=401 认成 Unauthorized，40081 + "Login required" 那条会原样
/// 上抛成普通业务错误。不在这里翻译一下，刷新重试就轮不到执行，token 一过期
/// 删除又会永久失败。批量错误还得往子项里看——外层 msg 只是 "One or more
/// operation failed"，真正的 "Login required" 在每个 uri 自己的条目里。
fn normalize_v4_auth_error(err: ApiError) -> ApiError {
    let expired = match &err {
        ApiError::Api { code, message }
        | ApiError::ApiWithData { code, message, .. } => v4_auth_expired(*code, message),
        ApiError::Aggregate { code, message, errors } => {
            v4_auth_expired(*code, message)
                || errors.values().any(|item| v4_auth_expired(item.code, &item.msg))
        }
        _ => false,
    };
    if expired {
        ApiError::Unauthorized(err.message().unwrap_or("Login required").to_string())
    } else {
        err
    }
}

/// 同样的判断，用在那些把失败摊平进结果里、不走 Err 的批量返回上。
/// 全部条目都是登录失效才算——个别文件的古怪消息不该触发整体刷新。
fn batch_auth_expired(failures: &[ItemFailure]) -> bool {
    !failures.is_empty()
        && failures
            .iter()
            .all(|item| v4_auth_expired(item.code.unwrap_or(0), &item.message))
}

fn v4_client_of<'a>(api: &'a CloudreveAPI, feature: &str) -> Result<&'a ApiV4Client, ApiError> {
    api.inner()
        .as_v4()
        .ok_or_else(|| ApiError::UnsupportedFeature(feature.to_string(), "non-v4".to_string()))
}

/// 发一个 V4 业务请求，把整包 {code,msg,data} 原样交回调用方。
/// 与 crate 的封装不同，这里不把非 0 的 code 折叠成 Error——data 得留给上层。
async fn v4_call_json(
    v4: &ApiV4Client,
    method: reqwest::Method,
    endpoint: &str,
    body: Option<serde_json::Value>,
) -> Result<V4ApiResponse<serde_json::Value>, ApiError> {
    let url = format!("{}{}", v4.base_url.trim_end_matches('/'), endpoint);
    let mut request = v4.http_client.request(method, &url);
    if let Some(body) = body {
        request = request.json(&body);
    }
    if let Some(token) = &v4.token {
        request = request.bearer_auth(token);
    }
    let response: V4ApiResponse<serde_json::Value> = request.send().await?.json().await?;
    if v4_auth_expired(response.code, &response.msg) {
        return Err(ApiError::Unauthorized(response.msg));
    }
    Ok(response)
}

/// 锁感知删除单个对象。返回服务端原始 {code,msg,data} 的 JSON 字符串：
/// code=40073 时 data 里是冲突文件与解锁 token，上层据此决定要不要解除占用。
#[napi]
pub async fn delete_object_lock_aware(path: String) -> napi::Result<String> {
    run_api_with_v4_refresh(|api| {
        let path = path.clone();
        async move {
            let v4 = v4_client_of(&api, "lock-aware delete")?;
            let request = V4DeleteFileRequest {
                uris: vec![path.as_str()],
                unlink: None,
                skip_soft_delete: None,
            };
            match v4.delete_files(&request).await {
                Ok(()) => Ok(json!({ "code": 0, "msg": "", "data": null }).to_string()),
                Err(err) => {
                    // 登录失效要往上抛，好让外层刷新 token 重试；其余按原来的
                    // {code,msg,data} 形状交回 ArkTS——40073 的锁 token 就在 data 里。
                    let err = normalize_v4_auth_error(err);
                    if matches!(err, ApiError::Unauthorized(_)) {
                        return Err(err);
                    }
                    Ok(json!({
                        "code": err.code().unwrap_or(-1),
                        "msg": err.message().unwrap_or_default(),
                        "data": err.data().cloned().unwrap_or(serde_json::Value::Null),
                    })
                    .to_string())
                }
            }
        }
    })
    .await
}

/// 按 token 批量解除云端文件锁。
#[napi]
pub async fn unlock_files(tokens: Vec<String>) -> napi::Result<()> {
    run_api_with_v4_refresh(|api| {
        let tokens = tokens.clone();
        async move {
            let v4 = v4_client_of(&api, "unlock")?;
            let request = UnlockFilesRequest {
                tokens: tokens.iter().map(String::as_str).collect(),
            };
            v4.unlock_files(&request)
                .await
                .map_err(normalize_v4_auth_error)
        }
    })
    .await
}

/// 单文件 metadata 点查，用于"上传中"角标。返回 metadata 对象的 JSON，没有则返回 "{}"。
#[napi]
pub async fn get_file_metadata(path: String) -> napi::Result<String> {
    run_api_with_v4_refresh(|api| {
        let path = path.clone();
        async move {
            let v4 = v4_client_of(&api, "file metadata")?;
            let endpoint = format!(
                "/api/v4/file/info?uri={}",
                encode_query_component(&v4_path_to_uri(&path))
            );
            let response = v4_call_json(v4, reqwest::Method::GET, &endpoint, None).await?;
            if response.code != 0 {
                return Err(ApiError::Api {
                    code: response.code,
                    message: response.msg,
                });
            }
            let metadata = response
                .data
                .as_ref()
                .and_then(|data| data.get("metadata"))
                .cloned()
                .unwrap_or_else(|| json!({}));
            serde_json::to_string(&metadata).map_err(ApiError::from)
        }
    })
    .await
}

/// 搜索结果比目录列表多一个 metadata（缩略图可用性要看它），单开一个序列化结构。
#[derive(Serialize)]
struct ApiSearchObjectInfo {
    id: String,
    name: String,
    path: String,
    thumb: bool,
    size: i64,
    #[serde(rename = "type")]
    object_type: &'static str,
    date: String,
    create_date: String,
    source_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<serde_json::Value>,
}

const SEARCH_PAGE_SIZE: u32 = 2000;
/// 搜索结果按修改时间倒序：最近动过的排前面，符合"找我刚存的那个文件"的直觉
const SEARCH_ORDER_BY: &str = "updated_at";
const SEARCH_ORDER_DIRECTION: &str = "desc";
const SEARCH_MAX_PAGES: u32 = 100;

fn map_v4_search_file(file: &V4File) -> ApiSearchObjectInfo {
    let metadata = file.metadata.clone().filter(|m| !m.is_null());
    let is_dir = matches!(file.r#type, V4FileType::Folder);
    // 服务端不给"有没有缩略图"的标志，只有显式禁用时才带 thumb:disabled。
    let thumb_disabled = metadata
        .as_ref()
        .map(|m| m.get("thumb:disabled").is_some())
        .unwrap_or(false);
    // 还得按扩展名筛一道，和目录列表同一套判断。否则搜出一堆压缩包、文档时，
    // 每个都会去请求一次注定失败的缩略图。
    let thumb = !is_dir && !thumb_disabled && supports_thumbnail(&file.name);
    let unix_path = v4_uri_to_unix(&file.path);
    ApiSearchObjectInfo {
        // id 必须是全路径，和 get_directory 的约定一致：下载、缩略图、预览在 V4 上
        // 都是拿 id 去拼 cloudreve://my/<id>。以前 ArkTS 侧这里填的是服务端的文件 id，
        // 搜索结果的缩略图和预览一律报 "Path not exist"。
        id: unix_path.clone(),
        name: file.name.clone(),
        path: unix_parent(&unix_path),
        thumb,
        size: file.size,
        object_type: if is_dir { "dir" } else { "file" },
        date: if file.updated_at.is_empty() {
            file.created_at.clone()
        } else {
            file.updated_at.clone()
        },
        create_date: file.created_at.clone(),
        source_enabled: true,
        metadata,
    }
}

async fn v4_search(api: &CloudreveAPI, keyword: &str, path: &str) -> Result<String, ApiError> {
    let v4 = v4_client_of(api, "search")?;
    let mut objects: Vec<ApiSearchObjectInfo> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut next_page_token: Option<String> = None;

    // 搜索走的是列目录同一个接口，只是 uri 自带 query。这里直接用 list_files 而不是
    // crate 的 search_files，为的是能指定排序——后者不带排序参数，服务端会按自己的
    // 内部顺序返回，看上去就是乱的。
    let uri = v4_search_uri(path, keyword, true);

    for page in 0..SEARCH_MAX_PAGES {
        let response = v4
            .list_files(&V4ListFilesRequest {
                path: &uri,
                page: Some(page),
                page_size: Some(SEARCH_PAGE_SIZE),
                order_by: Some(SEARCH_ORDER_BY),
                order_direction: Some(SEARCH_ORDER_DIRECTION),
                next_page_token: next_page_token.as_deref(),
            })
            .await?;

        let returned = response.files.len();
        let before = objects.len();
        for file in &response.files {
            let mapped = map_v4_search_file(file);
            if seen.insert(mapped.id.clone()) {
                objects.push(mapped);
            }
        }
        next_page_token = response.pagination.next_token.clone();
        // 服务端给不满一页、整页都是已见过的、或没有下一页游标，就到头了。
        if (returned as u32) < SEARCH_PAGE_SIZE
            || objects.len() == before
            || next_page_token.is_none()
        {
            break;
        }
    }

    // order_by 只在服务端自己的分组内生效：实测同一目录的结果连续有序，一换目录就
    // 跳回最新重新倒序。全局顺序只能这里来定。
    // date 是 ISO 8601 且时区一致，直接按字符串倒序即可；取不到时间的（会是 1970）
    // 自然沉到末尾。
    objects.sort_by(|a, b| b.date.cmp(&a.date));

    serde_json::to_string(&objects).map_err(ApiError::from)
}

async fn v3_search(api: &CloudreveAPI, keyword: &str, path: &str) -> Result<String, ApiError> {
    let v3 = api
        .inner()
        .as_v3()
        .ok_or_else(|| ApiError::UnsupportedFeature("search".to_string(), "non-v3".to_string()))?;
    // V3 的 objects 已经是 ArkTS 那边 ObjectInfo 的形状，原样透传。
    let list = v3.search_files(keyword, path).await?;
    serde_json::to_string(&list.objects).map_err(ApiError::from)
}

/// 服务端搜索。返回 ObjectInfo[] 的 JSON 数组，V3 / V4 形状一致。
#[napi]
pub async fn search_files(keyword: String, path: String) -> napi::Result<String> {
    let api = get_client()?;
    if api.inner().as_v3().is_some() {
        return v3_search(&api, &keyword, &path)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()));
    }
    run_api_with_v4_refresh(|api| {
        let keyword = keyword.clone();
        let path = path.clone();
        async move { v4_search(&api, &keyword, &path).await }
    })
    .await
}

#[napi]
pub async fn move_objects(
    items: Vec<String>,
    dirs: Vec<String>,
    src_dir: String,
    dst: String,
) -> napi::Result<()> {
    let api = get_client()?;
    if let Some(v3) = api.inner().as_v3() {
        let src = SourceItems {
            items: items.iter().map(String::as_str).collect(),
            dirs: dirs.iter().map(String::as_str).collect(),
        };
        let req = MoveObjectRequest {
            action: "move",
            src_dir: &src_dir,
            src,
            dst: &dst,
        };
        v3.move_object(&req)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    } else {
        run_api_with_v4_refresh(|api| {
            let items = items.clone();
            let dirs = dirs.clone();
            let dst = dst.clone();
            async move { v4_do_move(&api, &items, &dirs, &dst).await }
        }).await
    }
}

#[napi]
pub async fn copy_objects(
    items: Vec<String>,
    dirs: Vec<String>,
    src_dir: String,
    dst: String,
) -> napi::Result<()> {
    let api = get_client()?;
    if let Some(v3) = api.inner().as_v3() {
        let src = SourceItems {
            items: items.iter().map(String::as_str).collect(),
            dirs: dirs.iter().map(String::as_str).collect(),
        };
        let req = CopyObjectRequest {
            src_dir: &src_dir,
            src,
            dst: &dst,
        };
        v3.copy_object(&req)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    } else {
        run_api_with_v4_refresh(|api| {
            let items = items.clone();
            let dirs = dirs.clone();
            let dst = dst.clone();
            async move { v4_do_copy(&api, &items, &dirs, &dst).await }
        }).await
    }
}

// ---- 批量删除 / 移动 / 复制 ----
//
// 服务端这三个接口本来就吃 uri 数组，一次请求能处理一整批。以前 ArkTS 侧
// 一个文件发一次请求，是因为整批只回一个成败、拿不到逐项结果；现在 crate
// 会把 aggregated_error 和锁冲突拆开回报，逐项信息就能一路带回 UI 了。

#[derive(Serialize)]
struct BatchFailure {
    path: String,
    code: i32,
    message: String,
    /// 40073 时服务端给的解锁 token
    tokens: Vec<String>,
}

#[derive(Serialize)]
struct BatchOutcome {
    succeeded: u32,
    failed: Vec<BatchFailure>,
}

/// 锁冲突条目里的 token；条目可能是单个对象，也可能是一组。
fn conflict_tokens(data: Option<&serde_json::Value>) -> Vec<String> {
    let Some(data) = data else {
        return Vec::new();
    };
    let entries = match data.as_array() {
        Some(items) => items.as_slice(),
        None => std::slice::from_ref(data),
    };
    entries
        .iter()
        .filter_map(|item| item.get("token").and_then(|t| t.as_str()))
        .map(str::to_string)
        .collect()
}

fn to_batch_failures(failures: &[ItemFailure]) -> Vec<BatchFailure> {
    failures
        .iter()
        .map(|item| BatchFailure {
            path: item.path.clone(),
            code: item.code.unwrap_or(-1),
            message: item.message.clone(),
            tokens: conflict_tokens(item.data.as_ref()),
        })
        .collect()
}

/// 整批失败时没有逐项信息，只能把同一个原因摊到每一项上。
fn batch_outcome_all_failed(paths: &[String], err: &ApiError) -> BatchOutcome {
    BatchOutcome {
        succeeded: 0,
        failed: paths
            .iter()
            .map(|path| BatchFailure {
                path: path.clone(),
                code: err.code().unwrap_or(-1),
                message: err.message().unwrap_or("operation failed").to_string(),
                tokens: Vec::new(),
            })
            .collect(),
    }
}

fn serialize_outcome(outcome: &BatchOutcome) -> Result<String, ApiError> {
    serde_json::to_string(outcome).map_err(ApiError::from)
}

/// 一次删除多个对象，返回 {succeeded, failed:[{path,code,message,tokens}]} 的 JSON。
/// 没出现在 failed 里的就是删成功了。
#[napi]
pub async fn delete_objects_batch(items: Vec<String>, dirs: Vec<String>) -> napi::Result<String> {
    let api = get_client()?;
    let all: Vec<String> = items.iter().chain(dirs.iter()).cloned().collect();
    if all.is_empty() {
        return serialize_outcome(&BatchOutcome { succeeded: 0, failed: Vec::new() })
            .map_err(to_napi_error);
    }

    if let Some(v3) = api.inner().as_v3() {
        // V3 按 id 寻址，一次请求就能带上整批
        let req = DeleteObjectRequest {
            items: items.iter().map(String::as_str).collect(),
            dirs: dirs.iter().map(String::as_str).collect(),
            force: false,
            unlink: false,
        };
        let outcome = match v3.delete_object(&req).await {
            Ok(_) => BatchOutcome { succeeded: all.len() as u32, failed: Vec::new() },
            Err(err) => batch_outcome_all_failed(&all, &err),
        };
        return serialize_outcome(&outcome).map_err(to_napi_error);
    }

    run_api_with_v4_refresh(|api| {
        let all = all.clone();
        async move {
            let paths: Vec<&str> = all.iter().map(String::as_str).collect();
            let result: DeleteResult = api.batch_delete(&paths).await?;
            if batch_auth_expired(&result.errors) {
                return Err(ApiError::Unauthorized("Login required".to_string()));
            }
            serialize_outcome(&BatchOutcome {
                succeeded: result.deleted as u32,
                failed: to_batch_failures(&result.errors),
            })
        }
    })
    .await
}

/// 一次移动或复制多个对象，返回结构同 delete_objects_batch。
#[napi]
pub async fn transfer_objects_batch(
    items: Vec<String>,
    dirs: Vec<String>,
    src_dir: String,
    dst: String,
    copy: bool,
) -> napi::Result<String> {
    let api = get_client()?;
    let all: Vec<String> = items.iter().chain(dirs.iter()).cloned().collect();
    if all.is_empty() {
        return serialize_outcome(&BatchOutcome { succeeded: 0, failed: Vec::new() })
            .map_err(to_napi_error);
    }

    if let Some(v3) = api.inner().as_v3() {
        let src = SourceItems {
            items: items.iter().map(String::as_str).collect(),
            dirs: dirs.iter().map(String::as_str).collect(),
        };
        let outcome = if copy {
            let req = CopyObjectRequest { src_dir: &src_dir, src, dst: &dst };
            match v3.copy_object(&req).await {
                Ok(_) => BatchOutcome { succeeded: all.len() as u32, failed: Vec::new() },
                Err(err) => batch_outcome_all_failed(&all, &err),
            }
        } else {
            let req = MoveObjectRequest { action: "move", src_dir: &src_dir, src, dst: &dst };
            match v3.move_object(&req).await {
                Ok(_) => BatchOutcome { succeeded: all.len() as u32, failed: Vec::new() },
                Err(err) => batch_outcome_all_failed(&all, &err),
            }
        };
        return serialize_outcome(&outcome).map_err(to_napi_error);
    }

    run_api_with_v4_refresh(|api| {
        let all = all.clone();
        let dst = dst.clone();
        async move {
            let paths: Vec<&str> = all.iter().map(String::as_str).collect();
            let result: TransferResult = if copy {
                api.batch_copy(&paths, &dst).await?
            } else {
                api.batch_move(&paths, &dst).await?
            };
            if batch_auth_expired(&result.errors) {
                return Err(ApiError::Unauthorized("Login required".to_string()));
            }
            serialize_outcome(&BatchOutcome {
                succeeded: result.succeeded as u32,
                failed: to_batch_failures(&result.errors),
            })
        }
    })
    .await
}

#[napi]
pub async fn rename_object(id: String, new_name: String, is_dir: bool) -> napi::Result<()> {
    let api = get_client()?;
    if let Some(v3) = api.inner().as_v3() {
        let src = if is_dir {
            SourceItems { items: vec![], dirs: vec![&id] }
        } else {
            SourceItems { items: vec![&id], dirs: vec![] }
        };
        let req = RenameObjectRequest {
            action: "rename",
            src,
            new_name: &new_name,
        };
        v3.rename_object(&req)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    } else {
        run_api_with_v4_refresh(|api| {
            let id = id.clone();
            let new_name = new_name.clone();
            async move { api.rename(&id, &new_name).await }
        }).await
    }
}

#[napi]
pub async fn new_directory(path: String) -> napi::Result<()> {
    let api = get_client()?;
    if api.inner().as_v3().is_some() {
        api.create_directory(&path)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    } else {
        let v4 = api.inner().as_v4()
            .ok_or_else(|| napi::Error::from_reason("not a v4 client"))?;
        v4_create_object(v4, &path, "folder")
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }
}

#[napi]
pub async fn new_file(path: String) -> napi::Result<()> {
    let api = get_client()?;
    if let Some(v3) = api.inner().as_v3() {
        let req = CreateFileRequest { path: &path };
        v3.create_file(&req)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    } else {
        let v4 = api.inner().as_v4()
            .ok_or_else(|| napi::Error::from_reason("not a v4 client"))?;
        v4_create_object(v4, &path, "file")
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }
}

// ---- Download / Upload ----

async fn v4_get_download_url(api: &CloudreveAPI, path: &str) -> napi::Result<String> {
    let v4 = api.inner().as_v4()
        .ok_or_else(|| napi::Error::from_reason("not a v4 client"))?;
    let req = CreateDownloadUrlRequest {
        uris: vec![path],
        download: Some(true),
        redirect: None,
        entity: None,
        use_primary_site_url: None,
        skip_error: None,
        archive: None,
        no_cache: None,
    };
    let resp = v4.create_download_url(&req)
        .await
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    resp.urls.into_iter().next()
        .map(|item| item.url)
        .ok_or_else(|| napi::Error::from_reason("no download URL in response"))
}

#[napi]
pub async fn get_download_uri(id: String) -> napi::Result<String> {
    let api = get_client()?;
    if let Some(v3) = api.inner().as_v3() {
        let dl = v3
            .download_file(&id)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(dl.url)
    } else {
        let result = v4_get_download_url(&api, &id).await;
        match result {
            Err(_) => {
                if do_v4_refresh().await.is_ok() {
                    let api2 = get_client()?;
                    v4_get_download_url(&api2, &id).await
                } else {
                    result
                }
            }
            ok => ok,
        }
    }
}

/// Cloudreve V3「上传会话已过期」。删除一个服务端已经不认识的会话时会拿到它，
/// 对清理路径来说就是"已经没有残留了"，按成功处理。
const V3_CODE_UPLOAD_SESSION_EXPIRED: i32 = 40011;

/// Cancel an in-flight upload session so the server clears its placeholder.
/// V4: DELETE /file/upload。V3: DELETE /file/upload/{sessionId} —— 早先这里
/// 直接返回 Ok，注释说 V3 没有删除接口，其实是有的（routers 里的
/// `upload.DELETE(":sessionId")`）。空转的后果很重：V3 建会话时会插一条占位
/// 文件记录，删不掉的话同名文件再传就一直撞 40054 "Upload session existed"，
/// 要等服务端 GC（upload_session_timeout 默认 24h）才自己好。
#[napi]
pub async fn delete_upload_session(path: String, session_id: String) -> napi::Result<()> {
    let api = get_client()?;
    if let Some(v3) = api.inner().as_v3() {
        if session_id.is_empty() {
            return Ok(());
        }
        return match v3.delete_upload_session(&session_id).await {
            Ok(()) => Ok(()),
            Err(ApiError::Api { code, .. }) if code == V3_CODE_UPLOAD_SESSION_EXPIRED => Ok(()),
            Err(error) => Err(napi::Error::from_reason(error.to_string())),
        };
    }
    run_api_with_v4_refresh(|api| {
        let path = path.clone();
        let session_id = session_id.clone();
        async move {
            let v4 = api.inner().as_v4().ok_or_else(|| {
                ApiError::UnsupportedFeature(
                    "delete_upload_session".to_string(),
                    "non-v4".to_string(),
                )
            })?;
            v4.delete_upload_session(&path, &session_id).await
        }
    })
    .await
}

/// 兜底清理：删掉当前账号名下**全部**上传占位（V3 的 DELETE /file/upload）。
///
/// 用在 sessionId 已经丢了、按 id 删不掉的场景：进程被杀在上传中途、本地会话
/// 记录被清、或者建会话的响应根本没回来。这些孤儿占位会让同名文件永远撞 40054，
/// 而客户端手里没有任何 id 可以用来删它们。
///
/// 作用域是整个账号，正在进行中的上传也会被连带删掉占位，所以调用方必须确认
/// 此刻没有别的上传在跑。V4 没有对应接口，返回 false 表示什么都没做。
#[napi]
pub async fn delete_all_upload_sessions() -> napi::Result<bool> {
    let api = get_client()?;
    match api.inner().as_v3() {
        Some(v3) => match v3.delete_all_upload_sessions().await {
            Ok(()) => Ok(true),
            Err(ApiError::Api { code, .. }) if code == V3_CODE_UPLOAD_SESSION_EXPIRED => Ok(true),
            Err(error) => Err(napi::Error::from_reason(error.to_string())),
        },
        None => Ok(false),
    }
}

/// Returns upload session JSON: { sessionId, chunkSize, expires }
#[napi]
pub async fn get_upload_uri(
    // 整个文件的字节数。必须是 f64：u32 到 4GiB 就回绕，一个 5GB 的文件会被
    // 截成 705MB 报给服务端，会话按错误的大小建立，最后传出一个坏文件。
    // ArkTS 的 number 本来就是 f64，整数精确到 2^53，接得住任何真实文件大小。
    path: String,
    size: f64,
    name: String,
    // JavaScript Date milliseconds are ~1.7e12 and cannot fit in u32.
    // Using u32 truncated the high bits at the N-API boundary, turning
    // 2026 timestamps into dates around January 1970 on the server.
    last_modified: i64,
    mime_type: String,
    chunk_size: u32,
) -> napi::Result<String> {
    let api = get_client()?;
    if let Some(v3) = api.inner().as_v3() {
        let parent = if let Some(p) = path.rfind('/') {
            if p == 0 { "/" } else { &path[..p] }
        } else {
            "/"
        };
        // 目标目录不存在时先建出来，和下面 V4 分支一样。少了这一步，往一个还没
        // 建过的目录传东西（典型场景：相册备份第一次跑，备份目录还不存在）会被
        // 服务端挡回 40016 Path not exist，整批全灭。ensure_remote_directory 走的是
        // UnifiedClient 的 list/create，两个版本都支持。
        ensure_remote_directory(&api, parent)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let dir = v3
            .list_directory(parent)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let req = UploadFileRequest {
            path: parent,
            name: &name,
            policy_id: &dir.policy.id,
            size: size.max(0.0) as i64,
            // V3 和 V4 都要 Unix 毫秒。V3 服务端拿到就是
            // time.UnixMilli(service.LastModified)（service/explorer/upload.go），
            // 从这个字段引入时起一直如此。之前这里按秒除了 1000，2026 年的时间戳
            // 被当成 1.7e9 毫秒，云端把刚传上去的文件标成 1970-01-22。
            last_modified: if last_modified > 0 { last_modified } else { 0 },
            mime_type: &mime_type,
        };
        let session = v3
            .upload_file(&req)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let j = json!({
            "sessionId": session.session_id,
            "chunkSize": session.chunk_size,
            "expires": session.expires,
        });
        Ok(j.to_string())
    } else {
        let v4 = api.inner().as_v4()
            .ok_or_else(|| napi::Error::from_reason("not a v4 client"))?;
        // Build full file path from parent dir + filename
        let parent = if let Some(p) = path.rfind('/') {
            if p == 0 { "/" } else { &path[..p] }
        } else {
            "/"
        };
        let file_path = if parent == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent, name)
        };
        ensure_remote_directory(&api, parent)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let policy_id = resolve_upload_policy_id(&api, parent)
            .await
            .or_else(get_v4_policy_id)
            .unwrap_or_default();
        if !policy_id.is_empty() {
            set_v4_policy_id(Some(policy_id.clone()));
        }
        let file_uri = v4_path_to_uri(&file_path);
        let req = CreateUploadSessionRequest {
            uri: &file_uri,
            size: size.max(0.0) as u64,
            policy_id: &policy_id,
            last_modified: if last_modified > 0 { Some(last_modified as u64) } else { None },
            mime_type: if mime_type.is_empty() { None } else { Some(&mime_type) },
            metadata: None,
            entity_type: if chunk_size > 0 { Some("version") } else { None },
        };
        let response: V4ApiResponse<serde_json::Value> = v4
            .put("/file/upload", &req)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        if response.code != 0 {
            return Err(napi::Error::from_reason(format!(
                "API error: {} (code: {})",
                response.msg, response.code
            )));
        }
        let session = response
            .data
            .ok_or_else(|| napi::Error::from_reason(response.msg.clone()))?;
        let storage_policy = session.get("storage_policy").cloned().unwrap_or_else(|| json!({}));
        let j = json!({
            "sessionId": json_string(&session, "session_id"),
            "chunkSize": json_u64(&session, "chunk_size").unwrap_or(0),
            "expires": json_u64(&session, "expires").unwrap_or(0),
            "uploadUrls": session.get("upload_urls").cloned().unwrap_or(serde_json::Value::Null),
            "credential": session.get("credential").cloned().unwrap_or(serde_json::Value::Null),
            "completeUrl": session.get("completeURL").or_else(|| session.get("complete_url")).cloned().unwrap_or(serde_json::Value::Null),
            "storagePolicyType": json_string(&storage_policy, "type"),
            "storagePolicyRelay": storage_policy.get("relay").and_then(|v| v.as_bool()).unwrap_or(false),
        });
        Ok(j.to_string())
    }
}

fn json_string(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

fn json_u64(value: &serde_json::Value, key: &str) -> Option<u64> {
    value.get(key).and_then(|v| {
        v.as_u64()
            .or_else(|| v.as_i64().and_then(|n| if n >= 0 { Some(n as u64) } else { None }))
    })
}

/// 覆盖保存一个**已存在**文件的内容（文本编辑器的保存走这条）。
///
/// V3 必须单独走 PUT /file/update/{id}：它的上传会话没有 overwrite 语义，
/// 同名文件建会话会被服务端挡回 40004 Object existed，所以 upload_local_file
/// 的 overwrite 参数在 V3 上根本不起作用。
///
/// V4 沿用创建上传会话 + entity_type=version 的老路，也就是 upload_local_file
/// 本身，行为不变。
///
/// id 和 remote_path 都要传：V4 的对象 id 恰好就是全路径，V3 的是 hashid，
/// 两边需要的东西不一样，不能靠一个参数糊过去。
#[napi]
pub async fn update_file_content(
    id: String,
    remote_path: String,
    local_path: String,
    last_modified_ms: Option<i64>,
) -> napi::Result<()> {
    let api = get_client()?;
    if let Some(v3) = api.inner().as_v3() {
        let content = fs::read(&local_path)
            .map_err(|e| napi::Error::from_reason(format!("read local file failed: {}", e)))?;
        return v3
            .update_file_content(&id, content)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()));
    }
    upload_local_file(local_path, remote_path, true, last_modified_ms).await
}

#[napi]
pub async fn upload_local_file(
    local_path: String,
    remote_path: String,
    overwrite: bool,
    last_modified_ms: Option<i64>,
) -> napi::Result<()> {
    let content = fs::read(&local_path)
        .map_err(|e| napi::Error::from_reason(format!("read local file failed: {}", e)))?;
    let api = get_client()?;
    let parent = remote_parent(&remote_path);
    ensure_remote_directory(&api, &parent)
        .await
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let policy_id = resolve_upload_policy_id(&api, &parent).await;
    if let Some(policy_id) = &policy_id {
        set_v4_policy_id(Some(policy_id.clone()));
    }
    // 仅在正数时透传：0/负数视为未提供，服务端按当前时间打戳
    let last_modified_ms = last_modified_ms.and_then(|v| if v > 0 { Some(v as u64) } else { None });
    api.upload_file(&remote_path, content, policy_id.as_deref(), overwrite, last_modified_ms)
        .await
        .map_err(|e| napi::Error::from_reason(e.to_string()))
}

#[napi]
pub async fn upload_local_file_chunk(
    local_path: String,
    session_id: String,
    index: u32,
    offset: f64,
    length: f64,
) -> napi::Result<f64> {
    let (buffer, read_len) = read_local_chunk(&local_path, offset, length)?;

    let api = get_client()?;
    if let Some(v4) = api.inner().as_v4() {
        v4.upload_file_chunk(&session_id, index, &buffer)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    } else if let Some(v3) = api.inner().as_v3() {
        // 分片序号会被 v3 用来算 append 偏移（AppendStart = chunkSize * index），
        // 早先这里拦掉 index > 0 是因为 crate 把 URL 写死成 /0；现在 crate 会带上
        // 真实序号，多分片就能正常走了。
        v3.upload_chunk(&session_id, index, buffer)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    } else {
        return Err(napi::Error::from_reason("unsupported Cloudreve client"));
    }

    Ok(read_len as f64)
}

#[napi]
pub fn upload_local_file_chunk_with_progress(
    env: Env,
    local_path: String,
    session_id: String,
    index: u32,
    offset: f64,
    length: f64,
    progress: JsFunction,
) -> napi::Result<JsObject> {
    let tsfn: ThreadsafeFunction<f64, ErrorStrategy::Fatal> =
        progress.create_threadsafe_function(0, |ctx| Ok(vec![ctx.value]))?;
    let (deferred, promise) = env.create_deferred::<f64, _>()?;
    napi::bindgen_prelude::spawn(async move {
        let result = async {
            let api = get_client()?;
            if let Some(v4) = api.inner().as_v4() {
                let url = format!(
                    "{}/api/v4/file/upload/{}/{}",
                    v4.base_url.trim_end_matches('/'),
                    session_id,
                    index
                );
                let auth_header = v4.token.clone().map(|token| format!("Bearer {}", token));
                upload_local_file_to_url_with_progress(
                    local_path,
                    url,
                    ChunkUploadTarget::AuthHeader(auth_header),
                    offset,
                    length,
                    tsfn,
                )
                .await
            } else if let Some(v3) = api.inner().as_v3() {
                // 之前 V3 走的是"整块读进内存 → 传完 → 才回调一次进度"的兜底路径。
                // 后果有两条：一是 V3 本机策略常见 chunkSize=0，整个文件就是一块，
                // 上传全程进度停在 0%，用户只看得到流量在跑；二是几百 MB 的文件要
                // 一次性分配同样大的 Vec 再交给 reqwest，峰值内存翻倍，大文件直接传崩。
                // 现在和 V4 一样走流式，边读边发，顺带拿到分段进度。
                let url = format!(
                    "{}/api/v3/file/upload/{}/{}",
                    v3.base_url.trim_end_matches('/'),
                    session_id,
                    index
                );
                let cookie = v3.get_session_cookie().map(|value| value.to_string());
                upload_local_file_to_url_with_progress(
                    local_path,
                    url,
                    ChunkUploadTarget::V3Cookie(cookie),
                    offset,
                    length,
                    tsfn,
                )
                .await
            } else {
                Err(napi::Error::from_reason("unsupported Cloudreve client"))
            }
        }.await;

        match result {
            Ok(uploaded) => deferred.resolve(move |_| Ok(uploaded)),
            Err(error) => deferred.reject(error),
        }
    });
    Ok(promise)
}

#[napi]
pub fn upload_local_file_chunk_to_url_with_progress(
    env: Env,
    local_path: String,
    upload_url: String,
    credential: String,
    index: u32,
    offset: f64,
    length: f64,
    progress: JsFunction,
) -> napi::Result<JsObject> {
    let tsfn: ThreadsafeFunction<f64, ErrorStrategy::Fatal> =
        progress.create_threadsafe_function(0, |ctx| Ok(vec![ctx.value]))?;
    let (deferred, promise) = env.create_deferred::<f64, _>()?;
    napi::bindgen_prelude::spawn(async move {
        let separator = if upload_url.contains('?') { "&" } else { "?" };
        let url = format!("{}{}chunk={}", upload_url, separator, index);
        let auth_header = if credential.is_empty() { None } else { Some(credential) };
        let result = upload_local_file_to_url_with_progress(
            local_path,
            url,
            ChunkUploadTarget::AuthHeader(auth_header),
            offset,
            length,
            tsfn,
        )
        .await;

        match result {
            Ok(uploaded) => deferred.resolve(move |_| Ok(uploaded)),
            Err(error) => deferred.reject(error),
        }
    });
    Ok(promise)
}

#[napi]
pub async fn upload_local_file_chunk_to_url(
    local_path: String,
    upload_url: String,
    credential: String,
    index: u32,
    offset: f64,
    length: f64,
) -> napi::Result<f64> {
    let (buffer, read_len) = read_local_chunk(&local_path, offset, length)?;
    let separator = if upload_url.contains('?') { "&" } else { "?" };
    let url = format!("{}{}chunk={}", upload_url, separator, index);
    let mut request = http_client().post(url).body(buffer);
    if !credential.is_empty() {
        request = request.header("Authorization", credential);
    }
    let response = request
        .send()
        .await
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let status = response.status();
    if !status.is_success() {
        let error_text = response
            .text()
            .await
            .unwrap_or_else(|_| "Unknown upload URL error".to_string());
        return Err(napi::Error::from_reason(format!(
            "upload url failed: {} {}",
            status, error_text
        )));
    }

    Ok(read_len as f64)
}

/// 分片上传请求的鉴权方式，以及"这次算不算成功"的判据。
///
/// V4 和从机直传节点用 HTTP 状态码表达结果；V3 的上传接口一律回 200，真实结果
/// 放在 body 的 `code` 里（`c.JSON(200, serializer.Err(...))`），只看状态码会把
/// 失败当成功——分片没落盘却继续往前推 offset，最后传出一个坏文件。
enum ChunkUploadTarget {
    /// Authorization 头：V4 的 Bearer token，或从机节点的 credential。
    AuthHeader(Option<String>),
    /// V3 的 cloudreve-session cookie。
    V3Cookie(Option<String>),
}

/// V3 上传接口的响应体，只取判成败要用的两个字段。
#[derive(Deserialize)]
struct V3ChunkUploadResponse {
    code: i32,
    #[serde(default)]
    msg: String,
}

async fn upload_local_file_to_url_with_progress(
    local_path: String,
    url: String,
    target: ChunkUploadTarget,
    offset: f64,
    length: f64,
    tsfn: ThreadsafeFunction<f64, ErrorStrategy::Fatal>,
) -> napi::Result<f64> {
    let mut file = tokio::fs::File::open(&local_path)
        .await
        .map_err(|e| napi::Error::from_reason(format!("open local file failed: {}", e)))?;
    let start = offset.max(0.0) as u64;
    // Content-Length 必须和实际能读出的字节数一致。调用方传的 length 来自媒体库元数据，
    // 可能比文件真实大小大（刚拍的照片被相机后处理改写过）；真按它声明长度就会发出一个
    // 短于 Content-Length 的 body，hyper 只能把连接判定为出错，表现为一个含义不明的
    // "error sending request"，而且会稳定复现。这里按文件真实大小夹一次。
    let file_len = file
        .metadata()
        .await
        .map(|m| m.len())
        .map_err(|e| napi::Error::from_reason(format!("stat local file failed: {}", e)))?;
    let available = file_len.saturating_sub(start);
    // 收成 u64 而不是 u32：不分片时整个文件就是一块，length 就是文件大小，
    // 超过 4GiB 用 u32 会回绕。
    let length = (if length > 0.0 { length as u64 } else { 0 }).min(available);
    if length == 0 {
        return Err(napi::Error::from_reason(format!(
            "nothing to upload: file is {} bytes, offset {}",
            file_len, start
        )));
    }
    file.seek(std::io::SeekFrom::Start(start))
        .await
        .map_err(|e| napi::Error::from_reason(format!("seek local file failed: {}", e)))?;

    // 进度回调按 PROGRESS_NOTIFY_STEP 节流。每读 256KB 回调一次的话，5 并发满速
    // 能往 ETS 的 UI 线程事件循环里灌每秒两百多个任务；ETS 侧只拿它做长时任务通知的
    // 保活判断，不需要这个精度。`reported` 记录上一次已上报的字节数。
    const PROGRESS_NOTIFY_STEP: u64 = 4 * 1024 * 1024;
    let stream = futures_util::stream::unfold(
        (file, length, 0u64, 0u64, tsfn.clone()),
        |(mut file, remaining, sent, reported, tsfn)| async move {
            if remaining == 0 {
                return None;
            }
            let read_size = remaining.min(256 * 1024) as usize;
            let mut buffer = vec![0u8; read_size];
            match file.read(&mut buffer).await {
                Ok(0) => None,
                Ok(n) => {
                    buffer.truncate(n);
                    let next_sent = sent + n as u64;
                    let next_reported = if next_sent - reported >= PROGRESS_NOTIFY_STEP {
                        let _ = tsfn.call(next_sent as f64, ThreadsafeFunctionCallMode::NonBlocking);
                        next_sent
                    } else {
                        reported
                    };
                    Some((
                        Ok::<Vec<u8>, std::io::Error>(buffer),
                        (file, remaining - n as u64, next_sent, next_reported, tsfn),
                    ))
                }
                Err(e) => Some((Err(e), (file, 0, sent, reported, tsfn))),
            }
        },
    );

    let mut request = http_client()
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
        .header(reqwest::header::CONTENT_LENGTH, length.to_string())
        .body(reqwest::Body::wrap_stream(stream));

    let is_v3 = matches!(target, ChunkUploadTarget::V3Cookie(_));
    match target {
        ChunkUploadTarget::AuthHeader(Some(auth_header)) => {
            request = request.header(reqwest::header::AUTHORIZATION, auth_header);
        }
        ChunkUploadTarget::V3Cookie(Some(cookie)) => {
            request = request.header("Cookie", format!("cloudreve-session={}", cookie));
        }
        _ => {}
    }

    let response = request
        .send()
        .await
        .map_err(|e| napi::Error::from_reason(describe_error(e)))?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();

    if is_v3 {
        // V3 永远是 200，成败看 body 里的 code。解析不出来就退回按状态码判断。
        if let Ok(parsed) = serde_json::from_str::<V3ChunkUploadResponse>(&body) {
            if parsed.code != 0 {
                return Err(napi::Error::from_reason(format!(
                    "upload chunk failed: API error: {} (code: {})",
                    parsed.msg, parsed.code
                )));
            }
            let _ = tsfn.call(length as f64, ThreadsafeFunctionCallMode::NonBlocking);
            return Ok(length as f64);
        }
    }

    if !status.is_success() {
        let error_text = if body.is_empty() {
            "Unknown upload error".to_string()
        } else {
            body
        };
        return Err(napi::Error::from_reason(format!(
            "upload chunk failed: {} {}",
            status, error_text
        )));
    }

    let _ = tsfn.call(length as f64, ThreadsafeFunctionCallMode::NonBlocking);
    Ok(length as f64)
}

/// 把 [offset, offset+length) 这段读进内存。
///
/// 必须循环读满：`Read::read` 允许一次只返回一部分，缓冲区越大越容易短读，
/// 而调用方拿 read_len 当"这块的全部字节"往前推 offset，短读就等于把文件中间
/// 挖掉一段传上去，服务端还会因为 Content-Length 对不上直接判错。读到 EOF
/// 才停，返回真实读到的长度。
fn read_local_chunk(local_path: &str, offset: f64, length: f64) -> napi::Result<(Vec<u8>, usize)> {
    let mut file = fs::File::open(local_path)
        .map_err(|e| napi::Error::from_reason(format!("open local file failed: {}", e)))?;
    file.seek(SeekFrom::Start(offset.max(0.0) as u64))
        .map_err(|e| napi::Error::from_reason(format!("seek local file failed: {}", e)))?;

    let mut buffer = vec![0u8; if length > 0.0 { length as usize } else { 0 }];
    let mut read_len = 0usize;
    while read_len < buffer.len() {
        match file.read(&mut buffer[read_len..]) {
            Ok(0) => break,
            Ok(n) => read_len += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => {
                return Err(napi::Error::from_reason(format!(
                    "read local chunk failed: {}",
                    e
                )));
            }
        }
    }
    buffer.truncate(read_len);
    Ok((buffer, read_len))
}

// ---- Aria2 ----

async fn v4_aria2_downloading(api: &CloudreveAPI) -> napi::Result<String> {
    let v4 = api.inner().as_v4()
        .ok_or_else(|| napi::Error::from_reason("not a v4 client"))?;
    let resp: V4ApiResponse<TaskListResponse> = v4.get("/workflow?page_size=100&category=downloading")
        .await
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let task_list = resp.data.ok_or_else(|| napi::Error::from_reason(resp.msg.clone()))?;
    let tasks: Vec<serde_json::Value> = task_list.tasks.iter()
        .filter(|t| matches!(t.status, TaskStatus::Queued | TaskStatus::Processing | TaskStatus::Suspending))
        .map(|t| {
            let props = t.summary.as_ref().map(|s| &s.props);
            let dl = props.and_then(|p| p.get("download"));
            let name = task_name_from_props(props, dl, &t.id);
            let error = task_error_from_props(props, t.error.as_deref());
            let progress = dl
                .and_then(|d| json_number_for_keys(d, &["progress", "percent", "percentage"]))
                .or_else(|| props.and_then(|p| json_number_for_keys(p, &["progress", "percent", "percentage"])))
                .unwrap_or(0);
            let mut total = dl
                .and_then(|d| json_number_for_keys(d, &["total", "total_length", "totalLength", "length", "size"]))
                .or_else(|| props.and_then(|p| json_number_for_keys(p, &["total", "total_length", "totalLength", "length", "size"])))
                .unwrap_or_else(|| extract_size(props));
            let mut downloaded = dl
                .and_then(|d| json_number_for_keys(d, &["downloaded", "completed", "completed_length", "completedLength", "current"]))
                .or_else(|| props.and_then(|p| json_number_for_keys(p, &["downloaded", "completed", "completed_length", "completedLength", "current"])))
                .unwrap_or(0);
            if downloaded <= 0 && progress > 0 {
                if total > 0 {
                    downloaded = total * progress.min(100) / 100;
                } else {
                    total = 100;
                    downloaded = progress.min(100);
                }
            }
            let speed = dl
                .and_then(|d| json_number_for_keys(d, &["download_speed", "downloadSpeed", "speed"]))
                .or_else(|| props.and_then(|p| json_number_for_keys(p, &["download_speed", "downloadSpeed", "speed"])))
                .unwrap_or(0);
            let dst = props
                .and_then(|p| p.get("dst").and_then(|v| v.as_str())
                    .or_else(|| p.get("dst_str").and_then(|v| v.as_str()))
                    .or_else(|| p.get("path").and_then(|v| v.as_str())))
                .map(decode_v4_prop_path)
                .unwrap_or_default();
            json!({
                "name": name,
                "status": v4_task_status_to_i32(&t.status),
                "total": total,
                "downloaded": downloaded,
                "speed": speed,
                "interval": 5,
                "dst": dst,
                "node": t.node.as_ref().map(|n| n.name.as_str()).unwrap_or(""),
                "update": t.updated_at,
                "info": {
                    "gid": t.id,
                    "status": "active",
                    "totalLength": total.to_string(),
                    "completedLength": downloaded.to_string(),
                    "downloadSpeed": speed.to_string(),
                    "errorMessage": error,
                    "files": []
                }
            })
        })
        .collect();
    serde_json::to_string(&tasks).map_err(|e| napi::Error::from_reason(e.to_string()))
}

#[napi]
pub async fn aria2_downloading() -> napi::Result<String> {
    let api = get_client()?;
    if let Some(v3) = api.inner().as_v3() {
        let tasks = v3
            .list_downloading()
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        log::info!("v3 aria2_downloading tasks={}", tasks.len());
        serde_json::to_string(&tasks).map_err(|e| napi::Error::from_reason(e.to_string()))
    } else {
        let result = v4_aria2_downloading(&api).await;
        match result {
            Err(_) => {
                if do_v4_refresh().await.is_ok() {
                    let api2 = get_client()?;
                    v4_aria2_downloading(&api2).await
                } else {
                    result
                }
            }
            ok => ok,
        }
    }
}

async fn v4_aria2_finished(api: &CloudreveAPI) -> napi::Result<String> {
    let v4 = api.inner().as_v4()
        .ok_or_else(|| napi::Error::from_reason("not a v4 client"))?;
        // Probe raw JSON first so we can log statuses we may have missed before the
        // strongly-typed model filters anything out.
        let raw: V4ApiResponse<serde_json::Value> = v4
            .get("/workflow?page_size=100&category=downloaded")
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let statuses: Vec<String> = raw.data.as_ref()
            .and_then(|d| d.get("tasks").and_then(|t| t.as_array()))
            .map(|arr| arr.iter().filter_map(|t| {
                t.get("status").and_then(|s| s.as_str()).map(|s| s.to_string())
            }).collect())
            .unwrap_or_default();
        log::info!(
            "v4 aria2_finished(category=downloaded) code={} msg={} raw_tasks={} statuses={:?}",
            raw.code, raw.msg,
            raw.data.as_ref().and_then(|d| d.get("tasks").and_then(|t| t.as_array())).map(|a| a.len()).unwrap_or(0),
            statuses
        );
        let resp: V4ApiResponse<TaskListResponse> = v4.get("/workflow?page_size=100&category=downloaded")
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let task_list = resp.data.ok_or_else(|| napi::Error::from_reason(resp.msg.clone()))?;
        let tasks: Vec<serde_json::Value> = task_list.tasks.iter()
            .filter(|t| matches!(t.status,
                TaskStatus::Completed | TaskStatus::Error | TaskStatus::Canceled))
            .map(|t| {
                let props = t.summary.as_ref().map(|s| &s.props);
                let dl = props.and_then(|p| p.get("download"));
                let name = task_name_from_props(props, dl, &t.id);
                let error = task_error_from_props(props, t.error.as_deref());
                let total = dl
                    .and_then(|d| d.get("total").and_then(|v| v.as_i64()))
                    .unwrap_or_else(|| extract_size(props));
                let dst = props
                    .and_then(|p| p.get("dst").and_then(|v| v.as_str())
                        .or_else(|| p.get("dst_str").and_then(|v| v.as_str()))
                        .or_else(|| p.get("path").and_then(|v| v.as_str())))
                    .map(decode_v4_prop_path)
                    .unwrap_or_default();
                let status_num = v4_task_status_to_i32(&t.status);
                let files: Vec<serde_json::Value> = if let Some(v4_files) = dl
                    .and_then(|d| d.get("files"))
                    .and_then(|f| f.as_array())
                {
                    v4_files.iter().map(|f| {
                        let fname = f.get("name").and_then(|v| v.as_str()).unwrap_or(&name);
                        let fsize = f.get("size").and_then(|v| v.as_i64()).unwrap_or(total);
                        let fpath = if dst.is_empty() {
                            fname.to_string()
                        } else {
                            format!("{}/{}", dst.trim_end_matches('/'), fname)
                        };
                        let fcompleted = if status_num == 4 { fsize } else { 0 };
                        json!({
                            "index": f.get("index").and_then(|v| v.as_i64()).unwrap_or(0).to_string(),
                            "path": fpath,
                            "length": fsize.to_string(),
                            "completedLength": fcompleted.to_string(),
                            "selected": f.get("selected").and_then(|v| v.as_bool()).unwrap_or(true),
                            "uris": []
                        })
                    }).collect()
                } else {
                    let fpath = if dst.is_empty() { name.clone() } else { format!("{}/{}", dst.trim_end_matches('/'), name) };
                    let fcompleted = if status_num == 4 { total } else { 0 };
                    vec![json!({
                        "index": "0",
                        "path": fpath,
                        "length": total.to_string(),
                        "completedLength": fcompleted.to_string(),
                        "selected": true,
                        "uris": []
                    })]
                };
                json!({
                    "name": name,
                    "gid": t.id,
                    "status": status_num,
                    "total": total,
                    "task_status": status_num,
                    "task_error": error,
                    "files": files,
                    "create": t.created_at,
                    "update": t.updated_at,
                    "node": t.node.as_ref().map(|n| n.name.as_str()).unwrap_or(""),
                    "dst": dst
                })
            })
            .collect();
    serde_json::to_string(&tasks).map_err(|e| napi::Error::from_reason(e.to_string()))
}

#[napi]
pub async fn aria2_finished(page: i32) -> napi::Result<String> {
    let api = get_client()?;
    if let Some(v3) = api.inner().as_v3() {
        // Cloudreve v3 returns the full finished list in a single response (no pagination
        // wrapper); keep the page arg for ArkTS compatibility and short-circuit after the
        // first page so LazyForEach's `onReachEnd` stops issuing requests.
        if page.max(1) > 1 {
            return Ok("[]".to_string());
        }
        let mut tasks = v3
            .list_finished()
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        // Cloudreve v3 returns these chronologically ascending — the user's just-created
        // (often failed-fast) task ends up at the bottom of a long list, which reads as
        // "missing" in the UI. Surface newest first instead.
        tasks.sort_by(|a, b| b.update.cmp(&a.update));
        log::info!("v3 aria2_finished tasks={}", tasks.len());
        serde_json::to_string(&tasks).map_err(|e| napi::Error::from_reason(e.to_string()))
    } else {
        let result = v4_aria2_finished(&api).await;
        match result {
            Err(_) => {
                if do_v4_refresh().await.is_ok() {
                    let api2 = get_client()?;
                    v4_aria2_finished(&api2).await
                } else {
                    result
                }
            }
            ok => ok,
        }
    }
}

#[napi]
pub async fn aria2_create_task(dst: String, urls: Vec<String>) -> napi::Result<()> {
    let api = get_client()?;
    if let Some(v3) = api.inner().as_v3() {
        let url_refs: Vec<&str> = urls.iter().map(String::as_str).collect();
        let req = Aria2CreateRequest {
            dst: &dst,
            url: url_refs,
        };
        v3.create_download(&req)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    } else {
        let v4 = api.inner().as_v4()
            .ok_or_else(|| napi::Error::from_reason("not a v4 client"))?;
        let normalized_dst = if dst.starts_with("cloudreve://") {
            dst
        } else if dst == "/" {
            "cloudreve://my".to_string()
        } else if dst.starts_with('/') {
            format!("cloudreve://my{}", dst)
        } else {
            format!("cloudreve://my/{}", dst)
        };
        let url_refs: Vec<&str> = urls.iter().map(String::as_str).collect();
        let req = CreateDownloadRequest {
            dst: &normalized_dst,
            src: url_refs,
            preferred_node_id: None,
        };
        v4.create_download(&req)
            .await
            .map(|_| ())
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }
}

#[napi]
pub async fn aria2_delete_task(gid: String) -> napi::Result<()> {
    let api = get_client()?;
    if let Some(v3) = api.inner().as_v3() {
        v3.delete_task(&gid)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    } else {
        let v4 = api.inner().as_v4()
            .ok_or_else(|| napi::Error::from_reason("not a v4 client"))?;
        match v4.cancel_download_task(&gid).await {
            Ok(()) => Ok(()),
            Err(first_error) => {
                #[derive(Debug, serde::Deserialize)]
                struct EmptyResponse;

                let response: V4ApiResponse<EmptyResponse> = v4
                    .delete(&format!("/workflow/{}", gid))
                    .await
                    .map_err(|_| napi::Error::from_reason(first_error.to_string()))?;
                if response.code == 0 {
                    Ok(())
                } else {
                    Err(napi::Error::from_reason(format!(
                        "{}; fallback delete failed: API error {} ({})",
                        first_error, response.msg, response.code
                    )))
                }
            }
        }
    }
}

#[napi]
pub async fn get_user_tasks(page: i32) -> napi::Result<String> {
    let api = get_client()?;
    if let Some(v3) = api.inner().as_v3() {
        let list = v3
            .get_task_queue(page)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        serde_json::to_string(&list).map_err(|e| napi::Error::from_reason(e.to_string()))
    } else {
        let v4 = api.inner().as_v4()
            .ok_or_else(|| napi::Error::from_reason("not a v4 client"))?;
        let resp: V4ApiResponse<TaskListResponse> = v4.get("/workflow?page_size=100&category=general")
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let response = resp.data.ok_or_else(|| napi::Error::from_reason(resp.msg.clone()))?;
        let tasks: Vec<serde_json::Value> = response.tasks.iter().map(|t| {
            let props = t.summary.as_ref().map(|s| &s.props);
            let type_num = v4_task_type_to_i32(&t.r#type);
            let status_num = v4_task_status_to_i32(&t.status);
            let progress: i64 = props
                .and_then(|p| p.get("progress"))
                .and_then(|v| v.as_i64())
                .unwrap_or(if status_num == 4 { 100 } else { 0 });
            let name = task_name_from_props(props, props.and_then(|p| p.get("download")), &t.id);
            json!({
                "id": t.id,
                "name": name,
                "status": status_num,
                "type": type_num,
                "create_date": t.created_at,
                "progress": progress,
                "error": t.error.as_deref().unwrap_or("")
            })
        }).collect();
        let total = tasks.len() as i64;
        let result = json!({ "tasks": tasks, "total": total });
        serde_json::to_string(&result).map_err(|e| napi::Error::from_reason(e.to_string()))
    }
}

// ---- Thumbnail ----

#[derive(Debug, serde::Deserialize)]
struct V4ThumbData {
    url: String,
}

/// Fetch thumbnail binary. For V3: direct GET with session cookie.
/// For V4: call /file/thumb to get pre-signed URL then fetch image bytes.
#[napi]
pub async fn get_thumb(id: String) -> napi::Result<String> {
    let api = get_client()?;
    if let Some(v3) = api.inner().as_v3() {
        // V3 thumb endpoint requires session cookie auth; either returns raw image bytes
        // (local storage) or redirects to a signed CDN URL (cloud storage). Follow redirects
        // and return a `data:` URL so the ArkUI Image component can render without extra auth.
        let url = format!("{}/api/v3/file/thumb/{}", v3.base_url.trim_end_matches('/'), id);
        let cookie = api.get_session_cookie().unwrap_or_default();
        let cookie_header = if cookie.starts_with("cloudreve-session=") {
            cookie
        } else {
            format!("cloudreve-session={}", cookie)
        };
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let response = client
            .get(&url)
            .header(reqwest::header::COOKIE, cookie_header)
            .send()
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(napi::Error::from_reason(format!(
                "thumb request failed: {}",
                status
            )));
        }
        let mime = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.split(';').next().unwrap_or(v).trim().to_string())
            .filter(|v| v.starts_with("image/"))
            .unwrap_or_else(|| "image/jpeg".to_string());
        let bytes = response
            .bytes()
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
        Ok(format!("data:{};base64,{}", mime, encoded))
    } else {
        let v4 = api.inner().as_v4()
            .ok_or_else(|| napi::Error::from_reason("not a v4 client"))?;
        let v4_uri = v4_path_to_uri(&id);
        let endpoint = format!("/file/thumb?uri={}&width=200&height=200", v4_uri);
        let resp: V4ApiResponse<V4ThumbData> = v4.get(&endpoint).await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        // Return the pre-signed CDN URL — ArkTS createImageSource can fetch it without extra auth
        // 没有 data 时不能直接拿 resp.msg 当错误文案：Cloudreve 成功时 msg 就是空串，
        // 上层日志会打出个 "msg=" 什么也说明不了。这里补一句带业务码的说明。
        resp.data
            .ok_or_else(|| {
                let reason = if resp.msg.is_empty() {
                    format!("thumbnail unavailable (code {})", resp.code)
                } else {
                    format!("{} (code {})", resp.msg, resp.code)
                };
                napi::Error::from_reason(reason)
            })
            .map(|d| d.url)
    }
}

// ---- V4 Exclusive: Share Links ----

/// Create a share link for a file or folder. Returns the share URL string.
/// Retries once after a token refresh when the access token has expired.
#[napi]
pub async fn create_share_link(path: String, expire_days: i32, password: String) -> napi::Result<String> {
    run_api_with_v4_refresh(move |api| {
        let path = path.clone();
        let password = password.clone();
        async move {
            let v4 = api.inner().as_v4()
                .ok_or_else(|| ApiError::InvalidResponse("share links require V4".to_string()))?;
            let permissions = PermissionSetting {
                user_explicit: serde_json::json!({}),
                group_explicit: serde_json::json!({}),
                same_group: "read".to_string(),
                other: "read".to_string(),
                anonymous: "read".to_string(),
                everyone: "read".to_string(),
            };
            let req = CreateShareLinkRequest {
                permissions,
                uri: path,
                is_private: if password.is_empty() { None } else { Some(true) },
                share_view: None,
                expire: if expire_days > 0 { Some(expire_days as u32 * 24 * 60 * 60) } else { None },
                price: None,
                password: if password.is_empty() { None } else { Some(password) },
                show_readme: None,
            };
            v4.create_share_link(&req).await
        }
    }).await
}

/// List current user's share links. Returns JSON array.
/// Works for both v3 and v4 servers; v3 shares get mapped to the v4-shaped fields
/// the ETS layer expects (id/name/source_type/source/source_uri/...).
/// V4 retries once after a token refresh when the access token has expired.
#[napi]
pub async fn list_share_links() -> napi::Result<String> {
    let api = get_client()?;

    if let Some(v3) = api.inner().as_v3() {
        return list_share_links_v3(v3).await;
    }

    run_api_with_v4_refresh(|api| async move {
        let v4 = api.inner().as_v4()
            .ok_or_else(|| ApiError::InvalidResponse("share links require V3 or V4".to_string()))?;
        list_share_links_v4(v4).await
    }).await
}

async fn list_share_links_v4(v4: &ApiV4Client) -> Result<String, ApiError> {
    let mut all_shares: Vec<serde_json::Value> = Vec::new();
    let mut next_page_token: Option<String> = None;
    let mut page_count = 0;

    loop {
        let current_token = next_page_token.clone();
        let mut endpoint = "/share?page_size=100".to_string();
        if let Some(token) = &current_token {
            endpoint.push_str("&next_page_token=");
            endpoint.push_str(&encode_query_component(token));
        }

        let resp: V4ApiResponse<serde_json::Value> = v4.get(&endpoint).await?;
        let data = resp
            .data
            .ok_or_else(|| ApiError::InvalidResponse(resp.msg))?;

        let mut shares = data
            .get("shares")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();

        for share in &mut shares {
            enrich_share_source_uri(v4, share).await;
        }
        all_shares.extend(shares);

        page_count += 1;
        let next = share_next_page_token(&data);
        if next.is_none() || next == current_token || page_count >= 100 {
            break;
        }
        next_page_token = next;
    }

    serde_json::to_string(&all_shares).map_err(|e| ApiError::InvalidResponse(e.to_string()))
}

async fn list_share_links_v3(v3: &cloudreve_api::api::v3::ApiV3Client) -> napi::Result<String> {
    let base_url = v3.base_url.trim_end_matches('/').to_string();
    let mut out: Vec<serde_json::Value> = Vec::new();
    let mut page: u32 = 1;
    loop {
        let endpoint = format!(
            "/share?page={}&order_by=created_at&order=DESC",
            page
        );
        let resp: serde_json::Value = match v3.get(&endpoint).await {
            Ok(value) => value,
            Err(err) => return Err(napi::Error::from_reason(err.to_string())),
        };
        let data = resp.get("data").cloned().unwrap_or(serde_json::Value::Null);
        let items = data
            .get("items")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        if items.is_empty() {
            break;
        }
        for item in items {
            out.push(map_v3_share_to_unified(&item, &base_url));
        }
        page += 1;
        if page > 100 {
            break;
        }
    }
    serde_json::to_string(&out).map_err(|e| napi::Error::from_reason(e.to_string()))
}

fn map_v3_share_to_unified(item: &serde_json::Value, base_url: &str) -> serde_json::Value {
    let key = item.get("key").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let is_dir = item.get("is_dir").and_then(|v| v.as_bool()).unwrap_or(false);
    let password = item.get("password").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let source_name = item
        .get("source")
        .and_then(|v| v.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let expire_seconds = item.get("expire").and_then(|v| v.as_i64()).unwrap_or(-1);
    let downloads = item.get("downloads").and_then(|v| v.as_i64()).unwrap_or(0);
    let views = item.get("views").and_then(|v| v.as_i64()).unwrap_or(0);
    let create_date = item.get("create_date").and_then(|v| v.as_str()).unwrap_or("").to_string();

    let url = if password.is_empty() {
        format!("{}/s/{}", base_url, key)
    } else {
        format!("{}/s/{}/{}", base_url, key, password)
    };

    let expired = expire_seconds == 0;

    let mut obj = serde_json::Map::new();
    obj.insert("id".into(), serde_json::Value::String(key));
    obj.insert("name".into(), serde_json::Value::String(source_name.clone()));
    obj.insert("url".into(), serde_json::Value::String(url));
    obj.insert(
        "source_type".into(),
        serde_json::Value::Number(if is_dir { 1.into() } else { 0.into() }),
    );
    obj.insert("source_uri".into(), serde_json::Value::String(String::new()));
    obj.insert(
        "source".into(),
        serde_json::json!({ "name": source_name }),
    );
    obj.insert("password".into(), serde_json::Value::String(password.clone()));
    obj.insert(
        "password_protected".into(),
        serde_json::Value::Bool(!password.is_empty()),
    );
    obj.insert("expired".into(), serde_json::Value::Bool(expired));
    obj.insert("created_at".into(), serde_json::Value::String(create_date));
    obj.insert(
        "downloaded".into(),
        serde_json::Value::Number(downloads.into()),
    );
    obj.insert("visited".into(), serde_json::Value::Number(views.into()));
    obj.insert(
        "expires".into(),
        if expire_seconds > 0 {
            serde_json::Value::String(format!("{}", expire_seconds))
        } else {
            serde_json::Value::Null
        },
    );
    serde_json::Value::Object(obj)
}

/// Delete a share link by ID.
/// Retries once after a token refresh when the access token has expired.
#[napi]
pub async fn delete_share_link(share_id: String) -> napi::Result<()> {
    run_api_with_v4_refresh(move |api| {
        let share_id = share_id.clone();
        async move {
            let v4 = api.inner().as_v4()
                .ok_or_else(|| ApiError::InvalidResponse("share links require V4".to_string()))?;
            v4.delete_share_link(&share_id).await
        }
    }).await
}

// ---- V4 Exclusive: Archive Operations ----

/// Create a server-side archive from given paths. Returns task ID.
#[napi]
pub async fn create_archive(src_paths: Vec<String>, dst_path: String) -> napi::Result<String> {
    let api = get_client()?;
    let v4 = api.inner().as_v4()
        .ok_or_else(|| napi::Error::from_reason("archive requires V4"))?;
    let src_refs: Vec<&str> = src_paths.iter().map(String::as_str).collect();
    let req = CreateArchiveRequest {
        src: src_refs,
        dst: &dst_path,
    };
    let task = v4
        .create_archive(&req)
        .await
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    Ok(task.id)
}

/// Extract an archive to a destination path. Returns task ID.
#[napi]
pub async fn extract_archive(src_paths: Vec<String>, dst_path: String) -> napi::Result<String> {
    let api = get_client()?;
    let v4 = api.inner().as_v4()
        .ok_or_else(|| napi::Error::from_reason("archive requires V4"))?;
    let src_refs: Vec<&str> = src_paths.iter().map(String::as_str).collect();
    let req = ExtractArchiveRequest {
        src: src_refs,
        dst: &dst_path,
    };
    let task = v4
        .extract_archive(&req)
        .await
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    Ok(task.id)
}
