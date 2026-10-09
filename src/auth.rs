//! Аутентификация внутреннего (административного) API.
//!
//! Публичные эндпоинты (`/`, `/pool`, `/full`) не требуют токена. Внутренние
//! (`/add`, `/remove_heavy_nodes`, `/load-genesis`, `/raft/*`, `/mng/*`) обязаны
//! предъявлять кластерный токен `INTERNAL_API_TOKEN`, передаваемый в заголовке
//! `x-internal-token`. Сравнение выполняется в постоянном времени.

use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::Response;

use crate::app::App;

/// Заголовок, в котором передаётся кластерный токен.
pub const INTERNAL_TOKEN_HEADER: &str = "x-internal-token";

/// Возвращает `true`, если в заголовках предъявлен корректный токен.
pub fn is_authorized(headers: &HeaderMap, expected: &str) -> bool {
    if expected.is_empty() {
        // Пустой ожидаемый токен не должен разрешать доступ.
        return false;
    }
    match headers
        .get(INTERNAL_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
    {
        Some(provided) => constant_time_eq(provided.as_bytes(), expected.as_bytes()),
        None => false,
    }
}

/// Сравнение байтовых строк в постоянном времени (без раннего выхода по значению).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Middleware, пропускающая запрос только с корректным кластерным токеном.
pub async fn require_internal_token(
    State(app): State<App>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if is_authorized(request.headers(), &app.internal_api_token) {
        Ok(next.run(request).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers_with(token: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(token) = token {
            headers.insert(INTERNAL_TOKEN_HEADER, token.parse().unwrap());
        }
        headers
    }

    #[test]
    fn accepts_matching_token() {
        assert!(is_authorized(&headers_with(Some("secret")), "secret"));
    }

    #[test]
    fn rejects_wrong_token() {
        assert!(!is_authorized(&headers_with(Some("wrong")), "secret"));
    }

    #[test]
    fn rejects_missing_token() {
        assert!(!is_authorized(&headers_with(None), "secret"));
    }

    #[test]
    fn rejects_when_expected_token_is_empty() {
        assert!(!is_authorized(&headers_with(Some("")), ""));
    }

    #[test]
    fn constant_time_eq_matches_semantics() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }
}
