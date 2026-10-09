//! Аудит-логирование значимых операций (O7).
//!
//! Add/remove/membership фиксируются структурированными событиями с
//! `target = "audit"`. Источник операции различается, чтобы по логам можно было
//! понять, кто инициировал изменение: внешний API, фоновый процессор лидера,
//! загрузка генезиса или административный эндпоинт.

use crate::NodeId;
use crate::domain::Hash;

/// Источник инициатора операции.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditSource {
    /// Публичный/внутренний HTTP-хендлер добавления.
    Api,
    /// Фоновый процессор очистки «тяжёлых» узлов.
    Processor,
    /// Загрузка генезиса из файла.
    Genesis,
    /// Административный эндпоинт `/mng/*`.
    Management,
}

impl AuditSource {
    /// Строковое представление источника.
    pub fn as_str(&self) -> &'static str {
        match self {
            AuditSource::Api => "api",
            AuditSource::Processor => "processor",
            AuditSource::Genesis => "genesis",
            AuditSource::Management => "management",
        }
    }
}

impl std::fmt::Display for AuditSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Описание аудит-события (без привязки к логгеру — удобно тестировать).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEvent {
    /// Операция: `add`, `remove`, `membership`.
    pub action: &'static str,
    /// Источник.
    pub source: AuditSource,
    /// Узел кластера, на котором произошло событие.
    pub node: NodeId,
    /// Результат: `ok` или `error`.
    pub result: &'static str,
    /// Затронутые хэши (для add/remove).
    pub subjects: Vec<String>,
    /// Дополнительное пояснение (например, текст ошибки).
    pub detail: Option<String>,
}

impl AuditEvent {
    /// Человекочитаемая однострочная форма для тестов и текстовых логов.
    pub fn format_line(&self) -> String {
        format!(
            "action={} source={} node={} result={} subjects={} detail={}",
            self.action,
            self.source,
            self.node,
            self.result,
            self.subjects.join(","),
            self.detail.as_deref().unwrap_or("-")
        )
    }
}

/// Публикует событие в лог с `target = "audit"`.
fn emit(event: &AuditEvent) {
    tracing::info!(
        target: "audit",
        action = event.action,
        source = event.source.as_str(),
        node = event.node,
        result = event.result,
        subjects = %event.subjects.join(","),
        detail = event.detail.as_deref().unwrap_or(""),
        "audit"
    );
}

/// Аудит добавления узла.
pub fn add(source: AuditSource, node: NodeId, hash: &Hash, ok: bool, detail: Option<&str>) {
    emit(&AuditEvent {
        action: "add",
        source,
        node,
        result: if ok { "ok" } else { "error" },
        subjects: vec![hash.to_string()],
        detail: detail.map(str::to_string),
    });
}

/// Аудит удаления узлов.
pub fn remove(source: AuditSource, node: NodeId, hashes: &[Hash], ok: bool, detail: Option<&str>) {
    emit(&AuditEvent {
        action: "remove",
        source,
        node,
        result: if ok { "ok" } else { "error" },
        subjects: hashes.iter().map(|h| h.to_string()).collect(),
        detail: detail.map(str::to_string),
    });
}

/// Аудит изменения состава кластера.
pub fn membership(
    source: AuditSource,
    node: NodeId,
    members: &[NodeId],
    ok: bool,
    detail: Option<&str>,
) {
    emit(&AuditEvent {
        action: "membership",
        source,
        node,
        result: if ok { "ok" } else { "error" },
        subjects: members.iter().map(|m| m.to_string()).collect(),
        detail: detail.map(str::to_string),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_strings_are_stable() {
        assert_eq!(AuditSource::Api.as_str(), "api");
        assert_eq!(AuditSource::Processor.as_str(), "processor");
        assert_eq!(AuditSource::Genesis.as_str(), "genesis");
        assert_eq!(AuditSource::Management.as_str(), "management");
    }

    #[test]
    fn add_event_formats_all_fields() {
        let event = AuditEvent {
            action: "add",
            source: AuditSource::Api,
            node: 2,
            result: "ok",
            subjects: vec!["abc".to_string()],
            detail: None,
        };
        let line = event.format_line();
        assert!(line.contains("action=add"));
        assert!(line.contains("source=api"));
        assert!(line.contains("node=2"));
        assert!(line.contains("subjects=abc"));
    }

    #[test]
    fn remove_event_carries_error_detail() {
        let event = AuditEvent {
            action: "remove",
            source: AuditSource::Processor,
            node: 1,
            result: "error",
            subjects: vec!["h1".to_string(), "h2".to_string()],
            detail: Some("raft down".to_string()),
        };
        let line = event.format_line();
        assert!(line.contains("subjects=h1,h2"));
        assert!(line.contains("detail=raft down"));
    }
}
