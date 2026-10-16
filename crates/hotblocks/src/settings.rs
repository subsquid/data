//! Settings that change without a restart. Each starts from its flag, and a
//! change lasts until the process exits. A change reaches only the replica
//! that serves the request.

use std::{sync::Arc, time::Duration};

use anyhow::{Context, bail, ensure};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::info;

use crate::metrics::{set_query_thread_per_task, set_shadow_percent};

pub type SettingsRef = Arc<RuntimeSettings>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Settings {
    pub shadow: ShadowSettings,
    pub slow_tasks: SlowTaskSettings,
    pub query: QuerySettings
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ShadowSettings {
    pub percent: f64,
    /// A run is slow when the new engine takes this many times the legacy time...
    pub slow_ratio: f64,
    /// ...and at least this much longer.
    pub slow_margin_ms: u64,
    /// Detailed log lines per 5 minutes, so that one hot query cannot flood
    /// the logs; 0 turns them off.
    pub log_limit: usize
}

impl ShadowSettings {
    pub fn slow_margin(&self) -> Duration {
        Duration::from_millis(self.slow_margin_ms)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SlowTaskSettings {
    /// A query task is slow when it holds a query pool worker this long.
    pub threshold_ms: u64,
    /// Detailed log lines per 5 minutes, at most one per query shape; 0 turns
    /// them off.
    pub log_limit: usize
}

impl SlowTaskSettings {
    pub fn threshold(&self) -> Duration {
        Duration::from_millis(self.threshold_ms)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QuerySettings {
    /// Run each new query task on a single thread instead of the shared query
    /// pool. A task already queued keeps the pool it was sent to.
    pub thread_per_task: bool
}

impl Settings {
    fn validate(&self) -> anyhow::Result<()> {
        let percent = self.shadow.percent;
        ensure!(
            (0.0..=100.0).contains(&percent),
            "shadow.percent must be between 0 and 100, got {percent}"
        );
        Ok(())
    }
}

pub struct RuntimeSettings {
    current: RwLock<Settings>
}

impl RuntimeSettings {
    pub fn new(initial: Settings) -> anyhow::Result<Self> {
        initial.validate()?;
        publish(&initial);
        Ok(Self {
            current: RwLock::new(initial)
        })
    }

    pub fn get(&self) -> Settings {
        self.current.read().clone()
    }

    pub fn shadow(&self) -> ShadowSettings {
        self.current.read().shadow
    }

    pub fn slow_tasks(&self) -> SlowTaskSettings {
        self.current.read().slow_tasks
    }

    pub fn query(&self) -> QuerySettings {
        self.current.read().query
    }

    /// Apply a JSON Merge Patch (RFC 7386). A patch that leaves the settings
    /// invalid, names an unknown key or removes a key changes nothing.
    pub fn update(&self, patch: Value) -> anyhow::Result<Settings> {
        let mut current = self.current.write();

        let mut merged = serde_json::to_value(&*current)?;
        check_keys(&merged, &patch, "")?;
        merge(&mut merged, &patch);
        let next: Settings = serde_json::from_value(merged).context("invalid settings")?;
        next.validate()?;

        info!(%patch, settings = ?next, "settings changed");
        publish(&next);
        *current = next.clone();
        Ok(next)
    }
}

/// Settings that are also exported as metrics.
fn publish(settings: &Settings) {
    set_shadow_percent(settings.shadow.percent);
    set_query_thread_per_task(settings.query.thread_per_task);
}

/// The merge drops a `null` key before deserialization can reject it as
/// unknown, so every patch key must already be in the current settings.
fn check_keys(target: &Value, patch: &Value, path: &str) -> anyhow::Result<()> {
    let (Value::Object(target), Value::Object(patch)) = (target, patch) else {
        return Ok(());
    };

    for (key, value) in patch {
        let Some(field) = target.get(key) else {
            bail!("unknown setting `{path}{key}`");
        };
        check_keys(field, value, &format!("{path}{key}."))?;
    }
    Ok(())
}

fn merge(target: &mut Value, patch: &Value) {
    let Value::Object(patch) = patch else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = Value::Object(Default::default());
    }
    let target = target.as_object_mut().expect("an object since the line above");

    for (key, value) in patch {
        if value.is_null() {
            target.remove(key);
        } else {
            merge(target.entry(key).or_insert(Value::Null), value);
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn settings() -> RuntimeSettings {
        RuntimeSettings::new(Settings {
            shadow: ShadowSettings {
                percent: 0.0,
                slow_ratio: 2.0,
                slow_margin_ms: 50,
                log_limit: 20
            },
            slow_tasks: SlowTaskSettings {
                threshold_ms: 500,
                log_limit: 20
            },
            query: QuerySettings { thread_per_task: false }
        })
        .unwrap()
    }

    #[test]
    fn a_patch_changes_only_the_keys_it_names() {
        let settings = settings();
        let before = settings.get();

        let updated = settings
            .update(json!({"shadow": {"percent": 5, "logLimit": 0}}))
            .unwrap();

        let expected = ShadowSettings {
            percent: 5.0,
            slow_ratio: 2.0,
            slow_margin_ms: 50,
            log_limit: 0
        };
        assert_eq!(updated.shadow, expected);
        assert_eq!(settings.shadow(), expected);
        assert_eq!(updated.slow_tasks, before.slow_tasks);
        assert_eq!(updated.query, before.query);
    }

    #[test]
    fn a_rejected_patch_changes_nothing() {
        let settings = settings();
        let before = settings.get();

        for patch in [
            json!({"shadow": {"percent": 150}}),
            json!({"shadow": {"percent": 5, "percnt": 5}}),
            json!({"shadow": {"percent": 5, "percnt": null}}),
            json!({"shadow": {"percent": null}}),
            json!({"shadow": {"logLimit": -1}}),
            json!({"query": {"threadPerTask": 1}}),
            json!({"other": {}}),
            json!(5)
        ] {
            assert!(settings.update(patch.clone()).is_err(), "{patch} was accepted");
            assert_eq!(settings.get(), before, "{patch} changed the settings");
        }
    }
}
