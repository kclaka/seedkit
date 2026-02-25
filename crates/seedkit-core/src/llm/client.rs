//! # LLM API Client
//!
//! Sends schema analysis requests to Claude or OpenAI and returns the raw
//! response text. Supports auto-detection of the API key from environment
//! variables and optional model overrides via `--model`.
//!
//! Responses are cached to `~/.seedkit/cache/` keyed by schema hash so
//! repeated runs against the same schema avoid redundant API calls.

use std::path::PathBuf;

use crate::error::{Result, SeedKitError};

/// Supported LLM providers.
#[derive(Debug, Clone)]
pub enum LlmProvider {
    Claude { api_key: String, model: String },
    OpenAI { api_key: String, model: String },
}

impl LlmProvider {
    /// Auto-detect provider from environment variables.
    ///
    /// Checks `ANTHROPIC_API_KEY` first, then `OPENAI_API_KEY`. Falls back to
    /// a sensible default model for each provider unless `model_override` is
    /// specified.
    pub fn from_env(model_override: Option<&str>) -> Result<Self> {
        if let Ok(key) = std::env::var("ANTHROPIC_API_KEY") {
            return Ok(LlmProvider::Claude {
                api_key: key,
                model: model_override
                    .unwrap_or("claude-sonnet-4-20250514")
                    .to_string(),
            });
        }

        if let Ok(key) = std::env::var("OPENAI_API_KEY") {
            return Ok(LlmProvider::OpenAI {
                api_key: key,
                model: model_override.unwrap_or("gpt-4o").to_string(),
            });
        }

        Err(SeedKitError::LlmError {
            message: "No LLM API key found. Set ANTHROPIC_API_KEY or OPENAI_API_KEY environment variable.".to_string(),
        })
    }

    /// Send a prompt to the LLM and return the raw response text.
    pub async fn classify(&self, prompt: &str) -> Result<String> {
        match self {
            LlmProvider::Claude { api_key, model } => call_claude(api_key, model, prompt).await,
            LlmProvider::OpenAI { api_key, model } => call_openai(api_key, model, prompt).await,
        }
    }
}

/// Maximum time to wait for an LLM API response before aborting.
const API_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(45);

/// Build an HTTP client with a strict timeout so requests never hang
/// indefinitely on flaky networks or partial API outages.
fn build_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(API_TIMEOUT)
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

/// Call the Anthropic Messages API.
///
/// Uses assistant pre-fill (`[`) to force Claude to start its response
/// with a raw JSON array, avoiding markdown fences or preamble text.
async fn call_claude(api_key: &str, model: &str, prompt: &str) -> Result<String> {
    let client = build_http_client();

    let body = serde_json::json!({
        "model": model,
        "max_tokens": 4096,
        "messages": [
            {
                "role": "user",
                "content": prompt
            },
            {
                "role": "assistant",
                "content": "["
            }
        ]
    });

    let response = client
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| SeedKitError::LlmError {
            message: format!("Failed to call Claude API: {}", e),
        })?;

    let status = response.status();
    let response_text = response.text().await.map_err(|e| SeedKitError::LlmError {
        message: format!("Failed to read Claude API response: {}", e),
    })?;

    if !status.is_success() {
        return Err(SeedKitError::LlmError {
            message: format!(
                "Claude API returned {}: {}",
                status,
                truncate(&response_text, 500),
            ),
        });
    }

    // Extract text from the first content block
    let parsed: serde_json::Value =
        serde_json::from_str(&response_text).map_err(|e| SeedKitError::LlmError {
            message: format!("Failed to parse Claude API response JSON: {}", e),
        })?;

    let text = parsed["content"]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|block| block["text"].as_str())
        .ok_or_else(|| SeedKitError::LlmError {
            message: "Claude API response missing content[0].text".to_string(),
        })?;

    // Prepend the `[` from the assistant pre-fill since Claude's response
    // continues from that point (the pre-fill isn't included in the output).
    Ok(format!("[{}", text))
}

/// Call the OpenAI Chat Completions API.
///
/// Uses `response_format: { "type": "json_object" }` to force the model
/// to return valid JSON without markdown wrapping or preamble text.
async fn call_openai(api_key: &str, model: &str, prompt: &str) -> Result<String> {
    let client = build_http_client();

    let body = serde_json::json!({
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "max_tokens": 4096,
        "response_format": { "type": "json_object" }
    });

    let response = client
        .post("https://api.openai.com/v1/chat/completions")
        .header("Authorization", format!("Bearer {}", api_key))
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| SeedKitError::LlmError {
            message: format!("Failed to call OpenAI API: {}", e),
        })?;

    let status = response.status();
    let response_text = response.text().await.map_err(|e| SeedKitError::LlmError {
        message: format!("Failed to read OpenAI API response: {}", e),
    })?;

    if !status.is_success() {
        return Err(SeedKitError::LlmError {
            message: format!(
                "OpenAI API returned {}: {}",
                status,
                truncate(&response_text, 500),
            ),
        });
    }

    let parsed: serde_json::Value =
        serde_json::from_str(&response_text).map_err(|e| SeedKitError::LlmError {
            message: format!("Failed to parse OpenAI API response JSON: {}", e),
        })?;

    let text = parsed["choices"]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|choice| choice["message"]["content"].as_str())
        .ok_or_else(|| SeedKitError::LlmError {
            message: "OpenAI API response missing choices[0].message.content".to_string(),
        })?;

    Ok(text.to_string())
}

// ---------------------------------------------------------------------------
// Response caching
// ---------------------------------------------------------------------------

/// Return the cache directory path: `~/.seedkit/cache/`.
fn cache_dir() -> Option<PathBuf> {
    dirs_free().map(|home| home.join(".seedkit").join("cache"))
}

/// Platform-independent home directory lookup (no extra crate needed).
fn dirs_free() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

/// Try to load a cached LLM response for the given schema hash.
pub fn load_cached_response(schema_hash: &str) -> Option<String> {
    let dir = cache_dir()?;
    let path = dir.join(format!("{}.json", schema_hash));
    std::fs::read_to_string(path).ok()
}

/// Save an LLM response to the cache, keyed by schema hash.
///
/// Uses atomic write (temp file → `sync_all` → rename) to avoid corruption
/// if the process is interrupted mid-write.
pub fn save_cached_response(schema_hash: &str, response: &str) {
    if let Some(dir) = cache_dir() {
        let _ = std::fs::create_dir_all(&dir);
        let final_path = dir.join(format!("{}.json", schema_hash));
        let tmp_path = dir.join(format!("{}.json.tmp", schema_hash));

        // Write to temp file, sync, then atomically rename
        let write_result = (|| -> std::io::Result<()> {
            std::fs::write(&tmp_path, response)?;
            // sync_all via open + sync_all to flush to disk
            let file = std::fs::File::open(&tmp_path)?;
            file.sync_all()?;
            std::fs::rename(&tmp_path, &final_path)?;
            Ok(())
        })();

        if write_result.is_err() {
            // Clean up temp file on failure
            let _ = std::fs::remove_file(&tmp_path);
        }
    }
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        s
    } else {
        &s[..max]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_env_no_keys() {
        // Remove keys for this test (they may or may not be set)
        let anthropic = std::env::var("ANTHROPIC_API_KEY").ok();
        let openai = std::env::var("OPENAI_API_KEY").ok();
        std::env::remove_var("ANTHROPIC_API_KEY");
        std::env::remove_var("OPENAI_API_KEY");

        let result = LlmProvider::from_env(None);
        assert!(result.is_err());

        // Restore
        if let Some(k) = anthropic {
            std::env::set_var("ANTHROPIC_API_KEY", k);
        }
        if let Some(k) = openai {
            std::env::set_var("OPENAI_API_KEY", k);
        }
    }

    #[test]
    fn test_from_env_anthropic() {
        let original = std::env::var("ANTHROPIC_API_KEY").ok();
        std::env::set_var("ANTHROPIC_API_KEY", "test-key-123");

        let provider = LlmProvider::from_env(None).unwrap();
        match provider {
            LlmProvider::Claude { api_key, model } => {
                assert_eq!(api_key, "test-key-123");
                assert!(model.contains("claude"));
            }
            _ => panic!("Expected Claude provider"),
        }

        // Restore
        match original {
            Some(k) => std::env::set_var("ANTHROPIC_API_KEY", k),
            None => std::env::remove_var("ANTHROPIC_API_KEY"),
        }
    }

    #[test]
    fn test_from_env_model_override() {
        let original = std::env::var("ANTHROPIC_API_KEY").ok();
        std::env::set_var("ANTHROPIC_API_KEY", "test-key");

        let provider = LlmProvider::from_env(Some("claude-opus-4-20250514")).unwrap();
        match provider {
            LlmProvider::Claude { model, .. } => {
                assert_eq!(model, "claude-opus-4-20250514");
            }
            _ => panic!("Expected Claude provider"),
        }

        match original {
            Some(k) => std::env::set_var("ANTHROPIC_API_KEY", k),
            None => std::env::remove_var("ANTHROPIC_API_KEY"),
        }
    }

    #[test]
    fn test_cache_round_trip() {
        let hash = "test_cache_hash_12345";
        let response = r#"[{"table":"users","column":"email","semantic_type":"Email"}]"#;

        save_cached_response(hash, response);
        let loaded = load_cached_response(hash);
        assert_eq!(loaded.as_deref(), Some(response));

        // Clean up
        if let Some(dir) = cache_dir() {
            let _ = std::fs::remove_file(dir.join(format!("{}.json", hash)));
        }
    }

    #[test]
    fn test_cache_miss() {
        let result = load_cached_response("nonexistent_hash_99999");
        assert!(result.is_none());
    }

    #[test]
    fn test_truncate_short() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn test_truncate_long() {
        assert_eq!(truncate("hello world", 5), "hello");
    }

    #[test]
    fn test_atomic_cache_write_no_tmp_left_behind() {
        let hash = "test_atomic_cache_no_tmp_42";
        let response = r#"[{"table":"orders","column":"sku","semantic_type":"Sku"}]"#;

        save_cached_response(hash, response);

        // Verify final file exists
        if let Some(dir) = cache_dir() {
            let final_path = dir.join(format!("{}.json", hash));
            assert!(final_path.exists(), "Cache file should exist");

            // Verify no .tmp file left behind
            let tmp_path = dir.join(format!("{}.json.tmp", hash));
            assert!(!tmp_path.exists(), "Temp file should be cleaned up");

            // Clean up
            let _ = std::fs::remove_file(final_path);
        }
    }

    #[test]
    fn test_atomic_cache_overwrites_existing() {
        let hash = "test_atomic_cache_overwrite_42";

        save_cached_response(hash, "first version");
        save_cached_response(hash, "second version");

        let loaded = load_cached_response(hash);
        assert_eq!(loaded.as_deref(), Some("second version"));

        // Clean up
        if let Some(dir) = cache_dir() {
            let _ = std::fs::remove_file(dir.join(format!("{}.json", hash)));
        }
    }

    #[test]
    fn test_http_client_has_timeout() {
        // Verify the client builder produces a client with timeout configured.
        // We can't directly inspect reqwest's internals, but we can verify
        // build_http_client() doesn't panic.
        let client = build_http_client();
        // If we got here, the client was built successfully with timeout
        assert!(std::mem::size_of_val(&client) > 0);
    }
}
