use azure_core::auth::TokenCredential;
use azure_security_keyvault::KeyClient;
use azure_security_keyvault::prelude::{
    CryptographParamtersEncryption, DecryptParameters, EncryptParameters,
};
use log::error;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, mpsc};
use tink_core::TinkError;
use tink_core::utils::wrap_err;
use url::Url;

const VERSION_PREFIX: &[u8] = b"azure:v1:";
const VERSION_LENGTH: usize = 32;
// +1 for the ':' separating the version from the raw ciphertext bytes.
const HEADER_LENGTH: usize = VERSION_PREFIX.len() + VERSION_LENGTH + 1;

/// `AzureAead` represents a Azure KMS service to a particular URI.
///
/// Unlike AWS KMS and GCP KMS, Azure Key Vault wrap/unwrap operations are scoped to an explicit
/// key version and do not embed that version in the ciphertext. When `save_version` is enabled
/// (see `azure_driver::ENCRYPT_AZURE_KEY_VERSION_SAVE`), `encrypt` makes its output
/// self-describing by prepending the exact version that produced it:
/// `azure:v1:<32-character key version>:<raw ciphertext bytes>`.
///
/// `decrypt` always checks for this prefix regardless of the current `save_version` value, since
/// a DEK wrapped while the toggle was on must remain decryptable even after it is turned back off.
#[derive(Clone)]
pub struct AzureAead {
    kms: KeyClient,
    key_name: String,
    key_version: Option<String>,
    algorithm: CryptographParamtersEncryption,
    save_version: bool,
}

impl AzureAead {
    /// Return a new Azure KMS service.
    pub(crate) fn new(
        key_url: &str,
        creds: Arc<dyn TokenCredential>,
        algorithm: CryptographParamtersEncryption,
        save_version: bool,
    ) -> Result<AzureAead, TinkError> {
        let (vault_url, key_name, key_version) = get_key_info(key_url)?;
        let kms = KeyClient::new(&vault_url, creds)
            .map_err(|e| wrap_err("failed to create KeyClient", e))?;
        Ok(AzureAead {
            kms,
            key_name,
            key_version,
            algorithm,
            save_version,
        })
    }

    async fn encrypt_inner(&self, plaintext: Vec<u8>) -> Result<Vec<u8>, TinkError> {
        if !self.save_version {
            let params = EncryptParameters {
                encrypt_parameters_encryption: self.algorithm.clone(),
                plaintext,
            };
            let mut req = self.kms.encrypt(self.key_name.clone(), params);
            if let Some(version) = self.key_version.clone() {
                req = req.version(version);
            }
            return req
                .into_future()
                .await
                .map(|r| r.result)
                .map_err(|e| wrap_err("failed to encrypt", e));
        }

        // If the kek's key URI already pins an explicit version, respect it as-is and don't
        // resolve "current" -- resolving "latest" here would silently substitute a different key
        // version than the one the user explicitly configured, and would only matter for a
        // versionless key URI in the first place (a pinned version never "rotates" from this
        // caller's perspective).
        let version = if let Some(version) = self.key_version.clone() {
            version
        } else {
            let key = self
                .kms
                .get(self.key_name.clone())
                .into_future()
                .await
                .map_err(|e| {
                    wrap_err("failed to resolve current Azure Key Vault key version", e)
                })?;
            let resolved_id = key
                .key
                .id
                .ok_or_else(|| TinkError::new("resolved Azure Key Vault key is missing an id"))?;
            resolved_id
                .rsplit('/')
                .next()
                .unwrap_or_default()
                .to_string()
        };
        if !is_valid_version(&version) {
            // Mirrors decrypt's own validation: a DEK this method wraps must always be one this
            // same type can later unwrap.
            return Err(format!(
                "kms key version '{version}' must be a {VERSION_LENGTH}-character hex string; \
                 cannot be embedded in a fixed-width azure:v1: prefix"
            )
            .into());
        }

        let params = EncryptParameters {
            encrypt_parameters_encryption: self.algorithm.clone(),
            plaintext,
        };
        let ciphertext = self
            .kms
            .encrypt(self.key_name.clone(), params)
            .version(version.clone())
            .into_future()
            .await
            .map(|r| r.result)
            .map_err(|e| wrap_err("failed to encrypt", e))?;

        let mut output =
            Vec::with_capacity(VERSION_PREFIX.len() + version.len() + 1 + ciphertext.len());
        output.extend_from_slice(VERSION_PREFIX);
        output.extend_from_slice(version.as_bytes());
        output.push(b':');
        output.extend_from_slice(&ciphertext);
        Ok(output)
    }

    async fn decrypt_inner(&self, ciphertext: Vec<u8>) -> Result<Vec<u8>, TinkError> {
        let version = extract_version(&ciphertext);
        let (wrapped, target_version): (Vec<u8>, Option<String>) = match version {
            Some(v) => {
                if !is_valid_version(&v) {
                    // The encrypted key material is unauthenticated at this layer, so a
                    // corrupted or tampered value could otherwise smuggle arbitrary characters
                    // (e.g. '/') into the key version passed to the Azure SDK below.
                    return Err(format!(
                        "ciphertext carries an invalid azure:v1: key version: '{v}'"
                    )
                    .into());
                }
                (ciphertext[HEADER_LENGTH..].to_vec(), Some(v))
            }
            None => (ciphertext, self.key_version.clone()),
        };

        let params = DecryptParameters {
            decrypt_parameters_encryption: self.algorithm.clone(),
            ciphertext: wrapped,
        };
        let mut req = self.kms.decrypt(self.key_name.clone(), params);
        if let Some(version) = target_version {
            req = req.version(version);
        }
        req.into_future()
            .await
            .map(|r| r.result)
            .map_err(|e| wrap_err("request failed", e))
    }

    async fn encrypt_async(
        self,
        plaintext: Vec<u8>,
        _additional_data: Vec<u8>,
        sender: SyncSender<Result<Vec<u8>, TinkError>>,
    ) {
        let result = self.encrypt_inner(plaintext).await;
        if result.is_err() {
            error!("failed to encrypt: {result:?}");
        }
        if sender.send(result).is_err() {
            error!("failed to send result");
        }
    }

    async fn decrypt_async(
        self,
        ciphertext: Vec<u8>,
        _additional_data: Vec<u8>,
        sender: SyncSender<Result<Vec<u8>, TinkError>>,
    ) {
        let result = self.decrypt_inner(ciphertext).await;
        if result.is_err() {
            error!("failed to decrypt: {result:?}");
        }
        if sender.send(result).is_err() {
            error!("failed to send result");
        }
    }
}

impl tink_core::Aead for AzureAead {
    fn encrypt(&self, plaintext: &[u8], additional_data: &[u8]) -> Result<Vec<u8>, TinkError> {
        let (sender, receiver) = mpsc::sync_channel(1);
        let this = self.clone();
        let plaintext_vec = plaintext.to_vec();
        let ad_vec = additional_data.to_vec();
        tokio::spawn(async move { this.encrypt_async(plaintext_vec, ad_vec, sender).await });
        receiver
            .recv()
            .map_err(|e| wrap_err("failed to receive", e))?
    }

    /// Returns an error if the `key_id` field in the response does not match the `key_uri`
    /// provided when creating the client. If we don't do this, the possibility exists
    /// for the ciphertext to be replaced by one under a key we don't control/expect,
    /// but do have decrypt permissions on.
    fn decrypt(&self, ciphertext: &[u8], additional_data: &[u8]) -> Result<Vec<u8>, TinkError> {
        let (sender, receiver) = mpsc::sync_channel(1);
        let this = self.clone();
        let cipher_vec = ciphertext.to_vec();
        let ad_vec = additional_data.to_vec();
        tokio::spawn(async move { this.decrypt_async(cipher_vec, ad_vec, sender).await });
        receiver
            .recv()
            .map_err(|e| wrap_err("failed to receive", e))?
    }
}

fn get_key_info(key_uri: &str) -> Result<(String, String, Option<String>), TinkError> {
    let parsed = Url::parse(key_uri).map_err(|e| wrap_err("failed to parse URI", e))?;
    let path = parsed.path();
    let parts: Vec<&str> = path.split('/').collect();
    let len = parts.len();
    // Valid paths:
    // - /keys/{key_name} (3 parts: ["", "keys", "{key_name}"])
    // - /keys/{key_name}/{version} (4 parts: ["", "keys", "{key_name}", "{version}"])
    if (len != 3 && len != 4) || !parts[0].is_empty() || parts[1] != "keys" {
        return Err("invalid key uri".into());
    }
    let vault_url = parsed.scheme().to_string() + "://" + parsed.host_str().unwrap_or("localhost");
    let key_name = parts[2].to_string();
    let key_version = if len == 4 && !parts[3].is_empty() {
        Some(parts[3].to_string())
    } else {
        None
    };
    Ok((vault_url, key_name, key_version))
}

/// Returns true if `key_uri` has no explicit version segment. Used to warn when
/// `azure_driver::ENCRYPT_AZURE_KEY_VERSION_SAVE` is not enabled for a versionless key, without
/// performing any actual resolution (no `KeyClient` call).
pub(crate) fn is_versionless(key_uri: &str) -> Result<bool, TinkError> {
    let (_, _, version) = get_key_info(key_uri)?;
    Ok(version.is_none())
}

/// Returns true if `version` is exactly 32 hex characters, the only shape that can be embedded
/// in (and later parsed back out of) the fixed-width `azure:v1:` prefix. Used to validate both a
/// freshly resolved version (in encrypt) and one extracted from ciphertext (in decrypt), since
/// the encrypted key material is unauthenticated at this layer and could be corrupted or
/// tampered with.
fn is_valid_version(version: &str) -> bool {
    version.len() == VERSION_LENGTH && version.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Returns the embedded version if `ciphertext` carries the `azure:v1:` prefix (see struct doc),
/// or `None` if it does not (e.g. a legacy DEK wrapped before `ENCRYPT_AZURE_KEY_VERSION_SAVE`
/// was enabled on its KEK, or the toggle is not set). Returning `None` rather than an error is
/// deliberate: the toggle can be flipped on/off over a KEK's lifetime, and old, un-prefixed
/// ciphertext must remain decryptable.
fn extract_version(ciphertext: &[u8]) -> Option<String> {
    if ciphertext.len() < HEADER_LENGTH
        || &ciphertext[..VERSION_PREFIX.len()] != VERSION_PREFIX
        || ciphertext[HEADER_LENGTH - 1] != b':'
    {
        return None;
    }
    std::str::from_utf8(&ciphertext[VERSION_PREFIX.len()..VERSION_PREFIX.len() + VERSION_LENGTH])
        .ok()
        .map(|s| s.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    const VERSION_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn is_valid_version_accepts_exactly_32_hex_chars() {
        assert!(is_valid_version(VERSION_A));
        assert!(is_valid_version(&"F".repeat(32)));
    }

    #[test]
    fn is_valid_version_rejects_wrong_length() {
        assert!(!is_valid_version("not-32-chars"));
        assert!(!is_valid_version(""));
        assert!(!is_valid_version(&"a".repeat(31)));
        assert!(!is_valid_version(&"a".repeat(33)));
    }

    #[test]
    fn is_valid_version_rejects_non_hex_characters() {
        let non_hex = format!("g{}", &VERSION_A[1..]);
        assert!(!is_valid_version(&non_hex));
    }

    #[test]
    fn extract_version_returns_embedded_version_for_prefixed_ciphertext() {
        let ciphertext = format!("azure:v1:{VERSION_A}:wrapped-bytes");
        assert_eq!(
            Some(VERSION_A.to_string()),
            extract_version(ciphertext.as_bytes())
        );
    }

    #[test]
    fn extract_version_returns_none_for_legacy_unprefixed_ciphertext() {
        assert_eq!(None, extract_version(b"legacy-unprefixed-ciphertext"));
    }

    #[test]
    fn extract_version_returns_none_when_too_short() {
        assert_eq!(None, extract_version(b"azure:v1:short"));
    }

    #[test]
    fn extract_version_returns_none_when_missing_colon_separator() {
        // 32 characters after the prefix but no trailing ':' before the payload.
        let ciphertext = format!("azure:v1:{VERSION_A}Xwrapped-bytes");
        assert_eq!(None, extract_version(ciphertext.as_bytes()));
    }

    #[test]
    fn get_key_info_parses_versioned_id() {
        let (vault_url, key_name, key_version) = get_key_info(&format!(
            "https://vault.vault.azure.net/keys/key1/{VERSION_A}"
        ))
        .unwrap();
        assert_eq!("https://vault.vault.azure.net", vault_url);
        assert_eq!("key1", key_name);
        assert_eq!(Some(VERSION_A.to_string()), key_version);
    }

    #[test]
    fn get_key_info_parses_versionless_id() {
        let (_, _, key_version) = get_key_info("https://vault.vault.azure.net/keys/key1").unwrap();
        assert_eq!(None, key_version);
    }

    #[test]
    fn get_key_info_throws_for_malformed_id() {
        assert!(get_key_info("https://vault.vault.azure.net/notkeys/key1").is_err());
    }

    #[test]
    fn is_versionless_true_for_versionless_id() {
        assert!(is_versionless("https://vault.vault.azure.net/keys/key1").unwrap());
    }

    #[test]
    fn is_versionless_false_for_versioned_id() {
        assert!(
            !is_versionless(&format!(
                "https://vault.vault.azure.net/keys/key1/{VERSION_A}"
            ))
            .unwrap()
        );
    }
}
