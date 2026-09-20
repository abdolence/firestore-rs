# Quick start

Cargo.toml:

```toml
[dependencies]
firestore = "0.52"
```

## Crypto provider error

Depends on your other dependencies you may see the error like:

```text
no process-level CryptoProvider available -- call CryptoProvider::install_default() before this point 
```

This is because the TLS providers are not installed by default and you can choose different.
The easiest way to fix is just to include one of the provider, for example:

```toml
[dependencies]
rustls = "0.23"
```

If you have multiple you may need to call `CryptoProvider::install_default()` before using the Firestore client, e.g.:

```rust,ignore
rustls::crypto::ring::default_provider().install_default().expect("Failed to install rustls crypto provider");
```
