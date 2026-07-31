# cloudreve-api-native

Rust napi bindings layer for [Cloudrs](https://github.com/Cloudrs/Cloudrs-ohos), the HarmonyOS client for Cloudreve.

It wraps the [`cloudreve-api`](https://crates.io/crates/cloudreve-api) crate (which implements both the Cloudreve V3 and V4 client APIs) together with `reqwest` + `tokio` for async networking, and compiles into `libcloudreve.so` — the native library loaded by the Cloudrs ArkTS layer via N-API.

## Architecture

- **`napi` / `napi-derive`** — exposes Rust functions/types to ArkTS as N-API bindings.
- **`cloudreve-api`** — the actual Cloudreve V3/V4 HTTP client logic.
- **`reqwest` + `tokio`** — async runtime and HTTP transport (with `rustls`).
- The resulting `cdylib` (`libcloudreve.so`) is placed into the host project's `entry/libs/<abi>/`.

## Build

The pre-built `.so` files are already shipped in the parent project's `entry/libs/`, so most Cloudrs contributors do **not** need a Rust toolchain. To rebuild after changing native code:

### Prerequisites

- Rust toolchain with the HarmonyOS targets:
  ```sh
  rustup target add aarch64-unknown-linux-ohos x86_64-unknown-linux-ohos
  ```
- OpenHarmony native SDK (bundled with DevEco Studio).

### Build

```sh
# macOS / Linux
./build-ohos.sh

# Windows
pwsh ./build-ohos.ps1
```

The script cross-compiles both `arm64-v8a` and `x86_64` targets, verifies there are no undefined `ring_core_*` symbols (a guard against the silent empty-archive failure), and copies the artifacts to `../entry/libs/<abi>/libcloudreve.so`.

Set `OHOS_SDK_NATIVE` if your SDK is not at the default DevEco Studio location.

## License

Licensed under [**GPL-3.0**](LICENSE), Copyright © 2026 Dreamfly Tech and Cloudrs contributors.
