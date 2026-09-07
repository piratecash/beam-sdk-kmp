# Beam SDK KMP

Kotlin Multiplatform/JVM wrapper around Beam Core for Android and desktop wallets.

The implementation is pinned to Beam Core `beam-7.5.14493`
(`9c4366aae08e7fbde7bc8d65f828a0deace68bfc`). It supports Android arm64-v8a and
armeabi-v7a plus Linux x86_64, Windows x86_64, and macOS arm64 desktop binaries.

The SDK owns an encrypted Beam WalletDB. It never persists a mnemonic or BIP39 seed. Its security,
recovery, lifecycle, and release contract is summarized below.

## What is implemented

- encrypted Beam Core `WalletDB` as the only wallet ledger;
- create/open/height/date/full/snapshot restore flows;
- official mainnet/testnet pools with two-distinct-connection agreement before recovery bodies are
  committed;
- bounded reversible shielded-output count checkpoints for reorgs, with safe full-count fallback
  for a fork deeper than retained history;
- one-sided receive tokens, balance/history, fee preview and durable
  `prepare → commit → resolve` sending;
- serialized, idempotent `start/stop/close`, including cancellation during snapshot download/import
  and Android demo foreground/background handoff;
- a shared Compose demo hosted by Android and desktop JVM.

Height is inclusive. Date recovery starts at `00:00 UTC` with a 1,440-block safety window. A
snapshot is optional: the default official download is hundreds of MiB, while height/date restore
uses a consensus-bound genesis fallback and scans earlier bodies in count-only mode. No wallet
records are created before the chosen birthday, but without a newer trusted checkpoint this safe
fallback still transfers historical body data. A mainnet measurement for a wallet created less
than one day earlier received 1.74 GB by approximately 7.5% progress, so height/date recovery is
not recommended for normal end-user UX with the current checkpoint strategy. Prefer
`SnapshotThenScan`, show the server-reported size before download, and retain height/date only as
advanced or diagnostic recovery options. Recovery body data is accepted only after matching
responses through two different live node connections at the verified tip; there is no single-node
fallback or public custom-node option. New-wallet creation persists its creation timestamp in the
same encrypted DB transaction before any receive token can be issued. Initial sync resolves that
timestamp against authenticated historical headers and subtracts the same 1,440-block safety
window, so a payment made to an early token while headers are still syncing is not skipped.

The rotating official snapshot has no separately published signed digest. The SDK therefore treats
it only as an optional acceleration candidate: pinned Beam Core verifies its consensus rules,
chain-work proof and live-state roots before the encrypted WalletDB transaction is committed. An
explicit custom snapshot URL is accepted only together with a trusted SHA-256. A corrupt,
incompatible or cancelled import is rolled back and never replaces committed wallet state. An
interrupted import keeps its downloaded candidate so reopening the same WalletDB can resume without
downloading hundreds of MiB again; interrupted downloads resume from their strong ETag. Snapshot
HTTP clients do not follow redirects automatically: the downloader resolves at most five hops and
validates HTTPS, port, credentials, fragment and allowlisted host before issuing every next request.
mode, URL and optional trusted hash are committed to encrypted WalletDB before the first network
byte, so a reopen cannot silently fall back to a full scan. The candidate is deleted only after
native import reaches catch-up.

The SDK accepts a 64-byte BIP39 seed and a separate random 32-byte WalletDB key. It copies and wipes
both inputs at the Kotlin/JNI boundary and never stores the seed. The embedding Android application
is responsible for wrapping the DB key with Android Keystore.

The SDK logs lifecycle transitions, sync phases and failures through Kermit under the `BeamSDK`
tag, without logging seeds, database keys or payment tokens. The embedding application owns the
global Kermit writers and severity policy. Both sample hosts install `platformLogWriter()` at Debug
severity so their runs are diagnosable without additional setup; P.CASH should keep its existing
debug/release Kermit configuration and must not install a second writer for this SDK.

The selected height/date/full/snapshot restore source is written in the same encrypted WalletDB
transaction that creates the account, closing the process-death window before the first download or
node request. Durable send records can be resolved, and an unbroadcast `Prepared` record can be
aborted, while the network client is stopped.

## Build native libraries

Use JDK 21, CMake 3.24+ and the pinned inputs in `native/*.lock`.

`fetch-beam-core.sh` clones the exact commit from `native/beam-core.lock`, verifies `HEAD`, and
applies every checked-in `native/patches/*.patch` in lexical order. It records the complete ordered
patch-stack digest and resulting Core tree digest in the checkout's git directory, so a repeated run
is idempotent while any changed patch or post-apply source mutation fails closed. Before the first
application it also requires a pristine tracked tree/index, no non-ignored untracked files, and clean
submodules at their pinned revisions. The host/Android build scripts then compile that patched source.

```shell
./scripts/fetch-beam-core.sh
./scripts/fetch-host-dependencies.sh
./scripts/build-native-host.sh

# Also build the instrumented library and run native/JNI fixture regressions.
BEAM_NATIVE_BUILD_DIR=.native-cache/build-host-tests \
  BEAM_NATIVE_TESTS=1 ./scripts/build-native-host.sh
BEAM_EXPECT_NATIVE_TEST_FIXTURES=1 \
  ./gradlew :beam-sdk:desktopTest :sample-shared:desktopTest

# Restage a production library without test-only JNI fixtures before publishing.
BEAM_NATIVE_BUILD_DIR=.native-cache/build-host-release ./scripts/build-native-host.sh

./scripts/fetch-android-dependencies.sh
ANDROID_HOME=/path/to/android-sdk ./scripts/build-native-android.sh
```

The Android build uses NDK `27.0.12077973`, API 27, static C++ runtime and 16 KiB ELF page
alignment. It stages `arm64-v8a` and `armeabi-v7a` libraries under
`beam-sdk/prebuilt/android/native`. Release CI builds macOS arm64, Linux x86_64 and Windows x86_64
separately. Publication is also blocked until checked-in arm64 and armv7 device evidence matches the
exact tagged commit; `release-evidence.json` intentionally remains pending until those tests run.

Release CI, JitPack and consumers have different responsibilities:

**JitPack never applies the Beam Core patches.** It packages native archives that release CI has
already built from the pinned Core commit plus those patches.

1. Release CI reproduces Beam Core from the lock file plus the ordered patches, builds all native
   targets, records checksums and uploads the native archives to the matching GitHub tag.
2. JitPack accepts only a `v*` tag, downloads those checksum-verified native archives, stages them in
   the KMP module and publishes the Maven artifact. It does not rebuild C++ on a single JitPack host.
3. A normal Gradle/Maven consumer downloads the published SDK with its packaged native libraries;
   it neither downloads Beam Core nor applies patches.

Anyone auditing or rebuilding from source can execute the commands above and obtains the same
patched Core and host dependency inputs. `native/host-dependencies.lock` pins Boost for each host,
the Windows Beam library bundle and the exact official OpenSSL source commit used for Linux/macOS;
no rolling Homebrew or `libssl-dev` package is linked into release natives. Patch provenance and
purpose are documented in the patch headers.

## Local checks

Use JDK 21 for Gradle. `beam-sdk:desktopTest` also runs native load/create/reopen smoke tests, so
build and stage the host library first as shown above. With a production native, the single
fixture-only stopped-send test is reported as skipped; the instrumented commands above require and
execute it instead of silently skipping it:

```shell
./gradlew :beam-sdk:desktopTest :sample-shared:desktopTest
```

To run the desktop demo:

```shell
./gradlew :sample-desktop:run
```

For a local wallet test, put the mnemonic only in the Git-ignored root `local.properties`, following
the Bitcoin Kit demo convention. The generated config stays under `sample-shared/build/`:

```properties
words="<your test BIP39 seed phrase>"
beam.databaseKey=64-random-hex-characters
beam.network=Mainnet
beam.storagePath=/absolute/local/path/for/the/test-wallet
```

The official-node integration test is opt-in because it performs external network I/O:

```shell
BEAM_LIVE_TEST=1 ./gradlew :beam-sdk:desktopTest \
  --tests cash.p.beam.NativeLiveSyncTest --rerun-tasks
```

P.CASH integration is intentionally not part of this repository change and starts only after the
user confirms the target P.CASH branch containing Bitcoin KMP PR #496.
