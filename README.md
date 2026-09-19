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
- official mainnet/testnet pools, with optional two-distinct-connection agreement before recovery
  bodies are committed (`BeamSdkConfig.requireRecoveryQuorum`, off by default);
- bounded reversible shielded-output count checkpoints for reorgs, with safe full-count fallback
  for a fork deeper than retained history;
- one-sided receive tokens, balance/history, fee preview and durable
  `prepare → commit → resolve` sending;
- durable post-sync offline-signing context plus stopped-wallet `quoteSend`, `signOffline` and
  `exportSignedTransaction` flows for native BEAM one-sided sends;
- bounded stateless token parsing and signed-transaction inspection, plus isolated foreign
  transaction relay without opening or changing a wallet;
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
advanced or diagnostic recovery options. Recovery body data is accepted from a single node
connection by default, matching the official Beam wallet. Setting
`BeamSdkConfig.requireRecoveryQuorum` requires matching responses through two different live node
connections at the verified tip instead; a wallet that cannot reach two such nodes then reports
`BeamFailure.Quorum` rather than completing its scan. There is no public custom-node option in
either mode. New-wallet creation persists its creation timestamp in the
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
global Kermit writers and severity policy. Beam core's own log is separate and off by default:
`BeamSdkConfig.logLevel` above `BeamLogLevel.None` makes the core write a rotating file under
`$storagePath/logs` that contains addresses, amounts and heights, so it is a deliberate
diagnostic choice rather than a verbosity knob. Both sample hosts install `platformLogWriter()` at Debug
severity so their runs are diagnosable without additional setup. An embedding application that
already configures Kermit must not install a second writer for this SDK.

The selected height/date/full/snapshot restore source is written in the same encrypted WalletDB
transaction that creates the account, closing the process-death window before the first download or
node request. Durable send records can be resolved, and an unbroadcast `Prepared` record can be
aborted, while the network client is stopped.

After a normal synchronized session prepares a durable offline-signing context, its
`BeamOfflineSigningState.Ready.contextId` can be used with `quoteSend` and `signOffline` while the
wallet owner is stopped. Readiness describes the saved checkpoint and does not claim that it is the
current live tip. `signOffline` persists signed material without broadcasting or returning bytes.
Coins are reserved only while signing runs: once the signed material is durable the inputs are free
again and the expected change is dropped, like an unbroadcast Bitcoin transaction, so whoever spends
them first on chain wins. An interrupted sign is discarded by the next open, start or new offline
sign. A discarded or aborted sign still consumes one of the limited vouchers of an `Offline` or
`MaxPrivacy` token; once they run out the receiver has to provide a new token. `exportSignedTransaction` durably marks the operation `Exported` before returning canonical
bytes; repeated exports are byte-identical and cannot be revoked. An offline send is absent from
`transactions` until its kernel is observed on chain, then it is reported as `Completed`; the check
runs once per `start()`, for sends not yet observed.

`BeamTokenParser` and `BeamTransactionInspector` work without wallet state or network access.
Inspection accepts at most 1 MiB of canonical transaction bytes and also bounds vector counts,
aggregate elements and parsing depth. Its hash, kernel IDs, height ranges and counts are useful
display metadata, but context-free inspection does not prove amounts, receiver identity,
confirmation, spendability or source network. `BeamTransactionRelay` sends exact validated bytes
through an isolated client that creates no wallet, history or reservations. An `Accepted` relay
result means a node accepted the submission; it is not confirmation or finality.

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
BEAM_SNAPSHOT_REORG_FIXTURE="$PWD/.native-cache/build-host-tests/libbeam_sdk_kmp_snapshot_reorg_fixture.$([ "$(uname -s)" = Darwin ] && echo dylib || echo so)" \
BEAM_EXPECT_SNAPSHOT_REORG_FIXTURE=1 \
BEAM_SEND_ADMISSION_FIXTURE="$PWD/.native-cache/build-host-tests/libbeam_sdk_kmp_send_admission_fixture.$([ "$(uname -s)" = Darwin ] && echo dylib || echo so)" \
BEAM_EXPECT_SEND_ADMISSION_FIXTURE=1 \
BEAM_OFFLINE_HISTORY_FIXTURE="$PWD/.native-cache/build-host-tests/libbeam_sdk_kmp_offline_history_fixture.$([ "$(uname -s)" = Darwin ] && echo dylib || echo so)" \
BEAM_EXPECT_OFFLINE_HISTORY_FIXTURE=1 \
BEAM_EXPECT_NATIVE_TEST_FIXTURES=1 \
  ./gradlew :beam-sdk:desktopTest :sample-shared:desktopTest

# Optional and expensive: generate real production-sized offline signer proofs.
BEAM_NATIVE_BUILD_DIR=.native-cache/build-host-tests \
  BEAM_NATIVE_TESTS=1 BEAM_NATIVE_PRODUCTION_PROOFS=small ./scripts/build-native-host.sh

# Restage a production library without test-only JNI fixtures before publishing.
BEAM_NATIVE_BUILD_DIR=.native-cache/build-host-release ./scripts/build-native-host.sh

./scripts/fetch-android-dependencies.sh
ANDROID_HOME=/path/to/android-sdk ./scripts/build-native-android.sh
```

On Windows PowerShell, the instrumented test command is:

```powershell
$env:BEAM_SNAPSHOT_REORG_FIXTURE = (Resolve-Path '.native-cache/build-host-tests/Release/beam_sdk_kmp_snapshot_reorg_fixture.dll').Path
$env:BEAM_EXPECT_SNAPSHOT_REORG_FIXTURE = '1'
$env:BEAM_SEND_ADMISSION_FIXTURE = (Resolve-Path '.native-cache/build-host-tests/Release/beam_sdk_kmp_send_admission_fixture.dll').Path
$env:BEAM_EXPECT_SEND_ADMISSION_FIXTURE = '1'
$env:BEAM_OFFLINE_HISTORY_FIXTURE = (Resolve-Path '.native-cache/build-host-tests/Release/beam_sdk_kmp_offline_history_fixture.dll').Path
$env:BEAM_EXPECT_OFFLINE_HISTORY_FIXTURE = '1'
$env:BEAM_EXPECT_NATIVE_TEST_FIXTURES = '1'
./gradlew :beam-sdk:desktopTest :sample-shared:desktopTest
```

`BEAM_NATIVE_TESTS=1` runs the deterministic wallet, admission, reorg, offline-context,
offline-signing feasibility, stateless-codec, offline-signer and transaction-relay native tests on
supported desktop hosts. Production-size signer proof generation is intentionally opt-in with
`BEAM_NATIVE_PRODUCTION_PROOFS=small` (`--production-small-proofs`, historical 1,024-member proof)
or `full` (`--production-proofs`, 65,536-member proof). A manually dispatched CI run exposes the
same choice on the Linux desktop job only.

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
build and stage the host library first as shown above. With a production native, fixture-only tests
are reported as skipped; the instrumented commands above require and execute them instead of
silently skipping them:

```shell
./gradlew :beam-sdk:desktopTest :sample-shared:desktopTest
```

To run the desktop demo:

```shell
./gradlew :sample-desktop:run
```

For a local wallet test, put the mnemonic only in the Git-ignored root `local.properties`. The
generated config stays under `sample-shared/build/`:

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

The mainnet snapshot restore test is also opt-in. Run it with JDK 21; it reads `words` and
`beam.databaseKey` from the Git-ignored root `local.properties` through the generated demo config,
creates a fresh temporary wallet, and uses the official default snapshot URL. Expect approximately
352 MB of network traffic:

```shell
BEAM_LIVE_MAINNET_RESTORE=1 ./gradlew :sample-shared:desktopTest \
  --tests cash.p.beam.sample.MainnetSnapshotRestoreLiveTest --rerun-tasks
```

This live mainnet test must not be enabled in the default CI test run.
