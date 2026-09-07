# Third-party native components

The distributed native libraries are built from the following pinned inputs. Release CI records
their exact checksums in `natives.sha256`.

| Component | Revision | License |
| --- | --- | --- |
| Beam Core `beam-7.5.14493` | `9c4366aae08e7fbde7bc8d65f828a0deace68bfc` | Apache-2.0; see upstream `LICENSE` |
| Beam Android Boost bundle | `3686875630f11ee7f09a9f7c3ae45d448df3ffb0` | Boost Software License 1.0 |
| Beam Android OpenSSL bundle (OpenSSL 1.1.1i) | `14023e60e74bf616bfe08258f19568ceb2e1a246` | OpenSSL/SSLeay licenses |
| Beam host Boost bundles (Linux/macOS/Windows) | `603166c5dcd4a726247d5e6bd2d3a9b3fe18de8c` / `c2227480b6be0638c08f3012366877b9867e0cba` / `a53681df38cf63fad4dc7d1c778024a951da622b` | Boost Software License 1.0 |
| Boost.Filesystem 1.90.0 source (Linux PIC rebuild) | `868dc76bb6dd09923a0f4b90f2f2aad25d9f51d6` | Boost Software License 1.0 |
| OpenSSL 3.0.18 host source | `c10e643222ce5a185f2980d7352d545afacb0fed` | Apache-2.0 |
| BeamMW Windows library bundle | `7c009700b45f1206a388df9f79ec2f5ab6c3789f` | Component licenses supplied by the pinned upstream bundle |

Beam Core includes additional vendored dependencies under its `3rdparty` directory. Their notices
and source are available in the exact Core revision above. Before a public binary release, the
release checklist must archive the complete license inventory and generated SBOM alongside the
native artifacts.
