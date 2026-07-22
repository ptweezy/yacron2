# Licensing

This repository is **MIT-licensed by default**. The full text is in
[LICENSE](LICENSE), and it applies to everything in the repository **except** a
directory that ships its own `LICENSE` file, which governs that directory
instead.

## Why this file exists

cronstable is planned to grow beyond its open-source core: paid/premium features
and native apps (for example on iOS) will live alongside the MIT core. Because
MIT is a permissive, non-copyleft license, mixing it with proprietary code in
one repository is legally fine: there is no copyleft that "reaches" into adjacent
directories. The only real risk is *ambiguity*, so this file plus per-directory
`LICENSE` files make the boundary explicit, and no one can reasonably assume the
whole tree is MIT.

## The rule

1. The root [LICENSE](LICENSE) (MIT) governs the whole repository by default.
2. A directory that contains its own `LICENSE` file is governed **only** by that
   license, for that directory and everything under it.
3. For any file, the nearest `LICENSE` found walking up the directory tree wins.
   If none is found before the root, the root MIT LICENSE applies.

## Current layout

| Path | License | Notes |
| --- | --- | --- |
| `/` core (`cronstable/`, docs, tests, CI, packaging, ...) | MIT | See [LICENSE](LICENSE). |
| `pro/` | Proprietary | cronstable Pro (the `cronstable-pro` package). See [pro/LICENSE](pro/LICENSE). Not open source. |
| `ios/` | Proprietary | The native iOS app. See [ios/LICENSE](ios/LICENSE). Not open source. |

As more proprietary components are added, each gets its own `LICENSE` file under
the same rule, and a row here. Proprietary directories are pruned from the public
MIT sdist (see `MANIFEST.in`), so they are never distributed through PyPI.

## Keeping the boundary clean

- Proprietary code may **import** the MIT core; MIT permits proprietary software
  to build on it.
- Do **not** copy MIT-licensed source *into* a proprietary directory. Importing
  the core is fine; vendoring its source there would pull MIT-covered code (and
  its attribution obligation) into a proprietary tree. Keep the boundary at the
  import level.
- New files in a proprietary directory carry an SPDX header so the license is
  unambiguous even out of context:

  ```text
  # SPDX-License-Identifier: LicenseRef-cronstable-Proprietary
  ```

  Core files rely on the root LICENSE and need no header; they may optionally
  carry `# SPDX-License-Identifier: MIT`.

## Third-party code and dependencies

cronstable is a fork of [yacron](https://github.com/gjcarneiro/yacron) (MIT); the
root LICENSE preserves yacron's copyright alongside cronstable's, as MIT
requires.

Runtime dependencies are all permissive (MIT / BSD / Apache / PSF / MPL). A CI
guard ([.github/scripts/check_licenses.py](.github/scripts/check_licenses.py), run by the
`licenses` job) fails the build if a strong-copyleft (GPL / AGPL) or non-open
source-available (SSPL / BUSL) dependency is ever introduced, so the permissive
baseline cannot regress by accident. This matters because the shipped artifacts
(the PyInstaller binaries and Docker images) bundle the whole dependency tree.

## Trademarks

The MIT License covers the code, not the brand. The cronstable name and logo are
trademarks; see [TRADEMARKS.md](TRADEMARKS.md).
