# Kotoba ORC v1 (header / magic)

This tree is a **header-and-tail reader** for a tiny ORC fixture. It is a
sibling of `c++/` and `java/` on this fork only.

## Honest scope

What this does:

- Checks the 3-byte file magic `ORC`.
- Reads the last-byte postscript length, then the uncompressed protobuf
  PostScript (footer length, `CompressionKind`, trailing magic `ORC`).
- Reads enough of the uncompressed Footer to report **stripe count** and the
  first struct field name on the vendored fixture (`id`).

What this does **not** do:

- Stripe body decode, indexes, bloom filters, or column encodings.
- Predicate pushdown, projection, or a query engine.
- Generic compression (zlib / snappy / zstd). The fixture is `NONE`.
- IEEE floats. The module is **i64-v1** (`kotoba.value/i64-v1`).
- FFI into the C++ or Java readers.
- Robotics, or replacing those readers in production.

This is not a replacement for the Apache ORC C++ or Java libraries.

## Layout

| Path | Role |
| --- | --- |
| `orc.kotoba` | Kotoba 0.7.2 module: magic + postscript + footer fields |
| `fixtures/tiny.orc` | 42-byte uncompressed ORC file (0 stripes, schema `struct<id:bigint>`) |
| `checks.sh` | Downloads Kotoba 0.7.2, compiles `wasm32-kotoba-v1`, asserts header fields |

`tiny.orc` is hand-shrunk. A real writer footer is larger; this file keeps
only the fields the v1 reader claims.

## Checks

Requires Linux amd64, or `KOTOBA` pointing at a Kotoba 0.7.2 CLI.

```sh
kotoba/checks.sh
```

The script fails unless all of the following are observed (not assumed):

1. `kotoba compile orc.kotoba --target wasm --json` parses as JSON with
   `kotoba.cli/ok?` true and `kotoba.cli/code` equal to `emitted`, plus
   `value-profile` `i64-v1` and compatibility `target` `wasm32-kotoba-v1`.
2. The emitted wasm has no import section (section id 2).
3. `kotoba run orc.kotoba` reports `:kotoba.runtime/ok? true` and
   `:kotoba.runtime/value 111102`.
4. Every `fixture-byte` literal in `orc.kotoba` matches `fixtures/tiny.orc`.

`111102` means: header magic, postscript magic, compression `NONE`, field
name `id`, 0 stripes, 2 types.

Wasm32 **execution** via `compile --target wasm --run` is not claimed. The
0.7.2 Chicory host rejected that path on this fixture; compile to wasm32
and field checks via `kotoba run` are what this tree verifies.

## Operator

awai.network / Ryo Awai
