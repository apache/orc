#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Compile kotoba/orc.kotoba with Kotoba 0.7.2 (wasm32, i64-v1) and assert
# header fields from CLI output. Missing fields are failures.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURE="${ROOT}/fixtures/tiny.orc"
SRC="${ROOT}/orc.kotoba"
EXPECTED_VALUE="111102"
KOTOBA_VERSION="0.7.2"
KOTOBA_TARBALL="kotoba-linux-amd64.tar.gz"
# sha256 of https://github.com/kotoba-lang/kotoba/releases/download/v0.7.2/kotoba-linux-amd64.tar.gz
KOTOBA_SHA256="95e225461e1b8a21849b251e8c8b654693d2c8a516b258532771651e978e1977"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

need_file() {
  [[ -f "$1" ]] || fail "missing $1"
}

need_file "${FIXTURE}"
need_file "${SRC}"

python3 - "${FIXTURE}" "${SRC}" <<'PY'
import re
import sys

fixture_path, src_path = sys.argv[1], sys.argv[2]
data = open(fixture_path, "rb").read()
if len(data) < 4:
    print("FAIL: fixture shorter than 4 bytes", file=sys.stderr)
    sys.exit(1)
if data[:3] != b"ORC":
    print("FAIL: fixture header is %r, not ORC" % (data[:3],), file=sys.stderr)
    sys.exit(1)
ps_len = data[-1]
if ps_len < 4 or ps_len >= len(data):
    print("FAIL: postscript length byte %d is not usable" % ps_len, file=sys.stderr)
    sys.exit(1)
if data[-(1 + 3):-1] != b"ORC":
    print("FAIL: postscript does not end in ORC magic", file=sys.stderr)
    sys.exit(1)

src = open(src_path, encoding="utf-8").read()
pairs = [(int(i), int(b)) for i, b in re.findall(r"\(if \(= i (\d+)\) (\d+)", src)]
if not pairs:
    print("FAIL: no fixture-byte literals in orc.kotoba", file=sys.stderr)
    sys.exit(1)
indexes = [i for i, _ in pairs]
if indexes != list(range(len(data))):
    print("FAIL: fixture-byte indexes %s do not cover 0..%d" % (indexes, len(data) - 1), file=sys.stderr)
    sys.exit(1)
embedded = [b for _, b in pairs]
file_bytes = list(data)
if embedded != file_bytes:
    print("FAIL: orc.kotoba fixture-byte literals do not match fixtures/tiny.orc", file=sys.stderr)
    for i, (a, b) in enumerate(zip(embedded, file_bytes)):
        if a != b:
            print("  index %d: module=%d file=%d" % (i, a, b), file=sys.stderr)
            break
    sys.exit(1)
print("fixture: %d bytes, magic ORC, psLen=%d, module bytes match file" % (len(data), ps_len))
PY

if [[ -n "${KOTOBA:-}" ]]; then
  KOTOBA_BIN="${KOTOBA}"
  [[ -x "${KOTOBA_BIN}" ]] || fail "KOTOBA=${KOTOBA_BIN} is not executable"
else
  uname_s="$(uname -s)"
  uname_m="$(uname -m)"
  if [[ "${uname_s}" != "Linux" || "${uname_m}" != "x86_64" ]]; then
    fail "no KOTOBA set; automatic install is linux-amd64 only (this host is ${uname_s}/${uname_m})"
  fi
  cache="${ROOT}/.kotoba-cli/${KOTOBA_VERSION}"
  mkdir -p "${cache}"
  archive="${cache}/${KOTOBA_TARBALL}"
  if [[ ! -x "${cache}/kotoba" ]]; then
    url="https://github.com/kotoba-lang/kotoba/releases/download/v${KOTOBA_VERSION}/${KOTOBA_TARBALL}"
    echo "downloading Kotoba ${KOTOBA_VERSION} from ${url}"
    curl -fsSL -o "${archive}" "${url}"
    got="$(sha256sum "${archive}" | awk '{print $1}')"
    if [[ "${got}" != "${KOTOBA_SHA256}" ]]; then
      fail "checksum mismatch for ${KOTOBA_TARBALL}: got ${got} expected ${KOTOBA_SHA256}"
    fi
    tar -xzf "${archive}" -C "${cache}" kotoba
  fi
  KOTOBA_BIN="${cache}/kotoba"
  [[ -x "${KOTOBA_BIN}" ]] || fail "extracted kotoba binary missing"
fi

echo "using ${KOTOBA_BIN}"

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/kotoba-orc-v1.XXXXXX")"
trap 'rm -rf "${WORKDIR}"' EXIT
COMPILE_JSON="${WORKDIR}/compile.json"
WASM="${WORKDIR}/orc.wasm"

set +e
"${KOTOBA_BIN}" compile "${SRC}" --target wasm -o "${WASM}" --json >"${COMPILE_JSON}" 2>"${WORKDIR}/compile.err"
compile_rc=$?
set -e
if [[ "${compile_rc}" -ne 0 ]]; then
  cat "${COMPILE_JSON}" "${WORKDIR}/compile.err" >&2 || true
  fail "kotoba compile failed (exit ${compile_rc})"
fi
printf '%s\n' "$(cat "${COMPILE_JSON}")"

python3 - "${COMPILE_JSON}" "${WASM}" <<'PY'
import json
import sys
from pathlib import Path

WASM_IMPORT_SECTION = 2


def read_uleb128(buf, i):
    shift = 0
    value = 0
    while True:
        if i >= len(buf):
            raise ValueError("truncated uleb128")
        byte = buf[i]
        i += 1
        value |= (byte & 0x7F) << shift
        if byte & 0x80 == 0:
            return value, i
        shift += 7
        if shift > 35:
            raise ValueError("uleb128 too long")


def wasm_import_section(buf):
    if buf[:4] != b"\x00asm":
        raise ValueError("artifact magic %r is not wasm" % (buf[:4],))
    if len(buf) < 8:
        raise ValueError("truncated wasm header")
    i = 8
    found = False
    import_count = None
    while i < len(buf):
        section_id = buf[i]
        i += 1
        size, i = read_uleb128(buf, i)
        end = i + size
        if end > len(buf):
            raise ValueError("truncated wasm section")
        payload = buf[i:end]
        i = end
        if section_id == WASM_IMPORT_SECTION:
            found = True
            import_count, _ = read_uleb128(payload, 0) if payload else (0, 0)
    return found, import_count


# Fail closed on the checker itself before trusting the artifact.
_no_import = b"\x00asm\x01\x00\x00\x00"
_empty_import = b"\x00asm\x01\x00\x00\x00\x02\x01\x00"
if wasm_import_section(_no_import) != (False, None):
    sys.exit("FAIL: import-section checker failed on a no-section wasm")
if wasm_import_section(_empty_import) != (True, 0):
    sys.exit("FAIL: import-section checker failed to see an import section")

report = json.loads(Path(sys.argv[1]).read_text())
wasm = Path(sys.argv[2])
if report.get("kotoba.cli/ok?") is not True:
    sys.exit("FAIL: compile JSON kotoba.cli/ok? is %r" % (report.get("kotoba.cli/ok?"),))
if report.get("kotoba.cli/code") != "emitted":
    sys.exit("FAIL: compile JSON kotoba.cli/code is %r, expected emitted" % (report.get("kotoba.cli/code"),))
data = report.get("kotoba.cli/data") or {}
profile = data.get("value-profile")
compat = data.get("compatibility") or {}
target = compat.get("target")
if profile != "i64-v1":
    sys.exit("FAIL: value-profile %r is not i64-v1" % (profile,))
if target != "wasm32-kotoba-v1":
    sys.exit("FAIL: target %r is not wasm32-kotoba-v1" % (target,))
if not wasm.is_file() or wasm.stat().st_size == 0:
    sys.exit("FAIL: compile did not write a wasm artifact")
raw = wasm.read_bytes()
has_imports, import_count = wasm_import_section(raw)
if has_imports:
    sys.exit("FAIL: wasm has import section (count=%s); FFI is out of v1 scope" % (import_count,))
print("compile JSON: ok?=true code=emitted value-profile=i64-v1 target=wasm32-kotoba-v1 import-section=absent")
PY

run_out="$("${KOTOBA_BIN}" run "${SRC}")"
printf '%s\n' "${run_out}"

printf '%s\n' "${run_out}" | python3 -c '
import re
import sys
expected = sys.argv[1]
text = sys.stdin.read()
if ":kotoba.runtime/ok? true" not in text:
    print("FAIL: run output has no :kotoba.runtime/ok? true", file=sys.stderr)
    sys.exit(1)
match = re.search(r":kotoba.runtime/value (-?\d+)", text)
if match is None:
    print("FAIL: run output has no integer :kotoba.runtime/value", file=sys.stderr)
    sys.exit(1)
value = match.group(1)
if value != expected:
    print("FAIL: runtime value %s, expected %s" % (value, expected), file=sys.stderr)
    sys.exit(1)
print("run: header fields %s" % value)
' "${EXPECTED_VALUE}"

echo "PASS: Kotoba 0.7.2 compiled wasm32 i64-v1 and asserted ORC header fields ${EXPECTED_VALUE}"
