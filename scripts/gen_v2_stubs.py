"""Generate the dbt v2 state protocol stubs under src/dbt_hodor/v2/proto/.

Usage: uv run python scripts/gen_v2_stubs.py <proto_dir>

<proto_dir> holds the query_cache_protobuf/ tree from github.com/dbt-labs/dbt
(tag recorded in dbt_hodor.v2.PROTO_TAG). The output tree is wiped and
regenerated, and the generated absolute imports are rewritten to package-relative
ones so the stubs import without touching sys.path.
"""

from __future__ import annotations

import re
import shutil
import sys
from functools import partial
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "src" / "dbt_hodor" / "v2" / "proto"
ROOT_PACKAGE = "query_cache_protobuf"
IMPORT = re.compile(rf"^from ({ROOT_PACKAGE}(?:\.\w+)*) import (\w+)", re.MULTILINE)


def _relative(match: re.Match, *, file: Path) -> str:
    # The module path under the output dir; the file sits `depth` levels below it.
    depth = len(file.relative_to(OUT).parent.parts)
    dots = "." * depth
    sub = match.group(1).split(".")[1:]
    if sub:
        return f"from {dots}{'.'.join(sub)} import {match.group(2)}"
    return f"from {dots} import {match.group(2)}"


def main(proto_dir: Path) -> None:
    from grpc_tools import protoc

    files = sorted(p.relative_to(proto_dir).as_posix() for p in proto_dir.rglob("*.proto"))
    if not files:
        sys.exit(f"no .proto files under {proto_dir}")
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)
    well_known = Path(protoc.__file__).parent / "_proto"  # google/protobuf/*.proto
    args = [
        "protoc",
        f"-I{proto_dir}",
        f"-I{well_known}",
        f"--python_out={OUT}",
        f"--grpc_python_out={OUT}",
        *files,
    ]
    if protoc.main(args) != 0:
        sys.exit("protoc failed")
    for directory in [OUT, *(d for d in OUT.rglob("*") if d.is_dir())]:
        (directory / "__init__.py").write_text("", encoding="utf-8", newline="\n")
    for file in OUT.rglob("*_pb2*.py"):
        text = file.read_text(encoding="utf-8")
        text = IMPORT.sub(partial(_relative, file=file), text)
        file.write_text(text, encoding="utf-8", newline="\n")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(Path(sys.argv[1]))
