import hashlib
from pathlib import Path
import sys

root = Path.cwd()
paths = set((root / "src").rglob("*.rs")) | set((root / "tests").rglob("*.rs"))
paths.update(
    root / path
    for path in (
        "tools/storage_compare/src/bin/wal_values.rs",
        "tools/storage_compare/README.md",
        "README.md",
        "docs/experimental-wal-storage.md",
        "Cargo.toml",
        "Cargo.lock",
        "tools/storage_compare/Cargo.toml",
        "tools/storage_compare/Cargo.lock",
    )
)
for path in sorted(paths, key=lambda item: item.relative_to(root).as_posix()):
    if not path.is_file():
        raise SystemExit(f"required source input missing: {path.relative_to(root)}")
    print(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(root).as_posix()}")
