"""History pruner: remove old check-result entries to keep history files lean."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from pipewatch.history import _history_path


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def prune_pipeline(
    pipeline_name: str,
    *,
    history_dir: Optional[Path] = None,
    max_age_days: Optional[int] = None,
    max_entries: Optional[int] = None,
) -> int:
    """Remove entries from a pipeline's history file.

    Entries are filtered by *max_age_days* (entries older than this are dropped)
    and then trimmed to *max_entries* most-recent records.

    Returns the number of entries removed.
    """
    path = _history_path(pipeline_name, history_dir=history_dir)
    if not path.exists():
        return 0

    lines = path.read_text().splitlines()
    records = []
    for line in lines:
        line = line.strip()
        if line:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass

    original_count = len(records)

    if max_age_days is not None:
        cutoff = _now_utc() - timedelta(days=max_age_days)
        kept = []
        for r in records:
            ts_raw = r.get("checked_at") or r.get("timestamp")
            if ts_raw is None:
                kept.append(r)
                continue
            try:
                ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    kept.append(r)
            except ValueError:
                kept.append(r)
        records = kept

    if max_entries is not None and len(records) > max_entries:
        records = records[-max_entries:]

    removed = original_count - len(records)

    if removed > 0:
        path.write_text("".join(json.dumps(r) + "\n" for r in records))

    return removed


def prune_all(
    pipeline_names: list[str],
    *,
    history_dir: Optional[Path] = None,
    max_age_days: Optional[int] = None,
    max_entries: Optional[int] = None,
) -> dict[str, int]:
    """Prune history for every pipeline in *pipeline_names*.

    Returns a mapping of pipeline name -> number of entries removed.
    """
    return {
        name: prune_pipeline(
            name,
            history_dir=history_dir,
            max_age_days=max_age_days,
            max_entries=max_entries,
        )
        for name in pipeline_names
    }
