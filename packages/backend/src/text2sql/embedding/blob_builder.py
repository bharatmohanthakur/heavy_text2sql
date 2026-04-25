"""Build the text that gets embedded.

The embedding represents the *semantic content* of a table, nothing else.
Structural metadata (FK neighbors, PK, domains, sample rows) is what we
search OVER as payload filters — it does not belong in the embedded text.

Embed text shape (per table):

    {table description}

    {col1}: {col1 description} [values: v1, v2, v3, ...]
    {col2}: {col2 description}
    ...

Empty parts are simply omitted.
"""

from __future__ import annotations

from text2sql.table_catalog import ColumnInfo, TableEntry


# Cap on how many sample values to include per column in the embed text — the
# values themselves are searched over by the entity resolver in its own
# collection, so we don't need them all here. A small handful gives the
# embedding enough to disambiguate (e.g., GradeLevelDescriptor "Pre-K, K,
# 1st, 2nd, 3rd…").
DEFAULT_VALUES_PER_COLUMN = 6


def build_table_blob(
    entry: TableEntry,
    *,
    values_per_column: int = DEFAULT_VALUES_PER_COLUMN,
) -> str:
    """Render the semantic blob for one table.

    Includes ONLY:
      - table description
      - per-column description + a few unique values (when known)
    """
    lines: list[str] = []
    desc = (entry.description or "").strip()
    if desc:
        lines.append(desc)
        lines.append("")

    for col in entry.columns:
        line = _column_line(col, values_per_column)
        if line:
            lines.append(line)

    return "\n".join(lines).strip()


def _column_line(col: ColumnInfo, values_per_column: int) -> str:
    desc = (col.description or "").strip()
    samples = col.sample_values[:values_per_column] if col.sample_values else []
    if not desc and not samples:
        # Nothing meaningful to embed for this column.
        return ""
    parts = [f"{col.name}:"]
    if desc:
        parts.append(desc)
    if samples:
        parts.append(f"[values: {', '.join(samples)}]")
    return " ".join(parts)


def build_column_value_blobs(entry: TableEntry) -> list[tuple[str, str, dict]]:
    """For Component 6 (entity resolver): one record per low-cardinality value.

    Skips descriptor child tables — their only column is an opaque numeric ID
    and the human-readable codes live in edfi.Descriptor (handled separately
    by build_descriptor_code_blobs).
    """
    if entry.is_descriptor:
        return []
    if entry.fqn == "edfi.Descriptor":
        return []
    out: list[tuple[str, str, dict]] = []
    for col in entry.columns:
        if not col.sample_values:
            continue
        col_desc = (col.description or "").strip()
        for v in col.sample_values:
            text = f"{v}" if not col_desc else f"{v} — {col_desc}"
            id_ = f"{entry.fqn}::{col.name}::{v}"
            payload = {
                "fqn": entry.fqn,
                "schema": entry.schema,
                "table": entry.table,
                "column": col.name,
                "value": v,
                "domains": list(entry.domains),
                "kind": "column_value",
            }
            out.append((id_, text, payload))
    return out


def build_descriptor_code_blobs(
    descriptor_codes: list,
) -> list[tuple[str, str, dict]]:
    """One record per descriptor code, with full namespace + child-table info.

    The embed text is `<codevalue> [<short_description>] (<type_name>)` so a
    query like "Hispanc" or "non-Hispanic origin" finds the right row regardless
    of which descriptor type it belongs to. The payload carries everything the
    schema linker needs to assemble the join chain.
    """
    out: list[tuple[str, str, dict]] = []
    for d in descriptor_codes:
        cv = d.code_value
        bits = [cv]
        if d.short_description and d.short_description != cv:
            bits.append(f"[{d.short_description}]")
        if d.type_name:
            bits.append(f"({d.type_name})")
        text = " ".join(bits)
        id_ = f"edfi.Descriptor::CodeValue::{d.descriptor_id}"
        payload = {
            "fqn": "edfi.Descriptor",
            "schema": "edfi",
            "table": "Descriptor",
            "column": "CodeValue",
            "value": cv,
            "descriptor_id": d.descriptor_id,
            "namespace": d.namespace,
            "type_name": d.type_name,
            "child_fqn": d.child_fqn,
            "kind": "descriptor_code",
        }
        out.append((id_, text, payload))
    return out
