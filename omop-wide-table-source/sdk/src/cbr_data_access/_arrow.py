"""Arrow compatibility helpers used by the Polars conversion paths."""

from __future__ import annotations

from typing import Any

_EXTENSION_NAME_KEY = b"ARROW:extension:name"
_EXTENSION_METADATA_KEY = b"ARROW:extension:metadata"
_OPAQUE_EXTENSION_NAME = "arrow.opaque"


def opaque_to_storage(data: Any) -> Any:
    """Replace top-level ``arrow.opaque`` columns with their storage arrays.

    The ADBC PostgreSQL driver uses Arrow's canonical opaque extension for
    PostgreSQL types it cannot interpret. Polars does not attach semantics to
    that extension, so SDK DataFrame results intentionally expose the same
    underlying storage values without the extension annotation. Native Arrow
    APIs keep returning the original schema and arrays unchanged.
    """
    import pyarrow as pa

    fields = []
    columns = []
    changed = False

    for field, column in zip(data.schema, data.columns, strict=True):
        metadata = field.metadata or {}
        metadata_name = metadata.get(_EXTENSION_NAME_KEY, b"").decode("utf-8", errors="replace")
        extension_name = getattr(field.type, "extension_name", metadata_name)

        if extension_name != _OPAQUE_EXTENSION_NAME:
            fields.append(field)
            columns.append(column)
            continue

        changed = True
        storage_type = getattr(field.type, "storage_type", field.type)
        if isinstance(column, pa.ChunkedArray):
            column = pa.chunked_array(
                [getattr(chunk, "storage", chunk) for chunk in column.chunks],
                type=storage_type,
            )
        else:
            column = getattr(column, "storage", column)

        storage_metadata = {
            key: value
            for key, value in metadata.items()
            if key not in {_EXTENSION_NAME_KEY, _EXTENSION_METADATA_KEY}
        }
        fields.append(
            pa.field(
                field.name,
                storage_type,
                nullable=field.nullable,
                metadata=storage_metadata or None,
            )
        )
        columns.append(column)

    if not changed:
        return data

    schema = pa.schema(fields, metadata=data.schema.metadata)
    if isinstance(data, pa.RecordBatch):
        return pa.RecordBatch.from_arrays(columns, schema=schema)
    return pa.Table.from_arrays(columns, schema=schema)
