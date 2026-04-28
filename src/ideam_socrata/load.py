import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import UPSERT_CHUNK_SIZE, get_socrata_client
from .validation import validate_payload

logger = logging.getLogger(__name__)

PAYLOAD_COLUMNS = [
    "floating_id",
    "source_dataset_id",
    "codigoestacion",
    "codigosensor",
    "fechaobservacion",
    "valorobservado",
    "nombreestacion",
    "departamento",
    "municipio",
    "zonahidrografica",
    "latitud",
    "longitud",
    "descripcionsensor",
    "unidadmedida",
]


def build_payload(df, payload_columns=None):
    """Reduce dataframe to Socrata payload columns and convert NaN/NaT values to None."""
    columns = [c for c in (payload_columns or PAYLOAD_COLUMNS) if c in df.columns]
    payload_df = df.loc[:, columns].where(pd.notna(df.loc[:, columns]), None)
    return payload_df.to_dict("records")


def upsert_to_socrata(sink_dataset_id, records, retry_func, chunk_size=UPSERT_CHUNK_SIZE, dry_run=False):
    """Upsert validated records into a Socrata sink dataset using floating_id as row key.

    The Socrata sink dataset must be configured with floating_id as its row identifier.
    """
    if not records:
        return {"uploaded_rows": 0, "chunks": 0, "rejected_rows": 0, "dry_run": dry_run}

    accepted, rejected = validate_payload(records)
    if rejected:
        logger.warning("Registros rechazados por validacion Pydantic: %s", len(rejected))

    if dry_run:
        return {
            "uploaded_rows": 0,
            "chunks": 0,
            "validated_rows": len(accepted),
            "rejected_rows": len(rejected),
            "dry_run": True,
        }

    client = get_socrata_client(write=True)
    uploaded_rows = 0
    chunks = 0
    for start in range(0, len(accepted), chunk_size):
        chunk = accepted[start:start + chunk_size]
        retry_func(lambda: client.upsert(sink_dataset_id, chunk), f"upsert {sink_dataset_id}")
        uploaded_rows += len(chunk)
        chunks += 1

    return {
        "uploaded_rows": uploaded_rows,
        "chunks": chunks,
        "validated_rows": len(accepted),
        "rejected_rows": len(rejected),
        "dry_run": False,
    }


class ParquetChunkWriter:
    """Append dataframe chunks into one Parquet file without retaining all rows."""

    def __init__(self, output_path):
        self.output_path = output_path
        self.writer = None
        self.rows = 0
        self.chunks = 0

    def write(self, df):
        if df.empty:
            return

        table = pa.Table.from_pandas(df, preserve_index=False)
        if self.writer is None:
            self.writer = pq.ParquetWriter(self.output_path, table.schema)
        self.writer.write_table(table)
        self.rows += len(df)
        self.chunks += 1

    def close(self):
        if self.writer is not None:
            self.writer.close()
            self.writer = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()
