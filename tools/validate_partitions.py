# Robust validator for Teevra18 M6: safely reads files by casting dict-encoded cols.
import sys, glob, os
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

def safe_read_parquet(path: str) -> pa.Table:
    # Build a dataset from the single file so we can enforce a target schema.
    dataset = ds.dataset(path, format="parquet")
    in_schema = dataset.schema

    # Target schema: cast dictionary-encoded fields to their value types.
    # Ensure year/month are plain ints (not dictionary).
    fields = []
    for f in in_schema:
        t = f.type
        if f.name in ("year", "month"):
            # use small ints to be safe/compact
            tgt = pa.int16() if f.name == "year" else pa.int8()
            fields.append(pa.field(f.name, tgt))
        elif pa.types.is_dictionary(t):
            fields.append(pa.field(f.name, t.value_type))
        else:
            fields.append(f)

    target_schema = pa.schema(fields)

    try:
        # This casts as it loads, avoiding "Unable to merge ..." errors.
        return dataset.to_table(schema=target_schema)
    except Exception:
        # Fallback: read first row group to at least show schema.
        pf = pq.ParquetFile(path)
        return pf.read_row_group(0)

def main(root: str):
    paths = sorted(glob.glob(os.path.join(root, "**", "*.parquet"), recursive=True))
    print(f"Found {len(paths)} parquet files under {root}")
    for p in paths[:50]:  # show up to 50
        try:
            tbl = safe_read_parquet(p)
            cols = [f"{f.name}:{f.type}" for f in tbl.schema]
            print(f"[OK] {p} | rows={tbl.num_rows} | cols={', '.join(cols)}")
        except Exception as e:
            print(f"[ERR] {p} | {e}")
    print("Done.")

if __name__ == "__main__":
    root = sys.argv[1] if len(sys.argv) > 1 else r"C:\teevra18\data\history"
    main(root)
