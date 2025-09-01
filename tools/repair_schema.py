import os, sys, glob
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

root = sys.argv[1] if len(sys.argv) > 1 else r"C:\teevra18\data\history"
paths = sorted(glob.glob(os.path.join(root, "**", "*.parquet"), recursive=True))
print(f"Scanning {len(paths)} files under {root}")

def cast_table(tbl: pa.Table) -> pa.Table:
    fields = []
    for f in tbl.schema:
        if f.name == "year":
            fields.append(pa.field("year", pa.int16()))
        elif f.name == "month":
            fields.append(pa.field("month", pa.int8()))
        else:
            fields.append(f)
    target = pa.schema(fields)
    return tbl.cast(target, safe=False)

fixed = 0
for p in paths:
    try:
        t = ds.dataset(p, format="parquet").to_table()
        if "year" in t.schema.names and t.schema.field("year").type != pa.int16():
            t = cast_table(t)
            pq.write_table(t, p, compression="snappy")
            fixed += 1
        elif "month" in t.schema.names and t.schema.field("month").type != pa.int8():
            t = cast_table(t)
            pq.write_table(t, p, compression="snappy")
            fixed += 1
    except Exception as e:
        print(f"[SKIP] {p} | {e}")

print(f"Done. Rewritten files: {fixed}")
