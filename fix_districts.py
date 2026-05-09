import sys
sys.path.insert(0, '.')
from db.db import get_conn

mapping = {
    'H. Hoài Đức':  'Hoai Duc',
    'H. Thạch Thất': 'Thach That',
    'H. Thanh Trì':  'Thanh Tri',
}

conn = get_conn()
cur = conn.cursor()

for raw, canonical in mapping.items():
    cur.execute(
        "UPDATE apartments SET district = %s WHERE district = %s",
        (canonical, raw)
    )
    print(f"Updated {cur.rowcount} rows: {raw} → {canonical}")

conn.commit()
conn.close()
print("Done.")
