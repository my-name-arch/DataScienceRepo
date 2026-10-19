# AI Statment : To load and parse the data from gtfs used AI 
#What this script does
# It automatically Locates your project folder, no matter where it lives on your Mac.
# Reads every GTFS .txt file inside data/extracted/.
# Creates or opens this local DuckDB database file:

from pathlib import Path
import duckdb

project_dir = Path(__file__).resolve().parents[1]
gtfs_dir = project_dir / "data" / "extracted"
db_path = project_dir / "metro_transit.duckdb"

con = duckdb.connect(str(db_path))

for file_path in sorted(gtfs_dir.glob("*.txt")):
    table_name = file_path.stem.lower()

    con.execute(f"DROP TABLE IF EXISTS {table_name}")

    con.execute(f"""
        CREATE TABLE {table_name} AS
        SELECT *
        FROM read_csv(
            '{file_path.as_posix()}',
            header = true,
            all_varchar = true,
            nullstr = '',
            ignore_errors = true
        )
    """)

    row_count = con.execute(
        f"SELECT COUNT(*) FROM {table_name}"
    ).fetchone()[0]

    print(f"Loaded {table_name}: {row_count:,} rows")

con.close()

print(f"\nDone. Created: {db_path.name}")