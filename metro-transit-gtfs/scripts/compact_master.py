from pathlib import Path
import duckdb

PROJECT_DIR = Path(__file__).resolve().parents[1]
INPUT_GLOB = str(PROJECT_DIR / "data/clean/trip_updates_enriched/*/*/*/*.parquet")
OUTPUT_MASTER = PROJECT_DIR / "data/clean/route_10_master.parquet"
TEMP_MASTER = PROJECT_DIR / "data/clean/route_10_master.tmp.parquet"

def compact():
    print("Compacting all Route 10 batches into a single master Parquet...")
    con = duckdb.connect()

    # Write to a temp file first to prevent corruption if interrupted
    con.execute(f"""
        COPY (
            SELECT * 
            FROM read_parquet('{INPUT_GLOB}', union_by_name = true)
            ORDER BY observed_at_utc, trip_id, stop_sequence
        ) TO '{TEMP_MASTER}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    # Atomically replace the old master
    TEMP_MASTER.replace(OUTPUT_MASTER)

    # Use con.execute() which returns a cursor that supports .fetchone()
    count = con.execute(f"SELECT COUNT(*) FROM '{OUTPUT_MASTER}'").fetchone()[0]
    print(f"Master file created: {OUTPUT_MASTER.relative_to(PROJECT_DIR)}")
    print(f"Total rows in master: {count:,}")

if __name__ == "__main__":
    compact()
