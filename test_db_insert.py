import os

import pandas as pd
from sqlalchemy import create_engine, text


def main() -> None:
    database_url = os.getenv("DATABASE_URL_SYNC", "").strip()
    if not database_url:
        raise RuntimeError(
            "Missing DATABASE_URL_SYNC. "
            "Set DATABASE_URL_SYNC before running this script."
        )
    engine = create_engine(database_url)
    df = pd.read_csv("/tmp/test_advanced_telemetry.csv")

    cols = list(df.columns)
    records = df.to_dict(orient="records")

    insert_sql = f"""
        INSERT INTO telemetry ({', '.join(cols)}, source)
        VALUES ({', '.join(':' + c for c in cols)}, 'csv')
        ON CONFLICT DO NOTHING
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(insert_sql), records)
        print("Success")
    except Exception as exc:
        print(f"Exception: {exc}")


if __name__ == "__main__":
    main()
