import io
import os
import logging
from datetime import datetime

import boto3
import pandas as pd
import psycopg2
from botocore.client import Config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

PG = dict(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", 5432)),
    user=os.getenv("PG_USER", "etl_user"),
    password=os.getenv("PG_PASS", "etl_pass"),
    dbname=os.getenv("PG_DB", "source_db"),
)

S3 = dict(
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9010"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
)

BUCKET = "etl-data"
RUN_DATE = datetime.now().strftime("%Y-%m-%d")


def s3_client():
    return boto3.client(
        "s3",
        **S3,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(s3):
    try:
        s3.head_bucket(Bucket=BUCKET)
        log.info(f"Бакет '{BUCKET}' уже существует")
    except Exception:
        s3.create_bucket(Bucket=BUCKET)
        log.info(f"Создан бакет '{BUCKET}'")


def list_tables(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
        )
        return [row[0] for row in cur.fetchall()]


def transfer_table(conn, s3, table: str) -> int:
    log.info(f"  → Таблица: {table}")

    df = pd.read_sql(f'SELECT * FROM "{table}"', conn)
    log.info(f"    Прочитано строк: {len(df)}")

    buf = io.StringIO()
    df.to_csv(buf, index=False)
    key_csv = f"raw/{RUN_DATE}/{table}.csv"
    s3.put_object(Bucket=BUCKET, Key=key_csv, Body=buf.getvalue().encode(), ContentType="text/csv")
    log.info(f"    Загружен CSV:     s3://{BUCKET}/{key_csv}")

    buf_pq = io.BytesIO()
    df.to_parquet(buf_pq, index=False)
    buf_pq.seek(0)
    key_pq = f"raw/{RUN_DATE}/{table}.parquet"
    s3.put_object(Bucket=BUCKET, Key=key_pq, Body=buf_pq.read(), ContentType="application/octet-stream")
    log.info(f"    Загружен Parquet: s3://{BUCKET}/{key_pq}")

    return len(df)


def main():
    log.info("=" * 60)
    log.info("  PostgreSQL → MinIO")
    log.info(f"  Источник : {PG['host']}:{PG['port']}/{PG['dbname']}")
    log.info(f"  Приёмник : {S3['endpoint_url']}/{BUCKET}/raw/{RUN_DATE}/")
    log.info("=" * 60)

    conn = psycopg2.connect(**PG)
    log.info("Подключение к PostgreSQL: OK")

    s3 = s3_client()
    ensure_bucket(s3)

    tables = list_tables(conn)
    log.info(f"Найдено таблиц: {tables}")

    total_rows = 0
    for tbl in tables:
        total_rows += transfer_table(conn, s3, tbl)

    conn.close()

    log.info("=" * 60)
    log.info(f"  Готово: {len(tables)} таблиц, {total_rows} строк")
    log.info(f"  Данные: s3://{BUCKET}/raw/{RUN_DATE}/")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
