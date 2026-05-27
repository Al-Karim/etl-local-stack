from __future__ import annotations

import io
import os
import logging
from datetime import datetime, timedelta

import boto3
import pandas as pd
from botocore.client import Config

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

MINIO = dict(
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
)
BUCKET = "etl-data"


def _s3():
    return boto3.client(
        "s3",
        **MINIO,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


@dag(
    dag_id="process_sales_data",
    description="ETL: MinIO (raw CSV) → pandas transform → MinIO (processed)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=3)},
    tags=["etl", "sales", "minio"],
)
def process_sales_data():

    @task()
    def extract_from_minio() -> dict:
        s3 = _s3()
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix="raw/")
        objects = response.get("Contents", [])

        if not objects:
            raise FileNotFoundError(
                "Нет данных в MinIO. Сначала запустите трансфер: make transfer"
            )

        csv_keys = [o["Key"] for o in objects if o["Key"].endswith("sales.csv")]
        if not csv_keys:
            raise FileNotFoundError("Файл sales.csv не найден в MinIO.")

        # берём самый свежий файл
        latest = sorted(csv_keys)[-1]
        log.info(f"Читаем: s3://{BUCKET}/{latest}")

        obj = s3.get_object(Bucket=BUCKET, Key=latest)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))
        log.info(f"Извлечено строк: {len(df)}")

        return {"data": df.to_json(orient="records"), "source": latest, "rows": len(df)}

    @task()
    def transform_data(extracted: dict) -> dict:
        df = pd.read_json(extracted["data"], orient="records")
        df["sale_date"] = pd.to_datetime(df["sale_date"])
        df["revenue"] = df["quantity"] * df["price"]

        # Продажи по категории
        by_category = (
            df.groupby("category")
            .agg(
                total_quantity=("quantity", "sum"),
                total_revenue=("revenue", "sum"),
                avg_price=("price", "mean"),
                orders=("id", "count"),
            )
            .reset_index()
            .round(2)
        )

        # Продажи по региону
        by_region = (
            df.groupby("region")
            .agg(
                total_quantity=("quantity", "sum"),
                total_revenue=("revenue", "sum"),
                orders=("id", "count"),
            )
            .reset_index()
            .round(2)
        )

        # Топ-10 товаров по выручке
        top_products = (
            df.groupby("product_name")
            .agg(
                total_revenue=("revenue", "sum"),
                total_quantity=("quantity", "sum"),
            )
            .reset_index()
            .sort_values("total_revenue", ascending=False)
            .head(10)
            .round(2)
        )

        # Тренд по дням
        daily = (
            df.groupby(df["sale_date"].dt.date)
            .agg(total_revenue=("revenue", "sum"), orders=("id", "count"))
            .reset_index()
            .rename(columns={"sale_date": "date"})
            .round(2)
        )
        daily["date"] = daily["date"].astype(str)

        log.info(
            f"Трансформация: {len(by_category)} категорий, "
            f"{len(by_region)} регионов, {len(top_products)} топ-товаров"
        )

        return {
            "by_category": by_category.to_json(orient="records"),
            "by_region": by_region.to_json(orient="records"),
            "top_products": top_products.to_json(orient="records"),
            "daily_trend": daily.to_json(orient="records"),
            "total_revenue": float(df["revenue"].sum()),
            "total_rows": len(df),
        }

    @task()
    def load_to_minio(transformed: dict) -> str:
        s3 = _s3()
        run_date = datetime.now().strftime("%Y-%m-%d")

        datasets = {
            "sales_by_category": transformed["by_category"],
            "sales_by_region": transformed["by_region"],
            "top_products": transformed["top_products"],
            "daily_trend": transformed["daily_trend"],
        }

        saved = {}
        for name, json_data in datasets.items():
            df = pd.read_json(json_data, orient="records")
            buf = io.StringIO()
            df.to_csv(buf, index=False)
            key = f"processed/{run_date}/{name}.csv"
            s3.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=buf.getvalue().encode("utf-8"),
                ContentType="text/csv",
            )
            saved[name] = key
            log.info(f"Сохранён: s3://{BUCKET}/{key}")

        # Текстовый отчёт
        report_lines = [
            f"ETL Pipeline — Отчёт",
            f"{'=' * 40}",
            f"Дата запуска : {run_date}",
            f"Строк обраб. : {transformed['total_rows']}",
            f"Выручка итого: {transformed['total_revenue']:,.2f} руб.",
            "",
            "Сохранённые файлы:",
        ] + [f"  s3://{BUCKET}/{v}" for v in saved.values()]

        s3.put_object(
            Bucket=BUCKET,
            Key=f"processed/{run_date}/report.txt",
            Body="\n".join(report_lines).encode("utf-8"),
        )

        output_path = f"s3://{BUCKET}/processed/{run_date}/"
        log.info(f"Пайплайн завершён → {output_path}")
        return output_path

    raw = extract_from_minio()
    aggregated = transform_data(raw)
    load_to_minio(aggregated)


process_sales_data()
