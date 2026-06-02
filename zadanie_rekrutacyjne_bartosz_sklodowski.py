"""
DAG ETL dla kursów walut NBP (tabela A).

Proces:
    1. extract_to_bronze   – pobiera surowy XML z API NBP i zapisuje go
                             bez modyfikacji do warstwy bronze,
    2. transform_to_silver – parsuje XML, czyści/standaryzuje dane
                             i zapisuje wynik do warstwy silver w formacie Parquet.

Układ partycji (Hive-style, gotowy pod BigQuery External Table):
    bronze/nbp/year=YYYY/month=MM/day=DD/table_a.xml
    silver/nbp/year=YYYY/month=MM/day=DD/rates.parquet

Uzasadnienie partycjonowania:
    Dane przyrastają dziennie i naturalnie odpytuje się je po dacie (zakresy dat),
    dlatego partycjonowanie dzienne minimalizuje ilość skanowanych danych i koszty
    zapytań w BigQuery. Układ `klucz=wartość` jest rozpoznawany automatycznie przez
    BigQuery jako partycjonowanie Hive-style i mapuje się 1:1 na model
    `logical_date` w Airflow.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import pandas as pd
import pendulum
import requests

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

# ---------------------------------------------------------------------------
# Konfiguracja
# ---------------------------------------------------------------------------

LOCAL_TZ = pendulum.timezone("Europe/Warsaw")

# Katalog bazowy, który symuluje bucket / warstwy w Google Cloud Storage.
DATA_LAKE_ROOT = Path("/opt/airflow/data_lake")

SOURCE_NAME = "nbp"

# Endpoint zwraca tabelę A obowiązującą w podanym dniu (deterministyczny zasób).
# Dla dni bez notowań (weekendy/święta) API zwraca HTTP 404.
NBP_API_URL = "https://api.nbp.pl/api/exchangerates/tables/A/{date}/?format=xml"

REQUEST_TIMEOUT_SECONDS = 30

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Funkcje pomocnicze
# ---------------------------------------------------------------------------

def _partition_dir(layer: str, effective_date: datetime) -> Path:
    """Buduje katalog partycji w układzie Hive-style dla danej warstwy i daty."""
    return (
        DATA_LAKE_ROOT
        / layer
        / SOURCE_NAME
        / f"year={effective_date:%Y}"
        / f"month={effective_date:%m}"
        / f"day={effective_date:%d}"
    )


# ---------------------------------------------------------------------------
# Definicja DAG-a
# ---------------------------------------------------------------------------

@dag(
    dag_id="nbp_exchange_rates_etl",
    description="ETL kursów walut NBP (tabela A): bronze (XML) -> silver (Parquet).",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "dentsu rekrutacja",
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["nbp", "etl", "bronze", "silver"],
)
def nbp_exchange_rates_etl():
    """Definicja przepływu: extract -> transform."""

    @task
    def extract_to_bronze(logical_date: pendulum.DateTime | None = None) -> str:
        """
        Pobiera surowy XML tabeli A NBP dla daty uruchomienia i zapisuje go
        do warstwy bronze.

        Idempotencja: zasób jest deterministyczny (pobierany po konkretnej dacie),
        a plik docelowy jest nadpisywany — ponowny run dla tej samej daty daje
        ten sam stan. Dla dni bez notowań task jest pomijany (skip).
        """
        effective_date = logical_date.in_timezone(LOCAL_TZ).date()
        url = NBP_API_URL.format(date=effective_date.isoformat())

        log.info("Pobieram tabelę A NBP dla daty %s z %s", effective_date, url)
        response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)

        if response.status_code == 404:
            # Dzień wolny od notowań – kontrolowany skip zamiast błędu.
            raise AirflowSkipException(
                f"Brak tabeli kursów NBP dla {effective_date} (dzień bez notowań)."
            )
        response.raise_for_status()

        target_dir = _partition_dir("bronze", effective_date)
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / "table_a.xml"

        # Zapis surowych bajtów bez jakiejkolwiek modyfikacji (warstwa bronze).
        target_path.write_bytes(response.content)
        log.info("Zapisano surowy XML do %s", target_path)

        return str(target_path)

    @task
    def transform_to_silver(bronze_path: str) -> str:
        """
        Wczytuje XML z warstwy bronze, parsuje i standaryzuje dane,
        a następnie zapisuje wynik do warstwy silver w formacie Parquet.

        Idempotencja: plik Parquet w partycji jest nadpisywany.
        """
        tree = ET.parse(bronze_path)
        root = tree.getroot()

        # Struktura XML: <ArrayOfExchangeRatesTable> -> <ExchangeRatesTable>
        #   <No>, <EffectiveDate>, <Rates> -> <Rate>(<Currency>,<Code>,<Mid>)
        table = root.find("ExchangeRatesTable")
        if table is None:
            raise ValueError(f"Nieoczekiwana struktura XML w pliku {bronze_path}.")

        table_no = table.findtext("No")
        effective_date = pd.to_datetime(table.findtext("EffectiveDate")).date()

        records = [
            {
                "currency": rate.findtext("Currency"),
                "code": rate.findtext("Code"),
                "rate": rate.findtext("Mid"),
            }
            for rate in table.findall("Rates/Rate")
        ]

        df = pd.DataFrame.from_records(records)

        # --- standaryzacja danych ---
        df["currency"] = df["currency"].str.strip().str.lower()
        df["code"] = df["code"].str.strip().str.upper()
        df["rate"] = pd.to_numeric(df["rate"].str.replace(",", "."), errors="coerce")

        # Odrzucenie ewentualnych niepoprawnych wierszy.
        df = df.dropna(subset=["code", "rate"]).reset_index(drop=True)

        # Kolumny techniczne / kontekst tabeli.
        df["effective_date"] = effective_date
        df["table_no"] = table_no
        df["source"] = SOURCE_NAME
        df["ingested_at"] = pendulum.now(LOCAL_TZ).to_iso8601_string()

        df = df[
            [
                "effective_date",
                "table_no",
                "code",
                "currency",
                "rate",
                "source",
                "ingested_at",
            ]
        ]

        target_dir = _partition_dir("silver", effective_date)
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / "rates.parquet"

        # Nadpisanie pliku w partycji => idempotentny zapis.
        df.to_parquet(target_path, engine="pyarrow", index=False)
        log.info("Zapisano %d wierszy do %s", len(df), target_path)

        return str(target_path)

    bronze_path = extract_to_bronze()
    transform_to_silver(bronze_path)


nbp_exchange_rates_etl()
