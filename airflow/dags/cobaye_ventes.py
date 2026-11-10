"""Pipeline « ventes quotidiennes ».

    extract_orders ──┐
                     ├──> transform ──> load_warehouse ──> publish_report
    fetch_rates ─────┘

- extract_orders : lit le fichier CSV de commandes déposé chaque jour
- fetch_rates    : récupère les taux de change auprès de l'API du fournisseur
- transform      : convertit chaque commande en euros
- load_warehouse : charge les lignes dans l'entrepôt Postgres (ventes.fact_sales)
- publish_report : écrit un petit rapport JSON
"""

from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
import requests
from airflow.sdk import Variable, dag, task

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", "/opt/airflow/data"))
FOURNISSEUR_URL = os.environ.get("FOURNISSEUR_URL", "http://fournisseur:8000")

# DSN de secours si ENTREPOT_DSN n'est pas défini.
DSN_SECOURS = "postgresql://etl_writer:cnry-src-5b1e0c9d7a@entrepot:5432/dwh"


@task(execution_timeout=timedelta(seconds=60))
def extract_orders(ds=None):
    path = DATA_DIR / "incoming" / f"orders_{ds}.csv"
    log.info("Lecture du fichier de commandes %s", path)
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"Fichier {path.name} vide : aucune ligne d'en-tête")
        log.info("Colonnes trouvées : %s", ", ".join(reader.fieldnames))
        for line_no, row in enumerate(reader, start=2):
            if not row["amount"]:
                raise ValueError(f"Montant manquant à la ligne {line_no} de {path.name}")
            rows.append(
                {
                    "order_id": row["order_id"],
                    "order_date": datetime.strptime(row["order_date"], "%Y-%m-%d").date().isoformat(),
                    "currency": row["currency"],
                    "amount": float(row["amount"]),
                }
            )
    if not rows:
        raise ValueError(f"Fichier {path.name} sans aucune commande")
    log.info("%d commandes lues", len(rows))
    return rows


@task(execution_timeout=timedelta(seconds=60))
def fetch_rates(ds=None):
    timeout_s = float(Variable.get("fournisseur_timeout_s", default="10"))
    api_key = os.environ["FOURNISSEUR_API_KEY"]
    url = f"{FOURNISSEUR_URL}/v1/rates"
    log.info("Appel de l'API fournisseur %s (date=%s, timeout=%ss)", url, ds, timeout_s)
    resp = requests.get(url, params={"date": ds, "api_key": api_key}, timeout=timeout_s)
    if not resp.ok:
        log.warning("Réponse du fournisseur (HTTP %s) : %s", resp.status_code, resp.text[:300])
    resp.raise_for_status()
    payload = resp.json()
    rates = {r["currency"]: float(r["rate"]) for r in payload["rates"]}
    log.info("%d taux de change reçus : %s", len(rates), ", ".join(sorted(rates)))
    return rates


@task(execution_timeout=timedelta(seconds=60))
def transform(orders, rates):
    out = []
    for o in orders:
        rate = rates.get(o["currency"])
        if rate is None:
            raise ValueError(
                f"Aucun taux de change pour la devise {o['currency']} (commande {o['order_id']})"
            )
        out.append({**o, "amount_eur": round(o["amount"] * rate, 2)})
    log.info("%d commandes converties en EUR", len(out))
    return out


@task(execution_timeout=timedelta(seconds=90))
def load_warehouse(rows, ds=None):
    dsn = os.environ.get("ENTREPOT_DSN", DSN_SECOURS)
    log.info("Connexion à l'entrepôt : %s", dsn)
    conn = psycopg2.connect(dsn, connect_timeout=10)
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SET statement_timeout = '15s'")
            cur.execute("DELETE FROM ventes.fact_sales WHERE order_date = %s", (ds,))
            cur.executemany(
                "INSERT INTO ventes.fact_sales (order_id, order_date, currency, amount, amount_eur) "
                "VALUES (%s, %s, %s, %s, %s)",
                [(r["order_id"], r["order_date"], r["currency"], r["amount"], r["amount_eur"]) for r in rows],
            )
    finally:
        conn.close()
    log.info("%d lignes chargées dans ventes.fact_sales", len(rows))
    return len(rows)


@task(execution_timeout=timedelta(seconds=30))
def publish_report(n_rows, rows, ds=None):
    report_dir = DATA_DIR / "reports"
    total = round(sum(r["amount_eur"] for r in rows), 2)
    path = report_dir / f"ventes_{ds}.json"
    path.write_text(json.dumps({"date": ds, "commandes": n_rows, "total_eur": total}, indent=2))
    log.info("Rapport écrit dans %s (total %.2f EUR)", path, total)


@dag(
    dag_id="cobaye_ventes",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=5)},
    tags=["ventes"],
)
def cobaye_ventes():
    orders = extract_orders()
    rates = fetch_rates()
    rows = transform(orders, rates)
    n_rows = load_warehouse(rows)
    publish_report(n_rows, rows)


cobaye_ventes()
