from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# NiFi dépose les fichiers dans nifi_processed/ avant de déclencher ce DAG
NIFI_PROCESSED_DIR = "/opt/airflow/output/nifi_processed"
INPUT_FALLBACK    = "/opt/airflow/input/clients.csv"   # fallback si NiFi non configuré
OUTPUT_SUCCESS    = "/opt/airflow/output/success/clients_valides.csv"
OUTPUT_ERROR      = "/opt/airflow/output/error/clients_invalides.csv"
REPORT_FILE       = "/opt/airflow/output/rapport_pipeline.json"

NIFI_BASE_URL  = "https://nifi:8443/nifi-api"
NIFI_USER      = "admin"
NIFI_PASSWORD  = "adminadminadmin"


def _check_nifi(**context):
    """Vérifie la disponibilité de NiFi et log l'état de son flow."""
    import urllib.request, ssl
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        req = urllib.request.Request(
            f"{NIFI_BASE_URL}/system-diagnostics",
            headers={"Authorization": "Bearer " + _get_nifi_token()}
        )
        with urllib.request.urlopen(req, context=ctx, timeout=10) as r:
            data = json.loads(r.read())
            heap = data["systemDiagnostics"]["aggregateSnapshot"]["usedHeap"]
            logging.info("NiFi opérationnel — heap : %s", heap)
    except Exception as e:
        logging.warning("NiFi inaccessible (%s) — le pipeline continue.", e)


def _get_nifi_token():
    import urllib.request, urllib.parse, ssl
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    data = urllib.parse.urlencode({"username": NIFI_USER, "password": NIFI_PASSWORD}).encode()
    req = urllib.request.Request(
        f"{NIFI_BASE_URL}/access/token", data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=10) as r:
            return r.read().decode().strip()
    except Exception:
        return ""


def _resolve_input_file(**context) -> str:
    """
    Détermine le fichier à traiter :
    1. Si NiFi a déclenché le DAG, il passe le nom du fichier dans dag_run.conf
    2. Sinon, on cherche dans nifi_processed/
    3. En dernier recours, on utilise input/ directement (démo sans NiFi)
    """
    conf     = context["dag_run"].conf or {}
    filename = conf.get("file", "clients.csv")

    nifi_path = os.path.join(NIFI_PROCESSED_DIR, filename)
    if os.path.exists(nifi_path):
        logging.info("Fichier reçu de NiFi : %s", nifi_path)
        return nifi_path

    logging.warning("Fichier introuvable dans nifi_processed/, fallback sur input/")
    return INPUT_FALLBACK


def _ingest_clients(**context):
    """Lit le fichier source et pousse le total en XCom."""
    input_file = _resolve_input_file(**context)
    with open(input_file, encoding="utf-8") as f:
        clients = list(csv.DictReader(f))

    logging.info("Ingestion : %d clients lus depuis %s", len(clients), input_file)
    context["ti"].xcom_push(key="total", value=len(clients))
    context["ti"].xcom_push(key="input_file", value=input_file)
    return len(clients)


def _validate_clients(**context):
    """
    Applique les règles métier et route :
      - valides  → output/success/
      - invalides → output/error/  (avec colonne 'erreurs')
    """
    ti         = context["ti"]
    input_file = ti.xcom_pull(task_ids="ingest_clients", key="input_file")

    with open(input_file, encoding="utf-8") as f:
        clients = list(csv.DictReader(f))

    valid, invalid = [], []

    for client in clients:
        errors = []

        email = client.get("email", "")
        if "@" not in email or "." not in email.split("@")[-1]:
            errors.append("email_invalide")

        tel = client.get("telephone", "")
        if not tel.isdigit() or len(tel) != 10:
            errors.append("telephone_invalide")

        cp = client.get("code_postal", "")
        if not cp.isdigit() or len(cp) != 5:
            errors.append("code_postal_invalide")

        if not client.get("nom") or not client.get("prenom"):
            errors.append("nom_ou_prenom_manquant")

        if client.get("statut", "") not in ("actif", "inactif", "prospect", "archive"):
            errors.append("statut_inconnu")

        if errors:
            client["erreurs"] = "|".join(errors)
            invalid.append(client)
        else:
            valid.append(client)

    os.makedirs(os.path.dirname(OUTPUT_SUCCESS), exist_ok=True)
    os.makedirs(os.path.dirname(OUTPUT_ERROR), exist_ok=True)

    if valid:
        with open(OUTPUT_SUCCESS, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=valid[0].keys())
            writer.writeheader()
            writer.writerows(valid)

    if invalid:
        error_fields = list(clients[0].keys()) + ["erreurs"]
        with open(OUTPUT_ERROR, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=error_fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(invalid)

    logging.info("Validation : %d valides, %d invalides", len(valid), len(invalid))
    ti.xcom_push(key="valid",   value=len(valid))
    ti.xcom_push(key="invalid", value=len(invalid))


def _generate_report(**context):
    """Génère un rapport JSON de synthèse et le dépose dans output/."""
    ti      = context["ti"]
    total   = ti.xcom_pull(task_ids="ingest_clients",  key="total")
    valid   = ti.xcom_pull(task_ids="validate_clients", key="valid")
    invalid = ti.xcom_pull(task_ids="validate_clients", key="invalid")
    source  = (context["dag_run"].conf or {}).get("source", "manuel")

    report = {
        "timestamp":        datetime.utcnow().isoformat() + "Z",
        "dag_run_id":       context["run_id"],
        "declencheur":      source,          # "nifi" si déclenché par NiFi
        "total_clients":    total,
        "clients_valides":  valid,
        "clients_invalides": invalid,
        "taux_validite_pct": round(valid / total * 100, 2) if total else 0.0,
    }

    os.makedirs(os.path.dirname(REPORT_FILE), exist_ok=True)
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    logging.info("Rapport : %s", report)
    return report


default_args = {
    "owner": "poc",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="demo_pipeline",
    description="Déclenché par NiFi après ingestion — validation et routage des clients",
    default_args=default_args,
    schedule_interval=None,   # déclenché par NiFi via l'API REST (plus de scheduling daily)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["poc", "nifi", "clients"],
) as dag:

    check_nifi = PythonOperator(
        task_id="check_nifi",
        python_callable=_check_nifi,
    )

    ingest_clients = PythonOperator(
        task_id="ingest_clients",
        python_callable=_ingest_clients,
    )

    validate_clients = PythonOperator(
        task_id="validate_clients",
        python_callable=_validate_clients,
    )

    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=_generate_report,
    )

    check_nifi >> ingest_clients >> validate_clients >> generate_report
