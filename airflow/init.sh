#!/bin/bash
# Initialisation (une fois, en root) : dossiers partagés, migration de la base, utilisateurs.
set -euo pipefail
UID_AF="${AIRFLOW_UID:-50000}"

mkdir -p /opt/airflow/{logs,dags} /opt/airflow/data/{incoming,reports}
chown -R "${UID_AF}:0" /opt/airflow/logs /opt/airflow/data
chmod 775 /opt/airflow/data/reports

# _AIRFLOW_DB_MIGRATE et _AIRFLOW_WWW_USER_CREATE : migration + compte admin (entrypoint officiel)
/entrypoint airflow version

# Le compte en lecture seule du serveur MCP est créé par le service airflow-users (docker-compose.yml).
if [[ -z "${DIAG_READER_PASSWORD:-}" ]]; then
  echo "AIRFLOW_READER_PASSWORD manquant dans .env" >&2
  exit 1
fi

# Les commandes ci-dessus tournent en root : on rend les dossiers partagés à l'utilisateur Airflow.
chown -R "${UID_AF}:0" /opt/airflow/logs /opt/airflow/data
