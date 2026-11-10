#!/bin/bash
# Crée le rôle applicatif etl_writer avec le mot de passe fourni par l'environnement,
# puis applique le schéma et les droits nominaux. Exécuté par l'image postgres au premier démarrage.
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE etl_writer LOGIN PASSWORD '${ENTREPOT_PASSWORD}';
EOSQL
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /sql/init.sql
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /sql/reset.sql
