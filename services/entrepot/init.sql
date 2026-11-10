-- Entrepôt de données du pipeline cobaye (exécuté une seule fois à la création du conteneur).
-- Le mot de passe de etl_writer est fixé par services/entrepot/init-role.sh (variable ENTREPOT_PASSWORD).
CREATE SCHEMA IF NOT EXISTS ventes;

CREATE TABLE IF NOT EXISTS ventes.fact_sales (
    order_id    TEXT PRIMARY KEY,
    order_date  DATE NOT NULL,
    currency    CHAR(3) NOT NULL,
    amount      NUMERIC(12, 2) NOT NULL,
    amount_eur  NUMERIC(12, 2) NOT NULL,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
