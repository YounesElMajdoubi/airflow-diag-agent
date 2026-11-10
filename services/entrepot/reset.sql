-- Remet l'entrepôt dans son état sain (idempotent). Exécuté sur la base dwh
-- par faults/inject.py avant chaque scénario, en tant que superutilisateur.
-- (ALLOW_CONNECTIONS est rétabli à part, depuis la base "postgres".)

-- Rôle : connexion autorisée, pas de limite
ALTER ROLE etl_writer LOGIN CONNECTION LIMIT -1;

-- Colonne éventuellement renommée par le scénario S05
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'ventes' AND table_name = 'fact_sales' AND column_name = 'amount_euro') THEN
        ALTER TABLE ventes.fact_sales RENAME COLUMN amount_euro TO amount_eur;
    END IF;
END $$;

-- Droits nominaux
GRANT USAGE ON SCHEMA ventes TO etl_writer;
GRANT SELECT, INSERT, DELETE ON ventes.fact_sales TO etl_writer;

-- Libère les verrous laissés par le scénario T02 (sessions applicatives « chaos »)
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE application_name = 'chaos-lock' AND pid <> pg_backend_pid();
