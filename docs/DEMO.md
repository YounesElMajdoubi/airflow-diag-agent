# Déroulé de la vidéo de démonstration (2 à 3 minutes)

Préparation : pile lancée (`make up`), clés dans `.env`, écran partagé entre l'interface
Airflow (http://localhost:8080) et un terminal.

1. **Contexte (15 s)** : montrer le DAG `cobaye_ventes` dans Airflow, avec un run récent en vert.
2. **Provoquer la panne (40 s)** :
   ```bash
   PANNE=P03 make demo
   ```
   L'injecteur retire le droit `INSERT` de `etl_writer` sur la table et lance un run.
   Dans Airflow, `load_warehouse` passe au rouge et les tâches en aval sont bloquées.
3. **Diagnostic en direct (60 s)** : la même commande lance ensuite l'agent sur le **vrai** Airflow,
   via le serveur MCP du docker-compose (`--mcp-url http://localhost:8765/mcp`). Montrer :
   - la catégorie et la confiance ;
   - les lignes du journal citées, et les retrouver dans le journal Airflow (mêmes numéros) ;
   - la correction (« redonner le droit INSERT » : nature `acces`) ;
   - l'escalade : **oui**, catégorie `droits_insuffisants` ;
   - le coût et la durée en bas du rapport.
4. **Sécurité (15 s)** : montrer dans le journal Airflow la clé d'API ou le DSN en clair, puis
   `[SECRET_MASQUÉ]` dans ce que l'agent a reçu (par exemple avec la panne P01).
5. **Mesure (30 s)** : ouvrir MLflow, comparer deux runs (gros seul et routage) sur l'exactitude et le coût,
   puis montrer le tableau du README.
6. **Réparer** : `python -m faults.inject --env docker --reset`.

Astuce : `GROS=deepseek-pro PETIT=local PANNE=U01 make demo` montre le routage avec le modèle local.
