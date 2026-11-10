# Raccourcis du projet. Les scripts Python tournent sur la machine hôte dans .venv
# et parlent aux services docker via les ports publiés (voir .env).
SHELL := /bin/bash
PY := .venv/bin/python
ENV := set -a; source .env; set +a;

.PHONY: env venv up up-local down reset-all logs test pannes pannes-ci demo agent eval eval-local verif-juge accord-juge tableau baseline-ci ci inspecteur

env:            ## crée .env à partir du modèle (ne l'écrase pas)
	@test -f .env || (cp .env.example .env && echo ".env créé : renseigner les clés d'API")

venv:           ## environnement Python local (agent, injecteur, évaluation)
	python3 -m venv .venv && .venv/bin/pip install -q -e ".[eval,dev]"

up: env         ## démarre la pile SANS modèle local (~4 Go de RAM)
	docker compose up -d --build

up-local: env   ## démarre la pile AVEC le modèle local Ollama (~10 Go de RAM)
	docker compose --profile local up -d --build
	docker compose --profile local up ollama-pull

down:
	docker compose --profile local down

reset-all:      ## ATTENTION : supprime aussi les volumes (bases, journaux, modèles, MLflow)
	docker compose --profile local down -v

logs:
	docker compose logs -f --tail 50

test:           ## tests unitaires et de bout en bout (sans réseau, faux modèles)
	$(PY) -m pytest -q

pannes:         ## provoque les 30 pannes et capture les instantanés (~45 min)
	$(ENV) $(PY) -m faults.inject --env docker --all

pannes-ci:      ## seulement les 12 pannes du sous-ensemble CI
	$(ENV) $(PY) -m faults.inject --env docker --only D01,D04,S01,S03,T01,T04,C01,C03,U01,U03,P01,P03

demo:           ## démo : provoque U01 en laissant la panne en place, puis diagnostic en direct via le serveur MCP
	$(ENV) $(PY) -m faults.inject --env docker --only $${PANNE:-U01} --keep --no-capture
	$(ENV) $(PY) -m agent.cli --dag cobaye_ventes --mcp-url http://localhost:8765/mcp \
	    --strategie routage --petit $${PETIT:-mistral-small} --gros $${GROS:-deepseek-pro}

agent:          ## diagnostic du dernier échec réel (ex. make agent GROS=deepseek-pro)
	$(ENV) $(PY) -m agent.cli --dag cobaye_ventes --mcp-url http://localhost:8765/mcp --strategie gros --gros $${GROS:-deepseek-pro}

eval:           ## campagne complète : toutes les configurations, 30 pannes, juge, MLflow
	$(ENV) $(PY) -m eval.run_campaign --all
	$(ENV) $(PY) -m eval.make_results_table

eval-local:     ## campagne du seul modèle local (sans clé d'API, sans juge)
	$(ENV) $(PY) -m eval.run_campaign --config petit-local --no-judge

verif-juge:     ## tire l'échantillon à noter à la main (RUNS="id1 id2 ...")
	$(ENV) $(PY) -m eval.manual_check sample --runs $(RUNS) --n $${N:-15}

accord-juge:    ## calcule l'accord humain / juge une fois la feuille remplie
	$(ENV) $(PY) -m eval.manual_check accord eval/verification/feuille.csv
	$(ENV) $(PY) -m eval.make_results_table

tableau:        ## régénère le tableau du README depuis MLflow
	$(ENV) $(PY) -m eval.make_results_table

baseline-ci:    ## référence de la CI, à partir d'une vraie exécution
	$(ENV) $(PY) -m eval.ci_eval --write-baseline

ci:             ## même évaluation courte que GitHub Actions
	$(ENV) $(PY) -m eval.ci_eval

inspecteur:     ## inspecteur MCP officiel branché sur le serveur du docker-compose
	npx @modelcontextprotocol/inspector
