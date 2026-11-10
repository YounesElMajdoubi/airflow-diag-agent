# Guide des briques : ce qu'elles font et pourquoi elles sont faites ainsi

Ce guide sert à **comprendre et expliquer** chaque brique (en soutenance ou en entretien).
Pour chacune : le principe, le fonctionnement dans ce projet, et les questions qu'on risque
de te poser. Les décisions détaillées, avec leurs alternatives, sont dans [DECISIONS.md](../DECISIONS.md).

---

## 1. Airflow 3 et le DAG cobaye

**Principe.** Airflow orchestre des tâches définies en Python sous forme de DAG (graphe orienté
sans cycle). Dans Airflow 3, l'architecture est découpée en services séparés :
- l'**api-server** sert l'interface web et l'API REST v2 ;
- le **scheduler** décide quoi lancer (et, avec le LocalExecutor, exécute les tâches) ;
- le **dag-processor** lit les fichiers de DAG et enregistre une nouvelle **version** du DAG
  à chaque modification.

**Dans le projet.** `cobaye_ventes` ressemble à un vrai pipeline : CSV de commandes + API de taux de
change → conversion en euros → chargement dans un entrepôt Postgres → rapport. Il contient
volontairement trois mauvaises pratiques qui font fuiter des secrets : un DSN en clair dans le code, le DSN
écrit dans les journaux, la clé d'API passée dans l'URL. Le source ne le dit pas, puisque l'agent le lit.

**Ce qu'il faut savoir expliquer.**
- *Pourquoi `retries=1` ?* Pour que chaque tâche en échec ait deux tentatives, donc deux journaux.
  Le serveur MCP doit choisir la bonne tentative.
- *Journaux structurés.* Airflow 3 ne renvoie pas du texte mais une liste de messages JSON. Une
  exception y apparaît comme un champ `error_detail` (type, message, frames), pas comme un traceback texte.
- *API REST et JWT.* On obtient un jeton par `POST /auth/token` (identifiant + mot de passe), puis on
  l'envoie dans `Authorization: Bearer …`. C'est le même mécanisme avec le gestionnaire FAB (Docker)
  et le gestionnaire simple (mode local).

## 2. Le jeu de 30 pannes

**Principe.** Pour mesurer un agent, il faut une **vérité terrain** : pour chaque panne, la bonne
catégorie, la vraie cause, la tâche qui doit échouer et la bonne décision d'escalade.

**Dans le projet.** `faults/scenarios.yaml` décrit les 30 pannes ; `faults/inject.py` les provoque
une par une sur le vrai Airflow :

1. remise à zéro (API normale, droits, colonnes, services, DAG d'origine) ;
2. **exécution saine** : elle doit réussir, sinon on s'arrête (remise à zéro ratée) ;
3. injection de la panne puis exécution, qui doit échouer **sur la tâche attendue** ;
4. capture d'un **instantané** : toutes les réponses brutes d'Airflow pour ce run.

**Questions probables.**
- *Pourquoi provoquer les pannes de l'extérieur ?* Si le DAG contenait `if panne == "S01"`, l'agent
  lirait la réponse dans le source (DECISIONS D1).
- *Pourquoi l'escalade attendue est-elle calculée ?* Une seule règle sert à la fois à étiqueter et à
  décider : une erreur d'escalade vient forcément d'une erreur de classification (D6).
- *Biais du jeu de test ?* Il est petit (30), équilibré (5 par catégorie) et écrit par la même personne
  que l'agent. Les prompts n'ont pas le droit de mentionner un scénario précis. C'est une limite réelle.

## 3. Le serveur MCP

**Principe.** MCP (*Model Context Protocol*) est un protocole standard entre une application d'IA
(le client) et des fournisseurs d'outils ou de données (les serveurs). Le serveur déclare des **outils** avec
un schéma JSON de leurs paramètres ; le client les liste puis les appelle. Deux transports : **stdio**
(le client lance le serveur comme sous-processus) et **HTTP « streamable »** (serveur réseau).

**Dans le projet.** `mcp_server/server.py` déclare 4 outils avec `@server.tool()` (classe `MCPServer`
du SDK officiel v2, ex-FastMCP). Les schémas sont générés à partir des annotations Python. Le serveur ajoute
trois traitements avant de répondre :
- **troncature** des journaux aux lignes utiles, avec les numéros d'origine (`mcp_server/logs.py`) ;
- **traceback reconstruit**, avec la ligne de code du DAG ;
- **masquage des secrets** (`mcp_server/redaction.py`).

Trois modes d'accès à Airflow derrière la même interface (`mcp_server/airflow_api.py`) : *live*
(API REST), *enregistrement* (capture des instantanés) et *rejeu* (sans Airflow).

**Questions probables.**
- *Pourquoi passer par MCP plutôt que donner l'API Airflow à l'agent ?* Pour séparer les rôles : l'agent
  ne connaît ni l'URL, ni les identifiants, ni les routes d'Airflow. Il ne peut faire que ce que les
  4 outils permettent, en lecture seule. Le même serveur peut servir un autre client MCP (Claude Desktop,
  l'inspecteur…).
- *Comment garantir la lecture seule ?* À trois niveaux : GET uniquement, compte Airflow *Viewer*,
  annotations `readOnlyHint` (M2).
- *Comment tester le serveur seul ?* `make inspecteur`, puis se connecter à `http://localhost:8765/mcp`
  et appeler les outils à la main.

## 4. Le masquage des secrets

**Principe.** Tout ce qui part vers un modèle externe peut être journalisé chez le fournisseur.
Un secret ne doit jamais quitter le système.

**Dans le projet.** Des règles par **forme** et par **contexte** : `user:motdepasse@` dans une URI,
`api_key=…`, `Authorization: Bearer …`, formats de jetons connus. S'y ajoute un **filet de sécurité par
entropie** : une longue chaîne aléatoire mêlant lettres et chiffres est masquée.

**Le point méthodologique important** : on ne fait pas confiance au masquage, on le **mesure**.
Des secrets canaris sont plantés et on vérifie qu'aucune requête aux modèles ne les contient
(`eval/leaks.py`). Le masqueur ne connaît pas les canaris, sinon la mesure serait truquée.

## 5. L'agent LangGraph

**Principe.** LangGraph modélise un agent comme un **graphe d'états** : un état typé partagé
(`DiagState`), des **nœuds** (fonctions qui lisent l'état et en renvoient une mise à jour) et des
**arêtes**, éventuellement conditionnelles. Les **réducteurs** (`Annotated[list, operator.add]`) disent
comment fusionner les mises à jour. Ici, la liste des appels LLM s'allonge à chaque nœud au lieu d'être écrasée.

**Dans le projet** (`agent/graph.py`) :

```
collecter_indices → classer → [aiguillage] → (reclasser_gros) → diagnostiquer → proposer_correction → decider_escalade
```

- *collecter_indices* : sans LLM. Appelle `list_failed_runs`, puis le journal de la dernière tentative
  de la première tâche tombée, son historique (d'où le nombre d'**échecs consécutifs**) et le source exécuté.
- *classer* : 6 catégories + `indetermine`, avec une confiance.
- *aiguillage* : arête conditionnelle, utilisée seulement par la stratégie « routage ».
- *diagnostiquer* : cause racine + citations des lignes du journal, **vérifiées** par du code.
- *proposer_correction* : un diff sur le source du DAG, vérifié et jamais appliqué, ou des étapes manuelles.
- *decider_escalade* : sans LLM. Une règle explicite (`agent/escalation.py`), puis le rapport Pydantic.

**Questions probables.**
- *Pourquoi pas un agent ReAct qui choisit lui-même ses outils ?* La collecte est toujours la même :
  la coder en dur coûte zéro jeton, ne se trompe pas et rend les campagnes comparables. Le LLM sert là où
  il apporte quelque chose : interpréter, expliquer, proposer.
- *Que se passe-t-il si le modèle répond n'importe quoi ?* Pydantic rejette la sortie, et le modèle
  reçoit l'erreur précise, avec 2 relances au maximum. Si ça échoue encore, l'étape est marquée ratée et
  l'agent escalade (A3).
- *Comment éviter les hallucinations dans le diagnostic ?* Les citations sont contrôlées par du code :
  une ligne inventée est détectée, le modèle est relancé, puis l'agent escalade.

## 6. LiteLLM

**Principe.** Un proxy qui expose une API unique, compatible OpenAI, devant de nombreux fournisseurs.
Il traduit les requêtes, gère les réessais et calcule le coût de chaque appel.

**Dans le projet.** L'agent appelle des **alias** (`deepseek-pro`, `mistral-small`, `local`…).
`litellm/config.yaml` dit où ils pointent. Le coût de chaque appel est lu dans l'en-tête
`x-litellm-response-cost` et additionné par diagnostic.

**Les trois stratégies.**
- *gros seul* : la qualité maximale, au coût maximal ;
- *petit seul* (dont le modèle local) : pas cher, ou gratuit, mais moins fiable ;
- *routage* : le petit classe ; s'il est peu sûr, le gros reprend. On mesure le **taux de passage au gros** :
  s'il est proche de 100 %, le routage coûte plus cher que le gros seul.

## 7. L'évaluation et MLflow

**Principe.** MLflow enregistre des *runs* : des **paramètres** (ce qu'on a choisi), des **métriques**
(ce qu'on a mesuré) et des **artefacts** (fichiers). Un run = une configuration testée sur les 30 pannes.

**Dans le projet** (`eval/run_campaign.py`) : pour chaque configuration et chaque panne, l'agent
tourne sur l'instantané rejoué. Le juge note le diagnostic, puis tout est enregistré : exactitude, F1 macro,
matrice de confusion, escalades (précision, rappel), validité des citations et des diffs, coûts,
durées (moyenne, p50, p95), relances, fuites. Les rapports complets sont stockés en artefacts.

**Les points méthodologiques à savoir défendre** (ta formation en méthodologie est un atout ici) :
- **Même échantillon pour tous** : comparaison appariée, sans variabilité d'échantillonnage entre bras.
- **Intervalle de confiance de Wilson** sur l'exactitude : avec n = 30, un écart de 5 points n'est pas
  significatif.
- **Un plantage de l'agent est compté comme une erreur**, pas exclu (c'est l'analogue d'une analyse en
  intention de traiter).
- **Validation du juge** : notation humaine **à l'aveugle** d'un échantillon tiré avec une graine fixe, puis
  **kappa de Cohen à poids quadratiques** (l'échelle 0-1-2 est ordinale) et écart moyen : le juge est-il
  plus généreux que toi ?
- **Biais d'auto-préférence** : le juge est aussi un candidat. On le dit, et la vérification humaine sert de garde-fou.

## 8. GitHub Actions

Deux jobs : des tests sans réseau à chaque commit, et une **évaluation courte** (12 pannes, `deepseek-flash`,
sans juge) qui échoue si l'exactitude passe sous la référence `eval/baseline_ci.json`.
Cette référence vient d'une vraie exécution (`make baseline-ci`). Elle utilise les instantanés versionnés,
donc la CI n'a pas besoin de lancer Airflow. La clé DeepSeek est un *secret* du dépôt ; sans elle,
le job est ignoré.

## Ordre conseillé pour tout refaire et comprendre

1. `make up`, ouvrir Airflow, lancer à la main un run de `cobaye_ventes` (il doit réussir).
2. `python -m faults.inject --env docker --only D01` : regarder le run en échec dans l'interface,
   puis `faults/snapshots/D01.json`.
3. `make inspecteur` : appeler les 4 outils à la main sur ce run, et constater le masquage et la troncature.
4. `make agent` : lire le rapport, puis le comparer au journal.
5. `make eval-local` (gratuit), puis `make eval` ; ouvrir MLflow et comparer les runs.
