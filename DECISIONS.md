# Journal de décisions

Chaque entrée : le contexte, la décision, ce qu'on a écarté et pourquoi. Les entrées
marquées 🔎 viennent de problèmes **trouvés en testant** sur un vrai Airflow 3.3.1.

## Données et pannes

**D1. Les pannes sont provoquées de l'extérieur du DAG.**
Le DAG ne contient aucune branche « si panne X alors… ». On agit sur l'environnement : fichier
supprimé, API qui change de comportement, droits retirés, colonne renommée, variable Airflow modifiée.
Les erreurs de code modifient réellement le fichier du DAG.
*Écarté* : un paramètre `conf={"panne": ...}` lu par le DAG. L'agent lit le source via `get_dag_source`
et y aurait trouvé la réponse.

**D2. 🔎 Le source du DAG ne doit pas parler du banc de test.**
La première version avait une docstring qui expliquait l'injection de pannes, un commentaire
« valeur canari » et un tag `cobaye`. Tout cela partait au modèle. Les explications sont passées dans la doc, et
un test (`test_dag_source_does_not_give_hints_to_the_agent`) interdit ces mots dans le DAG. Les 30 pannes
ont été recapturées après la correction.

**D3. Une exécution saine avant chaque panne.**
Elle valide que la remise à zéro a marché : si elle échoue, l'injecteur s'arrête au lieu de produire
des étiquettes fausses. Elle donne aussi un historique propre : l'échec suivant n'est pas compté
comme une récidive. Seule T05 enchaîne volontairement 3 échecs.

**D4. Chaque capture est vérifiée.**
Le run doit échouer, et sur la tâche attendue. Sinon le scénario est marqué `valide: false` dans le
manifeste et exclu des campagnes. 🔎 Cette vérification a attrapé deux problèmes :
- U05 : l'API simulée était censée couper la connexion, mais le client recevait un *ReadTimeout*.
  `socket.close()` ne ferme pas une socket tant que des objets `makefile()` y sont rattachés ;
  la correction utilise `shutdown(SHUT_RDWR)`.
- P04 (dossier en lecture seule) est invalide en mode local, où Airflow tournait en root, qui ignore
  les droits. Dans Docker, les tâches tournent avec un utilisateur non-root.

**D5. Instantanés + rejeu pour l'évaluation.**
Après chaque panne, on enregistre les réponses brutes d'Airflow (journaux de chaque tentative,
source de la version exécutée, historiques). Le serveur MCP peut ensuite les rejouer sans Airflow.
Toutes les configurations voient exactement les mêmes indices, ce qui rend la comparaison équitable,
reproductible, et possible en CI sans démarrer Airflow. Les réponses sont stockées **avant**
masquage : une amélioration du masquage profite aussi aux rejeux, et la mesure des fuites reste honnête.
Dans le rejeu, seul le run du scénario apparaît « en échec » (monde isolé).

**D6. Escalade attendue calculée, pas écrite à la main.**
Une seule politique (`agent/policy.py`) sert à la vérité terrain et à l'agent : catégorie
`dependance_indisponible` ou `droits_insuffisants`, correction qui touche des données, ou ≥ 3 échecs
consécutifs. Une erreur d'escalade de l'agent vient donc d'une erreur de classification ou
d'estimation, jamais d'une règle différente.

## Serveur MCP

**M1. SDK `mcp` 2.3 : `MCPServer`.** L'énoncé dit FastMCP, le nom de la v1 du SDK. En v2, la classe
s'appelle `MCPServer` (`mcp.server.mcpserver`), avec le même principe de décorateur `@tool`.
*Écarté* : épingler `mcp<2`, ce qui aurait figé le projet sur une version ancienne.

**M2. Lecture seule à trois niveaux.** (1) Le client n'émet que des GET. (2) Dans docker-compose, le
serveur se connecte avec un compte Airflow `diag_reader` de rôle FAB *Viewer*. (3) Les outils
portent l'annotation MCP `readOnlyHint`. L'injecteur, qui doit écrire, a son propre client admin,
inaccessible à l'agent.

**M3. Le masquage est fait dans le serveur MCP**, donc avant toute sortie vers un client. Il
fonctionne sans connaître les secrets : identifiants dans les URI, `clé=valeur` à nom sensible, en-têtes
`Authorization`, formats de jetons connus, chaînes à forte entropie. 🔎 Sur un vrai journal
Airflow 3, la clé d'API apparaît **en clair** dans `error_detail.exc_value` : le masqueur intégré à
Airflow ne la connaît pas. 🔎 Dans le source du DAG, `api_key = os.environ[...]` était masqué à tort,
ce qui aurait faussé les diffs proposés. Le mode `source_code=True` épargne les références de code, mais
pas les valeurs entre guillemets.

**M4. Canaris et métrique de fuite.** De fausses valeurs secrètes sont plantées là où de vrais
secrets fuiraient : mot de passe dans le DSN journalisé, clé d'API dans l'URL et dans la réponse 401,
DSN en dur dans le code. Chaque requête envoyée à un modèle est inspectée, avec un objectif de 0.
Le masqueur n'a jamais accès à la liste des canaris, sinon la mesure serait truquée.

**M5. Journaux : numéros d'origine, tentative explicite.**
Airflow 3 renvoie des journaux structurés et un traceback sous forme de champ `error_detail`
(frames sans code). Le serveur reconstruit un traceback lisible, résume les frames internes d'Airflow et
ajoute la ligne de code du DAG pour chaque frame qui s'y trouve (version exécutée par le run).
Lignes gardées : traceback, 15 lignes avant chaque erreur, avertissements, fin du journal.
Chaque ligne garde son numéro dans le journal complet, ce qui permet de vérifier les citations.
`get_task_logs` prend un `try_number` (par défaut la dernière tentative), parce qu'une tâche réessayée
peut échouer différemment à chaque essai.

**M6. `get_dag_source(dag_id, run_id)`.** Avec un `run_id`, l'outil renvoie la version du DAG exécutée
par ce run, grâce au versionnage d'Airflow 3. Le code a pu changer depuis, comme pour les pannes C01 à C05,
restaurées après l'exécution.

**M7. Erreurs d'outil lisibles.** Le SDK masque les exceptions imprévues (« Error executing
tool… »). Le serveur lève donc `ToolError` avec un message utile, par exemple « tâche non exécutée (upstream_failed) ».

## Agent

**A1. Une 7e classe `indetermine`.** Sans elle, le modèle force chaque panne dans une case. Elle
compte comme une erreur d'exactitude, mais déclenche l'escalade.

**A2. Escalade par règle, pas par jugement libre.** Le modèle fournit catégorie, confiance et
« touche des données ? », et le code décide. On escalade aussi si la confiance est < 0,6, si les citations
sont invalides ou si une étape n'a pas produit de sortie valide.

**A3. Mode JSON + validation Pydantic + relance.** Le mode « JSON schema strict » n'est pas
disponible partout (Ollama, certains modèles). Le mode JSON simple, avec le schéma dans le prompt et une
validation locale, marche chez tous les fournisseurs : la comparaison reste équitable. Jusqu'à 2 relances, avec
la liste précise des erreurs. On conserve la dernière sortie conforme au schéma même si une
vérification métier échoue : elle reste visible, mais marquée invalide.

**A4. Citations vérifiées, diff vérifié mais jamais appliqué.** Une citation doit désigner une ligne
présente dans l'extrait, et son texte doit y figurer. Le diff doit s'appliquer au source reçu : on tolère
un décalage de numéros de ligne, pas un contexte faux. Le diff n'est appliqué qu'en mémoire, ce qui reste
cohérent avec un MCP en lecture seule.

**A5. Routage.** Le petit modèle classe. Si la sortie est invalide, `indetermine` ou de confiance
< 0,75, le gros modèle reclasse et fait le reste ; sinon le petit modèle continue. L'aiguillage est une
arête conditionnelle LangGraph, visible dans le graphe. Le seuil est un paramètre enregistré dans MLflow.

## Modèles et passerelle

**L1. L'agent ne connaît que des alias LiteLLM.** Changer de modèle se fait dans `litellm/config.yaml`,
sans toucher au code.
**L2. Modèles retenus (vérifiés en novembre 2026).** DeepSeek a retiré `deepseek-chat` et
`deepseek-reasoner` le 24 juillet 2026 : on utilise `deepseek-v4-pro` (gros) et `deepseek-flash` (petit).
Chez Mistral : `mistral-large-latest` et `mistral-small-latest`. En local : `qwen2.5:7b-instruct`, solide
en JSON et léger. Tous sont présents dans la table de prix de LiteLLM 1.108.4.
**L3. Coûts.** On lit l'en-tête `x-litellm-response-cost` de chaque réponse (vérifié sur un faux
backend : 1 000 jetons × 2e-6 + 100 × 8e-6 = 0,0028 $). `LITELLM_LOCAL_MODEL_COST_MAP=True` : les prix
viennent de la table de la version épinglée, donc les coûts sont reproductibles. Limite connue : c'est le tarif
plein, alors que DeepSeek facture moitié moins en heures creuses.
**L4. Juge sur un alias dédié (`juge`)**, pour ne pas mélanger son coût à celui de l'agent.

## Évaluation

**E1. Un plantage de l'agent est un résultat.** Il compte comme une erreur. L'exclure ferait paraître
meilleur un modèle qui plante sur les cas difficiles.
**E2. Juge : grille écrite, à l'aveugle, température 0.** *Biais connu* : le juge (DeepSeek V4 Pro)
est aussi l'un des candidats et pourrait favoriser ses propres réponses. Parades : la grille fixe et surtout
la **vérification humaine à l'aveugle** d'un échantillon (feuille sans les notes du juge), avec accord
exact et kappa de Cohen à poids quadratiques (échelle ordinale 0-1-2).
**E3. Intervalle de confiance de Wilson sur l'exactitude.** Avec n = 30, 80 % d'exactitude donne
[63 %, 91 %]. Le tableau l'affiche pour éviter de sur-interpréter de petits écarts.
**E4. Le tableau du README est généré depuis MLflow.** Seules les campagnes complètes, terminées et
faites avec de vrais modèles sont retenues. Les campagnes au faux modèle (`fake=true`) sont refusées.

## CI

**C1. Deux jobs.** (1) Des tests à chaque commit, sans réseau : vrais instantanés, faux modèles scriptés.
(2) Une évaluation courte sur les 12 pannes marquées `ci` (2 par catégorie) avec `deepseek-flash`, peu cher,
et sans juge. Elle échoue si l'exactitude passe sous la référence `eval/baseline_ci.json`.
**C2. La référence vient d'une vraie exécution** (`make baseline-ci`) et n'est jamais écrite à la main.
Sans référence, la CI échoue avec la consigne à suivre. Sans secret (PR depuis un fork), le job est ignoré avec un avertissement.
**C3. Tolérance 0 par défaut**, comme le demande l'énoncé (« échoue si l'exactitude baisse »). Même à
température 0, les modèles ne sont pas parfaitement déterministes : si la CI devient instable, il faudra relever
la tolérance dans la référence et le tracer ici.

## Infrastructure

**I1. LocalExecutor** au lieu de Celery : pas de Redis ni de worker, moins de mémoire. Les tâches
tournent dans le scheduler, qui monte donc le volume de données.
**I2. Images LiteLLM et MLflow construites depuis PyPI**, aux versions testées (1.108.4 et 3.18.0),
plutôt que des tags d'images que je n'ai pas pu vérifier. Ollama n'est pas épinglé (`OLLAMA_VERSION` dans `.env`).
**I3. MLflow 3.18** : `--allowed-hosts` est obligatoire pour qu'un autre conteneur y accède par son nom.
**I4. Mode local sans Docker (`faults/env.py: LocalEnv`)**, utilisé pour développer et tester l'injecteur
sur un vrai Airflow installé par pip. Il demande un Postgres local, et il faut lancer Airflow en non-root pour P04.
🔎 Un PID périmé empêchait d'arrêter l'API simulée (U02 invalide) : l'arrêt vérifie désormais que le
service ne répond vraiment plus.

**I5. 🔎 MLflow 3.18 : tâches de fond désactivées, un seul worker.** Sur une machine de 8 Go, le serveur
MLflow relançait en boucle ses workers de tâches de fond (scoring en ligne, archivage de traces),
inutiles ici, jusqu'à saturer la machine. Résultat : Airflow ne répondait plus à temps et l'injecteur
a planté. Correctifs : `MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false`, `--workers 1`, et des appels à Airflow
qui réessaient sur erreur réseau passagère (injecteur et serveur MCP).

**I6. 🔎 Une erreur du juge n'arrête pas la campagne.** Elle est enregistrée pour le scénario concerné
(`erreur_juge`, compteur `erreurs_juge` dans MLflow).

## Limites connues

- **Le jeu de pannes est probablement facile.** Le faux modèle de test, de simples mots-clés que j'ai écrits
  en connaissant les pannes, atteint 90 % d'exactitude sur les instantanés. Ce n'est pas une vraie
  référence (elle est taillée sur mesure), mais un LLM doit faire nettement mieux qu'une règle pour se
  justifier. Les cas où il peut le faire sont ceux qui demandent de croiser journal et source, comme C03
  (`KeyError` qui ressemble à un schéma modifié). Il faudrait ajouter des pannes ambiguës ou composées.
- Le nom `cobaye_ventes` (« cobaye » = pipeline de test) reste visible par l'agent. Il ne révèle aucune
  panne et le renommer imposerait de tout recapturer : laissé tel quel.
- Les instantanés versionnés ont été recapturés dans la pile Docker : les 30 pannes sont valides,
  P04 comprise, puisque les tâches n'y tournent pas en root.

## Après la première campagne (novembre 2026)

**R1. DeepSeek seul.** Faute de clé Mistral, seuls `deepseek-v4-pro` (gros) et `deepseek-flash` (petit)
ont été évalués. Une configuration de routage `routage-deepseek-flash-deepseek-pro` a été ajoutée pour
comparer quand même les trois stratégies.

**R2. Étiquettes figées malgré des cas discutables.** Après avoir vu les résultats, trois étiquettes
paraissent discutables : D04 (API qui renvoie une liste vide : la correction touche-t-elle des données ?),
T02 (verrou sur la table) et la classification de C02 (erreur de code ou schéma modifié ?).
Les modifier après coup reviendrait à ajuster la vérité terrain pour améliorer les chiffres, comme
changer de critère de jugement en cours d'étude. Elles restent donc telles quelles. Toute révision de
la définition se fera avant une nouvelle campagne, qui relancera toutes les configurations.

**R3. Validation du juge.** Accord humain/juge sur 15 diagnostics : kappa pondéré de 0,79 pour le
diagnostic et de 0,59 pour la correction. Seule la justesse du diagnostic figure dans le tableau.

**R4. 🔎 Problèmes trouvés en déployant sur GitHub Codespaces.**
- L'image `devcontainers/python:3.12` est passée sur Debian 13 (trixie), où la fonctionnalité
  docker-in-docker ne s'installe pas : image épinglée sur `3.12-bookworm`.
- `AIRFLOW_UID` n'était pas transmis à `airflow-init`, qui donnait les volumes au mauvais utilisateur :
  corrigé dans `docker-compose.yml`, et l'injecteur manipule désormais les fichiers en root.
- Le compte `diag_reader` n'avait pas été créé à l'initialisation, et l'erreur était masquée par un
  `|| echo`. 🔎 Cause : `init.sh` appelait `airflow` directement, sans passer par l'entrypoint officiel
  de l'image. En root, Python ne trouvait alors pas le paquet installé dans `/home/airflow/.local`
  (`ModuleNotFoundError: No module named 'airflow'`). Le compte est désormais créé par le service
  `airflow-users`, qui passe par l'entrypoint officiel une fois l'api-server prêt, et il est recréé à
  chaque démarrage pour que son mot de passe suive le `.env`.
