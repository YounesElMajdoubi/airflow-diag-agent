"""Prompts de l'agent (en français) et mise en forme des indices.

Les définitions de catégories sont écrites pour être DISCRIMINANTES : chaque
paire facile à confondre (donnée manquante / schéma modifié, schéma / code,
dépendance / droits) a une règle de départage explicite.
"""

from __future__ import annotations

import json

CATEGORIES_DEF = """\
- donnee_manquante : la donnée attendue est absente ou vide (fichier non déposé, fichier vide, \
aucune ligne, champ obligatoire vide, aucun enregistrement renvoyé). Le format est celui attendu, \
c'est le contenu qui manque.
- schema_modifie : la donnée est présente mais sa structure ou son format a changé par rapport à \
ce que le code attend (colonne ou champ renommé, type ou format différent, objet au lieu de liste, \
colonne de la table cible renommée). Le code du DAG, lui, est cohérent et n'a pas changé.
- delai_depasse : une opération dépasse une limite de temps (timeout réseau, statement_timeout, \
execution_timeout d'Airflow), que la cause soit une lenteur, un verrou ou une limite trop basse.
- erreur_code : bug dans le code du DAG lui-même (faute de frappe, variable non définie, mauvaise \
conversion, mauvaise clé interne, requête SQL mal formée, mauvais appel). Les données et services \
sont conformes. Règle de départage avec schema_modifie : si le nom ou le format attendu par le code \
est incohérent avec ce que produit le reste du code, c'est erreur_code.
- dependance_indisponible : un service externe ne répond pas ou refuse la connexion (erreur HTTP 5xx, \
hôte introuvable, connexion refusée ou coupée, base qui n'accepte pas de connexion).
- droits_insuffisants : l'authentification ou l'autorisation échoue (HTTP 401 ou 403, permission \
denied, rôle sans droit de connexion, fichier ou dossier non accessible en écriture).
- indetermine : les indices ne permettent pas de trancher entre plusieurs catégories."""

ROLE = ("Tu es un ingénieur data d'astreinte expérimenté. Tu diagnostiques une tâche Airflow en échec "
        "à partir d'indices réels. Tu ne supposes rien qui ne soit pas dans les indices. "
        "Les secrets ont été remplacés par [SECRET_MASQUÉ] : ce n'est pas une cause de panne.")

CLASSIFY_SYSTEM = f"""{ROLE}

Étape : CLASSER la panne dans exactement une catégorie.
{CATEGORIES_DEF}

`confiance` : ta probabilité que la catégorie soit la bonne (0 à 1). Sois honnête : \
une confiance élevée sur un cas ambigu est une faute.
`justification` : 1 à 3 phrases appuyées sur les indices."""

DIAGNOSE_SYSTEM = f"""{ROLE}

Étape : DIAGNOSTIQUER la cause racine (pas le symptôme : « l'API renvoie 503 » plutôt que \
« la tâche a échoué »). Catégorie retenue à l'étape précédente : {{categorie}}.

Tu dois citer de 1 à 6 lignes du journal qui prouvent ton diagnostic :
- `ligne` = le numéro L<n> exact affiché devant la ligne ;
- `extrait` = un morceau de texte recopié MOT POUR MOT depuis cette ligne.
Les citations sont vérifiées automatiquement ; une citation inventée invalide le diagnostic."""

FIX_SYSTEM = f"""{ROLE}

Étape : PROPOSER une correction pour la cause suivante.
Catégorie : {{categorie}}
Cause : {{cause}}

Règles :
- `nature` : code, configuration, donnees, infrastructure ou acces.
- `touche_donnees` = true si la correction exige de créer, redéposer, corriger, supprimer ou migrer \
des données ou la structure d'un stockage (fichier d'entrée, table de l'entrepôt) ; false si elle ne \
concerne que du code, de la configuration, des accès ou un service.
- Si la correction est dans le code du DAG, donne un `diff` unifié (format `diff -u`) qui s'applique \
au source fourni : en-tête @@ -a,b +c,d @@, lignes de contexte recopiées exactement, préfixes \
« - », « + » et espace. Le diff sera vérifié automatiquement mais JAMAIS appliqué.
- Si la correction est hors du code (service externe, accès, données à redéposer), `diff` = null \
et décris les actions dans `etapes`.
- Ne « corrige » jamais une panne en supprimant une vérification ou en masquant l'erreur."""


def format_log(log: dict) -> str:
    out, prev = [], 0
    for ln in log["lignes"]:
        if ln["n"] > prev + 1:
            out.append(f"[... lignes {prev + 1} à {ln['n'] - 1} omises ...]")
        out.append(f"L{ln['n']}: {ln['texte']}")
        prev = ln["n"]
    if prev < log["total_lignes"]:
        out.append(f"[... lignes {prev + 1} à {log['total_lignes']} omises ...]")
    return "\n".join(out)


def format_evidence(ind: dict) -> str:
    run, task, log, hist = ind["run"], ind["tache"], ind["journal"], ind["historique"]
    resume = hist["resume"]
    return f"""## Exécution
DAG {run['dag_id']} — run {run['run_id']} (date logique {run.get('date_logique')})
Tâche en échec : {task['task_id']} — tentative {log['tentative']} sur {task.get('tentatives_max')} \
(durée {task.get('duree_s')} s, opérateur {task.get('operateur')})
Autres tâches en échec : {', '.join(ind['autres_echecs']) or 'aucune'}
Tâches bloquées en aval : {', '.join(run.get('taches_bloquees') or []) or 'aucune'}

## Historique de la tâche {task['task_id']}
{resume['executions']} exécutions récentes : {resume['succes']} succès, {resume['echecs']} échecs ; \
échecs consécutifs jusqu'à ce run inclus : {ind['recidive']} ; durée médiane {resume['duree_mediane_s']} s, \
durée max {resume['duree_max_s']} s.

## Journal de la tentative {log['tentative']} (lignes utiles ; L<n> = numéro dans le journal complet)
{format_log(log)}

## Source du DAG (version {ind['source']['version']} exécutée par ce run)
```python
{ind['source']['source']}
```"""


def classify_prompt(ind: dict) -> str:
    return format_evidence(ind) + "\n\nClasse cette panne."


def diagnose_prompt(ind: dict, classification: dict) -> str:
    return (format_evidence(ind) + "\n\n## Classification retenue\n"
            + json.dumps(classification, ensure_ascii=False) + "\n\nDiagnostique la cause racine en citant le journal.")


def fix_prompt(ind: dict, diagnostic: dict | None) -> str:
    diag = json.dumps(diagnostic, ensure_ascii=False) if diagnostic else "(diagnostic indisponible)"
    return format_evidence(ind) + f"\n\n## Diagnostic\n{diag}\n\nPropose la correction."
