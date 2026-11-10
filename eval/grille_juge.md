# Grille de notation du juge

Le juge reçoit, pour une panne : la **cause réelle** (vérité terrain écrite à la main),
la **catégorie attendue**, puis le **diagnostic** et la **correction** produits par l'agent.
Il ne connaît ni le modèle ni la stratégie qui ont produit la réponse.

## Note du diagnostic (0, 1 ou 2)

- **2 — correct et précis** : identifie le même mécanisme de panne que la cause réelle
  ET l'élément concerné (le fichier, le champ, la colonne, le service, le droit, la ligne de code…).
  Une formulation différente est acceptée si le sens est le même.
- **1 — partiellement correct** : bonne zone du problème mais mécanisme flou ou incomplet,
  OU bon mécanisme mais élément concerné faux ou absent.
  Exemple : « problème de connexion à l'API » alors que la cause est un 401 (clé refusée).
- **0 — faux ou non étayé** : autre cause, simple paraphrase du symptôme
  (« la tâche a échoué »), ou affirmation contredite par la cause réelle.

Règles :
- On juge la **cause racine**, pas la classification (elle est mesurée à part).
- Un diagnostic qui mentionne la bonne cause parmi plusieurs hypothèses sans trancher vaut au plus 1.
- Les citations du journal sont vérifiées automatiquement ailleurs : ne pas les re-noter.

## Note de la correction (0, 1 ou 2)

- **2 — pertinente** : traiterait réellement la cause réelle, sans effet de bord dangereux.
- **1 — partielle** : va dans le bon sens mais incomplète ou trop vague pour être appliquée.
- **0 — inadaptée ou dangereuse** : ne traite pas la cause, ou masque l'erreur
  (supprimer une vérification, ignorer l'exception, élargir des droits sans raison).

## Sortie

Un objet JSON : `note_diagnostic`, `note_correction`, `justification` (2 phrases maximum).
