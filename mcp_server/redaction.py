"""Masquage des secrets AVANT tout envoi au modèle.

Principe : défense en profondeur, sans connaître les secrets à l'avance.
Le masqueur ne reçoit jamais la liste des « canaris » utilisés par l'évaluation
(sinon la mesure des fuites serait truquée). Il repère les secrets par leur forme
et par leur contexte :

1. identifiants dans une URI          postgresql://user:MOTDEPASSE@hote
2. clé=valeur à nom sensible          api_key=..., password: ..., "token": "..."
3. en-têtes d'authentification         Authorization: Bearer ...
4. formats de jetons connus            JWT, sk-/pk-/fk_live_..., AKIA..., ghp_..., clés PEM
5. chaînes à forte entropie            longues suites aléatoires (filet de sécurité)

Chaque masquage est compté pour pouvoir le rapporter (transparence).
"""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass, field

MASK = "[SECRET_MASQUÉ]"

_SENSITIVE_NAME = (
    r"[\w-]*(?:password|passwd|pwd|secret|token|api[_-]?key|apikey|access[_-]?key|"
    r"private[_-]?key|client[_-]?secret|credential|mot[_-]?de[_-]?passe)[\w-]*"
)

# (nom, motif, groupe à remplacer) — l'ordre compte : du plus spécifique au plus général.
_RULES: list[tuple[str, re.Pattern, int]] = [
    ("cle_pem", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.S), 0),
    ("uri_identifiants", re.compile(r"(?i)\b([a-z][a-z0-9+.\-]*://[^:/\s@'\"]+:)([^@\s/'\"]+)(@)"), 2),
    ("entete_auth", re.compile(r"(?i)(authorization['\"]?\s*[:=]\s*['\"]?(?:bearer|basic|token)?\s*)([^\s'\",}]+)"), 2),
    ("bearer", re.compile(r"(?i)(\bbearer\s+)([A-Za-z0-9._~+/=\-]{8,})"), 2),
    ("cle_valeur", re.compile(rf"(?i)\b({_SENSITIVE_NAME})(['\"]?\s*[:=]\s*['\"]?)([^\s'\"&,;}}\])(\[]+)"), 3),
    ("jwt", re.compile(r"\beyJ[A-Za-z0-9_\-]{5,}\.[A-Za-z0-9_\-]{5,}\.[A-Za-z0-9_\-]{5,}"), 0),
    ("jeton_prefixe", re.compile(r"\b(?:sk|pk|rk|fk)[-_](?:live[-_]|test[-_]|proj[-_])?[A-Za-z0-9]{16,}\b"), 0),
    ("cle_aws", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"), 0),
    ("jeton_github", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}\b"), 0),
    ("jeton_slack", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"), 0),
]

# Candidats pour la règle d'entropie : longues suites sans espace.
_ENTROPY_CANDIDATE = re.compile(r"[A-Za-z0-9+=_\-]{24,}")
# À ne jamais masquer par entropie : chemins, identifiants Airflow, dates…
_ENTROPY_ALLOW = re.compile(
    r"^(?:manual|scheduled|backfill|asset_triggered)__|^\d{4}-\d{2}-\d{2}|^unusual_prefix_|^[a-z_]+$|^[A-Z_]+$"
)


def _shannon(s: str) -> float:
    counts = Counter(s)
    return -sum(c / len(s) * math.log2(c / len(s)) for c in counts.values())


def _looks_random(token: str) -> bool:
    if _ENTROPY_ALLOW.search(token):
        return False
    # Un secret aléatoire mélange lettres ET chiffres ; un identifiant de code, rarement.
    if not (re.search(r"[0-9]", token) and re.search(r"[A-Za-z]", token)):
        return False
    if re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", token):
        return False  # UUID (identifiants Airflow), pas un secret
    # Hexadécimal long (≥ 32) : typique des clés ; sinon seuil d'entropie classique.
    if re.fullmatch(r"[0-9a-fA-F]{32,}", token):
        return True
    return _shannon(token) >= 4.0


def _is_code_reference(m: re.Match) -> bool:
    """`api_key = os.environ[...]` ou `"api_key": api_key` : du code, pas un secret.

    Une valeur non entre guillemets, faite uniquement de lettres minuscules, de
    « _ » et de « . » (nom de variable, attribut), ou suivie de ( ou [ (appel,
    indexation) est une référence. Un vrai secret contient presque toujours des
    chiffres ou des majuscules ; s'il est entre guillemets, on le masque toujours.
    """
    value, sep = m.group(3), m.group(2)
    quoted = sep.rstrip().endswith(("'", '"'))
    following = m.string[m.end(3):m.end(3) + 1]
    if quoted:
        return False
    return bool(re.fullmatch(r"[a-z_][a-z_.]*", value)) or following in ("(", "[")


@dataclass
class Redactor:
    """Masque les secrets dans un texte. Garde un compteur par règle.

    source_code=True (pour le source du DAG) : ne masque pas les références de
    code du type `api_key = os.environ[...]`, sinon les diffs proposés seraient faux.
    Dans les journaux, on reste strict : tout `password=xxx` est masqué.
    """

    counts: Counter = field(default_factory=Counter)
    source_code: bool = False

    def redact(self, text: str) -> str:
        if not text:
            return text
        for name, pattern, group in _RULES:
            text = pattern.sub(lambda m, n=name, g=group: self._replace(m, n, g), text)

        def _entropy(m: re.Match) -> str:
            tok = m.group(0)
            if MASK in tok or not _looks_random(tok):
                return tok
            self.counts["entropie"] += 1
            return MASK

        return _ENTROPY_CANDIDATE.sub(_entropy, text)

    def _replace(self, m: re.Match, name: str, group: int) -> str:
        if MASK in m.group(group):
            return m.group(0)
        if name == "cle_valeur" and self.source_code and _is_code_reference(m):
            return m.group(0)
        self.counts[name] += 1
        if group == 0:
            return MASK
        start, end = m.start(group) - m.start(0), m.end(group) - m.start(0)
        whole = m.group(0)
        return whole[:start] + MASK + whole[end:]

    @property
    def total(self) -> int:
        return sum(self.counts.values())


def redact_obj(obj, redactor: Redactor):
    """Applique le masquage récursivement à toutes les chaînes d'une structure JSON."""
    if isinstance(obj, str):
        return redactor.redact(obj)
    if isinstance(obj, list):
        return [redact_obj(x, redactor) for x in obj]
    if isinstance(obj, dict):
        return {k: redact_obj(v, redactor) for k, v in obj.items()}
    return obj
