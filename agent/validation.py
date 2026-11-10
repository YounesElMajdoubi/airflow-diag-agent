"""Vérifications déterministes des sorties du modèle (aucun LLM ici).

1. Citations : chaque ligne citée doit exister dans l'extrait de journal reçu
   ET son texte doit contenir l'extrait recopié. C'est ce qui rend l'étape
   « diagnostiquer en citant les lignes » vérifiable.
2. Diff : le diff proposé doit s'appliquer proprement au source du DAG que
   l'agent a reçu (masqué). On l'applique en mémoire, JAMAIS sur le vrai fichier.
"""

from __future__ import annotations

import re

from agent.schemas import Diagnostic


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip().casefold()


def check_citations(diag: Diagnostic, log_lines: dict[int, str]) -> list[str]:
    problems = []
    for c in diag.citations:
        if c.ligne not in log_lines:
            problems.append(f"la ligne {c.ligne} ne fait pas partie de l'extrait de journal fourni")
        elif _norm(c.extrait) not in _norm(log_lines[c.ligne]):
            problems.append(f"l'extrait cité pour la ligne {c.ligne} ne figure pas sur cette ligne "
                            f"(texte réel : {log_lines[c.ligne][:200]!r})")
    return problems


class DiffError(ValueError):
    pass


_HUNK = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")


def apply_unified_diff(source: str, diff: str) -> str:
    """Applique un diff unifié. Tolère un décalage de numéros de ligne (comme `patch`),
    mais exige que le contexte et les lignes supprimées correspondent exactement."""
    src = source.splitlines()
    lines = diff.strip("\n").splitlines()
    hunks: list[tuple[int, list[str]]] = []
    i = 0
    while i < len(lines):
        m = _HUNK.match(lines[i])
        if not m:
            i += 1  # en-têtes ---/+++ ou texte libre : ignorés
            continue
        start = int(m.group(1))
        body = []
        i += 1
        while i < len(lines) and not _HUNK.match(lines[i]):
            ln = lines[i]
            if ln.startswith(("---", "+++")) and not body:
                i += 1
                continue
            if ln.startswith("\\"):  # "\ No newline at end of file"
                i += 1
                continue
            if ln == "":
                ln = " "  # ligne de contexte vide dont l'espace initial a été perdu
            if ln[0] not in " +-":
                raise DiffError(f"ligne de diff invalide : {ln[:80]!r}")
            body.append(ln)
            i += 1
        hunks.append((start, body))
    if not hunks:
        raise DiffError("aucun bloc @@ trouvé dans le diff")

    out = list(src)
    offset = 0
    for start, body in hunks:
        old = [ln[1:] for ln in body if ln[0] in " -"]
        new = [ln[1:] for ln in body if ln[0] in " +"]
        if not any(ln[0] in "+-" for ln in body):
            raise DiffError("bloc sans aucune modification")
        pos = _find(out, old, start - 1 + offset)
        if pos is None:
            raise DiffError(f"le bloc @@ -{start} ne correspond pas au source (contexte introuvable)")
        out[pos:pos + len(old)] = new
        offset += len(new) - len(old)
    return "\n".join(out) + ("\n" if source.endswith("\n") else "")


def _find(haystack: list[str], needle: list[str], hint: int) -> int | None:
    if not needle:
        return max(0, min(hint, len(haystack)))
    n = len(needle)
    candidates = [p for p in range(len(haystack) - n + 1)
                  if [h.rstrip() for h in haystack[p:p + n]] == [x.rstrip() for x in needle]]
    if not candidates:
        return None
    return min(candidates, key=lambda p: abs(p - hint))


def check_diff(diff: str | None, source: str) -> tuple[bool | None, list[str]]:
    """(valide, problèmes). None si aucun diff n'est proposé."""
    if not diff or not diff.strip():
        return None, []
    try:
        apply_unified_diff(source, diff)
        return True, []
    except DiffError as e:
        return False, [f"le diff ne s'applique pas au source du DAG : {e}"]
