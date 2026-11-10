"""Mise en forme et troncature des journaux de tâches Airflow 3.

Airflow 3 renvoie des journaux STRUCTURÉS : une liste de messages JSON
({"timestamp", "level", "logger", "event", ...}). Une exception n'est pas un
texte de traceback mais un champ "error_detail" (type, message, liste de frames).

Ce module :
1. transforme ces messages en lignes de texte lisibles, numérotées dans l'ordre
   d'origine (le numéro ne change jamais, même après troncature) ;
2. reconstruit un traceback Python lisible, en ajoutant la ligne de code du DAG
   pour chaque frame située dans le fichier du DAG ;
3. ne garde que les « lignes utiles » : le traceback, une fenêtre de N lignes
   avant chaque erreur, les avertissements, et la fin du journal.

Les numéros de ligne d'origine permettent de vérifier les citations de l'agent.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Clés techniques répétées sur chaque message : inutiles pour le diagnostic.
_HIDDEN_KEYS = {
    "timestamp", "event", "level", "logger", "filename", "lineno", "error_detail", "ti_id",
    "task_id", "dag_id", "map_index", "run_id", "try_number", "chan", "loc",
}
_ERROR_PAT = re.compile(
    r"Traceback|Error\b|Exception\b|FATAL|Timeout|timed out|denied|refused|"
    r"not permitted|Unauthorized|Forbidden|No such file|does not exist|failed",
    re.I,
)


@dataclass
class LogLine:
    n: int          # numéro de ligne d'origine (1 = première ligne du journal)
    text: str
    level: str      # info / warning / error / ... ("" si inconnu)
    is_trace: bool = False

    def as_dict(self) -> dict:
        return {"n": self.n, "texte": self.text}


def _short_logger(name: str) -> str:
    # Airflow charge les DAG sous un nom de module « unusual_prefix_<hash>_<fichier> »
    m = re.match(r"unusual_prefix_[0-9a-f]+_(.+)", name or "")
    if m:
        return m.group(1)
    return (name or "").rsplit(".", 1)[-1]  # airflow.x.y.DagBundlesManager -> DagBundlesManager


def _fmt_time(ts: str | None) -> str:
    if not ts or "T" not in ts:
        return ""
    return ts.split("T", 1)[1][:12]  # HH:MM:SS.mmm


def _render_traceback(details: list[dict], dag_source: list[str] | None) -> list[str]:
    """Rend error_detail comme un traceback Python, causes d'abord (comme Python)."""
    out: list[str] = []
    chain = list(reversed(details)) if len(details) > 1 else details
    for i, exc in enumerate(chain):
        if i > 0:
            out.append("The above exception was the direct cause of the following exception:")
        out.append("Traceback (most recent call last):")
        hidden = 0
        for fr in exc.get("frames", []):
            fname = fr.get("filename", "")
            if "/site-packages/airflow/" in fname:
                hidden += 1  # mécanique interne d'Airflow : on la résume
                continue
            if hidden:
                out.append(f"  ... ({hidden} frames internes d'Airflow masquées)")
                hidden = 0
            out.append(f'  File "{fname}", line {fr.get("lineno")}, in {fr.get("name")}')
            lineno = fr.get("lineno") or 0
            # Frame dans le fichier du DAG : on affiche la ligne de code concernée.
            if dag_source and "/dags/" in fname and 0 < lineno <= len(dag_source):
                out.append(f"    {dag_source[lineno - 1].strip()}")
        if hidden:
            out.append(f"  ... ({hidden} frames internes d'Airflow masquées)")
        value = exc.get("exc_value") or ""
        exc_type = exc.get("exc_type", "Exception")
        out.append(f"{exc_type}: {value}" if value else exc_type)
        for note in exc.get("exc_notes") or []:
            out.append(f"  note: {note}")
    return out


def render(content: list, dag_source: list[str] | None = None) -> list[LogLine]:
    """Messages structurés (ou lignes texte) -> lignes numérotées.

    dag_source : lignes du source du DAG (version exécutée), pour annoter le traceback.
    """
    lines: list[LogLine] = []
    skip_group = False

    def add(text: str, level: str, is_trace: bool = False):
        for part in str(text).splitlines() or [""]:
            lines.append(LogLine(len(lines) + 1, part, level, is_trace))

    for msg in content:
        if isinstance(msg, str):  # ancien format texte
            for part in msg.splitlines():
                low = part.lower()
                lvl = "error" if " error " in low or "traceback" in low else ""
                add(part, lvl)
            continue
        event = str(msg.get("event", ""))
        if event.startswith("::group::Log message source details"):
            skip_group = True
            continue
        if event == "::endgroup::":
            skip_group = False
            continue
        if skip_group:
            continue
        if event.startswith("::group::"):
            add(f"===== {event[len('::group::'):]} =====", "info")
            continue
        level = str(msg.get("level", "")).lower()
        logger = _short_logger(msg.get("logger", ""))
        loc = ""
        if msg.get("filename") and str(msg.get("logger", "")).startswith("unusual_prefix_"):
            loc = f" ({msg['filename']}:{msg.get('lineno')})"
        extras = " ".join(f"{k}={v}" for k, v in msg.items() if k not in _HIDDEN_KEYS and v not in (None, "", [], {}))
        head = " ".join(p for p in (_fmt_time(msg.get("timestamp")), level.upper() or None, f"[{logger}]" if logger else None) if p)
        add(f"{head}{loc} {event}{(' ' + extras) if extras else ''}".strip(), level)
        if msg.get("error_detail"):
            for t in _render_traceback(msg["error_detail"], dag_source):
                add(t, "error", is_trace=True)
    return lines


def select_useful(lines: list[LogLine], before: int = 15, after: int = 5,
                  tail: int = 3, max_lines: int = 150) -> tuple[list[LogLine], list[list[int]]]:
    """Garde les lignes utiles. Renvoie (lignes gardées, plages omises [début, fin])."""
    if not lines:
        return [], []
    total = len(lines)
    anchors = [i for i, ln in enumerate(lines)
               if ln.is_trace or ln.level in ("error", "critical", "fatal") or _ERROR_PAT.search(ln.text)]
    keep: set[int] = set(range(max(0, total - tail), total))
    keep |= {i for i, ln in enumerate(lines) if ln.level == "warning"}
    if not anchors:
        keep |= set(range(max(0, total - max_lines), total))
    for a in anchors:
        keep |= set(range(max(0, a - before), min(total, a + after + 1)))

    ordered = sorted(keep)
    if len(ordered) > max_lines:
        # On privilégie la fin : la dernière erreur est presque toujours la cause de l'échec.
        must = {i for i in ordered if lines[i].is_trace}
        rest = [i for i in ordered if i not in must]
        budget = max(0, max_lines - len(must))
        ordered = sorted(must | set(rest[-budget:] if budget else []))

    kept = [lines[i] for i in ordered]
    omitted, prev = [], 0
    for ln in kept:
        if ln.n > prev + 1:
            omitted.append([prev + 1, ln.n - 1])
        prev = ln.n
    if prev < total:
        omitted.append([prev + 1, total])
    return kept, omitted
