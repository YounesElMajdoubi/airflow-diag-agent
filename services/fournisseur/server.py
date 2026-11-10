"""API « fournisseur de taux de change » simulée (bibliothèque standard uniquement).

Endpoints métier :
    GET  /v1/rates?date=YYYY-MM-DD&api_key=...   -> {"date": ..., "rates": [{"currency", "rate"}, ...]}
    GET  /health

Endpoints d'administration (utilisés UNIQUEMENT par faults/inject.py pour provoquer des pannes) :
    GET  /admin/mode
    POST /admin/mode   {"mode": "...", "params": {...}}

Modes disponibles :
    normal         réponse correcte
    down           503 Service Unavailable
    bad_gateway    502 Bad Gateway
    reset          coupe la connexion sans répondre
    slow           attend params.seconds avant de répondre
    trickle        envoie la réponse octet par octet pendant params.seconds au total
    unauthorized   401 (clé refusée)
    forbidden      403 (endpoint non inclus dans l'abonnement)
    empty          {"rates": []}
    rename_field   le champ "rate" devient "value"
    dict_rates     "rates" devient un objet {"USD": 0.92, ...} au lieu d'une liste
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

API_KEY = os.environ.get("FOURNISSEUR_API_KEY", "")
RATES = {"EUR": 1.0, "USD": 0.92, "GBP": 1.17, "CHF": 1.05}
_state = {"mode": "normal", "params": {}}
_lock = threading.Lock()


def _payload(date: str, mode: str) -> dict:
    if mode == "empty":
        return {"date": date, "rates": []}
    if mode == "rename_field":
        return {"date": date, "rates": [{"currency": c, "value": r} for c, r in RATES.items()]}
    if mode == "dict_rates":
        return {"date": date, "rates": dict(RATES)}
    return {"date": date, "rates": [{"currency": c, "rate": r} for c, r in RATES.items()]}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # journal plus court
        print(f"[fournisseur] {self.address_string()} {fmt % args}", flush=True)

    def _send(self, status: int, body: dict | str, content_type="application/json"):
        data = (json.dumps(body) if isinstance(body, dict) else body).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == "/health":
            return self._send(200, {"status": "ok"})
        if url.path == "/admin/mode":
            with _lock:
                return self._send(200, dict(_state))
        if url.path != "/v1/rates":
            return self._send(404, {"error": "not found"})

        with _lock:
            mode, params = _state["mode"], dict(_state["params"])
        query = parse_qs(url.query)
        date = query.get("date", [""])[0]
        key = query.get("api_key", [""])[0]

        if mode == "reset":
            # close() seul ne suffit pas : rfile/wfile gardent la socket ouverte.
            # shutdown() coupe réellement la connexion ; le client reçoit une fermeture sans réponse.
            self.close_connection = True
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            return
        if mode == "down":
            return self._send(503, "<html><body><h1>503 Service Unavailable</h1>maintenance en cours</body></html>", "text/html")
        if mode == "bad_gateway":
            return self._send(502, "<html><body><h1>502 Bad Gateway</h1></body></html>", "text/html")
        if mode == "unauthorized" or (API_KEY and key != API_KEY):
            # Mauvaise pratique volontaire : le fournisseur renvoie la clé reçue dans le message d'erreur.
            return self._send(401, {"error": "invalid_api_key", "message": f"Clé API refusée : {key}"})
        if mode == "forbidden":
            return self._send(403, {"error": "forbidden", "message": "Votre abonnement n'inclut pas l'endpoint /v1/rates"})
        if mode == "slow":
            time.sleep(float(params.get("seconds", 30)))

        body = json.dumps(_payload(date, mode)).encode()
        if mode == "trickle":
            total = float(params.get("seconds", 90))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            pause = total / max(len(body), 1)
            try:
                for b in body:
                    self.wfile.write(bytes([b]))
                    self.wfile.flush()
                    time.sleep(pause)
            except (BrokenPipeError, ConnectionResetError):
                pass
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if urlparse(self.path).path != "/admin/mode":
            return self._send(404, {"error": "not found"})
        length = int(self.headers.get("Content-Length", 0))
        req = json.loads(self.rfile.read(length) or b"{}")
        with _lock:
            _state["mode"] = req.get("mode", "normal")
            _state["params"] = req.get("params", {})
            return self._send(200, dict(_state))


def main():
    port = int(os.environ.get("PORT", 8000))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.daemon_threads = True
    print(f"[fournisseur] écoute sur :{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
