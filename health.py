"""Scheduled endpoint probes with debounced incident tracking."""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timezone
import http.client
import json
from pathlib import Path
import socket
import sqlite3
import time


SCHEMA = """
CREATE TABLE IF NOT EXISTS probes(id INTEGER PRIMARY KEY, service TEXT NOT NULL, checked_at TEXT NOT NULL, ok INTEGER NOT NULL, latency_ms REAL NOT NULL, detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS incidents(id INTEGER PRIMARY KEY, service TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS one_open_incident ON incidents(service) WHERE ended_at IS NULL;
CREATE INDEX IF NOT EXISTS probes_by_service ON probes(service,id DESC);
"""


def probe(item: dict) -> tuple[bool, float, str]:
    kind, host, port = item["type"], item["host"], int(item["port"])
    timeout = float(item.get("timeout", 2))
    if not 0 < timeout <= 60 or not 0 < port < 65536 or kind not in {"http", "redis", "tcp"}:
        raise ValueError("Invalid probe configuration")
    started = time.perf_counter()
    try:
        if kind == "http":
            connection = http.client.HTTPConnection(host, port, timeout=timeout)
            try:
                connection.request("GET", item.get("path", "/"))
                response = connection.getresponse()
                response.read()
                ok = 200 <= response.status < 400
                detail = f"HTTP {response.status}"
            finally:
                connection.close()
        else:
            with socket.create_connection((host, port), timeout=timeout) as stream:
                stream.settimeout(timeout)
                if kind == "redis":
                    stream.sendall(b"*1\r\n$4\r\nPING\r\n")
                    reply = stream.recv(64)
                    ok, detail = reply.startswith(b"+PONG\r\n"), "Redis PONG" if reply.startswith(b"+PONG\r\n") else "Unexpected Redis response"
                else:
                    ok, detail = True, "TCP connected"
    except (OSError, http.client.HTTPException) as exc:
        ok, detail = False, type(exc).__name__ + ": " + str(exc)
    return ok, (time.perf_counter() - started) * 1000, detail


def record(db: sqlite3.Connection, service: str, ok: bool, latency_ms: float, detail: str, timestamp: str) -> str | None:
    with db:
        db.execute("INSERT INTO probes(service,checked_at,ok,latency_ms,detail) VALUES(?,?,?,?,?)", (service, timestamp, int(ok), latency_ms, detail))
        open_id = db.execute("SELECT id FROM incidents WHERE service=? AND ended_at IS NULL", (service,)).fetchone()
        if ok and open_id:
            db.execute("UPDATE incidents SET ended_at=? WHERE id=?", (timestamp, open_id[0]))
            return "recovered"
        if not ok and not open_id:
            failures = db.execute("SELECT ok FROM probes WHERE service=? ORDER BY id DESC LIMIT 3", (service,)).fetchall()
            if len(failures) == 3 and all(not row[0] for row in failures):
                db.execute("INSERT INTO incidents(service,started_at) VALUES(?,?)", (service, timestamp))
                return "incident opened"
    return None


def run_once(db: sqlite3.Connection, services: list[dict]) -> list[dict]:
    results = []
    for item in services:
        ok, latency, detail = probe(item)
        stamp = datetime.now(timezone.utc).isoformat()
        event = record(db, item["name"], ok, latency, detail, stamp)
        results.append({"service": item["name"], "ok": ok, "latency_ms": round(latency, 2), "detail": detail, "event": event})
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--interval", type=float, default=10)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error("interval must be positive")
    config = json.loads(args.config.read_text(encoding="utf-8"))
    services = config["services"]
    names = [x["name"] for x in services]
    if len(set(names)) != len(names) or not names:
        parser.error("services need distinct names")
    with closing(sqlite3.connect(args.database)) as db:
        db.executescript(SCHEMA)
        try:
            while True:
                print(json.dumps(run_once(db, services)))
                if args.once:
                    break
                time.sleep(args.interval)
        except KeyboardInterrupt:
            pass

