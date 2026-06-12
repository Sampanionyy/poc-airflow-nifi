#!/usr/bin/env python3
"""
Configure automatiquement le flow NiFi via l'API REST au démarrage.

Flow créé :
  GetFile (input/) → PutFile (nifi_processed/) → ExecuteScript (trigger Airflow)
"""

import json
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

NIFI = "https://nifi:8443/nifi-api"
NIFI_USER = "admin"
NIFI_PASS = "adminadminadmin"

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


def call(method, path, body=None, token=None, form=False):
    url = f"{NIFI}{path}"
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    data = None
    if body is not None:
        if form:
            data = urllib.parse.urlencode(body).encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        else:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            if not raw.strip():
                return {}
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return raw.strip()
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8")
        raise RuntimeError(f"HTTP {e.code} sur {method} {path} : {err}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"URLError sur {method} {path} : {e.reason}")
    except Exception as e:
        raise RuntimeError(f"{type(e).__name__} sur {method} {path} : {e}")


def wait_for_nifi():
    print("Attente du démarrage de NiFi (jusqu'à 6 min)...", flush=True)
    for i in range(72):
        try:
            # /access/config est public (pas d'auth requise)
            call("GET", "/access/config")
            print("NiFi est prêt.\n", flush=True)
            return
        except RuntimeError as e:
            # 401/403 = NiFi répond → il est prêt
            if "HTTP 401" in str(e) or "HTTP 403" in str(e):
                print("NiFi est prêt (authentification requise).\n", flush=True)
                return
            print(f"  [{i + 1}/72] Pas encore prêt : {e}", flush=True)
            time.sleep(5)
        except Exception as e:
            print(f"  [{i + 1}/72] Pas encore prêt : {e}", flush=True)
            time.sleep(5)
    sys.exit("ERREUR : NiFi n'a pas démarré dans les temps.")


def get_token():
    resp = call("POST", "/access/token",
                {"username": NIFI_USER, "password": NIFI_PASS}, form=True)
    return resp if isinstance(resp, str) else str(resp)


def get_root_pg(token):
    r = call("GET", "/flow/process-groups/root", token=token)
    return r["processGroupFlow"]["id"]


def already_configured(token, pg):
    procs = call("GET", f"/process-groups/{pg}/processors", token=token)
    if procs.get("processors"):
        print(f"Flow déjà configuré ({len(procs['processors'])} processor(s)). Rien à faire.",
              flush=True)
        return True
    return False


def mk_proc(token, pg, name, ptype, x, y, props, auto_term=None):
    body = {
        "revision": {"version": 0},
        "component": {
            "type": ptype,
            "name": name,
            "position": {"x": x, "y": y},
            "config": {
                "properties": props,
                "autoTerminatedRelationships": auto_term or [],
            },
        },
    }
    r = call("POST", f"/process-groups/{pg}/processors", body, token)
    pid = r["id"]
    ver = r["revision"]["version"]
    print(f"  [OK] {name} (id={pid[:8]}...)", flush=True)
    return pid, ver


def mk_conn(token, pg, src, dst, rels):
    body = {
        "revision": {"version": 0},
        "component": {
            "source":      {"id": src, "type": "PROCESSOR", "groupId": pg},
            "destination": {"id": dst, "type": "PROCESSOR", "groupId": pg},
            "selectedRelationships":         rels,
            "backPressureObjectThreshold":   10000,
            "backPressureDataSizeThreshold": "1 GB",
            "flowFileExpiration":            "0 sec",
        },
    }
    call("POST", f"/process-groups/{pg}/connections", body, token)
    print(f"  [CONN] {rels}", flush=True)


def start_proc(token, pid, ver):
    call("PUT", f"/processors/{pid}/run-status",
         {"revision": {"version": ver}, "state": "RUNNING"}, token)


def main():
    wait_for_nifi()
    time.sleep(5)  # marge de sécurité après le ready-check

    print("Authentification...", flush=True)
    token = get_token()

    print("Récupération du process group racine...", flush=True)
    pg = get_root_pg(token)
    print(f"Root PG : {pg}\n", flush=True)

    if already_configured(token, pg):
        return

    # ── Création des processors ─────────────────────────────────────────────

    print("Création des processors...", flush=True)

    # 1. GetFile — surveille input/ et déplace le fichier dans le pipeline
    getfile_id, getfile_v = mk_proc(
        token, pg,
        "1. Lire fichiers CSV (input/)",
        "org.apache.nifi.processors.standard.GetFile",
        100, 250,
        {
            "Input Directory":  "/opt/nifi/input",
            "File Filter":      ".*\\.csv",
            "Keep Source File": "false",       # déplace (ne copie pas) le fichier
            "Polling Interval": "10 sec",
            "Ignore Hidden Files": "true",
            "Batch Size": "10",
        },
    )

    # 2. PutFile — copie dans nifi_processed/ (accessible par Airflow)
    putfile_id, putfile_v = mk_proc(
        token, pg,
        "2. Déposer dans nifi_processed/",
        "org.apache.nifi.processors.standard.PutFile",
        500, 250,
        {
            "Directory": "/opt/nifi/output/nifi_processed",
            "Conflict Resolution Strategy": "replace",
        },
        auto_term=["failure"],
    )

    # 3. ExecuteScript — déclenche le DAG Airflow via l'API REST
    script_id, script_v = mk_proc(
        token, pg,
        "3. Déclencher DAG Airflow",
        "org.apache.nifi.processors.script.ExecuteScript",
        900, 250,
        {
            "Script Engine": "Groovy",
            "Script File":   "/opt/nifi/groovy-scripts/trigger_airflow.groovy",
        },
        auto_term=["success", "failure"],
    )

    # ── Connexions ───────────────────────────────────────────────────────────

    print("\nCréation des connexions...", flush=True)
    mk_conn(token, pg, getfile_id, putfile_id, ["success"])
    mk_conn(token, pg, putfile_id, script_id,  ["success"])

    # ── Démarrage ────────────────────────────────────────────────────────────

    print("\nDémarrage des processors...", flush=True)
    time.sleep(2)
    start_proc(token, getfile_id, getfile_v)
    start_proc(token, putfile_id, putfile_v)
    start_proc(token, script_id,  script_v)

    print("\nFlow NiFi configuré et démarré !", flush=True)
    print("Déposez un fichier .csv dans input/ pour déclencher le pipeline.", flush=True)


if __name__ == "__main__":
    main()
