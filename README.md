# POC — Plateforme d'orchestration centralisée (Scénario 2)

Architecture : **Apache NiFi** (ingestion/routage) + **Apache Airflow** (orchestration) + **Prometheus/Grafana** (supervision)

---

## Services

| Service           | URL                        | Identifiants       |
|-------------------|----------------------------|--------------------|
| Airflow UI        | http://localhost:8080       | admin / admin      |
| NiFi UI           | https://localhost:8443/nifi | admin / adminadminadmin |
| Grafana           | http://localhost:3000       | admin / admin      |
| Prometheus        | http://localhost:9090       | —                  |
| StatsD Exporter   | http://localhost:9102/metrics | —                |

---

## Lancer le POC

```bash
docker-compose up -d
```

Attendre ~2 minutes que tous les services démarrent, puis accéder aux URLs ci-dessus.

---

## Architecture des données

```
input/clients.csv (5 000 clients)
        │
        ▼
  [Apache NiFi]  ─── ingestion & routage des flux
        │
        ▼
  [Apache Airflow]
        ├── check_nifi         → vérifie la disponibilité NiFi
        ├── ingest_clients     → lit clients.csv
        ├── validate_clients   → règles métier (email, téléphone, code postal)
        └── generate_report    → rapport JSON de synthèse
               │
        ┌──────┴──────┐
        ▼             ▼
 output/success/  output/error/
 clients_valides  clients_invalides
```

---

## Règles de validation (DAG Airflow)

| Champ         | Règle                                        |
|---------------|----------------------------------------------|
| `email`       | Doit contenir `@` et un domaine valide       |
| `telephone`   | 10 chiffres exactement                       |
| `code_postal` | 5 chiffres exactement                        |
| `nom/prenom`  | Non vide                                     |
| `statut`      | Parmi : actif, inactif, prospect, archive    |

---

## Supervision

Les métriques Airflow sont envoyées via **StatsD → statsd-exporter → Prometheus → Grafana**.

Le dashboard **"POC Airflow-NiFi — Supervision"** est provisionné automatiquement dans Grafana.

### Activer les métriques NiFi (étape manuelle)

Dans l'interface NiFi (`https://localhost:8443/nifi`) :

1. Menu hamburger → **Controller Settings** → onglet **Reporting Tasks**
2. Ajouter une tâche : `PrometheusReportingTask`
3. Configurer le port : **9092**
4. Démarrer la tâche

Prometheus scrape automatiquement `nifi:9092/metrics` une fois activé.

---

## Structure du projet

```
poc-airflow-nifi/
├── docker-compose.yml
├── dags/
│   └── demo_pipeline.py          # DAG Airflow principal
├── input/
│   └── clients.csv               # 5 000 clients de test
├── output/
│   ├── success/                  # clients validés (généré par le DAG)
│   └── error/                    # clients rejetés (généré par le DAG)
├── prometheus/
│   └── prometheus.yml            # config scrape
├── statsd-exporter/
│   └── statsd_mapping.yml        # mapping métriques Airflow
└── grafana/
    ├── provisioning/
    │   ├── datasources/          # datasource Prometheus auto-provisionnée
    │   └── dashboards/           # provider de dashboards
    └── dashboards/
        └── poc_overview.json     # dashboard supervision
```

---

## Arrêter et nettoyer

```bash
# Arrêter les conteneurs
docker-compose down

# Supprimer aussi les volumes (base Airflow, données Prometheus/Grafana)
docker-compose down -v
```
