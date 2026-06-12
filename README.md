# POC — Plateforme d'orchestration centralisée (Scénario 2)

Architecture : **Apache NiFi** (ingestion/routage) + **Apache Airflow** (orchestration) + **Prometheus/Grafana** (supervision)

> Pour les explications détaillées du projet, voir [explication.md](explication.md).

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

## Activer les métriques NiFi (étape manuelle)

Dans l'interface NiFi (`https://localhost:8443/nifi`) :

1. Menu hamburger → **Controller Settings** → onglet **Reporting Tasks**
2. Ajouter une tâche : `PrometheusReportingTask`
3. Configurer le port : **9092**
4. Démarrer la tâche

Prometheus scrape automatiquement `nifi:9092/metrics` une fois activé.

---

## Arrêter et nettoyer

```bash
# Arrêter les conteneurs
docker-compose down

# Supprimer aussi les volumes (base Airflow, données Prometheus/Grafana)
docker-compose down -v
```
