# Explication du projet — Scénario 2 : Plateforme d'orchestration centralisée

---

## Structure du projet

```
poc-airflow-nifi/
├── docker-compose.yml
├── dags/
│   └── demo_pipeline.py
├── nifi-init/
│   └── setup_flow.py
├── nifi-scripts/
│   └── trigger_airflow.groovy
├── prometheus/
│   └── prometheus.yml
├── statsd-exporter/
│   └── statsd_mapping.yml
├── grafana/
│   ├── dashboards/
│   │   └── poc_overview.json
│   └── provisioning/
│       ├── dashboards/provider.yml
│       └── datasources/prometheus.yml
└── output/
    ├── success/clients_valides.csv
    ├── error/clients_invalides.csv
    ├── nifi_processed/
    └── rapport_pipeline.json
```

---

## 1. `docker-compose.yml`

**Rôle général :**
C'est le fichier qui lance tous les services du projet en même temps avec `docker compose up`. Il décrit chaque conteneur, ses ports, ses variables d'environnement et ses volumes.

**Services déclarés :**
- `postgres` — la base de données interne d'Airflow (stocke les DAGs, les runs, les logs)
- `nifi` — le moteur d'ingestion qui surveille `input/` et route les fichiers
- `nifi-init` — un conteneur Python qui s'exécute une seule fois au démarrage pour configurer NiFi via son API REST, puis s'arrête
- `airflow` — l'orchestrateur qui exécute le DAG à chaque déclenchement reçu de NiFi
- `statsd-exporter` — le pont qui traduit les métriques Airflow (format StatsD) en format Prometheus
- `prometheus` — collecte et stocke toutes les métriques (Airflow + NiFi) toutes les 15 secondes
- `grafana` — affiche les métriques sous forme de dashboard visuel

**Points importants :**
- Les volumes partagés (`input/`, `output/`) permettent à NiFi et Airflow de lire/écrire les mêmes fichiers
- Le réseau `poc-network` permet aux conteneurs de se parler par nom (`airflow`, `nifi`, etc.)
- `depends_on` assure que postgres est prêt avant qu'Airflow démarre

---

## 2. `nifi-init/setup_flow.py`

**Rôle général :**
Il est le fichier général appelé pour interpreter les données et les envoyer par la suite à airflow pour qu'Airflow les traite avec son dag

**Ce qu'il fait au démarrage :**

### `wait_for_nifi()`
Attendre que le serveur de nifi démarre bien afin d'appeler par la suite les endpoints nécessaires 

### `get_token()`
Prendre le token pour les droits d'appels

### `get_root_pg()`
Pour travailler dans un processeur, il faut un pid (id du groupe racine du processeur)

### `already_configured()`
Vérifie si des processors existent déjà dans le groupe racine. Si oui, le script s'arrête sans rien faire — pour éviter de créer les processors en double si le conteneur redémarre.

### `mk_proc()`
Les 3 processors créés :
1. `GetFile` — qui prend le fichier dans input/
2. `PutFile` — qui place le fichier dans nifi_processed/
3. `ExecuteScript` — qui lance le fichier .groovy

### `mk_conn()`
Les 2 connexions créées :
- `GetFile → PutFile` — après avoir pris le fichier, on le place dans le bon dossier
- `PutFile → ExecuteScript` — bon dossier, on exécute le fichier pour qu'Airflow traite après

### `start_proc()`
Démarrer les processeurs

---

## 3. `nifi-scripts/trigger_airflow.groovy`

**Rôle général :**
Appel d'Airflow et démarrer le DAG

**Comment il appelle Airflow :**
- Endpoint appelé : `POST /api/v1/dags/demo_pipeline/dagRuns`
- Authentification : utilisation de Authorization en en-tête
- Données envoyées : les pwd de airflow définis dans le docker-compose

**Pourquoi Groovy et pas Python ?**
Groovy est natif à Nifi, même si Python aurait pu bien faire l'affaire

---

## 4. `dags/demo_pipeline.py`

**Rôle général :**
C'est le DAG Airflow qui traite les fichiers clients. Il est déclenché par NiFi via l'API REST, lit le fichier déposé, valide les données et génère un rapport.

**Pourquoi `schedule_interval=None` ?**
Le DAG ne doit pas tourner tout seul à heure fixe. C'est NiFi qui décide quand le lancer, à chaque fois qu'un fichier arrive. Sans NiFi, personne ne déclencherait le DAG.

**Les 4 tâches du DAG :**

### Tâche 1 — `check_nifi`
Appelle l'API REST de NiFi pour vérifier qu'il tourne bien avant de traiter. Si NiFi est inaccessible, ça log un warning mais le pipeline continue quand même.

### Tâche 2 — `ingest_clients`
- Lit depuis : `output/nifi_processed/` (nom du fichier transmis par NiFi dans le `conf` du DAG run), avec fallback sur `input/` si introuvable
- Pousse en XCom : le nombre total de clients (`key="total"`) et le chemin du fichier (`key="input_file"`) pour que les tâches suivantes puissent les utiliser

### Tâche 3 — `validate_clients`
Règles de validation appliquées :
| Champ | Règle |
|---|---|
| `email` | Doit contenir `@` et un `.` dans le domaine |
| `telephone` | Exactement 10 chiffres |
| `code_postal` | Exactement 5 chiffres |
| `nom` / `prenom` | Non vide |
| `statut` | Parmi : actif, inactif, prospect, archive |

- Clients valides → `output/success/`
- Clients invalides → `output/error/` avec colonne `erreurs` qui indique le(s) problème(s)

### Tâche 4 — `generate_report`
- Génère : un fichier JSON `output/rapport_pipeline.json` avec le total, le nombre de valides/invalides, le taux de validité et le déclencheur (`"nifi"`)
- Envoie les métriques vers : StatsD via `Stats.gauge()` → StatsD Exporter → Prometheus → Grafana

---

## 5. `prometheus/prometheus.yml`

**Rôle général :**
Indique à Prometheus quelles adresses aller interroger toutes les 15 secondes pour collecter les métriques.

**Les 3 sources scrapées :**
1. `prometheus` (lui-même) — pour surveiller la santé de Prometheus lui-même
2. `airflow-statsd` — le StatsD Exporter sur le port 9102, qui expose les métriques Airflow traduites
3. `nifi` — NiFi sur le port 9092 via le PrometheusReportingTask activé dans l'interface NiFi

**Pourquoi `fallback_scrape_protocol` pour NiFi ?**
NiFi ne renvoie pas de header `Content-Type` dans ses réponses. Les nouvelles versions de Prometheus rejettent ces réponses par défaut. Le `fallback_scrape_protocol: "PrometheusText0.0.4"` dit à Prometheus d'accepter quand même et de traiter la réponse comme du texte Prometheus standard.

---

## 6. `statsd-exporter/statsd_mapping.yml`

**Rôle général :**
Définit les règles de traduction entre les noms de métriques StatsD (envoyées par Airflow) et les noms de métriques Prometheus (lues par Grafana).

**Pourquoi un exporter est nécessaire ?**
Airflow envoie ses métriques en UDP au format StatsD (`airflow.scheduler.heartbeat:1|c`). Prometheus ne comprend pas ce format — il attend du HTTP avec un format texte précis. Le StatsD Exporter écoute les UDP d'Airflow et les expose en HTTP pour Prometheus.

**Les métriques mappées :**
- Métriques Airflow standard (scheduler, DAG runs, tâches) — durée des DAG runs, état des tâches, heartbeat du scheduler, tâches en cours
- Métriques métier custom (`clients_valides`, `clients_invalides`, `taux_validite`) — envoyées par `generate_report` via `Stats.gauge()`, visibles dans Grafana

---

## 7. `grafana/dashboards/poc_overview.json`

**Rôle général :**
Fichier JSON qui décrit entièrement le dashboard Grafana : disposition des panels, requêtes Prometheus, seuils de couleur. Il est chargé automatiquement au démarrage via le provisioning.

**Les panels du dashboard :**
- `Statut Airflow` — UP si Prometheus reçoit des métriques Airflow, DOWN sinon
- `Statut NiFi` — UP si le PrometheusReportingTask de NiFi répond sur le port 9092
- `Clients valides` — nombre de clients valides du dernier run
- `Clients invalides` — nombre de clients invalides (orange si > 10, rouge si > 50)
- `Taux de validité` — jauge en pourcentage (rouge < 70%, orange < 90%, vert ≥ 90%)
- `Valides vs Invalides (historique)` — graphique en courbe sur le temps pour voir l'évolution
- `Heartbeat Scheduler` — indique si le scheduler Airflow est vivant (envoie un signal toutes les quelques secondes)

---

## 8. `grafana/provisioning/`

### `datasources/prometheus.yml`
**Rôle :**
Dit à Grafana que sa source de données s'appelle "Prometheus" et qu'elle est à l'adresse `http://prometheus:9090`. C'est ce qui permet aux panels du dashboard d'interroger Prometheus.

### `dashboards/provider.yml`
**Rôle :**
Dit à Grafana d'aller chercher les fichiers JSON de dashboards dans le dossier `/var/lib/grafana/dashboards/` (qui contient `poc_overview.json` via le volume Docker).

**Pourquoi le provisioning ?**
Sans provisioning, il faudrait ouvrir Grafana dans le navigateur, ajouter Prometheus manuellement, puis importer le dashboard à la main — à refaire à chaque `docker compose down`. Avec ces fichiers, Grafana se configure tout seul au démarrage.

---

## 9. Fichiers de sortie (`output/`)

### `output/nifi_processed/`
Zone de transit entre NiFi et Airflow. NiFi y dépose le fichier CSV après l'avoir pris dans `input/`. Airflow vient le lire ici. C'est le seul point de contact entre les deux outils côté fichiers.

### `output/success/clients_valides.csv`
Les clients qui ont passé toutes les règles de validation. Mis à jour à chaque run.

### `output/error/clients_invalides.csv`
Les clients rejetés avec une colonne `erreurs` qui liste exactement pourquoi (ex: `email_invalide|telephone_invalide`). Permet de corriger les données à la source.

### `output/rapport_pipeline.json`
Synthèse du dernier run : date, total clients, nombre de valides/invalides, taux de validité en %, et le déclencheur (`"nifi"` ou `"manuel"`). Trace écrite de chaque exécution du pipeline.

---

## 10. Flux complet de bout en bout

```
1. [Fichier CSV déposé dans input/]
         ↓
2. [NiFi GetFile détecte le fichier]
         ↓
3. [NiFi PutFile copie dans nifi_processed/]
         ↓
4. [NiFi ExecuteScript appelle l'API Airflow]
         ↓
5. [Airflow lance le DAG demo_pipeline]
         ↓
6. [check_nifi → ingest_clients → validate_clients → generate_report]
         ↓
7. [Résultats dans output/success/ et output/error/]
         ↓
8. [Métriques envoyées à StatsD → Prometheus → Grafana]
```

À chaque étape, ce qui se passe concrètement :
- Étape 1 : on dépose manuellement un fichier CSV dans `input/` (simule une source métier comme Kelio ou Yooz)
- Étape 2 : NiFi scanne `input/` toutes les 10 secondes — il détecte le fichier et le prend dans son pipeline
- Étape 3 : NiFi copie le fichier dans `output/nifi_processed/` pour qu'Airflow puisse le lire
- Étape 4 : le script Groovy fait un appel HTTP POST vers l'API Airflow en lui passant le nom du fichier
- Étape 5 : Airflow reçoit la requête et crée un nouveau DAG run pour `demo_pipeline`
- Étape 6 : les 4 tâches s'enchaînent — vérification NiFi, lecture du fichier, validation ligne par ligne, génération du rapport
- Étape 7 : les clients valides et invalides sont écrits dans des fichiers CSV séparés dans `output/`
- Étape 8 : `generate_report` envoie les métriques en UDP → StatsD Exporter les traduit → Prometheus les stocke → Grafana les affiche
