# WAX Pipeline - ETL Data Ingestion

## 📋 Description du Projet

**WAX Pipeline** est un pipeline ETL modulaire et intelligent conçu pour ingérer des données CSV/Excel dans Delta Lake sur Databricks.

Le projet se distingue par sa **détection automatique d'environnement** : le même code fonctionne sans modification en local (pour les tests) et sur Databricks (en production). Il utilise l'architecture moderne **Databricks Asset Bundle** pour un déploiement standardisé et reproductible.

Le pipeline gère automatiquement :
- L'extraction de fichiers ZIP contenant des CSV
- La validation des données selon des règles définies dans Excel
- Les transformations (uppercase, lowercase, regex, dates, etc.)
- L'ingestion en mode FULL_SNAPSHOT, DELTA, ou MERGE
- La génération de logs de qualité et d'exécution dans Delta Lake

---

## 📂 Structure du Projet

```
wax_pipeline/
├── databricks.yml                    # Configuration principale du bundle
├── .databricks/project.json          # Profil de connexion Databricks
├── resources/                        # Configurations YAML des ressources
│   ├── wax_job.yml                  # Job ETL (cluster, schedule, retry)
│   └── wax_pipeline.yml             # Delta Live Tables pipeline
├── notebooks/                        # Notebooks Databricks
│   ├── wax_pipeline_main.py         # Point d'entrée principal
│   ├── wax_dlt_pipeline.py          # Pipeline DLT (Bronze/Silver/Gold)
│   └── notebook_original.py         # Notebook monolithique original
├── src/                              # Code source Python modulaire
│   ├── main.py                      # Point d'entrée
│   ├── config.py                    # Configuration auto-détectée
│   ├── file_processor.py            # Traitement des fichiers
│   ├── ingestion.py                 # Modes d'ingestion
│   ├── validators.py                # Validation de données
│   ├── delta_manager.py             # Gestion Delta Lake
│   ├── logging_manager.py           # Logs
│   └── utils.py                     # Fonctions utilitaires
├── data/
│   ├── input/                       # Fichiers d'entrée (ZIP + Excel)
│   └── output/                      # Résultats (logs + tables)
├── setup.py                          # Configuration du package Python
├── requirements.txt                  # Dépendances
└── build.sh                          # Script de build
```

---

## 🚀 Installation des dépendances Python

Installer PySpark, Pandas, openpyxl et les autres dépendances nécessaires au pipeline.

```bash
pip install -r requirements.txt
```

Vous devriez voir :
```
Successfully installed pyspark-3.3.0 pandas-2.0.0 openpyxl-3.1.2 delta-spark-2.3.0
```

---

## ☕ Installation de Java 17

PySpark a besoin de Java pour fonctionner. Java 17 est la version recommandée.

```bash
sudo apt-get update
sudo apt-get install openjdk-17-jdk-headless
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

Vérifier l'installation :
```bash
java -version
# Output: openjdk version "17.0.x"
```

---

## 🔧 Installation de Databricks CLI

La CLI Databricks permet de déployer et gérer les bundles sur Databricks directement depuis votre terminal.

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

Vérifier l'installation :
```bash
databricks --version
# Output: Databricks CLI v0.x.x
```

---

## 🔄 Comment le code s'adapte automatiquement ?

Le pipeline détecte automatiquement s'il s'exécute en **local** (tests) ou sur **Databricks** (production) grâce à 3 mécanismes.

**Détection de l'environnement** dans `src/config.py` :
```python
def is_databricks() -> bool:
    """Détecte si on est sur Databricks"""
    return 'DATABRICKS_RUNTIME_VERSION' in os.environ
```

**Session Spark** dans `src/main.py` :
```python
def initialize_spark():
    # Essaie de récupérer la session existante (Databricks)
    spark = SparkSession.getActiveSession()
    if spark is not None:
        return spark  # ✓ Databricks
    # Sinon, crée une nouvelle session (Local)
    return SparkSession.builder.appName("WAX").getOrCreate()
```

**Configuration** dans `src/config.py` :
```python
def get_config(dbutils=None):
    if dbutils is not None:
        return get_databricks_config(dbutils)  # Chemins DBFS
    else:
        return get_local_config()  # Chemins locaux
```

**Adaptations automatiques :**

| Composant | Local | Databricks |
|-----------|-------|------------|
| **Session Spark** | Créée | Existante récupérée |
| **Chemins fichiers** | `data/input/` | `dbfs:/FileStore/` |
| **Format tables** | Parquet | Delta Lake |
| **Format logs** | Parquet | Delta Lake |
| **Metastore** | Désactivé | Hive metastore |

Aucune modification de code nécessaire entre les environnements !

---

## 💻 Préparer les données d'entrée (local)

Le pipeline a besoin d'un fichier ZIP contenant les CSV et d'un fichier Excel de configuration. Placer vos fichiers dans le dossier `data/input/`.

```bash
ls data/input/
```

Vous devriez voir :
```
site_20251201_120001.zip
waxsite_config.xlsx
```

---

## ▶️ Exécuter le pipeline en local

Lancer le pipeline en mode local pour tester le traitement.

```bash
python3 src/main.py
```

Vous devriez voir :
```
💻 Mode: LOCAL détecté (TEST)
💻 Création d'une nouvelle session Spark (mode local)...
📥 CONFIGURATION CHARGÉE
  zip_path            : /path/to/data/input/site_20251201_120001.zip
  excel_path          : /path/to/data/input/waxsite_config.xlsx
  ...
📦 Extraction ZIP ...
📑 Lecture Excel : waxsite_config.xlsx
▶️ Table : wax_table
✔️ Delta saved (mode: append) -> data/output/wax_table_all
✅ WAX Processing Completed
```

---

## ✅ Vérifier les résultats (local)

S'assurer que les données ont été ingérées et que les logs sont générés.

```bash
ls data/output/
```

Vous devriez voir :
```
logs_execution/
logs_quality/
wax_table_all/       # Tables Parquet générées
wax_table_last/
```

---

## 🔐 Authentifier Databricks CLI

Connecter votre CLI au workspace Databricks pour pouvoir déployer. Remplacer `your-workspace` par votre URL workspace.

```bash
databricks auth login --host https://your-workspace.cloud.databricks.com
```

Vous devriez voir :
```
✓ Successfully logged in to Databricks
Profile: DEFAULT
```

---

## ✔️ Valider le bundle

Vérifier que la configuration YAML (databricks.yml, resources/) est correcte avant de déployer.

```bash
databricks bundle validate
```

Vous devriez voir :
```
✓ Configuration is valid
✓ All resources are correctly defined
```

---

## 🚀 Déployer en environnement DEV

Déployer le pipeline sur Databricks en mode développement pour tester. Les notebooks seront uploadés, le job créé avec cluster et schedule (2h du matin), et le pipeline DLT créé.

```bash
databricks bundle deploy -t dev
```

Vous devriez voir :
```
✓ Uploading notebooks to /Workspace/Users/you/.bundle/wax_pipeline/dev/files
✓ Creating job: WAX ETL Pipeline - dev
✓ Creating pipeline: WAX Delta Live Tables - dev
✓ Deployment complete
```

---

## 🎯 Exécuter le pipeline sur Databricks

Lancer manuellement le job pour tester l'exécution sur Databricks.

```bash
databricks bundle run wax_etl_pipeline -t dev
```

Vous devriez voir :
```
✓ Job run started (run_id: 12345)
✓ Job is running...
✓ Job completed successfully
Duration: 5m 23s
```

---

## 📊 Vérifier les logs d'exécution

Consulter les logs pour s'assurer que tout s'est bien passé. Remplacer `<job_id>` par l'ID de votre job.

```bash
databricks jobs list-runs --job-id <job_id> --limit 1
```

Vous devriez voir :
```json
{
  "run_id": 12345,
  "state": {
    "life_cycle_state": "TERMINATED",
    "result_state": "SUCCESS"
  },
  "duration": 323000
}
```

---

## 🗄️ Consulter les tables Delta créées

Vérifier que les données ont bien été ingérées dans Delta Lake. Exécuter dans un notebook Databricks :

```sql
SELECT * FROM delta.`/mnt/wax/dev/internal/v1/wax_table_last`
LIMIT 10;
```

Vous devriez voir une table avec vos données ingérées, incluant les colonnes `FILE_NAME_RECEIVED`, `FILE_PROCESS_DATE`, `yyyy`, `mm`, `dd`, et vos colonnes métier.

---

## 🏭 Déployer en PRODUCTION

Une fois testé en dev, déployer en production avec isolation et permissions configurées pour les groupes data_engineers (MANAGE) et data_analysts (RUN).

```bash
databricks bundle deploy -t prod
```

Vous devriez voir :
```
✓ Uploading to /Workspace/Shared/.bundle/wax_pipeline
✓ Job created with schedule (daily at 2 AM)
✓ Permissions applied: data_engineers (MANAGE), data_analysts (RUN)
✓ Production deployment complete
```

---

## 🔄 Mode d'ingestion : FULL_SNAPSHOT

Écrase complètement la table `_last` à chaque exécution. Utilisé pour des snapshots complets. La table `_last` contient toujours la dernière version complète du fichier.

Configurer dans le fichier Excel, colonne "Ingestion mode" :
```
FULL_SNAPSHOT
```

---

## 🔄 Mode d'ingestion : DELTA_FROM_LOT

Ajoute les nouvelles données sans écraser les anciennes (append). La table `_last` accumule toutes les données historiques.

Configurer dans le fichier Excel, colonne "Ingestion mode" :
```
DELTA_FROM_LOT
```

---

## 🔄 Mode d'ingestion : DELTA_FROM_NON_HISTORIZED

Fait un merge (upsert) basé sur les clés de merge définies dans Excel. Met à jour les lignes existantes, insère les nouvelles. La table `_last` contient toujours la version la plus récente de chaque enregistrement.

Configurer dans le fichier Excel, colonne "Ingestion mode" :
```
DELTA_FROM_NON_HISTORIZED
```

Définir les clés de merge dans Excel, colonne "isMergeKey" : mettre `1` pour les colonnes clés.

---

## 🔄 Mode d'ingestion : FULL_KEY_REPLACE

Supprime toutes les lignes correspondant aux clés de merge présentes dans le fichier, puis insère les nouvelles. Permet un remplacement complet par clés.

Configurer dans le fichier Excel, colonne "Ingestion mode" :
```
FULL_KEY_REPLACE
```

Définir les clés de merge dans Excel, colonne "isMergeKey" : mettre `1` pour les colonnes clés.

---

## 📝 Configuration Excel : Feuille "Field-Column"

Cette feuille définit les métadonnées de chaque colonne (type, validation, transformation). Voici les colonnes importantes :

- `Delta Table Name` : Nom de la table Delta
- `Column Name` : Nom de la colonne
- `Field type` : Type (STRING, INTEGER, DATE, TIMESTAMP, FLOAT, etc.)
- `Is Nullable` : TRUE si la colonne peut être NULL, FALSE sinon
- `Transformation Type` : uppercase, lowercase, regex, email, address, enumeration
- `Transformation Pattern` : Pattern regex pour les transformations
- `Enumeration Values` : Valeurs autorisées (séparées par virgule)
- `Default Value when Invalid` : Valeur par défaut si invalide
- `isMergeKey` : 1 si c'est une clé de merge, 0 sinon
- `isExtractValidity` : 1 si c'est la colonne de validité temporelle

---

## 📝 Configuration Excel : Feuille "File-Table"

Cette feuille définit la configuration de traitement de chaque fichier. Voici les colonnes importantes :

- `Delta Table Name` : Nom de la table cible
- `Source Table` : Nom de la table source
- `Filename Pattern` : Pattern du nom de fichier (ex: `wax_{yyyy}_{mm}_{dd}.csv`)
- `Input Format` : csv, excel, etc.
- `Ingestion mode` : FULL_SNAPSHOT, DELTA_FROM_LOT, DELTA_FROM_NON_HISTORIZED, FULL_KEY_REPLACE
- `Input delimiter` : Délimiteur (`;`, `,`, `|`, etc.)
- `Input Header` : HEADER_USE (utilise le header), FIRST_LINE (première ligne uniquement), ou vide
- `Input charset` : UTF-8, ANSI, Latin1
- `Trim` : TRUE pour nettoyer les espaces, FALSE sinon
- `Merge concordant file` : TRUE pour merger plusieurs fichiers, FALSE pour un seul
- `Rejected line per file tolerance` : Tolérance d'erreur (ex: "10%" ou "5")

---

## 📊 Consulter les jobs déployés

Lister tous les jobs WAX déployés sur Databricks avec leurs détails.

```bash
databricks jobs list --output json | jq '.jobs[] | select(.settings.name | contains("WAX"))'
```

Vous devriez voir :
```json
{
  "job_id": 123,
  "settings": {
    "name": "WAX ETL Pipeline - dev",
    "schedule": {
      "quartz_cron_expression": "0 0 2 * * ?",
      "timezone_id": "Europe/Paris"
    }
  }
}
```

---

## 📊 Consulter les dernières exécutions

Afficher les 10 dernières exécutions d'un job avec leur statut, durée, et timestamp.

```bash
databricks jobs list-runs --job-id 123 --limit 10
```

Vous devriez voir une liste des exécutions avec leur statut (SUCCESS, FAILED), durée en millisecondes, et timestamp.

---

## 📊 Consulter les logs de qualité

Consulter les erreurs de qualité détectées (NULL_KEY, DUPLICATE_KEY, INVALID_DATE, etc.) dans un notebook Databricks.

```sql
SELECT table_name, filename, column_name, error_message, error_count, log_ts
FROM delta.`/mnt/logs/wax_data_quality_errors`
ORDER BY log_ts DESC
LIMIT 50;
```

Vous devriez voir une table avec les erreurs détectées lors du traitement des fichiers.

---

## 📊 Consulter les logs d'exécution

Consulter l'historique des exécutions avec statut, durée, et nombre d'erreurs dans un notebook Databricks.

```sql
SELECT tableName, fileName, ingestionMode, status, errorCount, duration, ts
FROM delta.`/mnt/logs/wax_execution_logs_delta`
ORDER BY ts DESC
LIMIT 50;
```

Vous devriez voir l'historique complet des exécutions du pipeline avec les métriques de performance.

---

## 🔧 Personnaliser les variables par environnement

Définir des variables spécifiques pour dev vs prod (log level, retention, etc.) dans le fichier `databricks.yml`.

```yaml
targets:
  prod:
    variables:
      log_level: "ERROR"
      retention_days: 90
```

Utiliser dans un notebook :
```python
log_level = spark.conf.get("bundle.log_level", "INFO")
```

---

## 🔐 Utiliser des Secrets Databricks

Stocker des informations sensibles (tokens, mots de passe) dans Databricks Secrets au lieu de les mettre en clair dans `resources/wax_job.yml`.

```yaml
tasks:
  - task_key: wax_ingestion
    notebook_task:
      base_parameters:
        jfrog_token: "{{secrets/wax_pipeline/jfrog_token}}"
```

Créer le secret :
```bash
databricks secrets create-scope wax_pipeline
databricks secrets put-secret wax_pipeline jfrog_token
```

---

## 🧪 Tests et Validation

Le projet dispose d'une suite complète de tests pour garantir la qualité et la non-régression.

### Structure des Tests

```
tests/
├── __init__.py
├── test_non_regression.py         # Tests de non-régression (~45 tests)
├── test_calculation_accuracy.py   # Tests de validation des calculs (~25 tests)
├── test_transformations.py        # Tests des transformations (~30 tests)
└── test_compare_results.py        # Comparaison notebook original vs refactorisé (~10 tests)

Total: ~110 tests avec ~80% de couverture
```

### Installation des dépendances de test

```bash
pip install pytest pytest-cov
```

### Exécution rapide des tests

```bash
# Tous les tests
pytest tests/ -v

# Tests rapides uniquement (sans les tests de performance)
pytest tests/ -v -m "not slow"

# Avec rapport de couverture
pytest tests/ --cov=src --cov-report=html

# Un test spécifique
pytest tests/test_non_regression.py::TestConfig::test_get_local_config -v
```

### Types de tests disponibles

#### 1. Tests de Non-Régression (`test_non_regression.py`)

Vérifient que les fonctionnalités existantes continuent de fonctionner :

- ✅ Configuration (local/databricks)
- ✅ Traitement de fichiers (ZIP, CSV, Excel)
- ✅ Validation de données (types, dates, qualité)
- ✅ Modes d'ingestion (APPEND, MERGE, OVERWRITE)
- ✅ Gestion des cas limites (fichiers vides, caractères spéciaux, gros fichiers)
- ✅ Performance (temps de traitement < 30s pour 50K lignes)

**Exemple :**
```bash
# Tous les tests de non-régression
pytest tests/test_non_regression.py -v

# Tests de configuration uniquement
pytest tests/test_non_regression.py::TestConfig -v
```

#### 2. Tests de Validation des Calculs (`test_calculation_accuracy.py`)

Vérifient que les calculs sont exacts :

- ✅ Calculs de base (somme, moyenne, comptage)
- ✅ Statistiques (écart-type, spread, percentiles)
- ✅ Tolérance (%, absolue)
- ✅ Précision décimale
- ✅ Calculs de dates
- ✅ Formules métier (revenu, pourcentage, croissance)

**Exemples de calculs testés :**
```python
# Somme : [1,2,3,4,5] → 15
# Moyenne : [10,20,30,40,50] → 30.0
# Spread : [10,20,30,40,50] → 40 (max - min)
# Tolérance : 10% de 100 → 10
```

**Exemple :**
```bash
# Tous les tests de calculs
pytest tests/test_calculation_accuracy.py -v

# Tests statistiques uniquement
pytest tests/test_calculation_accuracy.py::TestStatisticalCalculations -v
```

#### 3. Tests des Transformations (`test_transformations.py`)

Vérifient toutes les transformations de colonnes :

| Type | Input | Transformation | Output |
|------|-------|----------------|--------|
| Uppercase | `"alice"` | `uppercase` | `"ALICE"` |
| Lowercase | `"BOB"` | `lowercase` | `"bob"` |
| Trim | `"  alice  "` | `trim` | `"alice"` |
| Regex extract | `"user123"` | `user(\d+)` | `"123"` |
| Substring | `"ABCDEFGH"` | `1,4` | `"ABCD"` |
| Lpad | `"1"` | `5,0` | `"00001"` |
| Round | `3.14159` | `2 decimals` | `3.14` |
| Date format | `2025-01-01` | `dd/MM/yyyy` | `"01/01/2025"` |

**Exemple :**
```bash
# Tous les tests de transformations
pytest tests/test_transformations.py -v

# Tests de casse uniquement
pytest tests/test_transformations.py::TestCaseTransformations -v
```

#### 4. Tests de Comparaison avec le Notebook Original (`test_compare_results.py`)

**Vérifient que le code refactorisé produit exactement les mêmes résultats que le notebook original** :

- ✅ Même nombre de lignes traitées
- ✅ Mêmes transformations appliquées (uppercase, capitalize, etc.)
- ✅ Même précision numérique (décimales conservées)
- ✅ Même parsing de dates
- ✅ Mêmes contrôles qualité détectés
- ✅ Mêmes agrégations (moyennes, sommes)
- ✅ Ordre des colonnes préservé

**Exemples de comparaisons :**
```python
# Transformation: "alice dupont" → "ALICE DUPONT" (comme le notebook original)
# Précision: 50000.50 reste 50000.50 (pas d'arrondi)
# Moyenne salaires: 51800.30 (exactement comme l'original)
```

**Exemple :**
```bash
# Tous les tests de comparaison
pytest tests/test_compare_results.py -v

# Test de comparaison des transformations
pytest tests/test_compare_results.py::TestResultsComparison::test_transformations_match_original -v
```

### Workflow recommandé

#### Avant chaque commit
```bash
# Tests rapides
pytest tests/ -v -m "not slow"

# Si OK, commit
git add .
git commit -m "Feature: nouvelle fonctionnalité"
```

#### Avant chaque merge
```bash
# Tests complets + couverture
pytest tests/ --cov=src --cov-report=html

# Vérifier > 80% de couverture
open htmlcov/index.html
```

### Résolution de problèmes

**Problème : Version Python différente pour Spark**
```
PySparkRuntimeError: [PYTHON_VERSION_MISMATCH]
```

**Solution :**
```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
pytest tests/
```

**Problème : pytest non trouvé**
```bash
pip install pytest pytest-cov pytest-mock
```

**Problème : Java non trouvé**
```bash
sudo apt-get install openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

---

## 🛠️ Workflow CI/CD : Développement local

Créer une branche, modifier le code dans `src/` ou `notebooks/`, et tester en local.

```bash
git checkout -b feature/new-transformation
# Modifier le code
python3 src/main.py
```

---

## 🛠️ Workflow CI/CD : Validation

Valider le bundle avant de déployer.

```bash
databricks bundle validate
```

---

## 🛠️ Workflow CI/CD : Déploiement en dev

Déployer en dev et exécuter le job pour tester.

```bash
databricks bundle deploy -t dev
databricks bundle run wax_etl_pipeline -t dev
```

---

## 🛠️ Workflow CI/CD : Tests et validation

Vérifier les résultats de l'exécution et consulter les tables Delta créées.

```bash
databricks jobs list-runs --job-id <job_id>
```

---

## 🛠️ Workflow CI/CD : Merge et production

Merger la branche dans main et déployer en production.

```bash
git checkout main
git merge feature/new-transformation
databricks bundle deploy -t prod
```

---

## 🐛 Erreur : Java non trouvé

Si vous voyez `JAVA_HOME is not set`, exporter la variable d'environnement et l'ajouter au fichier bashrc.

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
```

---

## 🐛 Erreur : Module pyspark non trouvé

Si vous voyez `ModuleNotFoundError: No module named 'pyspark'`, installer pyspark.

```bash
pip install pyspark
```

---

## 🐛 Erreur : databricks CLI non trouvé

Si vous voyez `command not found: databricks`, installer la CLI et relancer le terminal.

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
# Relancer le terminal
```

---

## ✅ Checklist première fois sur Databricks

- [ ] Installer Databricks CLI
- [ ] S'authentifier : `databricks auth login`
- [ ] Adapter `databricks.yml` (workspace URL)
- [ ] Adapter `resources/wax_job.yml` (cluster, schedule, emails)
- [ ] Uploader les fichiers source sur DBFS : `dbfs:/FileStore/tables/`
- [ ] Valider : `databricks bundle validate`
- [ ] Déployer en dev : `databricks bundle deploy -t dev`
- [ ] Tester : `databricks bundle run wax_etl_pipeline -t dev`
- [ ] Vérifier les résultats (tables Delta, logs)
- [ ] Déployer en prod : `databricks bundle deploy -t prod`
- [ ] Configurer les permissions (groupes data_engineers, data_analysts)

