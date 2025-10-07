# WAX Pipeline - Data Ingestion Framework

Pipeline ETL pour ingestion CSV → Delta Lake

## 🚀 Installation

```bash
cd wax_pipeline
python -m venv venv
source venv/bin/activate  # Linux/Mac ou venv\Scripts\activate (Windows)
pip install -r requirements.txt
```

## ▶️ Exécution

```bash
# Placer vos fichiers dans data/input/
python src/main.py
```

## 📊 Modes d'ingestion

1. **FULL_SNAPSHOT** - Écrase la table
2. **DELTA_FROM_LOT** - Append simple  
3. **DELTA_FROM_NON_HISTORIZED** - Merge avec update
4. **FULL_KEY_REPLACE** - Delete + Insert par clé
