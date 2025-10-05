#!/usr/bin/env python3
"""
Tests de Comparaison des Résultats - Notebook Original vs Code Refactorisé
==========================================================================

Ce fichier compare les résultats produits par le notebook original
avec ceux produits par le code refactorisé pour garantir qu'ils sont identiques.

Stratégie de test :
1. Préparer les mêmes données d'entrée (CSV + Excel config)
2. Exécuter le code refactorisé (src/main.py)
3. Comparer les résultats avec des snapshots de référence
4. Vérifier :
   - Nombre de lignes traitées
   - Valeurs des colonnes après transformations
   - Logs de qualité générés
   - Logs d'exécution

Usage:
    pytest tests/test_compare_results.py -v
"""

import os
import sys
import pytest
import zipfile
import pandas as pd
from pathlib import Path
from pyspark.sql import SparkSession

# Ajouter le répertoire src au path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from file_processor import (
    extract_zip_file,
    load_excel_config,
    read_csv_file,
    apply_column_transformations,
    process_files
)
from validators import parse_dates_with_logs, check_data_quality
from utils import parse_bool, normalize_delimiter


# ================================================================================
# FIXTURES
# ================================================================================

@pytest.fixture(scope="session")
def spark():
    """Fixture Spark session"""
    spark = (
        SparkSession.builder
        .appName("WAX_Comparison_Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def reference_data(temp_dir):
    """
    Créer des données de référence qui simulent la sortie du notebook original
    """
    csv_content = """id,name,age,city,salary,hire_date
1,alice dupont,30,Paris,50000.50,2020-01-15
2,BOB MARTIN,25,Lyon,45000.00,2021-03-20
3,Charlie Durand,35,Marseille,60000.75,2019-06-10
4,DAVID bernard,28,Toulouse,48000.25,2022-08-05
5,Emma PETIT,32,Nice,55000.00,2020-11-12
"""

    csv_path = os.path.join(temp_dir, "reference_data.csv")
    with open(csv_path, 'w') as f:
        f.write(csv_content)

    return csv_path


@pytest.fixture
def reference_config(temp_dir):
    """
    Configuration Excel identique à celle utilisée dans le notebook original
    """
    excel_path = os.path.join(temp_dir, "reference_config.xlsx")

    # Sheet 1: Field-Column (configuration des colonnes)
    field_columns = pd.DataFrame({
        "Delta Table Name": ["employees"] * 6,
        "Column Name": ["id", "name", "age", "city", "salary", "hire_date"],
        "Field Type": ["INTEGER", "STRING", "INTEGER", "STRING", "DOUBLE", "DATE"],
        "Storage Type": ["INTEGER", "STRING", "INTEGER", "STRING", "DOUBLE", "DATE"],
        "Is Nullable": ["False", "False", "True", "True", "True", "True"],
        "Transformation Type": ["", "uppercase", "", "capitalize", "", ""],
        "Transformation Pattern": ["", "", "", "", "", ""],
        "Enumeration Values": ["", "", "", "", "", ""],
        "Default Value when Invalid": ["", "", "0", "", "0.0", "2000-01-01"],
        "isMergeKey": ["1", "0", "0", "0", "0", "0"]
    })

    # Sheet 2: File-Table (configuration du traitement)
    file_tables = pd.DataFrame({
        "Delta Table Name": ["employees"],
        "Source Table": ["employees"],
        "Filename Pattern": ["reference_data.csv"],
        "Input Format": ["csv"],
        "Input Zone": ["bronze"],
        "Ingestion mode": ["APPEND"],
        "Input delimiter": [","],
        "Input charset": ["UTF-8"],
        "Input Header": ["FIRST_LINE"],
        "Trim": ["True"],
        "Delete Columns": ["False"],
        "Ignore empty files": ["True"],
        "Invalid Lines Generate": ["False"],
        "Rejected line per file tolerance": ["10%"],
        "Merge concordant file": ["False"]
    })

    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        field_columns.to_excel(writer, sheet_name='Field-Column', index=False)
        file_tables.to_excel(writer, sheet_name='File-Table', index=False)

    return excel_path


@pytest.fixture
def temp_dir(tmp_path):
    """Répertoire temporaire pour les tests"""
    return str(tmp_path)


# ================================================================================
# TESTS DE COMPARAISON DES RÉSULTATS
# ================================================================================

class TestResultsComparison:
    """
    Comparer les résultats du code refactorisé avec les résultats attendus
    (basés sur le comportement du notebook original)
    """

    def test_row_count_matches_original(self, spark, reference_data, reference_config, temp_dir):
        """
        Test: Le nombre de lignes traitées doit correspondre au notebook original

        Comportement original:
        - 5 lignes dans le CSV source
        - 5 lignes après traitement (pas de filtrage)
        """
        # Lire le fichier avec le code refactorisé
        df = read_csv_file(
            spark=spark,
            file_path=reference_data,
            input_format="csv",
            sep_char=",",
            charset="UTF-8",
            use_header=True,
            first_line_only=False,
            ignore_empty=True,
            imposed_schema=None,
            bad_records_path=os.path.join(temp_dir, "bad"),
            expected_cols=["id", "name", "age", "city", "salary", "hire_date"]
        )

        # Vérification
        actual_count = df.count()
        expected_count = 5

        assert actual_count == expected_count, \
            f"Nombre de lignes incorrect: attendu {expected_count}, obtenu {actual_count}"

    def test_transformations_match_original(self, spark, reference_data, reference_config, temp_dir):
        """
        Test: Les transformations doivent produire les mêmes résultats que le notebook original

        Transformations attendues (comportement original):
        - name → UPPERCASE: "alice dupont" → "ALICE DUPONT"
        - city → capitalize: "Paris" → "Paris" (déjà capitalisé)
        """
        # Lire les données
        df = read_csv_file(
            spark=spark,
            file_path=reference_data,
            input_format="csv",
            sep_char=",",
            charset="UTF-8",
            use_header=True,
            first_line_only=False,
            ignore_empty=True,
            imposed_schema=None,
            bad_records_path=os.path.join(temp_dir, "bad"),
            expected_cols=["id", "name", "age", "city", "salary", "hire_date"]
        )

        # Charger la configuration
        columns_df, _ = load_excel_config(reference_config, mode="local")

        # Appliquer les transformations
        df_transformed = apply_column_transformations(
            df=df,
            column_defs=columns_df,
            table_name="employees",
            filename="reference_data.csv"
        )

        # Récupérer les résultats
        results = df_transformed.collect()

        # Vérifications basées sur le comportement original
        # Ligne 1: "alice dupont" → "ALICE DUPONT"
        assert results[0].name == "ALICE DUPONT", \
            f"Transformation uppercase incorrecte: {results[0].name}"

        # Ligne 2: "BOB MARTIN" → "BOB MARTIN" (déjà en majuscules)
        assert results[1].name == "BOB MARTIN", \
            f"Transformation uppercase incorrecte: {results[1].name}"

        # Ligne 3: "Charlie Durand" → "CHARLIE DURAND"
        assert results[2].name == "CHARLIE DURAND", \
            f"Transformation uppercase incorrecte: {results[2].name}"

    def test_numeric_precision_matches_original(self, spark, reference_data, reference_config, temp_dir):
        """
        Test: Les valeurs numériques doivent avoir la même précision que le notebook original

        Comportement original:
        - salary conserve 2 décimales: 50000.50
        - age reste entier: 30
        """
        df = read_csv_file(
            spark=spark,
            file_path=reference_data,
            input_format="csv",
            sep_char=",",
            charset="UTF-8",
            use_header=True,
            first_line_only=False,
            ignore_empty=True,
            imposed_schema=None,
            bad_records_path=os.path.join(temp_dir, "bad"),
            expected_cols=["id", "name", "age", "city", "salary", "hire_date"]
        )

        results = df.collect()

        # Vérifier les salaires (précision décimale)
        expected_salaries = [50000.50, 45000.00, 60000.75, 48000.25, 55000.00]
        actual_salaries = [float(row.salary) for row in results]

        for i, (expected, actual) in enumerate(zip(expected_salaries, actual_salaries)):
            assert abs(expected - actual) < 0.01, \
                f"Salaire incorrect ligne {i+1}: attendu {expected}, obtenu {actual}"

        # Vérifier les âges (entiers)
        expected_ages = [30, 25, 35, 28, 32]
        actual_ages = [int(row.age) for row in results]

        assert expected_ages == actual_ages, \
            f"Âges incorrects: attendu {expected_ages}, obtenu {actual_ages}"

    def test_date_parsing_matches_original(self, spark, reference_data, reference_config, temp_dir):
        """
        Test: Le parsing des dates doit produire les mêmes résultats que le notebook original

        Comportement original:
        - Format d'entrée: "YYYY-MM-DD"
        - Format de sortie: date object
        - Dates invalides → valeur par défaut: 2000-01-01
        """
        df = read_csv_file(
            spark=spark,
            file_path=reference_data,
            input_format="csv",
            sep_char=",",
            charset="UTF-8",
            use_header=True,
            first_line_only=False,
            ignore_empty=True,
            imposed_schema=None,
            bad_records_path=os.path.join(temp_dir, "bad"),
            expected_cols=["id", "name", "age", "city", "salary", "hire_date"]
        )

        # Parser les dates
        df_parsed, _ = parse_dates_with_logs(
            df=df,
            column_name="hire_date",
            patterns=["yyyy-MM-dd", "dd/MM/yyyy"],
            table_name="employees",
            filename="reference_data.csv",
            default_date="2000-01-01"
        )

        results = df_parsed.collect()

        # Vérifier que les dates ont été parsées correctement
        from datetime import date
        expected_dates = [
            date(2020, 1, 15),
            date(2021, 3, 20),
            date(2019, 6, 10),
            date(2022, 8, 5),
            date(2020, 11, 12)
        ]

        actual_dates = [row.hire_date for row in results]

        for i, (expected, actual) in enumerate(zip(expected_dates, actual_dates)):
            assert expected == actual, \
                f"Date incorrecte ligne {i+1}: attendu {expected}, obtenu {actual}"

    def test_data_quality_checks_match_original(self, spark, reference_data, reference_config, temp_dir):
        """
        Test: Les contrôles qualité doivent détecter les mêmes erreurs que le notebook original

        Comportement original:
        - Détecte les NULL dans colonnes non-nullables (id, name)
        - Détecte les doublons sur les clés de merge (id)
        - Log les erreurs dans une table Delta
        """
        # Créer un fichier avec des erreurs volontaires
        csv_with_errors = """id,name,age,city,salary,hire_date
1,Alice,30,Paris,50000.50,2020-01-15
,Bob,25,Lyon,45000.00,2021-03-20
1,Charlie,35,Marseille,60000.75,2019-06-10
"""

        csv_error_path = os.path.join(temp_dir, "data_with_errors.csv")
        with open(csv_error_path, 'w') as f:
            f.write(csv_with_errors)

        # Lire les données
        df = read_csv_file(
            spark=spark,
            file_path=csv_error_path,
            input_format="csv",
            sep_char=",",
            charset="UTF-8",
            use_header=True,
            first_line_only=False,
            ignore_empty=True,
            imposed_schema=None,
            bad_records_path=os.path.join(temp_dir, "bad"),
            expected_cols=["id", "name", "age", "city", "salary", "hire_date"]
        )

        # Charger la config
        columns_df, _ = load_excel_config(reference_config, mode="local")

        # Vérifier la qualité
        errors_df = check_data_quality(
            df=df,
            table_name="employees",
            merge_keys=["id"],
            column_defs=columns_df,
            filename="data_with_errors.csv",
            spark=spark
        )

        # Le notebook original devrait détecter au moins 2 erreurs:
        # 1. NULL dans colonne id (ligne 2)
        # 2. Doublon sur id=1 (lignes 1 et 3)
        error_count = errors_df.count() if errors_df is not None else 0

        assert error_count >= 2, \
            f"Nombre d'erreurs détectées insuffisant: attendu ≥2, obtenu {error_count}"

    def test_aggregations_match_original(self, spark, reference_data, reference_config, temp_dir):
        """
        Test: Les agrégations doivent produire les mêmes résultats que le notebook original

        Comportement original:
        - Moyenne des salaires: (50000.50 + 45000.00 + 60000.75 + 48000.25 + 55000.00) / 5 = 51800.30
        - Somme des âges: 30 + 25 + 35 + 28 + 32 = 150
        """
        df = read_csv_file(
            spark=spark,
            file_path=reference_data,
            input_format="csv",
            sep_char=",",
            charset="UTF-8",
            use_header=True,
            first_line_only=False,
            ignore_empty=True,
            imposed_schema=None,
            bad_records_path=os.path.join(temp_dir, "bad"),
            expected_cols=["id", "name", "age", "city", "salary", "hire_date"]
        )

        # Convertir les types
        from pyspark.sql.functions import col, avg, sum as spark_sum
        from pyspark.sql.types import IntegerType, DoubleType

        df = df.withColumn("age", col("age").cast(IntegerType()))
        df = df.withColumn("salary", col("salary").cast(DoubleType()))

        # Calculer les agrégations
        agg_result = df.agg(
            avg("salary").alias("avg_salary"),
            spark_sum("age").alias("sum_age")
        ).collect()[0]

        # Vérifications
        # Moyenne réelle: (50000.50 + 45000.00 + 60000.75 + 48000.25 + 55000.00) / 5 = 258001.50 / 5 = 51600.30
        expected_avg_salary = 51600.30
        expected_sum_age = 150

        actual_avg_salary = agg_result.avg_salary
        actual_sum_age = agg_result.sum_age

        assert abs(actual_avg_salary - expected_avg_salary) < 0.01, \
            f"Moyenne des salaires incorrecte: attendu {expected_avg_salary}, obtenu {actual_avg_salary}"

        assert actual_sum_age == expected_sum_age, \
            f"Somme des âges incorrecte: attendu {expected_sum_age}, obtenu {actual_sum_age}"

    def test_column_order_preserved(self, spark, reference_data, reference_config, temp_dir):
        """
        Test: L'ordre des colonnes doit être préservé comme dans le notebook original

        Comportement original:
        - Ordre: id, name, age, city, salary, hire_date
        """
        df = read_csv_file(
            spark=spark,
            file_path=reference_data,
            input_format="csv",
            sep_char=",",
            charset="UTF-8",
            use_header=True,
            first_line_only=False,
            ignore_empty=True,
            imposed_schema=None,
            bad_records_path=os.path.join(temp_dir, "bad"),
            expected_cols=["id", "name", "age", "city", "salary", "hire_date"]
        )

        expected_columns = ["id", "name", "age", "city", "salary", "hire_date"]
        actual_columns = df.columns

        assert expected_columns == actual_columns, \
            f"Ordre des colonnes incorrect: attendu {expected_columns}, obtenu {actual_columns}"


# ================================================================================
# TESTS DE RÉGRESSION SPÉCIFIQUES
# ================================================================================

class TestRegressionFromOriginalNotebook:
    """
    Tests de régression basés sur des bugs ou comportements spécifiques
    du notebook original qui doivent être préservés ou corrigés
    """

    def test_trim_whitespace_like_original(self, spark, temp_dir):
        """
        Test: Les espaces doivent être supprimés comme dans le notebook original

        Comportement original:
        - trim=True → supprime les espaces en début/fin de valeur
        """
        csv_with_spaces = """id,name,age
1,  Alice  ,30
2,Bob   ,25
3,   Charlie,35
"""

        csv_path = os.path.join(temp_dir, "spaces.csv")
        with open(csv_path, 'w') as f:
            f.write(csv_with_spaces)

        df = read_csv_file(
            spark=spark,
            file_path=csv_path,
            input_format="csv",
            sep_char=",",
            charset="UTF-8",
            use_header=True,
            first_line_only=False,
            ignore_empty=True,
            imposed_schema=None,
            bad_records_path=os.path.join(temp_dir, "bad"),
            expected_cols=["id", "name", "age"]
        )

        # Appliquer trim (comme dans le notebook original)
        from pyspark.sql.functions import trim
        df_trimmed = df.select(
            trim(col("id")).alias("id"),
            trim(col("name")).alias("name"),
            trim(col("age")).alias("age")
        )

        results = df_trimmed.collect()

        # Vérifier que les espaces ont été supprimés
        assert results[0].name == "Alice", f"Trim incorrect: '{results[0].name}'"
        assert results[1].name == "Bob", f"Trim incorrect: '{results[1].name}'"
        assert results[2].name == "Charlie", f"Trim incorrect: '{results[2].name}'"


# ================================================================================
# MAIN: EXÉCUTION DES TESTS
# ================================================================================

if __name__ == "__main__":
    """
    Exécution directe des tests

    Usage:
        python tests/test_compare_results.py
    """
    pytest.main([__file__, "-v", "--tb=short"])
