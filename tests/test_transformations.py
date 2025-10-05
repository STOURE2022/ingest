#!/usr/bin/env python3
"""
Tests des Transformations de Colonnes - WAX Pipeline
====================================================

Ce fichier teste toutes les transformations possibles sur les colonnes.

Usage:
    # Exécuter tous les tests de transformations
    pytest tests/test_transformations.py -v

    # Exécuter un test spécifique
    pytest tests/test_transformations.py::test_uppercase_transformation -v
"""

import os
import sys
import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Ajouter le répertoire src au path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from file_processor import apply_column_transformations


# ================================================================================
# FIXTURES
# ================================================================================

@pytest.fixture(scope="session")
def spark():
    """Fixture Spark session pour tous les tests"""
    spark = (
        SparkSession.builder
        .appName("WAX_Transformations_Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# ================================================================================
# TESTS: TRANSFORMATIONS DE CASSE
# ================================================================================

class TestCaseTransformations:
    """Tests des transformations de casse (majuscules/minuscules)"""

    def test_uppercase_transformation(self, spark):
        """Test: Transformation en MAJUSCULES"""
        data = [
            ("alice",),
            ("bob",),
            ("Charlie",),
            ("DAVID",)
        ]
        df = spark.createDataFrame(data, ["name"])

        column_defs = pd.DataFrame({
            "Column Name": ["name"],
            "Transformation Type": ["uppercase"],
            "Transformation Pattern": [""]
        })

        df_transformed = apply_column_transformations(
            df=df,
            column_defs=column_defs,
            table_name="test",
            filename="test.csv"
        )

        results = [row.name for row in df_transformed.collect()]
        expected = ["ALICE", "BOB", "CHARLIE", "DAVID"]

        assert results == expected, f"Uppercase incorrecte: {results}"

    def test_lowercase_transformation(self, spark):
        """Test: Transformation en minuscules"""
        data = [
            ("ALICE",),
            ("Bob",),
            ("charlie",),
            ("DaViD",)
        ]
        df = spark.createDataFrame(data, ["name"])

        column_defs = pd.DataFrame({
            "Column Name": ["name"],
            "Transformation Type": ["lowercase"],
            "Transformation Pattern": [""]
        })

        df_transformed = apply_column_transformations(
            df=df,
            column_defs=column_defs,
            table_name="test",
            filename="test.csv"
        )

        results = [row.name for row in df_transformed.collect()]
        expected = ["alice", "bob", "charlie", "david"]

        assert results == expected, f"Lowercase incorrecte: {results}"

    def test_capitalize_transformation(self, spark):
        """Test: Transformation capitalisation (première lettre en majuscule)"""
        data = [
            ("alice",),
            ("BOB",),
            ("charlie",)
        ]
        df = spark.createDataFrame(data, ["name"])

        column_defs = pd.DataFrame({
            "Column Name": ["name"],
            "Transformation Type": ["capitalize"],
            "Transformation Pattern": [""]
        })

        df_transformed = apply_column_transformations(
            df=df,
            column_defs=column_defs,
            table_name="test",
            filename="test.csv"
        )

        results = [row.name for row in df_transformed.collect()]
        expected = ["Alice", "Bob", "Charlie"]

        assert results == expected, f"Capitalize incorrecte: {results}"


# ================================================================================
# TESTS: TRANSFORMATIONS D'ESPACES
# ================================================================================

class TestSpaceTransformations:
    """Tests des transformations d'espaces (trim, strip, etc.)"""

    def test_trim_transformation(self, spark):
        """Test: Suppression des espaces en début et fin"""
        data = [
            ("  alice  ",),
            ("bob   ",),
            ("   charlie",),
            ("david",)
        ]
        df = spark.createDataFrame(data, ["name"])

        column_defs = pd.DataFrame({
            "Column Name": ["name"],
            "Transformation Type": ["trim"],
            "Transformation Pattern": [""]
        })

        df_transformed = apply_column_transformations(
            df=df,
            column_defs=column_defs,
            table_name="test",
            filename="test.csv"
        )

        results = [row.name for row in df_transformed.collect()]
        expected = ["alice", "bob", "charlie", "david"]

        assert results == expected, f"Trim incorrecte: {results}"

    def test_ltrim_transformation(self, spark):
        """Test: Suppression des espaces à gauche"""
        data = [
            ("  alice",),
            ("   bob",),
            ("charlie  ",)
        ]
        df = spark.createDataFrame(data, ["name"])

        column_defs = pd.DataFrame({
            "Column Name": ["name"],
            "Transformation Type": ["ltrim"],
            "Transformation Pattern": [""]
        })

        df_transformed = apply_column_transformations(
            df=df,
            column_defs=column_defs,
            table_name="test",
            filename="test.csv"
        )

        results = [row.name for row in df_transformed.collect()]
        expected = ["alice", "bob", "charlie  "]

        assert results == expected, f"Ltrim incorrecte: {results}"

    def test_rtrim_transformation(self, spark):
        """Test: Suppression des espaces à droite"""
        data = [
            ("alice  ",),
            ("bob   ",),
            ("  charlie",)
        ]
        df = spark.createDataFrame(data, ["name"])

        column_defs = pd.DataFrame({
            "Column Name": ["name"],
            "Transformation Type": ["rtrim"],
            "Transformation Pattern": [""]
        })

        df_transformed = apply_column_transformations(
            df=df,
            column_defs=column_defs,
            table_name="test",
            filename="test.csv"
        )

        results = [row.name for row in df_transformed.collect()]
        expected = ["alice", "bob", "  charlie"]

        assert results == expected, f"Rtrim incorrecte: {results}"


# ================================================================================
# TESTS: TRANSFORMATIONS DE REGEX
# ================================================================================

class TestRegexTransformations:
    """Tests des transformations par expression régulière"""

    def test_regex_extract_transformation(self, spark):
        """Test: Extraction par regex"""
        data = [
            ("user123",),
            ("user456",),
            ("user789",)
        ]
        df = spark.createDataFrame(data, ["code"])

        column_defs = pd.DataFrame({
            "Column Name": ["code"],
            "Transformation Type": ["regex_extract"],
            "Transformation Pattern": [r"user(\d+)"]  # Extraire les chiffres
        })

        df_transformed = apply_column_transformations(
            df=df,
            column_defs=column_defs,
            table_name="test",
            filename="test.csv"
        )

        results = [row.code for row in df_transformed.collect()]
        expected = ["123", "456", "789"]

        assert results == expected, f"Regex extract incorrecte: {results}"

    def test_regex_replace_transformation(self, spark):
        """Test: Remplacement par regex"""
        data = [
            ("hello-world",),
            ("foo-bar",),
            ("test-data",)
        ]
        df = spark.createDataFrame(data, ["text"])

        column_defs = pd.DataFrame({
            "Column Name": ["text"],
            "Transformation Type": ["regex_replace"],
            "Transformation Pattern": ["-", "_"]  # Remplacer - par _
        })

        df_transformed = apply_column_transformations(
            df=df,
            column_defs=column_defs,
            table_name="test",
            filename="test.csv"
        )

        results = [row.text for row in df_transformed.collect()]
        expected = ["hello_world", "foo_bar", "test_data"]

        assert results == expected, f"Regex replace incorrecte: {results}"


# ================================================================================
# TESTS: TRANSFORMATIONS DE SUBSTRING
# ================================================================================

class TestSubstringTransformations:
    """Tests des transformations de sous-chaînes"""

    def test_substring_transformation(self, spark):
        """Test: Extraction de sous-chaîne"""
        data = [
            ("ABCDEFGH",),
            ("12345678",),
            ("XYZWVUTS",)
        ]
        df = spark.createDataFrame(data, ["code"])

        column_defs = pd.DataFrame({
            "Column Name": ["code"],
            "Transformation Type": ["substring"],
            "Transformation Pattern": ["1,4"]  # Position 1, longueur 4
        })

        df_transformed = apply_column_transformations(
            df=df,
            column_defs=column_defs,
            table_name="test",
            filename="test.csv"
        )

        results = [row.code for row in df_transformed.collect()]
        expected = ["ABCD", "1234", "XYZW"]

        assert results == expected, f"Substring incorrecte: {results}"

    def test_left_substring_transformation(self, spark):
        """Test: Extraction des N premiers caractères"""
        data = [
            ("ABCDEFGH",),
            ("12345678",)
        ]
        df = spark.createDataFrame(data, ["code"])

        column_defs = pd.DataFrame({
            "Column Name": ["code"],
            "Transformation Type": ["left"],
            "Transformation Pattern": ["3"]  # 3 premiers caractères
        })

        df_transformed = apply_column_transformations(
            df=df,
            column_defs=column_defs,
            table_name="test",
            filename="test.csv"
        )

        results = [row.code for row in df_transformed.collect()]
        expected = ["ABC", "123"]

        assert results == expected, f"Left incorrecte: {results}"

    def test_right_substring_transformation(self, spark):
        """Test: Extraction des N derniers caractères"""
        data = [
            ("ABCDEFGH",),
            ("12345678",)
        ]
        df = spark.createDataFrame(data, ["code"])

        column_defs = pd.DataFrame({
            "Column Name": ["code"],
            "Transformation Type": ["right"],
            "Transformation Pattern": ["3"]  # 3 derniers caractères
        })

        df_transformed = apply_column_transformations(
            df=df,
            column_defs=column_defs,
            table_name="test",
            filename="test.csv"
        )

        results = [row.code for row in df_transformed.collect()]
        expected = ["FGH", "678"]

        assert results == expected, f"Right incorrecte: {results}"


# ================================================================================
# TESTS: TRANSFORMATIONS DE PADDING
# ================================================================================

class TestPaddingTransformations:
    """Tests des transformations de padding (remplissage)"""

    def test_lpad_transformation(self, spark):
        """Test: Padding à gauche"""
        data = [
            ("1",),
            ("42",),
            ("123",)
        ]
        df = spark.createDataFrame(data, ["code"])

        column_defs = pd.DataFrame({
            "Column Name": ["code"],
            "Transformation Type": ["lpad"],
            "Transformation Pattern": ["5,0"]  # Longueur 5, caractère '0'
        })

        df_transformed = apply_column_transformations(
            df=df,
            column_defs=column_defs,
            table_name="test",
            filename="test.csv"
        )

        results = [row.code for row in df_transformed.collect()]
        expected = ["00001", "00042", "00123"]

        assert results == expected, f"Lpad incorrecte: {results}"

    def test_rpad_transformation(self, spark):
        """Test: Padding à droite"""
        data = [
            ("A",),
            ("BC",),
            ("DEF",)
        ]
        df = spark.createDataFrame(data, ["code"])

        column_defs = pd.DataFrame({
            "Column Name": ["code"],
            "Transformation Type": ["rpad"],
            "Transformation Pattern": ["5,X"]  # Longueur 5, caractère 'X'
        })

        df_transformed = apply_column_transformations(
            df=df,
            column_defs=column_defs,
            table_name="test",
            filename="test.csv"
        )

        results = [row.code for row in df_transformed.collect()]
        expected = ["AXXXX", "BCXXX", "DEFXX"]

        assert results == expected, f"Rpad incorrecte: {results}"


# ================================================================================
# TESTS: TRANSFORMATIONS DE CONCATÉNATION
# ================================================================================

class TestConcatenationTransformations:
    """Tests des transformations de concaténation"""

    def test_concat_transformation(self, spark):
        """Test: Concaténation de colonnes"""
        data = [
            ("John", "Doe"),
            ("Jane", "Smith"),
            ("Bob", "Johnson")
        ]
        df = spark.createDataFrame(data, ["first_name", "last_name"])

        # Ajouter une colonne full_name par concaténation
        from pyspark.sql.functions import concat, lit

        df_transformed = df.withColumn(
            "full_name",
            concat(col("first_name"), lit(" "), col("last_name"))
        )

        results = [row.full_name for row in df_transformed.collect()]
        expected = ["John Doe", "Jane Smith", "Bob Johnson"]

        assert results == expected, f"Concat incorrecte: {results}"


# ================================================================================
# TESTS: TRANSFORMATIONS NUMÉRIQUES
# ================================================================================

class TestNumericTransformations:
    """Tests des transformations numériques"""

    def test_abs_transformation(self, spark):
        """Test: Valeur absolue"""
        from pyspark.sql.functions import abs as spark_abs

        data = [
            (-10,),
            (20,),
            (-30,),
            (0,)
        ]
        df = spark.createDataFrame(data, ["value"])

        df_transformed = df.withColumn("abs_value", spark_abs(col("value")))

        results = [row.abs_value for row in df_transformed.collect()]
        expected = [10, 20, 30, 0]

        assert results == expected, f"Abs incorrecte: {results}"

    def test_round_transformation(self, spark):
        """Test: Arrondi"""
        from pyspark.sql.functions import round as spark_round

        data = [
            (3.14159,),
            (2.71828,),
            (1.61803,)
        ]
        df = spark.createDataFrame(data, ["value"])

        df_transformed = df.withColumn("rounded", spark_round(col("value"), 2))

        results = [row.rounded for row in df_transformed.collect()]
        expected = [3.14, 2.72, 1.62]

        for i, (res, exp) in enumerate(zip(results, expected)):
            assert abs(res - exp) < 0.01, f"Round incorrecte à l'index {i}: {res}"

    def test_ceil_transformation(self, spark):
        """Test: Arrondi supérieur"""
        from pyspark.sql.functions import ceil

        data = [
            (3.1,),
            (2.9,),
            (1.5,)
        ]
        df = spark.createDataFrame(data, ["value"])

        df_transformed = df.withColumn("ceiled", ceil(col("value")))

        results = [row.ceiled for row in df_transformed.collect()]
        expected = [4, 3, 2]

        assert results == expected, f"Ceil incorrecte: {results}"

    def test_floor_transformation(self, spark):
        """Test: Arrondi inférieur"""
        from pyspark.sql.functions import floor

        data = [
            (3.9,),
            (2.1,),
            (1.5,)
        ]
        df = spark.createDataFrame(data, ["value"])

        df_transformed = df.withColumn("floored", floor(col("value")))

        results = [row.floored for row in df_transformed.collect()]
        expected = [3, 2, 1]

        assert results == expected, f"Floor incorrecte: {results}"


# ================================================================================
# TESTS: TRANSFORMATIONS DE DATE
# ================================================================================

class TestDateTransformations:
    """Tests des transformations de dates"""

    def test_to_date_transformation(self, spark):
        """Test: Conversion en date"""
        from pyspark.sql.functions import to_date

        data = [
            ("2025-01-01",),
            ("2025-12-31",),
            ("2025-06-15",)
        ]
        df = spark.createDataFrame(data, ["date_str"])

        df_transformed = df.withColumn("date", to_date(col("date_str")))

        results = [row.date for row in df_transformed.collect()]

        from datetime import date
        expected = [date(2025, 1, 1), date(2025, 12, 31), date(2025, 6, 15)]

        assert results == expected, f"To_date incorrecte: {results}"

    def test_date_format_transformation(self, spark):
        """Test: Formatage de date"""
        from pyspark.sql.functions import to_date, date_format

        data = [
            ("2025-01-01",),
            ("2025-12-31",)
        ]
        df = spark.createDataFrame(data, ["date_str"])

        df_transformed = df.withColumn("date", to_date(col("date_str"))) \
            .withColumn("formatted", date_format(col("date"), "dd/MM/yyyy"))

        results = [row.formatted for row in df_transformed.collect()]
        expected = ["01/01/2025", "31/12/2025"]

        assert results == expected, f"Date_format incorrecte: {results}"


# ================================================================================
# TESTS: TRANSFORMATIONS AVEC VALEURS PAR DÉFAUT
# ================================================================================

class TestDefaultValueTransformations:
    """Tests des transformations avec valeurs par défaut"""

    def test_coalesce_transformation(self, spark):
        """Test: Remplacement des NULL par valeur par défaut"""
        from pyspark.sql.functions import coalesce, lit

        data = [
            ("Alice",),
            (None,),
            ("Charlie",),
            (None,)
        ]
        df = spark.createDataFrame(data, ["name"])

        df_transformed = df.withColumn(
            "name_filled",
            coalesce(col("name"), lit("Unknown"))
        )

        results = [row.name_filled for row in df_transformed.collect()]
        expected = ["Alice", "Unknown", "Charlie", "Unknown"]

        assert results == expected, f"Coalesce incorrecte: {results}"


# ================================================================================
# TESTS: TRANSFORMATIONS MULTIPLES
# ================================================================================

class TestMultipleTransformations:
    """Tests de transformations multiples sur plusieurs colonnes"""

    def test_multiple_transformations(self, spark):
        """Test: Plusieurs transformations simultanées"""
        data = [
            ("  alice  ", "BOB"),
            ("charlie   ", "DAVID"),
        ]
        df = spark.createDataFrame(data, ["name1", "name2"])

        column_defs = pd.DataFrame({
            "Column Name": ["name1", "name2"],
            "Transformation Type": ["trim", "lowercase"],
            "Transformation Pattern": ["", ""]
        })

        df_transformed = apply_column_transformations(
            df=df,
            column_defs=column_defs,
            table_name="test",
            filename="test.csv"
        )

        results = df_transformed.collect()

        assert results[0].name1 == "alice"
        assert results[0].name2 == "bob"
        assert results[1].name1 == "charlie"
        assert results[1].name2 == "david"


# ================================================================================
# MAIN: EXÉCUTION DES TESTS
# ================================================================================

if __name__ == "__main__":
    """
    Exécution directe des tests

    Usage:
        python tests/test_transformations.py
    """
    pytest.main([__file__, "-v", "--tb=short"])
