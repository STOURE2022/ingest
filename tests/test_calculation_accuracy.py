#!/usr/bin/env python3
"""
Tests de Validation des Calculs - WAX Pipeline
==============================================

Ce fichier teste la précision et l'exactitude des calculs effectués par le pipeline.
On vérifie que les résultats (moyennes, spreads, transformations) sont corrects.

Usage:
    # Exécuter tous les tests de calculs
    pytest tests/test_calculation_accuracy.py -v

    # Exécuter un test spécifique
    pytest tests/test_calculation_accuracy.py::test_average_calculation -v
"""

import os
import sys
import pytest
import pandas as pd
from decimal import Decimal
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, count, sum as spark_sum

# Ajouter le répertoire src au path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from file_processor import apply_column_transformations
from utils import parse_tolerance


# ================================================================================
# FIXTURES
# ================================================================================

@pytest.fixture(scope="session")
def spark():
    """Fixture Spark session pour tous les tests"""
    spark = (
        SparkSession.builder
        .appName("WAX_Calculation_Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# ================================================================================
# TESTS: CALCULS DE BASE
# ================================================================================

class TestBasicCalculations:
    """Tests des calculs de base (sommes, moyennes, etc.)"""

    def test_sum_calculation(self, spark):
        """Test: Calcul de somme"""
        data = [(1,), (2,), (3,), (4,), (5,)]
        df = spark.createDataFrame(data, ["value"])

        result = df.agg(spark_sum("value")).collect()[0][0]
        expected = 15

        assert result == expected, f"Somme incorrecte: attendu {expected}, obtenu {result}"

    def test_average_calculation(self, spark):
        """Test: Calcul de moyenne"""
        data = [(10,), (20,), (30,), (40,), (50,)]
        df = spark.createDataFrame(data, ["value"])

        result = df.agg(avg("value")).collect()[0][0]
        expected = 30.0

        assert result == expected, f"Moyenne incorrecte: attendu {expected}, obtenu {result}"

    def test_average_with_nulls(self, spark):
        """Test: Calcul de moyenne avec des valeurs NULL"""
        data = [(10,), (20,), (None,), (40,), (50,)]
        df = spark.createDataFrame(data, ["value"])

        result = df.agg(avg("value")).collect()[0][0]
        expected = 30.0  # (10+20+40+50)/4 = 120/4 = 30

        assert result == expected, f"Moyenne avec NULL incorrecte: attendu {expected}, obtenu {result}"

    def test_count_calculation(self, spark):
        """Test: Calcul de comptage"""
        data = [(1,), (2,), (3,), (None,), (5,)]
        df = spark.createDataFrame(data, ["value"])

        total_count = df.count()
        non_null_count = df.filter(col("value").isNotNull()).count()

        assert total_count == 5, f"Comptage total incorrect: attendu 5, obtenu {total_count}"
        assert non_null_count == 4, f"Comptage non-null incorrect: attendu 4, obtenu {non_null_count}"


# ================================================================================
# TESTS: CALCULS STATISTIQUES
# ================================================================================

class TestStatisticalCalculations:
    """Tests des calculs statistiques avancés"""

    def test_stddev_calculation(self, spark):
        """Test: Calcul d'écart-type"""
        data = [(2,), (4,), (4,), (4,), (5,), (5,), (7,), (9,)]
        df = spark.createDataFrame(data, ["value"])

        result = df.agg(stddev("value")).collect()[0][0]
        # Écart-type de cette série: environ 2.138
        expected = 2.138

        assert abs(result - expected) < 0.01, f"Écart-type incorrect: attendu {expected}, obtenu {result}"

    def test_spread_calculation(self, spark):
        """Test: Calcul du spread (écart entre min et max)"""
        data = [(10,), (20,), (30,), (40,), (50,)]
        df = spark.createDataFrame(data, ["value"])

        min_val = df.agg({"value": "min"}).collect()[0][0]
        max_val = df.agg({"value": "max"}).collect()[0][0]
        spread = max_val - min_val

        expected_spread = 40  # 50 - 10 = 40

        assert spread == expected_spread, f"Spread incorrect: attendu {expected_spread}, obtenu {spread}"

    def test_percentile_calculation(self, spark):
        """Test: Calcul de percentiles"""
        data = [(i,) for i in range(1, 101)]  # 1 à 100
        df = spark.createDataFrame(data, ["value"])

        # Médiane (50e percentile)
        median = df.approxQuantile("value", [0.5], 0.01)[0]

        assert median == 50 or median == 51, f"Médiane incorrecte: attendu 50-51, obtenu {median}"


# ================================================================================
# TESTS: TRANSFORMATIONS DE DONNÉES
# ================================================================================

class TestDataTransformations:
    """Tests des transformations de données"""

    def test_uppercase_transformation(self, spark):
        """Test: Transformation en majuscules"""
        data = [("alice",), ("bob",), ("charlie",)]
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

        result = [row.name for row in df_transformed.collect()]
        expected = ["ALICE", "BOB", "CHARLIE"]

        assert result == expected, f"Transformation uppercase incorrecte: attendu {expected}, obtenu {result}"

    def test_lowercase_transformation(self, spark):
        """Test: Transformation en minuscules"""
        data = [("ALICE",), ("BOB",), ("CHARLIE",)]
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

        result = [row.name for row in df_transformed.collect()]
        expected = ["alice", "bob", "charlie"]

        assert result == expected, f"Transformation lowercase incorrecte: attendu {expected}, obtenu {result}"

    def test_trim_transformation(self, spark):
        """Test: Transformation trim (suppression espaces)"""
        data = [("  alice  ",), ("bob   ",), ("   charlie",)]
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

        result = [row.name for row in df_transformed.collect()]
        expected = ["alice", "bob", "charlie"]

        assert result == expected, f"Transformation trim incorrecte: attendu {expected}, obtenu {result}"


# ================================================================================
# TESTS: CALCULS DE TOLÉRANCE
# ================================================================================

class TestToleranceCalculations:
    """Tests des calculs de tolérance"""

    def test_percentage_tolerance(self):
        """Test: Calcul de tolérance en pourcentage"""
        result = parse_tolerance("10%", 100)
        assert result == 10, f"Tolérance 10% incorrecte: attendu 10, obtenu {result}"

        result = parse_tolerance("5%", 200)
        assert result == 10, f"Tolérance 5% incorrecte: attendu 10, obtenu {result}"

        result = parse_tolerance("0%", 100)
        assert result == 0, f"Tolérance 0% incorrecte: attendu 0, obtenu {result}"

    def test_absolute_tolerance(self):
        """Test: Calcul de tolérance absolue"""
        result = parse_tolerance("10", 100)
        assert result == 10, f"Tolérance absolue incorrecte: attendu 10, obtenu {result}"

        result = parse_tolerance("0", 100)
        assert result == 0, f"Tolérance 0 incorrecte: attendu 0, obtenu {result}"

    def test_tolerance_edge_cases(self):
        """Test: Cas limites de tolérance"""
        result = parse_tolerance("100%", 100)
        assert result == 100, f"Tolérance 100% incorrecte: attendu 100, obtenu {result}"

        result = parse_tolerance("0", 0)
        assert result == 0, f"Tolérance avec total=0 incorrecte: attendu 0, obtenu {result}"


# ================================================================================
# TESTS: AGRÉGATIONS COMPLEXES
# ================================================================================

class TestComplexAggregations:
    """Tests d'agrégations complexes"""

    def test_group_by_aggregation(self, spark):
        """Test: Agrégation avec GROUP BY"""
        data = [
            ("A", 10),
            ("A", 20),
            ("B", 30),
            ("B", 40),
            ("C", 50)
        ]
        df = spark.createDataFrame(data, ["group", "value"])

        result = df.groupBy("group").agg(
            spark_sum("value").alias("total"),
            avg("value").alias("average")
        ).orderBy("group").collect()

        # Vérifier les résultats
        assert result[0]["group"] == "A"
        assert result[0]["total"] == 30
        assert result[0]["average"] == 15.0

        assert result[1]["group"] == "B"
        assert result[1]["total"] == 70
        assert result[1]["average"] == 35.0

        assert result[2]["group"] == "C"
        assert result[2]["total"] == 50
        assert result[2]["average"] == 50.0

    def test_multiple_aggregations(self, spark):
        """Test: Multiples agrégations simultanées"""
        data = [(i,) for i in range(1, 11)]
        df = spark.createDataFrame(data, ["value"])

        result = df.agg(
            spark_sum("value").alias("total"),
            avg("value").alias("average"),
            count("value").alias("count")
        ).collect()[0]

        assert result["total"] == 55  # 1+2+...+10 = 55
        assert result["average"] == 5.5  # (1+10)/2 = 5.5
        assert result["count"] == 10

    def test_window_function_calculation(self, spark):
        """Test: Calculs avec fonctions de fenêtre"""
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number

        data = [
            ("A", 10),
            ("A", 20),
            ("B", 30),
            ("B", 40)
        ]
        df = spark.createDataFrame(data, ["group", "value"])

        window_spec = Window.partitionBy("group").orderBy("value")
        df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))

        result = df_with_row_num.collect()

        # Vérifier les numéros de ligne
        group_a = [r for r in result if r["group"] == "A"]
        assert group_a[0]["row_num"] == 1
        assert group_a[1]["row_num"] == 2


# ================================================================================
# TESTS: PRÉCISION DES DÉCIMALES
# ================================================================================

class TestDecimalPrecision:
    """Tests de précision des calculs décimaux"""

    def test_decimal_addition(self, spark):
        """Test: Addition de décimales"""
        data = [(0.1,), (0.2,)]
        df = spark.createDataFrame(data, ["value"])

        result = df.agg(spark_sum("value")).collect()[0][0]
        expected = 0.3

        # Tolérance pour les erreurs de précision flottante
        assert abs(result - expected) < 0.0001, f"Addition décimale incorrecte: attendu {expected}, obtenu {result}"

    def test_decimal_multiplication(self, spark):
        """Test: Multiplication de décimales"""
        from pyspark.sql.functions import lit

        data = [(1.5,), (2.0,), (2.5,)]
        df = spark.createDataFrame(data, ["value"])

        df_multiplied = df.withColumn("result", col("value") * lit(2))
        results = [row.result for row in df_multiplied.collect()]

        expected = [3.0, 4.0, 5.0]

        for i, (res, exp) in enumerate(zip(results, expected)):
            assert abs(res - exp) < 0.0001, f"Multiplication décimale incorrecte à l'index {i}: attendu {exp}, obtenu {res}"


# ================================================================================
# TESTS: DATES ET TIMESTAMPS
# ================================================================================

class TestDateCalculations:
    """Tests des calculs sur les dates"""

    def test_date_difference(self, spark):
        """Test: Calcul de différence entre dates"""
        from pyspark.sql.functions import datediff, to_date

        data = [
            ("2025-01-01", "2025-01-10"),
            ("2025-01-01", "2025-02-01")
        ]
        df = spark.createDataFrame(data, ["start_date", "end_date"])

        df_with_diff = df.withColumn(
            "diff",
            datediff(to_date("end_date"), to_date("start_date"))
        )

        results = [row.diff for row in df_with_diff.collect()]
        expected = [9, 31]  # 10-1=9 jours, 01/02-01/01=31 jours

        assert results == expected, f"Différence de dates incorrecte: attendu {expected}, obtenu {results}"

    def test_date_addition(self, spark):
        """Test: Addition de jours à une date"""
        from pyspark.sql.functions import date_add, to_date

        data = [("2025-01-01",)]
        df = spark.createDataFrame(data, ["date"])

        df_with_added = df.withColumn(
            "new_date",
            date_add(to_date("date"), 10)
        )

        result = df_with_added.collect()[0]["new_date"]
        expected = date(2025, 1, 11)

        assert result == expected, f"Addition de dates incorrecte: attendu {expected}, obtenu {result}"


# ================================================================================
# TESTS: VALIDATION DE FORMULES MÉTIER
# ================================================================================

class TestBusinessFormulas:
    """Tests des formules métier spécifiques"""

    def test_revenue_calculation(self, spark):
        """Test: Calcul de revenu (quantité × prix)"""
        data = [
            (10, 5.0),   # 50
            (20, 10.0),  # 200
            (5, 7.5)     # 37.5
        ]
        df = spark.createDataFrame(data, ["quantity", "price"])

        df_with_revenue = df.withColumn("revenue", col("quantity") * col("price"))

        results = [row.revenue for row in df_with_revenue.collect()]
        expected = [50.0, 200.0, 37.5]

        for i, (res, exp) in enumerate(zip(results, expected)):
            assert abs(res - exp) < 0.01, f"Calcul de revenu incorrect à l'index {i}: attendu {exp}, obtenu {res}"

    def test_percentage_calculation(self, spark):
        """Test: Calcul de pourcentages"""
        data = [
            (50, 200),   # 25%
            (75, 300),   # 25%
            (10, 100)    # 10%
        ]
        df = spark.createDataFrame(data, ["part", "total"])

        df_with_pct = df.withColumn("percentage", (col("part") / col("total")) * 100)

        results = [row.percentage for row in df_with_pct.collect()]
        expected = [25.0, 25.0, 10.0]

        for i, (res, exp) in enumerate(zip(results, expected)):
            assert abs(res - exp) < 0.01, f"Calcul de pourcentage incorrect à l'index {i}: attendu {exp}, obtenu {res}"

    def test_growth_rate_calculation(self, spark):
        """Test: Calcul de taux de croissance"""
        data = [
            (100, 150),  # +50%
            (200, 180),  # -10%
            (50, 75)     # +50%
        ]
        df = spark.createDataFrame(data, ["old_value", "new_value"])

        df_with_growth = df.withColumn(
            "growth_rate",
            ((col("new_value") - col("old_value")) / col("old_value")) * 100
        )

        results = [row.growth_rate for row in df_with_growth.collect()]
        expected = [50.0, -10.0, 50.0]

        for i, (res, exp) in enumerate(zip(results, expected)):
            assert abs(res - exp) < 0.01, f"Taux de croissance incorrect à l'index {i}: attendu {exp}, obtenu {res}"


# ================================================================================
# MAIN: EXÉCUTION DES TESTS
# ================================================================================

if __name__ == "__main__":
    """
    Exécution directe des tests

    Usage:
        python tests/test_calculation_accuracy.py
    """
    pytest.main([__file__, "-v", "--tb=short"])
