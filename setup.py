"""
Setup configuration for WAX Pipeline
Build script pour créer le package wheel (.whl)
"""

from setuptools import setup, find_packages
import os

# Lire le README
def read_readme():
    readme_path = os.path.join(os.path.dirname(__file__), "README.md")
    if os.path.exists(readme_path):
        with open(readme_path, "r", encoding="utf-8") as f:
            return f.read()
    return "WAX Pipeline - Data Ingestion Framework"

# Lire les requirements
def read_requirements():
    req_path = os.path.join(os.path.dirname(__file__), "requirements.txt")
    if os.path.exists(req_path):
        with open(req_path, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip() and not line.startswith("#")]
    return []

setup(
    # Métadonnées du package
    name="wax-pipeline",
    version="1.0.0",
    author="WAX Team",
    author_email="wax-team@example.com",
    description="Pipeline ETL pour l'ingestion de données WAX dans Delta Lake",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/wax-pipeline",

    # Package configuration
    packages=find_packages(where=".", include=["src", "src.*"]),
    package_dir={"": "."},

    # Dépendances
    install_requires=read_requirements(),

    # Python version
    python_requires=">=3.8",

    # Classification
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],

    # Entry points
    entry_points={
        "console_scripts": [
            "wax-pipeline=src.main:main",
        ],
    },

    # Include additional files
    include_package_data=True,
    package_data={
        "": ["*.md", "*.txt"],
    },

    # Extras (optionnels)
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=3.0.0",
            "black>=22.0.0",
            "flake8>=4.0.0",
        ],
        "databricks": [
            "delta-spark>=2.0.0",
        ],
    },

    # Zip safe
    zip_safe=False,
)