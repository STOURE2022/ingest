#!/bin/bash
# WAX Pipeline - Build Script
# Crée le package wheel (.whl) pour déploiement

set -e  # Exit on error

echo "========================================="
echo "WAX Pipeline - Build Script"
echo "========================================="

# Couleurs pour l'output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Fonction d'aide
function show_help() {
    echo "Usage: ./build.sh [OPTION]"
    echo ""
    echo "Options:"
    echo "  (aucune)    Builder le package wheel"
    echo "  clean       Nettoyer les builds précédents"
    echo "  test        Builder et tester localement"
    echo "  help        Afficher cette aide"
    echo ""
}

# Fonction de nettoyage
function clean_build() {
    echo -e "${YELLOW}🧹 Nettoyage des builds précédents...${NC}"

    rm -rf build/
    rm -rf dist/
    rm -rf *.egg-info
    rm -rf src/*.egg-info

    # Nettoyer les __pycache__
    find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
    find . -type f -name "*.pyc" -delete 2>/dev/null || true

    echo -e "${GREEN}✓ Nettoyage terminé${NC}"
}

# Fonction de build
function build_package() {
    echo -e "${YELLOW}📦 Build du package...${NC}"

    # Vérifier que setup.py existe
    if [ ! -f "setup.py" ]; then
        echo -e "${RED}❌ Erreur: setup.py non trouvé${NC}"
        exit 1
    fi

    # Installer/mettre à jour les outils de build
    echo "   Installation des outils de build..."
    pip install --upgrade pip setuptools wheel build -q

    # Builder le package
    echo "   Construction du wheel..."
    python -m build --wheel

    # Vérifier que le build a réussi
    if [ -d "dist" ] && [ "$(ls -A dist/*.whl 2>/dev/null)" ]; then
        echo -e "${GREEN}✓ Build réussi${NC}"
        echo ""
        echo "📦 Package créé:"
        ls -lh dist/*.whl
        echo ""
    else
        echo -e "${RED}❌ Erreur: Build échoué${NC}"
        exit 1
    fi
}

# Fonction de test
function test_package() {
    echo -e "${YELLOW}🧪 Test du package...${NC}"

    # Trouver le fichier .whl
    WHL_FILE=$(ls dist/*.whl 2>/dev/null | head -n 1)

    if [ -z "$WHL_FILE" ]; then
        echo -e "${RED}❌ Erreur: Aucun fichier .whl trouvé${NC}"
        exit 1
    fi

    echo "   Installation du package: $WHL_FILE"
    pip install "$WHL_FILE" --force-reinstall -q

    echo "   Vérification de l'installation..."
    if pip show wax-pipeline >/dev/null 2>&1; then
        echo -e "${GREEN}✓ Package installé avec succès${NC}"
        echo ""
        echo "ℹ️  Pour tester:"
        echo "   python -c 'from src.main import main; main()'"
        echo ""
    else
        echo -e "${RED}❌ Erreur: Installation échouée${NC}"
        exit 1
    fi
}

# Main
case "$1" in
    clean)
        clean_build
        ;;
    test)
        clean_build
        build_package
        test_package
        ;;
    help|--help|-h)
        show_help
        ;;
    "")
        clean_build
        build_package
        echo -e "${GREEN}=========================================${NC}"
        echo -e "${GREEN}✅ Build terminé avec succès${NC}"
        echo -e "${GREEN}=========================================${NC}"
        echo ""
        echo "📤 Prochaines étapes:"
        echo "   1. Tester localement:"
        echo "      ./build.sh test"
        echo ""
        echo "   2. Uploader sur JFrog:"
        echo "      curl -u user:password -T dist/*.whl https://jfrog.example.com/..."
        echo ""
        echo "   3. Installer sur Databricks:"
        echo "      %pip install /dbfs/jfrog/wax_pipeline-1.0.0-py3-none-any.whl"
        echo ""
        ;;
    *)
        echo -e "${RED}❌ Option invalide: $1${NC}"
        echo ""
        show_help
        exit 1
        ;;
esac