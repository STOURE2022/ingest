#!/bin/bash
################################################################################
# Script d'exécution des tests de non-régression - WAX Pipeline
################################################################################

set -e

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  WAX Pipeline - Tests de Non-Régression${NC}"
echo -e "${BLUE}========================================${NC}\n"

# Fonction d'aide
show_help() {
    echo "Usage: ./run_tests.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help          Afficher l'aide"
    echo "  -a, --all           Exécuter tous les tests (défaut)"
    echo "  -f, --fast          Exécuter uniquement les tests rapides"
    echo "  -c, --coverage      Exécuter avec le rapport de couverture"
    echo "  -v, --verbose       Mode verbose"
    echo "  -s, --specific TEST Exécuter un test spécifique"
    echo ""
    echo "Exemples:"
    echo "  ./run_tests.sh                      # Tous les tests"
    echo "  ./run_tests.sh --fast               # Tests rapides uniquement"
    echo "  ./run_tests.sh --coverage           # Avec couverture"
    echo "  ./run_tests.sh -s TestConfig        # Classe de tests spécifique"
    echo ""
}

# Vérifier les dépendances
check_dependencies() {
    echo -e "${YELLOW}📦 Vérification des dépendances...${NC}"

    if ! command -v pytest &> /dev/null; then
        echo -e "${RED}❌ pytest n'est pas installé${NC}"
        echo -e "${YELLOW}Installation de pytest...${NC}"
        pip install pytest pytest-cov pytest-mock
    else
        echo -e "${GREEN}✅ pytest est installé${NC}"
    fi

    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}❌ Python 3 n'est pas installé${NC}"
        exit 1
    else
        echo -e "${GREEN}✅ Python 3 est installé${NC}"
    fi

    echo ""
}

# Exécuter tous les tests
run_all_tests() {
    echo -e "${BLUE}🧪 Exécution de tous les tests...${NC}\n"
    pytest tests/test_non_regression.py -v
}

# Exécuter les tests rapides
run_fast_tests() {
    echo -e "${BLUE}⚡ Exécution des tests rapides...${NC}\n"
    pytest tests/test_non_regression.py -v -m "not slow"
}

# Exécuter avec couverture
run_with_coverage() {
    echo -e "${BLUE}📊 Exécution avec rapport de couverture...${NC}\n"
    pytest tests/test_non_regression.py \
        --cov=src \
        --cov-report=html \
        --cov-report=term-missing \
        -v

    echo ""
    echo -e "${GREEN}✅ Rapport de couverture généré dans htmlcov/index.html${NC}"
}

# Exécuter un test spécifique
run_specific_test() {
    local test_name=$1
    echo -e "${BLUE}🎯 Exécution du test: ${test_name}${NC}\n"
    pytest tests/test_non_regression.py::${test_name} -v
}

# Exécuter en mode verbose
run_verbose() {
    echo -e "${BLUE}🔍 Exécution en mode verbose...${NC}\n"
    pytest tests/test_non_regression.py -vv -s
}

# Parser les arguments
MODE="all"
VERBOSE=false
SPECIFIC_TEST=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -a|--all)
            MODE="all"
            shift
            ;;
        -f|--fast)
            MODE="fast"
            shift
            ;;
        -c|--coverage)
            MODE="coverage"
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -s|--specific)
            MODE="specific"
            SPECIFIC_TEST="$2"
            shift 2
            ;;
        *)
            echo -e "${RED}❌ Option inconnue: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

# Vérifier les dépendances
check_dependencies

# Exécuter les tests selon le mode
case $MODE in
    all)
        if [ "$VERBOSE" = true ]; then
            run_verbose
        else
            run_all_tests
        fi
        ;;
    fast)
        run_fast_tests
        ;;
    coverage)
        run_with_coverage
        ;;
    specific)
        if [ -z "$SPECIFIC_TEST" ]; then
            echo -e "${RED}❌ Veuillez spécifier un test avec -s TEST_NAME${NC}"
            exit 1
        fi
        run_specific_test "$SPECIFIC_TEST"
        ;;
esac

# Afficher le résultat
if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  ✅ TESTS RÉUSSIS${NC}"
    echo -e "${GREEN}========================================${NC}"
    exit 0
else
    echo ""
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}  ❌ TESTS ÉCHOUÉS${NC}"
    echo -e "${RED}========================================${NC}"
    exit 1
fi
