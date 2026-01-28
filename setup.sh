#!/bin/bash

# Kenya Health Data Pipeline - Setup Script
# This script initializes the complete data pipeline environment

set -e  # Exit on error

echo "=========================================="
echo "Kenya Health Data Pipeline Setup"
echo "=========================================="
echo ""

# Color codes for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Check if Docker is running
echo "Checking Docker status..."
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker Desktop and try again."
    exit 1
fi
print_status "Docker is running"

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null 2>&1; then
    print_error "docker-compose or 'docker compose' command not found"
    exit 1
fi
print_status "Docker Compose is available"

# Determine docker compose command
if docker compose version &> /dev/null 2>&1; then
    DOCKER_COMPOSE="docker compose"
else
    DOCKER_COMPOSE="docker-compose"
fi

echo ""
echo "Step 1: Stopping any existing containers..."
$DOCKER_COMPOSE down -v 2>/dev/null || true
print_status "Cleaned up existing containers"

echo ""
echo "Step 2: Building and starting services..."
echo "This may take a few minutes on first run..."
$DOCKER_COMPOSE up -d

echo ""
echo "Step 3: Waiting for services to be healthy..."

# Wait for Postgres
echo -n "Waiting for PostgreSQL"
for i in {1..30}; do
    if docker exec kenya_health_postgres pg_isready -U user > /dev/null 2>&1; then
        echo ""
        print_status "PostgreSQL is ready"
        break
    fi
    echo -n "."
    sleep 2
done

# Wait for Airflow
echo -n "Waiting for Airflow"
for i in {1..60}; do
    if curl -f http://localhost:8080/health > /dev/null 2>&1; then
        echo ""
        print_status "Airflow is ready"
        break
    fi
    echo -n "."
    sleep 3
done

# Wait for Metabase
echo -n "Waiting for Metabase"
for i in {1..30}; do
    if curl -f http://localhost:3000/api/health > /dev/null 2>&1; then
        echo ""
        print_status "Metabase is ready"
        break
    fi
    echo -n "."
    sleep 2
done

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Access your services:"
echo ""
echo "  📊 Airflow:  http://localhost:8080"
echo "     Username: airflow"
echo "     Password: airflow"
echo ""
echo "  📈 Metabase: http://localhost:3000"
echo "     (Complete setup wizard on first access)"
echo ""
echo "  🗄️  PostgreSQL: localhost:5432"
echo "     Database: kenya_health_db"
echo "     Username: user"
echo "     Password: pass"
echo ""
echo "Next steps:"
echo "  1. Open Airflow UI and trigger the 'kenya_health_etl_pipeline' DAG"
echo "  2. Wait for the pipeline to complete (check DAG status)"
echo "  3. Set up Metabase connection to Postgres"
echo "  4. Create dashboards using the transformed data"
echo ""
echo "Logs: $DOCKER_COMPOSE logs -f [service_name]"
echo "Stop:  $DOCKER_COMPOSE down"
echo ""
print_status "Ready to run the pipeline!"