#!/bin/bash

################################################################################
# Dataset Pipeline API Server Runner
#
# This script handles setup and deployment for both serverless and VPS environments.
#
# Usage:
#   ./run.sh setup              # First-time setup (install dependencies)
#   ./run.sh dev                # Run in development mode
#   ./run.sh prod               # Run in production mode
#   ./run.sh docker             # Run with Docker
#   ./run.sh test               # Run API tests
#   ./run.sh pipeline           # Run pipeline directly (no API)
#   ./run.sh stop               # Stop running server
################################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Default environment variables
export API_HOST="${API_HOST:-0.0.0.0}"
export API_PORT="${API_PORT:-8000}"
export API_WORKERS="${API_WORKERS:-4}"
export DEBUG="${DEBUG:-false}"

# Detect Python executable
if command -v python3 &> /dev/null; then
    PYTHON="python3"
elif command -v python &> /dev/null; then
    PYTHON="python"
else
    log_error "Python not found. Please install Python 3.8+"
    exit 1
fi

# Check Python version
PYTHON_VERSION=$($PYTHON --version 2>&1 | awk '{print $2}')
PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

if [ "$PYTHON_MAJOR" -lt 3 ] || ([ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 8 ]); then
    log_error "Python 3.8+ required. Found: $PYTHON_VERSION"
    exit 1
fi

log_info "Using Python $PYTHON_VERSION"

################################################################################
# SETUP
################################################################################

setup() {
    log_info "Setting up environment..."
    
    # Check if virtual environment exists
    if [ ! -d "venv" ]; then
        log_info "Creating virtual environment..."
        $PYTHON -m venv venv
        log_success "Virtual environment created"
    else
        log_info "Virtual environment already exists"
    fi
    
    # Activate virtual environment
    log_info "Activating virtual environment..."
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
        source venv/Scripts/activate
    else
        source venv/bin/activate
    fi
    
    # Upgrade pip
    log_info "Upgrading pip..."
    $PYTHON -m pip install --upgrade pip --quiet
    
    # Install requirements
    log_info "Installing dependencies..."
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt --quiet
        log_success "Dependencies installed"
    else
        log_error "requirements.txt not found"
        exit 1
    fi
    
    # Create necessary directories
    log_info "Creating directories..."
    mkdir -p logs
    mkdir -p data/raw
    mkdir -p data/processed
    mkdir -p data/intermediate
    mkdir -p data_pipeline/configs
    
    # Copy .env.example if .env doesn't exist
    if [ ! -f ".env" ] && [ -f ".env.example" ]; then
        log_warning ".env not found, creating from .env.example"
        cp .env.example .env
        log_info "Please edit .env file with your configuration"
    fi
    
    log_success "Setup complete!"
    log_info "Next steps:"
    log_info "  1. Edit .env file with your configuration"
    log_info "  2. Place your config YAML in data_pipeline/configs/"
    log_info "  3. Run: ./run.sh dev"
}

################################################################################
# DEVELOPMENT MODE
################################################################################

run_dev() {
    log_info "Starting API server in DEVELOPMENT mode..."
    
    # Load environment
    if [ -f ".env" ]; then
        log_info "Loading .env file..."
        export $(cat .env | grep -v '^#' | xargs)
    fi
    
    export DEBUG="true"
    export API_WORKERS="1"  # Single worker in dev
    
    # Activate virtual environment
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
        source venv/Scripts/activate
    else
        source venv/bin/activate
    fi
    
    log_info "API Server: http://${API_HOST}:${API_PORT}"
    log_info "Debug mode: ENABLED"
    log_info "Workers: 1 (dev mode)"
    log_info "Press Ctrl+C to stop"
    echo ""
    
    # Add src to PYTHONPATH
    export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
    
    # Run with uvicorn (auto-reload enabled)
    cd src && uvicorn api_server:app \
        --host "$API_HOST" \
        --port "$API_PORT" \
        --reload \
        --log-level info
}

################################################################################
# PRODUCTION MODE
################################################################################

run_prod() {
    log_info "Starting API server in PRODUCTION mode..."
    
    # Load environment
    if [ -f ".env" ]; then
        log_info "Loading .env file..."
        export $(cat .env | grep -v '^#' | xargs)
    fi
    
    export DEBUG="false"
    
    # Activate virtual environment
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
        source venv/Scripts/activate
    else
        source venv/bin/activate
    fi
    
    log_info "API Server: http://${API_HOST}:${API_PORT}"
    log_info "Debug mode: DISABLED"
    log_info "Workers: ${API_WORKERS}"
    log_info "Logs: logs/api.log"
    echo ""
    
    # Add src to PYTHONPATH
    export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
    
    # Run with gunicorn (production-ready)
    if command -v gunicorn &> /dev/null; then
        log_info "Using Gunicorn (production mode)"
        cd src && gunicorn api_server:app \
            --workers "$API_WORKERS" \
            --worker-class uvicorn.workers.UvicornWorker \
            --bind "${API_HOST}:${API_PORT}" \
            --access-logfile ../logs/api_access.log \
            --error-logfile ../logs/api_error.log \
            --log-level info \
            --daemon
        
        log_success "Server started in background (daemon mode)"
        log_info "Check logs in: logs/api_*.log"
        log_info "Stop with: ./run.sh stop"
    else
        log_warning "Gunicorn not found, using Uvicorn instead"
        cd src && uvicorn api_server:app \
            --host "$API_HOST" \
            --port "$API_PORT" \
            --workers "$API_WORKERS" \
            --log-level info
    fi
}

################################################################################
# DOCKER MODE
################################################################################

run_docker() {
    log_info "Starting with Docker Compose..."
    
    if ! command -v docker-compose &> /dev/null && ! command -v docker &> /dev/null; then
        log_error "Docker not found. Please install Docker"
        exit 1
    fi
    
    # Build and run
    if command -v docker-compose &> /dev/null; then
        docker-compose up --build -d
    else
        docker compose up --build -d
    fi
    
    log_success "Docker containers started"
    log_info "API Server: http://localhost:8000"
    log_info "View logs: docker-compose logs -f"
    log_info "Stop: docker-compose down"
}

################################################################################
# STOP SERVER
################################################################################

stop_server() {
    log_info "Stopping API server..."
    
    # Kill gunicorn processes
    if pgrep -f "gunicorn.*api_server" > /dev/null; then
        pkill -f "gunicorn.*api_server"
        log_success "Gunicorn processes stopped"
    fi
    
    # Kill uvicorn processes
    if pgrep -f "uvicorn.*api_server" > /dev/null; then
        pkill -f "uvicorn.*api_server"
        log_success "Uvicorn processes stopped"
    fi
    
    # Stop Docker if running
    if [ -f "docker-compose.yml" ]; then
        if command -v docker-compose &> /dev/null; then
            docker-compose down 2>/dev/null && log_success "Docker containers stopped"
        elif command -v docker &> /dev/null; then
            docker compose down 2>/dev/null && log_success "Docker containers stopped"
        fi
    fi
    
    log_success "Server stopped"
}

################################################################################
# RUN PIPELINE DIRECTLY
################################################################################

run_pipeline() {
    log_info "Running pipeline directly (no API)..."
    
    # Activate virtual environment
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
        source venv/Scripts/activate
    else
        source venv/bin/activate
    fi
    
    # Add src to PYTHONPATH
    export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
    
    CONFIG="${1:-data_pipeline/configs/pipeline_config.yaml}"
    
    if [ ! -f "$CONFIG" ]; then
        log_error "Config file not found: $CONFIG"
        exit 1
    fi
    
    log_info "Using config: $CONFIG"
    
    cd src && $PYTHON pipeline_orchestrator.py \
        --config "../$CONFIG" \
        --log-level INFO \
        --log-file "../logs/pipeline_$(date +%Y%m%d_%H%M%S).log"
}

################################################################################
# TEST API
################################################################################

test_api() {
    log_info "Testing API endpoints..."
    
    BASE_URL="http://localhost:${API_PORT}"
    
    # Health check
    log_info "Testing health endpoint..."
    response=$(curl -s "${BASE_URL}/health")
    if echo "$response" | grep -q "healthy"; then
        log_success "Health check: OK"
    else
        log_error "Health check: FAILED"
        exit 1
    fi
    
    # Root endpoint
    log_info "Testing root endpoint..."
    response=$(curl -s "${BASE_URL}/")
    if echo "$response" | grep -q "Dataset Pipeline API"; then
        log_success "Root endpoint: OK"
    else
        log_error "Root endpoint: FAILED"
        exit 1
    fi
    
    log_success "All API tests passed!"
}

################################################################################
# MAIN
################################################################################

show_usage() {
    cat << EOF
Dataset Pipeline API Server

Usage: ./run.sh [command]

Commands:
  setup       First-time setup (install dependencies)
  dev         Run in development mode (auto-reload)
  prod        Run in production mode (multi-worker)
  docker      Run with Docker Compose
  pipeline    Run pipeline directly (no API server)
  test        Test API endpoints
  stop        Stop running server
  help        Show this help message

Examples:
  ./run.sh setup                           # Initial setup
  ./run.sh dev                             # Start dev server
  ./run.sh prod                            # Start production server
  ./run.sh pipeline                        # Run pipeline once
  ./run.sh pipeline configs/custom.yaml    # Run with custom config
  ./run.sh stop                            # Stop server

Environment Variables:
  API_HOST      Host to bind (default: 0.0.0.0)
  API_PORT      Port to bind (default: 8000)
  API_WORKERS   Number of workers (default: 4)
  DEBUG         Enable debug mode (default: false)

Configuration:
  Edit .env file to customize settings

EOF
}

# Main command router
case "${1:-help}" in
    setup)
        setup
        ;;
    dev)
        run_dev
        ;;
    prod)
        run_prod
        ;;
    docker)
        run_docker
        ;;
    pipeline)
        run_pipeline "${2:-}"
        ;;
    test)
        test_api
        ;;
    stop)
        stop_server
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        log_error "Unknown command: $1"
        echo ""
        show_usage
        exit 1
        ;;
esac
