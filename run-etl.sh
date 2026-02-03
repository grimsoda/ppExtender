#!/bin/bash
set -e

echo "╔════════════════════════════════════════════════════════════╗"
echo "║           osu! Recommender ETL Pipeline                    ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PYTHON_VENV="venv"

# Parse arguments
DRY_RUN=""
PHASE=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --dry-run)
      DRY_RUN="--dry-run"
      shift
      ;;
    --phase)
      PHASE="$2"
      shift 2
      ;;
    --help|-h)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  --phase bronze|silver|gold  Run specific phase only"
      echo "  --dry-run                   Show what would be done without executing"
      echo "  --help, -h                  Show this help message"
      echo ""
      echo "Examples:"
      echo "  $0                          Run full pipeline"
      echo "  $0 --phase bronze           Run only bronze phase"
      echo "  $0 --dry-run                Show what would be done"
      exit 0
      ;;
    *)
      echo -e "${RED}Unknown option: $1${NC}"
      exit 1
      ;;
  esac
done

# Check if virtual environment exists
if [ ! -d "$PYTHON_VENV" ]; then
    echo -e "${RED}Error: Python virtual environment not found at $PYTHON_VENV${NC}"
    echo "Please create it first: python -m venv venv"
    exit 1
fi

# Activate Python virtual environment
echo -e "${BLUE}📦 Activating Python virtual environment...${NC}"
source "$PYTHON_VENV/bin/activate"

# Build command
CMD="python pipelines/run_pipeline.py"

if [ -n "$PHASE" ]; then
  CMD="$CMD --phase $PHASE"
else
  CMD="$CMD --full"
fi

if [ -n "$DRY_RUN" ]; then
  CMD="$CMD $DRY_RUN"
fi

# Run pipeline
echo ""
echo -e "${BLUE}🚀 Running pipeline...${NC}"
echo "   Command: $CMD"
echo ""

if $CMD; then
  echo ""
  echo -e "${GREEN}✅ Pipeline completed successfully!${NC}"
  echo ""
  echo "Next steps:"
  echo "  1. Start backend:  npm run backend:dev"
  echo "  2. Start frontend: npm run frontend:dev"
  echo "  3. Open browser:   http://localhost:5173"
  echo ""
  echo "Or run both with: npm run dev"
else
  echo ""
  echo -e "${RED}❌ Pipeline failed!${NC}"
  echo ""
  echo "Check the logs above for error details."
  exit 1
fi
