#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}🔧 Building thanos-parquet-gateway locally...${NC}"

# Show version information
echo -e "${BLUE}📋 Version information:${NC}"
make version

echo ""

# Build the project
make build

echo -e "${GREEN}✅ Build completed successfully!${NC}"

# Test the binary
echo -e "${YELLOW}📋 Testing binary version...${NC}"
./parquet-gateway --version

echo -e "${GREEN}🐳 Building Docker image...${NC}"

# Build Docker image
make docker-build-local

echo -e "${GREEN}✅ Docker image built successfully!${NC}"

# Test Docker image
echo -e "${YELLOW}📋 Testing Docker image...${NC}"
make docker-test

echo -e "${GREEN}🎉 All tests passed! Ready for deployment.${NC}"
