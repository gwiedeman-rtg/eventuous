#!/bin/bash

# Bash script to verify Azure Event Hubs projects are properly integrated

echo "Verifying Azure Event Hubs projects integration..."

# Check if main project exists
MAIN_PROJECT="src/Azure/src/Eventuous.Azure.EventHubs/Eventuous.Azure.EventHubs.csproj"
if [ -f "$MAIN_PROJECT" ]; then
    echo "✓ Main project found: $MAIN_PROJECT"
else
    echo "✗ Main project not found: $MAIN_PROJECT"
    exit 1
fi

# Check if test project exists
TEST_PROJECT="src/Azure/test/Eventuous.Tests.Azure.EventHubs/Eventuous.Tests.Azure.EventHubs.csproj"
if [ -f "$TEST_PROJECT" ]; then
    echo "✓ Test project found: $TEST_PROJECT"
else
    echo "✗ Test project not found: $TEST_PROJECT"
    exit 1
fi

# Check if solution file includes the projects
SOLUTION_FILE="Eventuous.slnx"
if [ -f "$SOLUTION_FILE" ]; then
    if grep -q "Eventuous.Azure.EventHubs.csproj" "$SOLUTION_FILE"; then
        echo "✓ Main project found in solution file"
    else
        echo "✗ Main project not found in solution file"
    fi
    
    if grep -q "Eventuous.Tests.Azure.EventHubs.csproj" "$SOLUTION_FILE"; then
        echo "✓ Test project found in solution file"
    else
        echo "✗ Test project not found in solution file"
    fi
else
    echo "✗ Solution file not found: $SOLUTION_FILE"
    exit 1
fi

# Check Azure solution filter
AZURE_SOLUTION_FILTER="src/Azure/Eventuous.Azure.slnf"
if [ -f "$AZURE_SOLUTION_FILTER" ]; then
    echo "✓ Azure solution filter created: $AZURE_SOLUTION_FILTER"
else
    echo "✗ Azure solution filter not found: $AZURE_SOLUTION_FILTER"
fi

echo ""
echo "Integration verification completed!"
echo ""
echo "To build the Azure Event Hubs projects:"
echo "  dotnet build src/Azure/src/Eventuous.Azure.EventHubs/"
echo "  dotnet build src/Azure/test/Eventuous.Tests.Azure.EventHubs/"
echo ""
echo "To run tests (requires Azure resources):"
echo "  dotnet test src/Azure/test/Eventuous.Tests.Azure.EventHubs/"
echo ""
echo "To work with Azure projects only:"
echo "  Open src/Azure/Eventuous.Azure.slnf in Visual Studio"