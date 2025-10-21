#!/bin/bash

# Bash script to fix Azure Event Hubs build issues

echo "Fixing Azure Event Hubs build issues..."

# Clean build artifacts
echo "Cleaning build artifacts..."
if [ -d "src/Azure/src/Eventuous.Azure.EventHubs/bin" ]; then
    rm -rf "src/Azure/src/Eventuous.Azure.EventHubs/bin"
    echo "✓ Removed bin folder from main project"
fi

if [ -d "src/Azure/src/Eventuous.Azure.EventHubs/obj" ]; then
    rm -rf "src/Azure/src/Eventuous.Azure.EventHubs/obj"
    echo "✓ Removed obj folder from main project"
fi

if [ -d "src/Azure/test/Eventuous.Tests.Azure.EventHubs/bin" ]; then
    rm -rf "src/Azure/test/Eventuous.Tests.Azure.EventHubs/bin"
    echo "✓ Removed bin folder from test project"
fi

if [ -d "src/Azure/test/Eventuous.Tests.Azure.EventHubs/obj" ]; then
    rm -rf "src/Azure/test/Eventuous.Tests.Azure.EventHubs/obj"
    echo "✓ Removed obj folder from test project"
fi

# Verify Directory.Packages.props has the required packages
echo "Verifying Directory.Packages.props..."
PACKAGES_FILE="Directory.Packages.props"
if [ -f "$PACKAGES_FILE" ]; then
    REQUIRED_PACKAGES=(
        "Azure.Messaging.EventHubs"
        "Azure.Messaging.EventHubs.Processor"
        "Azure.Storage.Blobs"
        "Microsoft.Extensions.Configuration.EnvironmentVariables"
        "Microsoft.Extensions.Logging.Console"
    )
    
    for package in "${REQUIRED_PACKAGES[@]}"; do
        if grep -q "$package" "$PACKAGES_FILE"; then
            echo "✓ Found package: $package"
        else
            echo "✗ Missing package: $package"
        fi
    done
else
    echo "✗ Directory.Packages.props not found"
fi

# Verify project files don't have version attributes
echo "Verifying project files..."
PROJECT_FILES=(
    "src/Azure/src/Eventuous.Azure.EventHubs/Eventuous.Azure.EventHubs.csproj"
    "src/Azure/test/Eventuous.Tests.Azure.EventHubs/Eventuous.Tests.Azure.EventHubs.csproj"
)

for project_file in "${PROJECT_FILES[@]}"; do
    if [ -f "$project_file" ]; then
        if grep -q 'Version\s*=' "$project_file"; then
            echo "✗ Found version attribute in: $project_file"
            echo "   Please remove version attributes from PackageReference items"
        else
            echo "✓ No version attributes in: $project_file"
        fi
    else
        echo "✗ Project file not found: $project_file"
    fi
done

echo ""
echo "Next steps:"
echo "1. Run 'dotnet clean' to clean the entire solution"
echo "2. Run 'dotnet restore' to restore packages"
echo "3. Run 'dotnet build' to build the solution"
echo ""
echo "If the error persists, try:"
echo "- Close Visual Studio if open"
echo "- Delete all bin and obj folders in the solution"
echo "- Run the dotnet commands above"