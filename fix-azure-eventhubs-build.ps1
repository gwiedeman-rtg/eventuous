# PowerShell script to fix Azure Event Hubs build issues

Write-Host "Fixing Azure Event Hubs build issues..." -ForegroundColor Green

# Clean build artifacts
Write-Host "Cleaning build artifacts..." -ForegroundColor Yellow
if (Test-Path "src/Azure/src/Eventuous.Azure.EventHubs/bin") {
    Remove-Item -Recurse -Force "src/Azure/src/Eventuous.Azure.EventHubs/bin"
    Write-Host "✓ Removed bin folder from main project" -ForegroundColor Green
}

if (Test-Path "src/Azure/src/Eventuous.Azure.EventHubs/obj") {
    Remove-Item -Recurse -Force "src/Azure/src/Eventuous.Azure.EventHubs/obj"
    Write-Host "✓ Removed obj folder from main project" -ForegroundColor Green
}

if (Test-Path "src/Azure/test/Eventuous.Tests.Azure.EventHubs/bin") {
    Remove-Item -Recurse -Force "src/Azure/test/Eventuous.Tests.Azure.EventHubs/bin"
    Write-Host "✓ Removed bin folder from test project" -ForegroundColor Green
}

if (Test-Path "src/Azure/test/Eventuous.Tests.Azure.EventHubs/obj") {
    Remove-Item -Recurse -Force "src/Azure/test/Eventuous.Tests.Azure.EventHubs/obj"
    Write-Host "✓ Removed obj folder from test project" -ForegroundColor Green
}

# Verify Directory.Packages.props has the required packages
Write-Host "Verifying Directory.Packages.props..." -ForegroundColor Yellow
$packagesFile = "Directory.Packages.props"
if (Test-Path $packagesFile) {
    $content = Get-Content $packagesFile -Raw
    
    $requiredPackages = @(
        "Azure.Messaging.EventHubs",
        "Azure.Messaging.EventHubs.Processor", 
        "Azure.Storage.Blobs",
        "Microsoft.Extensions.Configuration.EnvironmentVariables",
        "Microsoft.Extensions.Logging.Console"
    )
    
    foreach ($package in $requiredPackages) {
        if ($content -match $package) {
            Write-Host "✓ Found package: $package" -ForegroundColor Green
        } else {
            Write-Host "✗ Missing package: $package" -ForegroundColor Red
        }
    }
} else {
    Write-Host "✗ Directory.Packages.props not found" -ForegroundColor Red
}

# Verify project files don't have version attributes
Write-Host "Verifying project files..." -ForegroundColor Yellow
$projectFiles = @(
    "src/Azure/src/Eventuous.Azure.EventHubs/Eventuous.Azure.EventHubs.csproj",
    "src/Azure/test/Eventuous.Tests.Azure.EventHubs/Eventuous.Tests.Azure.EventHubs.csproj"
)

foreach ($projectFile in $projectFiles) {
    if (Test-Path $projectFile) {
        $content = Get-Content $projectFile -Raw
        if ($content -match 'Version\s*=') {
            Write-Host "✗ Found version attribute in: $projectFile" -ForegroundColor Red
            Write-Host "   Please remove version attributes from PackageReference items" -ForegroundColor Yellow
        } else {
            Write-Host "✓ No version attributes in: $projectFile" -ForegroundColor Green
        }
    } else {
        Write-Host "✗ Project file not found: $projectFile" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Run 'dotnet clean' to clean the entire solution" -ForegroundColor Cyan
Write-Host "2. Run 'dotnet restore' to restore packages" -ForegroundColor Cyan
Write-Host "3. Run 'dotnet build' to build the solution" -ForegroundColor Cyan
Write-Host ""
Write-Host "If the error persists, try:" -ForegroundColor Yellow
Write-Host "- Close Visual Studio if open" -ForegroundColor Cyan
Write-Host "- Delete all bin and obj folders in the solution" -ForegroundColor Cyan
Write-Host "- Run the dotnet commands above" -ForegroundColor Cyan