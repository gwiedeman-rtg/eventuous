# PowerShell script to verify Azure Event Hubs projects are properly integrated

Write-Host "Verifying Azure Event Hubs projects integration..." -ForegroundColor Green

# Check if main project exists
$mainProject = "src/Azure/src/Eventuous.Azure.EventHubs/Eventuous.Azure.EventHubs.csproj"
if (Test-Path $mainProject) {
    Write-Host "✓ Main project found: $mainProject" -ForegroundColor Green
} else {
    Write-Host "✗ Main project not found: $mainProject" -ForegroundColor Red
    exit 1
}

# Check if test project exists
$testProject = "src/Azure/test/Eventuous.Tests.Azure.EventHubs/Eventuous.Tests.Azure.EventHubs.csproj"
if (Test-Path $testProject) {
    Write-Host "✓ Test project found: $testProject" -ForegroundColor Green
} else {
    Write-Host "✗ Test project not found: $testProject" -ForegroundColor Red
    exit 1
}

# Check if solution file includes the projects
$solutionFile = "Eventuous.slnx"
if (Test-Path $solutionFile) {
    $solutionContent = Get-Content $solutionFile -Raw
    if ($solutionContent -match "Eventuous.Azure.EventHubs.csproj") {
        Write-Host "✓ Main project found in solution file" -ForegroundColor Green
    } else {
        Write-Host "✗ Main project not found in solution file" -ForegroundColor Red
    }
    
    if ($solutionContent -match "Eventuous.Tests.Azure.EventHubs.csproj") {
        Write-Host "✓ Test project found in solution file" -ForegroundColor Green
    } else {
        Write-Host "✗ Test project not found in solution file" -ForegroundColor Red
    }
} else {
    Write-Host "✗ Solution file not found: $solutionFile" -ForegroundColor Red
    exit 1
}

# Check Azure solution filter
$azureSolutionFilter = "src/Azure/Eventuous.Azure.slnf"
if (Test-Path $azureSolutionFilter) {
    Write-Host "✓ Azure solution filter created: $azureSolutionFilter" -ForegroundColor Green
} else {
    Write-Host "✗ Azure solution filter not found: $azureSolutionFilter" -ForegroundColor Red
}

Write-Host ""
Write-Host "Integration verification completed!" -ForegroundColor Green
Write-Host ""
Write-Host "To build the Azure Event Hubs projects:" -ForegroundColor Yellow
Write-Host "  dotnet build src/Azure/src/Eventuous.Azure.EventHubs/" -ForegroundColor Cyan
Write-Host "  dotnet build src/Azure/test/Eventuous.Tests.Azure.EventHubs/" -ForegroundColor Cyan
Write-Host ""
Write-Host "To run tests (requires Azure resources):" -ForegroundColor Yellow
Write-Host "  dotnet test src/Azure/test/Eventuous.Tests.Azure.EventHubs/" -ForegroundColor Cyan
Write-Host ""
Write-Host "To work with Azure projects only:" -ForegroundColor Yellow
Write-Host "  Open src/Azure/Eventuous.Azure.slnf in Visual Studio" -ForegroundColor Cyan