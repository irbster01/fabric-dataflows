# Helper script to check if output tables are referenced in notebooks
# Run this for each candidate dataflow's output table

param(
    [Parameter(Mandatory=$true)]
    [string]$TableName
)

Write-Host "`n=== Searching for references to: $TableName ===" -ForegroundColor Cyan

# Search in all notebook files
Write-Host "`nSearching notebooks..." -ForegroundColor Yellow
$notebookResults = Get-ChildItem -Path . -Recurse -Filter "notebook-content.py" | 
    Where-Object { Select-String -Path $_.FullName -Pattern $TableName -Quiet } |
    ForEach-Object { $_.Directory.Parent.Name }

if ($notebookResults) {
    Write-Host "⚠️ FOUND in notebooks:" -ForegroundColor Red
    $notebookResults | ForEach-Object { Write-Host "  - $_" -ForegroundColor Yellow }
} else {
    Write-Host "✅ Not found in any notebooks" -ForegroundColor Green
}

# Search in all dataflow files (double-check)
Write-Host "`nSearching other dataflows..." -ForegroundColor Yellow
$dataflowResults = Get-ChildItem -Path . -Recurse -Filter "mashup.pq" | 
    Where-Object { 
        $_.Directory.Name -like "*.Dataflow" -and
        (Select-String -Path $_.FullName -Pattern $TableName -Quiet)
    } |
    ForEach-Object { $_.Directory.Name }

if ($dataflowResults) {
    Write-Host "⚠️ FOUND in dataflows:" -ForegroundColor Red
    $dataflowResults | ForEach-Object { Write-Host "  - $_" -ForegroundColor Yellow }
} else {
    Write-Host "✅ Not found in any other dataflows" -ForegroundColor Green
}

# Search in pipeline definitions
Write-Host "`nSearching pipelines..." -ForegroundColor Yellow
$pipelineResults = Get-ChildItem -Path "pipelines" -Recurse -Filter "*.json" -ErrorAction SilentlyContinue |
    Where-Object { Select-String -Path $_.FullName -Pattern $TableName -Quiet } |
    ForEach-Object { $_.Name }

if ($pipelineResults) {
    Write-Host "⚠️ FOUND in pipelines:" -ForegroundColor Red
    $pipelineResults | ForEach-Object { Write-Host "  - $_" -ForegroundColor Yellow }
} else {
    Write-Host "✅ Not found in any pipeline definitions" -ForegroundColor Green
}

Write-Host "`n=== Summary for: $TableName ===" -ForegroundColor Cyan
$hasReferences = $notebookResults -or $dataflowResults -or $pipelineResults

if ($hasReferences) {
    Write-Host "⚠️ This table IS referenced elsewhere - NOT safe to archive" -ForegroundColor Red
    Write-Host "References found in: " -NoNewline
    if ($notebookResults) { Write-Host "Notebooks " -NoNewline -ForegroundColor Yellow }
    if ($dataflowResults) { Write-Host "Dataflows " -NoNewline -ForegroundColor Yellow }
    if ($pipelineResults) { Write-Host "Pipelines" -NoNewline -ForegroundColor Yellow }
    Write-Host ""
} else {
    Write-Host "✅ No references found - CANDIDATE for archival (still check Power BI reports!)" -ForegroundColor Green
}

Write-Host "`nNote: This script does NOT check Power BI reports/semantic models." -ForegroundColor Cyan
Write-Host "You must manually check the Fabric workspace for report dependencies.`n" -ForegroundColor Cyan
