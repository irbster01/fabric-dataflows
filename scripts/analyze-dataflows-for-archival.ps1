# Analyze dataflows for archival candidates
# Focus on identifying LOW RISK flows that can be safely archived

$ErrorActionPreference = "Continue"

# Get all dataflow mashup.pq files
$dataflows = Get-ChildItem -Path . -Recurse -Filter "mashup.pq" | 
    Where-Object { $_.Directory.Name -like "*.Dataflow" }

Write-Host "Analyzing $($dataflows.Count) dataflows..." -ForegroundColor Cyan

$analysis = @()

foreach ($df in $dataflows) {
    $content = Get-Content $df.FullName -Raw -ErrorAction SilentlyContinue
    if (-not $content) { continue }
    
    $dfName = $df.Directory.Name
    $relativePath = $df.FullName.Replace((Get-Location).Path, "").TrimStart('\')
    
    # Extract output table name(s) - look for DataDestination patterns
    $outputTables = @()
    if ($content -match 'DataDestinations\s*=\s*\{([^\}]+)\}') {
        $destBlock = $matches[1]
        # Look for QueryName patterns
        if ($destBlock -match 'QueryName\s*=\s*"([^"]+)"') {
            $outputTables += $matches[1]
        }
    }
    
    # Look for shared queries that might be output tables
    $sharedQueries = [regex]::Matches($content, 'shared\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*=') | 
        ForEach-Object { $_.Groups[1].Value }
    
    # Determine location risk
    $isTestFolder = $relativePath -match '(scratch|future|test|temp|backup|old|archive)'
    $isTestName = $dfName -match '(test|scratch|temp|old|backup|copy|draft|experimental|demo|sample)'
    
    # Check if name suggests duplication
    $isDuplicate = $dfName -match '(_copy|_backup|_old|_v\d+|_2|_new|\d{8})'
    
    $obj = [PSCustomObject]@{
        Name = $dfName
        Path = $relativePath
        OutputTables = ($outputTables -join ', ')
        SharedQueries = ($sharedQueries -join ', ')
        IsTestFolder = $isTestFolder
        IsTestName = $isTestName
        IsDuplicate = $isDuplicate
        CreatedDate = $df.Directory.CreationTime
        ModifiedDate = $df.Directory.LastWriteTime
        Content = $content
    }
    
    $analysis += $obj
}

Write-Host "`nAnalysis complete. Processing references..." -ForegroundColor Cyan

# Now check which flows reference each other
foreach ($item in $analysis) {
    $referencedBy = @()
    
    # Check all output tables and shared queries
    $allPossibleTables = @()
    if ($item.OutputTables) { $allPossibleTables += $item.OutputTables -split ', ' }
    if ($item.SharedQueries) { $allPossibleTables += $item.SharedQueries -split ', ' }
    
    foreach ($table in $allPossibleTables) {
        if ([string]::IsNullOrWhiteSpace($table)) { continue }
        
        # Check if any other dataflow references this table
        foreach ($other in $analysis) {
            if ($other.Name -eq $item.Name) { continue }
            
            if ($other.Content -match "\b$([regex]::Escape($table))\b") {
                $referencedBy += $other.Name
            }
        }
    }
    
    $item | Add-Member -MemberType NoteProperty -Name "ReferencedBy" -Value ($referencedBy -join ', ') -Force
    $item | Add-Member -MemberType NoteProperty -Name "ReferenceCount" -Value $referencedBy.Count -Force
}

# Calculate risk scores
foreach ($item in $analysis) {
    $riskScore = 0
    $riskReasons = @()
    
    # CRITICAL: If a flow has references, it's NOT low risk regardless of folder!
    if ($item.ReferenceCount -gt 0) {
        $riskScore = 10  # HIGH RISK - has dependencies
        $riskReasons += "Referenced by $($item.ReferenceCount) other flows - NOT SAFE TO DELETE"
    }
    # Test folder with NO references = safe
    elseif ($item.IsTestFolder -and $item.ReferenceCount -eq 0) { 
        $riskScore = 0
        $riskReasons += "In test/scratch folder with zero references"
    }
    # Test name with no references
    elseif ($item.IsTestName -and $item.ReferenceCount -eq 0) {
        $riskScore = 1
        $riskReasons += "Test name, no references"
    }
    # Duplicate/backup with no references
    elseif ($item.ReferenceCount -eq 0 -and $item.IsDuplicate) {
        $riskScore = 2
        $riskReasons += "No references, appears to be duplicate/backup"
    }
    # No references at all
    elseif ($item.ReferenceCount -eq 0) {
        $riskScore = 3
        $riskReasons += "No references from other flows"
    }
    else {
        $riskScore = 5
        $riskReasons += "Unclear status"
    }
    
    $item | Add-Member -MemberType NoteProperty -Name "RiskScore" -Value $riskScore -Force
    $item | Add-Member -MemberType NoteProperty -Name "RiskReasons" -Value ($riskReasons -join '; ') -Force
}

# Export results
$analysis | Select-Object Name, Path, RiskScore, RiskReasons, ReferenceCount, ReferencedBy, OutputTables, IsTestFolder, IsTestName, IsDuplicate, CreatedDate, ModifiedDate | 
    Sort-Object RiskScore, ReferenceCount, Name |
    Export-Csv -Path "docs/dataflow-archival-analysis.csv" -NoTypeInformation

Write-Host "`n=== ANALYSIS COMPLETE ===" -ForegroundColor Green
Write-Host "Results saved to docs/dataflow-archival-analysis.csv" -ForegroundColor Green

# Show summary
$byRisk = $analysis | Group-Object RiskScore | Sort-Object Name
Write-Host "`n=== SUMMARY BY RISK ===" -ForegroundColor Cyan
foreach ($group in $byRisk) {
    $label = switch ($group.Name) {
        "0" { "ZERO RISK (Test folders, no refs)" }
        "1" { "VERY LOW RISK (Test names, no refs)" }
        "2" { "LOW RISK (Duplicates, no refs)" }
        "3" { "LOW RISK (No references)" }
        "10" { "HIGH RISK - HAS DEPENDENCIES" }
        default { "NEEDS INVESTIGATION" }
    }
    Write-Host "$label`: $($group.Count) dataflows" -ForegroundColor Yellow
}

# Show top candidates
Write-Host "`n=== TOP 20 ARCHIVAL CANDIDATES ===" -ForegroundColor Cyan
$analysis | Sort-Object RiskScore, ReferenceCount, Name | Select-Object -First 20 | 
    Format-Table Name, RiskScore, ReferenceCount, RiskReasons -AutoSize

Write-Host "`nFull details in: docs/dataflow-archival-analysis.csv" -ForegroundColor Green
