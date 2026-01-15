# Dataflow Archival Analysis - Quick Reference

## TL;DR - Actionable Results

### ✅ Can Delete TODAY (1 flow)
1. **lh_attend_gold.Dataflow** 
   - Path: `servicepoint\lh_attend_gold.Dataflow\`
   - No output table, no references
   - In test folder, last modified 1/2/2026
   - **Action:** Delete after quick verification

### 🔍 Investigate Next (31 flows with no dataflow dependencies)

These flows have NO other dataflows consuming them, but may still be used by:
- Power BI reports
- Direct queries
- Manual processes

**Top Priorities for Investigation (oldest, most likely unused):**

1. **raw_credible_employees.Dataflow** - No output table defined
2. **raw_mcadoo_room_list.Dataflow** - No output table defined  
3. **agg_ssvf_gl_month.Dataflow** - No output table defined
4. **division-bundle-counter-flow.Dataflow** - No output table defined
5. **transform_hmis_export.Dataflow** - No output table defined

These 5 flows have no defined output tables in their mashup.pq, making them prime candidates.

**Flows with defined outputs to check:**

6. **raw_maint_incidents.Dataflow** → `raw_maint_incident_log_DataDestination`
7. **raw_maintenance_requests.Dataflow** → `Maintenance Request and History_DataDestination`
8. **raw_historical_maintenance_invoices.Dataflow** → `appended_historical_maint_invoices_DataDestination`
9. **raw_paycor_pipe.Dataflow** → `punches_DataDestination`
10. **raw_shoc_locations.Dataflow** → `Housing Locations - Matt Turek xlsx_DataDestination`

---

## Investigation Workflow

For each candidate flow:

### Step 1: Check Notebooks
```powershell
./scripts/check-table-references.ps1 -TableName "TABLE_NAME_HERE"
```

### Step 2: Check Power BI (Manual)
1. Open Fabric workspace
2. Check each Report → Data Sources
3. Check each Semantic Model → Tables
4. Search for the table name

### Step 3: Verify Modified Date
```powershell
Get-ChildItem -Path "PATH_TO_DATAFLOW" -Recurse | 
    Select-Object Name, LastWriteTime, CreationTime
```

### Step 4: Review Business Purpose
- Check with data owners
- Look for documentation
- Consider if it's a "final output" by design

### Step 5: Make Decision
If ALL are true, archive:
- ✅ No notebook references
- ✅ No report references  
- ✅ No business need
- ✅ Not modified in 3+ months

---

## ⚠️ Critical Findings

### Mis-categorized "Test" Flows
These are in test/raw folders but ARE heavily used:

1. **raw_caddo_tests.Dataflow** - Referenced by **37 flows** ❌ DO NOT DELETE
   - Should be renamed/moved to production location
   
2. **jcampus_caddo_test_transforms.Dataflow** - Referenced by **6 flows** ❌ DO NOT DELETE
   - Should be renamed/moved to production location

**Action Required:** Refactor these out of test folders immediately!

---

## Statistics

- **Total Dataflows:** 118
- **Zero Risk:** 1 (0.8%)
- **Low Risk (investigate):** 31 (26.3%)
- **High Risk (has dependencies):** 86 (72.9%)

**Realistic Archival Goal:** 5-10 flows after full investigation

---

## Next Actions

### This Week
- [ ] Delete `lh_attend_gold.Dataflow` after final verification
- [ ] Investigate top 5 flows with no output tables
- [ ] Rename/move the 2 "test" flows that are in production use

### Next Week  
- [ ] Check Power BI workspace for references to candidate tables
- [ ] Document findings in this file
- [ ] Archive 2-3 confirmed unused flows

### Ongoing
- [ ] Continue investigating remaining 26 low-risk candidates
- [ ] Establish archival process/policy
- [ ] Monitor for new unused flows

---

## Files Generated

1. **docs/dataflow-archival-candidates.md** - Full detailed report (528 lines)
2. **docs/dataflow-archival-analysis.csv** - Raw data export
3. **scripts/analyze-dataflows-for-archival.ps1** - Analysis script (re-runnable)
4. **scripts/check-table-references.ps1** - Helper to check table usage

---

## Re-run Analysis

```powershell
# Full analysis
./scripts/analyze-dataflows-for-archival.ps1

# Check specific table
./scripts/check-table-references.ps1 -TableName "YOUR_TABLE_NAME"

# Find all flows modified in last 30 days
Get-ChildItem -Path . -Recurse -Filter "mashup.pq" | 
    Where-Object { 
        $_.Directory.Name -like "*.Dataflow" -and 
        $_.LastWriteTime -gt (Get-Date).AddDays(-30) 
    } | Select-Object Directory, LastWriteTime
```

---

*Last Updated: 2026-01-15*
