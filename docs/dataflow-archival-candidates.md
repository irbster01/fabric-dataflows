# Dataflow Archival Analysis - Low Risk Candidates
*Analysis Date: 2026-01-15 10:46*
*Total Dataflows Analyzed: 118*

## ⚠️ Critical Analysis Notes

**IMPORTANT:** This analysis identifies flows based on **dataflow-to-dataflow references only**.

**What this means:**
- ✅ "No references" = No other **dataflow mashup.pq files** reference this flow's output
- ❌ This does NOT check if the output is used by:
  - Power BI Reports
  - Semantic Models
  - Notebooks
  - Pipelines
  - Direct queries from external tools

**Before archiving ANY flow:**
1. Check Power BI workspace for reports using the output table
2. Search for the table name in notebooks (*.Notebook/)
3. Check if pipelines reference it
4. Verify last modified date and creation context
5. Consult with business owners

---

## Executive Summary
- **Zero Risk (Delete Today):** 1 dataflow
- **Very Low Risk (Evaluate for Archive):** 31 dataflows  
- **HIGH RISK (Has Dependencies):** 86 dataflows - **DO NOT ARCHIVE**
- **Total Low-Risk Candidates:** 32 dataflows

---

## 🟢 Zero Risk - Can Delete Today
*In test/scratch folders AND has zero dataflow references*

### ✅ lh_attend_gold.Dataflow
- **Path:** `servicepoint\lh_attend_gold.Dataflow\mashup.pq`
- **Output Tables:** 
- **Referenced By:** None
- **Why Zero Risk:** Located in test/development folder with no downstream dataflow dependencies
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **⚠️ Action Required:** Still verify no reports/notebooks use this before deleting!

---

## 🔵 Very Low Risk - Evaluate for Archival
*These dataflows have NO downstream dataflow dependencies*

These 31 flows are not referenced by any other dataflow. However, they may still be used by:
- Power BI reports consuming their output tables directly
- Notebooks reading the data
- Manual queries or external tools

**Recommendation:** Review each flow's output table usage in Power BI workspace before archiving.

### No Dataflow References (31)
*Not referenced by other dataflows, but may still be used elsewhere*

#### 📦 active_credible_clients.Dataflow
- **Path:** `Credible\transformations\active_credible_clients.Dataflow\mashup.pq`
- **Output Tables:** bundle_count_byClient_DataDestination
- **Created:** 1/2/2026 12:06:43 PM
- **Modified:** 1/2/2026 12:06:43 PM
- **Next Step:** Check if `bundle_count_byClient_DataDestination` is used in Power BI reports

#### 📦 agg_ssvf_gl_month.Dataflow
- **Path:** `staging\agg_ssvf_gl_month.Dataflow\mashup.pq`
- **Output Tables:** 
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `` is used in Power BI reports

#### 📦 credible_employees_biDataflow.Dataflow
- **Path:** `credible_employees_biDataflow.Dataflow\mashup.pq`
- **Output Tables:** API - Employees_DataDestination
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `API - Employees_DataDestination` is used in Power BI reports

#### 📦 division-bundle-counter-flow.Dataflow
- **Path:** `staging\Credible\division-bundle-counter-flow.Dataflow\mashup.pq`
- **Output Tables:** 
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `` is used in Power BI reports

#### 📦 lighthouse_cis_comb.Dataflow
- **Path:** `staging\lighthouse_cis_comb.Dataflow\mashup.pq`
- **Output Tables:** Matching_CIS_LH_Final_DataDestination
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `Matching_CIS_LH_Final_DataDestination` is used in Power BI reports

#### 📦 raw_cisdm_school_goals.Dataflow
- **Path:** `cisdm\raw\raw_cisdm_school_goals.Dataflow\mashup.pq`
- **Output Tables:** school_goals_DataDestination
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `school_goals_DataDestination` is used in Power BI reports

#### 📦 raw_cisdm_student_planned_supports.Dataflow
- **Path:** `cisdm\raw\raw_cisdm_student_planned_supports.Dataflow\mashup.pq`
- **Output Tables:** planned_supports_DataDestination
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `planned_supports_DataDestination` is used in Power BI reports

#### 📦 raw_cisdm_student_support_detail.Dataflow
- **Path:** `cisdm\raw\raw_cisdm_student_support_detail.Dataflow\mashup.pq`
- **Output Tables:** student_support_detail_DataDestination
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `student_support_detail_DataDestination` is used in Power BI reports

#### 📦 raw_client_referrals.Dataflow
- **Path:** `raw_client_referrals.Dataflow\mashup.pq`
- **Output Tables:** Client Referral_DataDestination
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `Client Referral_DataDestination` is used in Power BI reports

#### 📦 raw_credible_eligibility.Dataflow
- **Path:** `Credible\raw\raw_credible_eligibility.Dataflow\mashup.pq`
- **Output Tables:** raw_cred_assess_assign_DataDestination
- **Created:** 1/2/2026 12:06:43 PM
- **Modified:** 1/2/2026 12:06:43 PM
- **Next Step:** Check if `raw_cred_assess_assign_DataDestination` is used in Power BI reports

#### 📦 raw_credible_employees.Dataflow
- **Path:** `Credible\raw\raw_credible_employees.Dataflow\mashup.pq`
- **Output Tables:** 
- **Created:** 1/2/2026 12:06:43 PM
- **Modified:** 1/2/2026 12:06:43 PM
- **Next Step:** Check if `` is used in Power BI reports

#### 📦 raw_credible_outreach.Dataflow
- **Path:** `raw_credible_outreach.Dataflow\mashup.pq`
- **Output Tables:** API - Outreach Service_DataDestination
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `API - Outreach Service_DataDestination` is used in Power BI reports

#### 📦 raw_historical_maintenance_invoices.Dataflow
- **Path:** `admin\raw\raw_historical_maintenance_invoices.Dataflow\mashup.pq`
- **Output Tables:** appended_historical_maint_invoices_DataDestination
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `appended_historical_maint_invoices_DataDestination` is used in Power BI reports

#### 📦 raw_maint_incidents.Dataflow
- **Path:** `admin\raw\raw_maint_incidents.Dataflow\mashup.pq`
- **Output Tables:** raw_maint_incident_log_DataDestination
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `raw_maint_incident_log_DataDestination` is used in Power BI reports

#### 📦 raw_maintenance_requests.Dataflow
- **Path:** `admin\raw\raw_maintenance_requests.Dataflow\mashup.pq`
- **Output Tables:** Maintenance Request and History_DataDestination
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `Maintenance Request and History_DataDestination` is used in Power BI reports

#### 📦 raw_mcadoo_room_list.Dataflow
- **Path:** `raw_mcadoo_room_list.Dataflow\mashup.pq`
- **Output Tables:** 
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `` is used in Power BI reports

#### 📦 raw_paycor_pipe.Dataflow
- **Path:** `admin\raw\raw_paycor_pipe.Dataflow\mashup.pq`
- **Output Tables:** punches_DataDestination
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `punches_DataDestination` is used in Power BI reports

#### 📦 raw_risk_ingest.Dataflow
- **Path:** `admin\raw\raw_risk_ingest.Dataflow\mashup.pq`
- **Output Tables:** Risk Management_DataDestination
- **Created:** 1/14/2026 10:48:00 AM
- **Modified:** 1/14/2026 10:48:00 AM
- **Next Step:** Check if `Risk Management_DataDestination` is used in Power BI reports

#### 📦 raw_shoc_locations.Dataflow
- **Path:** `raw_shoc_locations.Dataflow\mashup.pq`
- **Output Tables:** Housing Locations - Matt Turek xlsx_DataDestination
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `Housing Locations - Matt Turek xlsx_DataDestination` is used in Power BI reports

#### 📦 raw_sp_client_addresses.Dataflow
- **Path:** `servicepoint\raw\raw_sp_client_addresses.Dataflow\mashup.pq`
- **Output Tables:** raw_client_addresses_DataDestination
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `raw_client_addresses_DataDestination` is used in Power BI reports

#### 📦 raw_sp_lighthouse_reg.Dataflow
- **Path:** `servicepoint\raw\raw_sp_lighthouse_reg.Dataflow\mashup.pq`
- **Output Tables:** sp_lighthouse_registration csv_DataDestination
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `sp_lighthouse_registration csv_DataDestination` is used in Power BI reports

#### 📦 raw_ssvf_workflow.Dataflow
- **Path:** `raw_ssvf_workflow.Dataflow\mashup.pq`
- **Output Tables:** SSVF Workflow_DataDestination
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `SSVF Workflow_DataDestination` is used in Power BI reports

#### 📦 raw-credible-ibhh.Dataflow
- **Path:** `Credible\raw\raw-credible-ibhh.Dataflow\mashup.pq`
- **Output Tables:** Query_DataDestination
- **Created:** 1/2/2026 12:06:43 PM
- **Modified:** 1/2/2026 12:06:43 PM
- **Next Step:** Check if `Query_DataDestination` is used in Power BI reports

#### 📦 raw-credible-ledger-v3.Dataflow
- **Path:** `Credible\raw\raw-credible-ledger-v3.Dataflow\mashup.pq`
- **Output Tables:** raw-credible-ledger-v3_DataDestination
- **Created:** 1/2/2026 12:06:43 PM
- **Modified:** 1/2/2026 12:06:43 PM
- **Next Step:** Check if `raw-credible-ledger-v3_DataDestination` is used in Power BI reports

#### 📦 raw-credible-ledger.Dataflow
- **Path:** `Credible\raw\raw-credible-ledger.Dataflow\mashup.pq`
- **Output Tables:** raw-credible-ledger_DataDestination
- **Created:** 1/2/2026 12:06:43 PM
- **Modified:** 1/2/2026 12:06:43 PM
- **Next Step:** Check if `raw-credible-ledger_DataDestination` is used in Power BI reports

#### 📦 raw-credible-services-filtered.Dataflow
- **Path:** `Credible\raw\raw-credible-services-filtered.Dataflow\mashup.pq`
- **Output Tables:** raw-credible-services-filtered_DataDestination
- **Created:** 1/2/2026 12:06:43 PM
- **Modified:** 1/2/2026 12:06:43 PM
- **Next Step:** Check if `raw-credible-services-filtered_DataDestination` is used in Power BI reports

#### 📦 raw-netsuite-fullregister.Dataflow
- **Path:** `admin\NetSuite\raw-netsuite-fullregister.Dataflow\mashup.pq`
- **Output Tables:** raw-netsuite-fullregister_DataDestination
- **Created:** 1/2/2026 12:06:43 PM
- **Modified:** 1/2/2026 12:06:43 PM
- **Next Step:** Check if `raw-netsuite-fullregister_DataDestination` is used in Power BI reports

#### 📦 staging_credible_addresses.Dataflow
- **Path:** `staging\Credible\staging_credible_addresses.Dataflow\mashup.pq`
- **Output Tables:** raw_credible_clients-warehouse_DataDestination
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `raw_credible_clients-warehouse_DataDestination` is used in Power BI reports

#### 📦 transform_hmis_export.Dataflow
- **Path:** `lsndc\transformations\transform_hmis_export.Dataflow\mashup.pq`
- **Output Tables:** 
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `` is used in Power BI reports

#### 📦 transform-credible-services-2.Dataflow
- **Path:** `staging\Credible\transform-credible-services-2.Dataflow\mashup.pq`
- **Output Tables:** bundle-secondaries_DataDestination
- **Created:** 1/2/2026 12:06:44 PM
- **Modified:** 1/2/2026 12:06:44 PM
- **Next Step:** Check if `bundle-secondaries_DataDestination` is used in Power BI reports

**Additional flows (1 more):**
- transform-lsndc-client.Dataflow → 

---

## 🔴 HIGH RISK - DO NOT ARCHIVE
*These flows ARE referenced by other dataflows - archiving would break dependencies*

86 dataflows have downstream dependencies. These should NOT be archived.

### Most Referenced Flows (Top 10)
- **stage1_clean_entries.Dataflow** → Referenced by **8** other flows
  - Path: `servicepoint\transformations\lighthouse_pipeline\stage1_clean_entries.Dataflow\mashup.pq`
  - Output: 

- **staging-credible-clients-episodes.Dataflow** → Referenced by **8** other flows
  - Path: `staging\Credible\staging-credible-clients-episodes.Dataflow\mashup.pq`
  - Output: transform-credible-clients-active_DataDestination

- **stage1_clean_entries.Dataflow** → Referenced by **8** other flows
  - Path: `servicepoint\transformations\Lighthouse Attendance\stage1_clean_entries.Dataflow\mashup.pq`
  - Output: clean_lighthouse_entries_DataDestination

- **stage5_sessions_possible.Dataflow** → Referenced by **7** other flows
  - Path: `servicepoint\transformations\Lighthouse Attendance\stage5_sessions_possible.Dataflow\mashup.pq`
  - Output: lighthouse_sessions_possible_DataDestination

- **raw_sp_entries.Dataflow** → Referenced by **60** other flows
  - Path: `servicepoint\raw\raw_sp_entries.Dataflow\mashup.pq`
  - Output: raw_sp_entries_new_DataDestination

- **raw_paf_submission.Dataflow** → Referenced by **60** other flows
  - Path: `admin\raw\raw_paf_submission.Dataflow\mashup.pq`
  - Output: 

- **raw_lsndc_tfa.Dataflow** → Referenced by **60** other flows
  - Path: `raw_lsndc_tfa.Dataflow\mashup.pq`
  - Output: 

- **raw_lsndc_entries.Dataflow** → Referenced by **60** other flows
  - Path: `lsndc\raw\raw_lsndc_entries.Dataflow\mashup.pq`
  - Output: lsndc_entries_DataDestination

- **raw_lsndc_clients.Dataflow** → Referenced by **60** other flows
  - Path: `lsndc\raw\raw_lsndc_clients.Dataflow\mashup.pq`
  - Output: lsndc_clients_DataDestination

- **arkansas_xml_export.Dataflow** → Referenced by **60** other flows
  - Path: `lsndc\raw\arkansas_xml_export.Dataflow\mashup.pq`
  - Output: bronze_hmis_clients_DataDestination

---

## 📊 Statistics

### By Risk Category
- **Zero Risk:** 1 dataflows
- **Low Risk (No dataflow refs):** 31 dataflows
- **High Risk (Has dependencies):** 86 dataflows

### By Type (Low Risk Candidates Only)
- **Test/Temp Names:** 0 dataflows
- **Duplicates/Backups:** 0 dataflows
- **Other (no refs):** 31 dataflows

### Location Analysis- **In test folders:** 3 dataflows (but 2 are actually used by other flows!)
- **Test/temp names:** 4 dataflows
- **Duplicate/backup patterns:** 1 dataflows

---

## 🎯 Recommended Action Plan

### Phase 1: Immediate (This Week) - ZERO RISK
**Only 1 dataflow qualifies:**1. Verify no reports use output of `lh_attend_gold.Dataflow`
2. If clear, delete `servicepoint\lh_attend_gold.Dataflow\mashup.pq`

### Phase 2: Investigation Required (Week 2-3)
**For the 31 "Very Low Risk" candidates:**

1. **Generate Table Usage Report**
   - Query Fabric workspace for all reports/semantic models
   - Identify which reports consume each output table
   - Cross-reference with the output tables from our low-risk candidates

2. **Check Notebooks**
   - Search all `*.Notebook/` folders for references to output table names
   - Example: Search for `raw_client_referrals` across all notebooks

3. **Review Business Context**
   - Check with data owners about the purpose of each flow
   - Some flows may be "leaf nodes" by design (final output for manual consumption)

4. **Prioritize by Last Modified Date**
   - Flows not modified in 6+ months are better candidates
   - Recently modified flows likely still in active use

### Phase 3: Safe Archival (Week 4+)
After Phase 2 investigation, archive flows that meet ALL criteria:
- ✅ Zero dataflow references (already confirmed)
- ✅ Zero report/semantic model references (from Phase 2)
- ✅ Zero notebook references (from Phase 2)  
- ✅ Business owner confirms not needed (from Phase 2)
- ✅ Not modified in last 3+ months

**Estimated realistic archival target:** 5-10 flows after full investigation

---

## 🔍 Detailed Candidate List for Investigation

### Top 20 Candidates by Modified Date (Oldest First)
#### active_credible_clients.Dataflow
- Path: `Credible\transformations\active_credible_clients.Dataflow\mashup.pq`
- Output: `bundle_count_byClient_DataDestination`
- Last Modified: 1/2/2026 12:06:43 PM (**12 days ago**)
- Investigation: Check if `bundle_count_byClient_DataDestination` appears in any reports

#### raw-netsuite-fullregister.Dataflow
- Path: `admin\NetSuite\raw-netsuite-fullregister.Dataflow\mashup.pq`
- Output: `raw-netsuite-fullregister_DataDestination`
- Last Modified: 1/2/2026 12:06:43 PM (**12 days ago**)
- Investigation: Check if `raw-netsuite-fullregister_DataDestination` appears in any reports

#### raw-credible-services-filtered.Dataflow
- Path: `Credible\raw\raw-credible-services-filtered.Dataflow\mashup.pq`
- Output: `raw-credible-services-filtered_DataDestination`
- Last Modified: 1/2/2026 12:06:43 PM (**12 days ago**)
- Investigation: Check if `raw-credible-services-filtered_DataDestination` appears in any reports

#### raw-credible-ledger.Dataflow
- Path: `Credible\raw\raw-credible-ledger.Dataflow\mashup.pq`
- Output: `raw-credible-ledger_DataDestination`
- Last Modified: 1/2/2026 12:06:43 PM (**12 days ago**)
- Investigation: Check if `raw-credible-ledger_DataDestination` appears in any reports

#### raw-credible-ledger-v3.Dataflow
- Path: `Credible\raw\raw-credible-ledger-v3.Dataflow\mashup.pq`
- Output: `raw-credible-ledger-v3_DataDestination`
- Last Modified: 1/2/2026 12:06:43 PM (**12 days ago**)
- Investigation: Check if `raw-credible-ledger-v3_DataDestination` appears in any reports

#### raw-credible-ibhh.Dataflow
- Path: `Credible\raw\raw-credible-ibhh.Dataflow\mashup.pq`
- Output: `Query_DataDestination`
- Last Modified: 1/2/2026 12:06:43 PM (**12 days ago**)
- Investigation: Check if `Query_DataDestination` appears in any reports

#### raw_credible_eligibility.Dataflow
- Path: `Credible\raw\raw_credible_eligibility.Dataflow\mashup.pq`
- Output: `raw_cred_assess_assign_DataDestination`
- Last Modified: 1/2/2026 12:06:43 PM (**12 days ago**)
- Investigation: Check if `raw_cred_assess_assign_DataDestination` appears in any reports

#### raw_credible_employees.Dataflow
- Path: `Credible\raw\raw_credible_employees.Dataflow\mashup.pq`
- Output: ``
- Last Modified: 1/2/2026 12:06:43 PM (**12 days ago**)
- Investigation: Check if `` appears in any reports

#### transform_hmis_export.Dataflow
- Path: `lsndc\transformations\transform_hmis_export.Dataflow\mashup.pq`
- Output: ``
- Last Modified: 1/2/2026 12:06:44 PM (**12 days ago**)
- Investigation: Check if `` appears in any reports

#### staging_credible_addresses.Dataflow
- Path: `staging\Credible\staging_credible_addresses.Dataflow\mashup.pq`
- Output: `raw_credible_clients-warehouse_DataDestination`
- Last Modified: 1/2/2026 12:06:44 PM (**12 days ago**)
- Investigation: Check if `raw_credible_clients-warehouse_DataDestination` appears in any reports

#### raw_ssvf_workflow.Dataflow
- Path: `raw_ssvf_workflow.Dataflow\mashup.pq`
- Output: `SSVF Workflow_DataDestination`
- Last Modified: 1/2/2026 12:06:44 PM (**12 days ago**)
- Investigation: Check if `SSVF Workflow_DataDestination` appears in any reports

#### raw_sp_lighthouse_reg.Dataflow
- Path: `servicepoint\raw\raw_sp_lighthouse_reg.Dataflow\mashup.pq`
- Output: `sp_lighthouse_registration csv_DataDestination`
- Last Modified: 1/2/2026 12:06:44 PM (**12 days ago**)
- Investigation: Check if `sp_lighthouse_registration csv_DataDestination` appears in any reports

#### raw_sp_client_addresses.Dataflow
- Path: `servicepoint\raw\raw_sp_client_addresses.Dataflow\mashup.pq`
- Output: `raw_client_addresses_DataDestination`
- Last Modified: 1/2/2026 12:06:44 PM (**12 days ago**)
- Investigation: Check if `raw_client_addresses_DataDestination` appears in any reports

#### raw_shoc_locations.Dataflow
- Path: `raw_shoc_locations.Dataflow\mashup.pq`
- Output: `Housing Locations - Matt Turek xlsx_DataDestination`
- Last Modified: 1/2/2026 12:06:44 PM (**12 days ago**)
- Investigation: Check if `Housing Locations - Matt Turek xlsx_DataDestination` appears in any reports

#### raw_paycor_pipe.Dataflow
- Path: `admin\raw\raw_paycor_pipe.Dataflow\mashup.pq`
- Output: `punches_DataDestination`
- Last Modified: 1/2/2026 12:06:44 PM (**12 days ago**)
- Investigation: Check if `punches_DataDestination` appears in any reports

#### raw_mcadoo_room_list.Dataflow
- Path: `raw_mcadoo_room_list.Dataflow\mashup.pq`
- Output: ``
- Last Modified: 1/2/2026 12:06:44 PM (**12 days ago**)
- Investigation: Check if `` appears in any reports

#### raw_maintenance_requests.Dataflow
- Path: `admin\raw\raw_maintenance_requests.Dataflow\mashup.pq`
- Output: `Maintenance Request and History_DataDestination`
- Last Modified: 1/2/2026 12:06:44 PM (**12 days ago**)
- Investigation: Check if `Maintenance Request and History_DataDestination` appears in any reports

#### raw_maint_incidents.Dataflow
- Path: `admin\raw\raw_maint_incidents.Dataflow\mashup.pq`
- Output: `raw_maint_incident_log_DataDestination`
- Last Modified: 1/2/2026 12:06:44 PM (**12 days ago**)
- Investigation: Check if `raw_maint_incident_log_DataDestination` appears in any reports

#### raw_historical_maintenance_invoices.Dataflow
- Path: `admin\raw\raw_historical_maintenance_invoices.Dataflow\mashup.pq`
- Output: `appended_historical_maint_invoices_DataDestination`
- Last Modified: 1/2/2026 12:06:44 PM (**12 days ago**)
- Investigation: Check if `appended_historical_maint_invoices_DataDestination` appears in any reports

#### raw_credible_outreach.Dataflow
- Path: `raw_credible_outreach.Dataflow\mashup.pq`
- Output: `API - Outreach Service_DataDestination`
- Last Modified: 1/2/2026 12:06:44 PM (**12 days ago**)
- Investigation: Check if `API - Outreach Service_DataDestination` appears in any reports

---

## ⚠️ Critical Findings

### "Test" Folders That Are NOT Safe to Delete
Found **2 flows in test/scratch folders that ARE actively used:**
- ⚠️ **jcampus_caddo_test_transforms.Dataflow** (`jcampus\transforms\jcampus_caddo_test_transforms.Dataflow\mashup.pq`) - Referenced by 6 flows - **DO NOT DELETE**
- ⚠️ **raw_caddo_tests.Dataflow** (`jcampus\raw\raw_caddo_tests.Dataflow\mashup.pq`) - Referenced by 37 flows - **DO NOT DELETE**

**Action Required:** Consider renaming or moving these flows out of test folders since they're production dependencies!

---

## Next Steps

1. **Immediate:** Delete the 1 truly unused test folder flow(s)
2. **This Week:** Start Phase 2 investigation for top 10-15 oldest unreferenced flows
3. **Next Week:** Continue investigation and begin archival of confirmed unused flows
4. **Ongoing:** Refactor "test" folder flows that are actually in production use

---

*Full data available in: docs/dataflow-archival-analysis.csv*
*Re-run analysis: ./scripts/analyze-dataflows-for-archival.ps1*
