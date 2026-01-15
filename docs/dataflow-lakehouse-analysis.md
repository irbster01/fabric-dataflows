# Dataflow Lakehouse Analysis

## Current State by Lakehouse

### Bronze Lakehouse
**63 dataflows** - Raw data ingestion layer

- arkansas_xml_export
- caddo_grades_transforms
- caddo-census
- choice_jcampus_transforms
- cis_accred_transforms
- cis_jcampus_joins
- cisdm_progress_monitoring_trans
- client served transform
- credible_assessment_track_transform
- hmis_csv_ingest
- hmis_export_ingest
- jcampus_caddo_test_transforms
- pipe_caddo_attend
- pipe_caddo_grades
- pipe_caddo_referrals
- raw_caddo_attend_summary
- raw_caddo_locator
- raw_caddo_tests
- raw_cisdm_caselist
- raw_cisdm_caseManagement_entries
- raw_cisdm_progress_monitoring
- raw_cisdm_school_goals
- raw_cisdm_student_planned_supports
- raw_cisdm_student_support_detail
- raw_cred_assess_assignment
- raw_credible_eligibility
- raw_desoto_attendance
- raw_desoto_census
- raw_desoto_grade_detail
- raw_desoto_outreach
- raw_desoto_referral_detail
- raw_kudos_submissions
- raw_lh_full_registration
- raw_lighthouse_grades
- raw_lsndc_clients
- raw_lsndc_entries
- raw_lsndc_exits
- raw_lsndc_services
- raw_lsndc_tfa
- raw_mileage_logs
- raw_oig_check
- raw_paf_submission
- raw_paycor_pipe
- raw_paycor_turnover
- raw_screeners
- raw_shoc_locations
- raw_sp_choice_intake
- raw_sp_client_addresses
- raw_sp_clients_new
- raw_sp_demographics
- raw_sp_entries
- raw_sp_lighthouse_reg
- raw_sp_services
- raw-cisdm-accreditation
- raw-credible-ibhh
- raw-credible-services
- raw-credible-services-filtered
- ssvf_extension_ingest
- staging_credible_addresses
- staging-credible-clients-episodes
- trans_caddo_jcampus_idLocator
- trans_lsndc_clients
- transform-credible-services-2

### credible_lakehouse (System Lakehouse)
**10 dataflows** - Credible system transformations

- active_credible_clients
- credible_employees_biDataflow
- credible_outreach_transforms
- credible_paycor_merge
- employee_supervision
- raw_client_referrals
- raw_credible_clients
- raw_credible_episodes
- raw_credible_fep_assessment
- raw_credible_outreach

### ServicePoint Lakehouse (System Lakehouse)
**8 dataflows** - ServicePoint system transformations

- active_sp_client_counts
- active_sp_clients
- active-client-service-counts
- lh_attend_gold
- lighthhouse_dates
- lighthouse_client_attend_final
- stage1_clean_entries
- stage2_clean_services

### LSNDC Lakehouse (System Lakehouse)
**14 dataflows** - LSNDC/GPD system transformations

- active_lsndc_clients
- choice_intake_transform
- gpd_entry_transforms
- lighthouse_grades_transform
- raw_program_documentation
- raw_risk_ingest
- raw_ssvf_workflow
- sp_client_int_id_staging
- stage1_clean_entries
- stage2_clean_services
- stage3_client_master
- stage4_sessions_attended
- stage5_sessions_possible
- stage6_attendance_final

### VOA_Nexus Lakehouse (Gold/Analytics)
**1 dataflow** - Cross-system analytics

- lighthouse_cis_comb

### Accounting Lakehouse
**8 dataflows** - Finance/accounting data

- credible_brs_transform
- raw_credible_brs
- raw_credible_brs_cross
- raw_credible_lookup_dict
- raw-credible-ledger
- raw-netsuite-fullregister
- raw-netsuite-gl-ssvf
- ssvf_gl

### Other/Unknown
**14 dataflows** - Need classification review

- agg_ssvf_gl_month
- division-bundle-counter-flow
- raw_credible_employees
- raw_historical_maintenance_invoices
- raw_maint_incidents
- raw_maintenance_requests
- raw_mcadoo_room_list
- raw_ns_byrnes_invoices
- raw-credible-brs
- raw-credible-ledger-v3
- servicepoint_services_transform
- trans_sp_lighthouse_registration
- transform_hmis_export
- transform-lsndc-client

---

## Recommended Migration Map

### Keep in Bronze (Raw Ingestion)
All dataflows starting with `raw_` should remain in Bronze:

- All 63 raw data ingestion flows currently in Bronze
- Move from credible_lakehouse:
  - raw_client_referrals
  - raw_credible_clients
  - raw_credible_episodes
  - raw_credible_fep_assessment
  - raw_credible_outreach

### credible_lakehouse (System Lake)
Single-system Credible transformations:

- active_credible_clients
- credible_assessment_track_transform
- credible_brs_transform
- credible_employees_biDataflow
- credible_outreach_transforms
- credible_paycor_merge
- employee_supervision

### ServicePoint Lakehouse (System Lake)
Single-system ServicePoint transformations:

- active_sp_client_counts
- active_sp_clients
- active-client-service-counts
- lh_attend_gold
- lighthouse_client_attend_final
- lighthhouse_dates
- servicepoint_services_transform
- stage1_clean_entries
- stage2_clean_services
- trans_sp_lighthouse_registration

### LSNDC Lakehouse (System Lake)
Single-system LSNDC/GPD transformations:

- active_lsndc_clients
- gpd_entry_transforms
- raw_program_documentation
- raw_risk_ingest
- raw_ssvf_workflow
- stage1_clean_entries
- stage2_clean_services
- stage3_client_master
- stage4_sessions_attended
- stage5_sessions_possible
- stage6_attendance_final
- transform-lsndc-client

### Move to VOA_Nexus (Gold)
Cross-system analytics joining 2+ systems:

- choice_intake_transform (joins ServicePoint + JCampus)
- choice_jcampus_transforms (joins ServicePoint + JCampus)
- cis_accred_transforms (joins multiple CIS sources)
- cis_jcampus_joins (joins CIS + JCampus)
- lighthouse_cis_comb (already in VOA_Nexus - joins Lighthouse + CIS)
- lighthouse_grades_transform (joins ServicePoint + JCampus)
- sp_client_int_id_staging (joins ServicePoint + LSNDC)
- trans_caddo_jcampus_idLocator (joins Caddo + JCampus)

### Accounting Lakehouse
Finance-specific transformations:

- agg_ssvf_gl_month
- credible_brs_transform
- raw_credible_brs
- raw_credible_brs_cross
- raw_credible_lookup_dict
- raw-credible-ledger
- raw-netsuite-fullregister
- raw-netsuite-gl-ssvf
- ssvf_gl

---

## Quick Stats

**Total dataflows analyzed:** 118

### Current Distribution
- Bronze: 63 (53%)
- credible_lakehouse: 10 (8%)
- ServicePoint: 8 (7%)
- LSNDC: 14 (12%)
- VOA_Nexus: 1 (1%)
- Accounting: 8 (7%)
- Other/Unknown: 14 (12%)

### Recommended Distribution
- Bronze (Raw): 68 dataflows (58%)
- credible_lakehouse (System): 7 dataflows (6%)
- ServicePoint (System): 10 dataflows (8%)
- LSNDC (System): 12 dataflows (10%)
- VOA_Nexus (Gold): 9 dataflows (8%)
- Accounting: 9 dataflows (8%)
- Needs Review: 3 dataflows (3%)

### Key Movements
- **5 raw ingestion flows** should move FROM credible_lakehouse TO Bronze
- **8 cross-system transforms** should move TO VOA_Nexus
- **10 system-specific transforms** properly placed in system lakehouses
- **63 raw ingestion flows** correctly in Bronze
