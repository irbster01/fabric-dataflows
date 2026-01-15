# Dataflow Documentation Tasks

**Project**: Document all dataflows in the fabric-dataflows repository with technical and non-technical guides

**Status**: 2 of 19 completed (11%)

---

## ✅ Completed

### ServicePoint Transformations
- [x] **lighthouse_grades_transform** - Quarterly GPA performance tracking (Completed: Jan 14, 2026)
  - File: `docs/Lighthouse-Grades-Pipeline.md`
  - Dependencies: raw_lighthouse_grades, lighthouse_client_master
  
- [x] **choice_intake_transform** - Intake assessments & federal reporting (Completed: Jan 14, 2026)
  - File: `docs/Choice-Intake-Pipeline.md`
  - Dependencies: raw_sp_choice_intake, active_choice_clients

---

## 🚧 In Progress

### ServicePoint Transformations
- [ ] **choice_jcampus_transforms** - PRIORITY NEXT
  - Matches Choice students to school district (JCampus) records
  - Dependencies: TBD

---

## 📋 Backlog - ServicePoint Transformations

### High Priority (Core Pipelines)
- [ ] **active_sp_clients** 
  - Active client master list
  - Foundation for many other transforms
  
- [ ] **lighthouse_client_attend_final**
  - Lighthouse attendance final calculations
  - Related to existing Lighthouse-Attendance-Pipeline.md

- [ ] **sp_client_int_id_staging**
  - Client ID staging/matching logic

### Standard Priority
- [ ] **active-client-service-counts**
  - Service utilization metrics
  
- [ ] **active_sp_client_counts**
  - Client count aggregations

---

## 📋 Backlog - ServicePoint Raw Data Ingestion

### High Priority (Data Sources)
- [ ] **raw_sp_entries**
  - Program enrollment records
  - Critical dependency for many pipelines
  
- [ ] **raw_sp_services**
  - Service delivery records
  - Used by attendance and service count pipelines
  
- [ ] **raw_sp_demographics**
  - Client demographic information
  
- [ ] **raw_sp_clients_new**
  - Client master records

### Standard Priority
- [ ] **raw_lh_full_registration**
  - Lighthouse full registration data
  
- [ ] **raw_sp_lighthouse_reg**
  - Lighthouse registration subset
  
- [ ] **raw_sp_client_addresses**
  - Client address information

---

## 📋 Backlog - Other Areas

### CISDM (Communities In Schools)
- [ ] Document CIS dataflows (if not already covered in CIS-Data-Reference.md)
  - raw_cisdm_* dataflows
  - cis_accred_transforms
  - cis_jcampus_joins

### JCampus (School District Data)
- [ ] Document JCampus raw ingestion
  - raw_caddo_* dataflows
  
### LSNDC
- [ ] Document LSNDC dataflows
  - raw_lsndc_* dataflows
  - gpd_entry_transforms

### Credible
- [ ] Document Credible system dataflows (if applicable)

---

## 📊 Progress Tracking

| Category | Total | Completed | Remaining | % Done |
|----------|-------|-----------|-----------|--------|
| ServicePoint Transforms | 8 | 2 | 6 | 25% |
| ServicePoint Raw | 8 | 0 | 8 | 0% |
| Other Systems | ~10 | 0 | ~10 | 0% |
| **TOTAL** | **~26** | **2** | **~24** | **8%** |

---

## 📝 Documentation Template Checklist

Each dataflow doc should include:
- [ ] Pipeline flow diagram (Mermaid)
- [ ] Stage-by-stage architecture
- [ ] Input sources and file formats
- [ ] Transformation logic
- [ ] Output tables with column definitions
- [ ] Business rules and thresholds
- [ ] Plain-language "How the Numbers Are Calculated" section
- [ ] Example walk-through with real scenario
- [ ] Data quality notes
- [ ] Future enhancements
- [ ] Related documentation links

---

## 🎯 Next Session Goals

1. Complete **choice_jcampus_transforms** documentation
2. Decide on next high-priority pipeline
3. Consider grouping related raw ingestion dataflows into single documents

---

## 💡 Notes

- Following pattern from Lighthouse-Grades-Pipeline.md and Choice-Intake-Pipeline.md
- Aiming for both technical (developers/analysts) and non-technical (program staff) audiences
- Working backward from transforms to their raw data sources when documenting
