# Lighthouse Attendance Pipeline

Multi-stage dataflow pipeline for calculating lighthouse program attendance rates across 4 nine-week periods.

## Pipeline Architecture

### Stage 1: clean_lighthouse_entries
**Input**: `raw_sp_entries`  
**Output**: `clean_lighthouse_entries` (lakehouse table)  
**Purpose**: Deduplicate entries, keep only lighthouse programs  
**Logic**: Group by client_id, take max entry_date and exit_date  
**Columns**: client_id, entry_date, exit_date, program_name

### Stage 2: clean_lighthouse_services  
**Input**: `raw_sp_services`  
**Output**: `clean_lighthouse_services` (lakehouse table)  
**Purpose**: Filter services, add nine_week_period  
**Logic**: Filter to valid dates, assign period based on service_date  
**Columns**: client_id, service_date, nine_week_period, location, service_id

### Stage 3: lighthouse_client_master
**Input**: `clean_lighthouse_entries`, `raw_sp_demographics`, `raw_lighthouse_registration`  
**Output**: `lighthouse_client_master` (lakehouse table)  
**Purpose**: Create one master record per client  
**Logic**: Left joins to get all client info  
**Columns**: client_id, first_name, last_name, entry_date, exit_date, lighthouse_location_enrolling

### Stage 4: lighthouse_sessions_attended
**Input**: `clean_lighthouse_services`  
**Output**: `lighthouse_sessions_attended` (lakehouse table)  
**Purpose**: Count services per client per period  
**Logic**: Group by client_id and nine_week_period, count rows  
**Columns**: client_id, nine_week_period, sessions_attended

### Stage 5: lighthouse_sessions_possible
**Input**: `lighthouse_client_master`, `lighthouse_dates`  
**Output**: `lighthouse_sessions_possible` (lakehouse table)  
**Purpose**: Calculate possible sessions per client per period  
**Logic**: For each client/period, count session days between entry/exit dates  
**Columns**: client_id, nine_week_period, sessions_possible

### Stage 6: lighthouse_attendance_final
**Input**: `lighthouse_sessions_attended`, `lighthouse_sessions_possible`, `lighthouse_client_master`, `silver_lighthouse.Lighthouse Matched with JCampus`  
**Output**: `lighthouse_attendance_detail`, `attendance_summary_80_schools`, `attendance_summary_other_schools`, `attendance_averages_by_location`, `unmatched_clients`  
**Purpose**: Calculate attendance rates, apply thresholds, create reports  
**Logic**: Join all inputs, calculate rates, flag threshold compliance

## Nine-Week Periods
- 1st 9 Weeks: 8/13/2025 - 10/17/2025
- 2nd 9 Weeks: 10/20/2025 - 12/17/2025
- 3rd 9 Weeks: 1/6/2026 - 3/6/2026
- 4th 9 Weeks: 3/16/2026 - 5/21/2026

## Attendance Thresholds
- 80% schools: Bethune/Oak Park, Broadmoor STEM, Creswell Elementary
- 60% schools: All other locations

## Pipeline Execution Order
Run dataflows in sequence: Stage 1 → Stage 2 → Stage 3 → Stage 4 → Stage 5 → Stage 6

Each stage writes to lakehouse, next stage reads from lakehouse tables.
