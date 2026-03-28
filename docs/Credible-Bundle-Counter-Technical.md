# Credible Bundle Counter - Technical Documentation

**Pipeline:** `staging/Credible/transform-credible-services-2.Dataflow`  
**Purpose:** Count and flag BUN (bundled service) eligibility per client per month  
**Output Tables:** `bundle_count_byClient`, `bundle-secondaries`, `transform-credible-bundle-primary`, `division-bundle-counter`, `client-bundle-counter`

---

## Overview

This dataflow processes Credible services data to identify when clients qualify for bundled billing (BUN codes). Medicaid allows bundled billing when a client receives 4+ qualifying services in a calendar month, which changes the billing rate structure.

## Business Rule

**Bundle Eligibility Threshold:** `>= 4 services per month`
- Primary services + Secondary services combined must equal 4 or more
- Services must be BUN-eligible service types
- Grouped by client + calendar month

## Architecture

```
raw_credible_services (Bronze)
         |
         ├─> bundle-secondaries (count secondary services)
         ├─> transform-credible-bundle-primary (count primary services)
         |
         └─> JOIN on (client_id, month)
                  |
                  ├─> division-bundle-counter (division-level aggregates)
                  └─> client-bundle-counter (client-level details)
```

---

## Data Flow Details

### Input Table
**Source:** `raw_credible_services` (from credible_lakehouse)

**Key Fields:**
- `client_id` - Unique client identifier
- `service_date` - Date service was provided
- `service_type` - Type of service (filtered to BUN codes)
- `service_id` - Unique service identifier
- `comb_primary` - Boolean flag indicating if service is primary or secondary

### Eligible Service Types

All services containing "BUN" in the service type:
- `BUN: ARST` (Applied Behavioral Analysis/Rehabilitative Services)
- `BUN: ARST-G` (Group ARST)
- `BUN: Case Management`
- `BUN: IST` (Individual Support Training)
- `CPST` (Community Psychiatric Support and Treatment)
- `PSR` (Psychosocial Rehabilitation)
- `PSR Group`

---

## Transformation Steps

### Step 1: Extract Bundle-Secondaries

**Query:** `bundle-secondaries`

```powerquery
1. Load raw_credible_services
2. Add column: Start of month from service_date
3. Filter: service_type contains "BUN"
4. Filter: comb_primary = "false" (secondary services only)
5. Group by: client_id, Month, comb_primary
6. Aggregate: COUNT(DISTINCT service_id)
7. Sort by: Month DESC
```

**Output:** Count of secondary BUN services per client per month

### Step 2: Extract Bundle-Primary

**Query:** `transform-credible-bundle-primary`

```powerquery
1. Load raw_credible_services
2. Add column: Start of month from service_date
3. Filter: service_type contains "BUN"
4. Filter: comb_primary = "true" (primary services only)
5. Group by: client_id, Month, service_id
6. Aggregate: COUNT(DISTINCT service_id)
7. Sort by: Month DESC
```

**Output:** Count of primary BUN services per client per month

### Step 3: Join and Calculate Total

**Query:** `client-bundle-counter`

```powerquery
1. JOIN transform-credible-bundle-primary 
   WITH bundle-secondaries
   ON (Client ID, Month)
   [INNER JOIN]
   
2. Add column: Addition = primary_count + secondary_count

3. Add column: bundle-flag = 
   IF Addition >= 4 THEN "BUN" ELSE "NO"
   
4. Sort by: Month DESC
```

**Output Schema:**
| Column | Type | Description |
|--------|------|-------------|
| Client ID | text | Client identifier |
| Month | date | First day of service month |
| service_id | text | Service identifier |
| bundle-count | int | Total services (primary + secondary) |
| bundle-flag | text | "BUN" if >= 4 services, else "NO" |

### Step 4: Division-Level Aggregation

**Query:** `division-bundle-counter`

```powerquery
1. Use same JOIN logic as client-bundle-counter
2. Group by: Month, bundle-flag
3. Aggregate: COUNT(*) as bundle-count
4. Sort by: Month DESC
```

**Output:** Monthly counts of how many clients qualify for BUN billing

---

## Output Tables

### `bundle_count_byClient`
**Location:** credible_lakehouse  
**Refresh:** On-demand  
**Purpose:** Simple count of BUN services per client per month (simplified version)

**Schema:**
- `client_id` (text)
- `service_month` (date)
- `Count` (int)

### `client-bundle-counter`
**Location:** Silver lakehouse  
**Refresh:** Daily  
**Purpose:** Detailed bundle eligibility per client

**Schema:**
- `Client ID` (text)
- `Month` (date)
- `service_id` (text)
- `bundle-count` (int)
- `bundle-flag` (text: "BUN" or "NO")

### `division-bundle-counter`
**Location:** Silver lakehouse  
**Refresh:** Daily  
**Purpose:** Monthly aggregates for division reporting

**Schema:**
- `Month` (date)
- `bundle-flag` (text: "BUN" or "NO")
- `bundle-count` (int)

---

## Technical Notes

### Performance Considerations
- **INNER JOIN** used to only keep clients with BOTH primary AND secondary services
- Services with `comb_primary = null` are excluded
- Uses `DISTINCT service_id` to prevent duplicate counting

### Data Quality Checks
1. ✅ Verify all BUN service types are captured in filter
2. ✅ Confirm `comb_primary` field is populated correctly in source
3. ✅ Validate month boundaries (uses `Date.StartOfMonth()`)
4. ⚠️ INNER JOIN excludes clients with only primary OR only secondary services

### Known Limitations
- **INNER JOIN logic**: Clients with only primary or only secondary services are excluded from final counts
- If a client has 3 primary + 0 secondary (or vice versa), they won't appear in output
- Consider changing to LEFT JOIN if you need to capture all bundle attempts

---

## Usage Examples

### Query: Find clients who qualified for bundle billing in December 2025
```sql
SELECT 
    [Client ID],
    [bundle-count],
    [bundle-flag]
FROM [client-bundle-counter]
WHERE 
    Month = '2025-12-01'
    AND [bundle-flag] = 'BUN'
ORDER BY [bundle-count] DESC
```

### Query: Monthly bundle qualification rate
```sql
SELECT 
    Month,
    SUM(CASE WHEN [bundle-flag] = 'BUN' THEN [bundle-count] ELSE 0 END) as qualified_clients,
    SUM([bundle-count]) as total_clients,
    (SUM(CASE WHEN [bundle-flag] = 'BUN' THEN [bundle-count] ELSE 0 END) * 100.0 / SUM([bundle-count])) as pct_qualified
FROM [division-bundle-counter]
GROUP BY Month
ORDER BY Month DESC
```

---

## Dependencies

### Upstream
- `raw_credible_services` (must complete first)

### Downstream
- Billing reports
- Financial forecasting models
- Program utilization dashboards

---

## Maintenance

### When to Update This Dataflow
1. New BUN service types added to Credible
2. Medicaid bundle eligibility rules change (currently 4+ services)
3. Changes to `comb_primary` field logic in source system

### Testing Checklist
- [ ] Verify service type filter captures all BUN codes
- [ ] Validate counts against manual Credible report
- [ ] Check for nulls in `comb_primary` field
- [ ] Confirm month boundaries align with billing periods

---

## Related Documentation
- [Credible Services Data Dictionary](./CIS-Data-Reference.md)
- [Billing Pipeline Overview](./index.md)

---

**Last Updated:** January 15, 2026  
**Owner:** Data Engineering Team  
**Source Code:** `staging/Credible/transform-credible-services-2.Dataflow/mashup.pq`
