# Understanding Bundle Billing Counts - For Everyone

**What is this?** A report that tracks when clients qualify for special bundled billing rates  
**Why does it matter?** It affects how much we can bill Medicaid for services  
**Where does it come from?** Credible EHR system → Automated data pipeline → Reports

---

## The Big Picture

When our behavioral health programs provide multiple services to a client in one month, Medicaid sometimes allows us to bill at a "bundled" rate instead of billing each service separately. This can be more favorable financially and reduces administrative burden.

This pipeline **automatically counts** how many qualifying services each client receives per month and flags whether they meet the bundle threshold.

---

## What Are "Bundles"?

### Simple Definition
A **bundle** is when a client receives 4 or more qualifying services in a calendar month.

### Why It Matters
- **Bundled billing = different reimbursement rate**
- **Simplifies billing:** One bundle code instead of multiple individual claims
- **Compliance:** Medicaid requires accurate counting to use bundle codes

### Real-World Example
**Client: Jane Doe**  
**Month: December 2025**

Services received:
- 12/5: Case Management (BUN eligible) ✅
- 12/10: Individual Support Training (BUN eligible) ✅
- 12/15: PSR Group session (BUN eligible) ✅
- 12/20: Case Management (BUN eligible) ✅

**Total BUN Services:** 4  
**Result:** Jane qualifies for bundle billing this month! 🎉

---

## What Services Count Toward Bundles?

### Qualifying Services (BUN Codes)
These service types count toward the bundle threshold:

| Service Type | What It Is |
|--------------|------------|
| **BUN: ARST** | Applied Behavioral Analysis/Rehab Services (individual) |
| **BUN: ARST-G** | Applied Behavioral Analysis/Rehab Services (group) |
| **BUN: Case Management** | Coordinating care and resources |
| **BUN: IST** | Individual Support Training |
| **CPST** | Community Psychiatric Support and Treatment |
| **PSR** | Psychosocial Rehabilitation (individual) |
| **PSR Group** | Psychosocial Rehabilitation (group) |

### Non-Qualifying Services
Things like assessments, medication management, or crisis intervention typically **don't count** toward bundles (unless specifically coded as BUN services).

---

## How the Counting Works

### Step 1: Gather Services
The system looks at all services provided each month from our Credible database.

### Step 2: Filter to BUN Services
Only services with "BUN" in the name (or CPST/PSR) are counted.

### Step 3: Separate Primary & Secondary
- **Primary services:** The main therapeutic service (counted as 1)
- **Secondary services:** Additional support services (also counted as 1 each)

### Step 4: Add Them Up
Primary + Secondary = Total Service Count

### Step 5: Apply the Rule
- **If Total ≥ 4:** Client gets flagged as "BUN" (qualifies for bundle)
- **If Total < 4:** Client flagged as "NO" (bill services individually)

---

## What Reports Does This Feed?

### 1. Client-Level Report
**Who:** Program managers, case managers  
**Shows:** Each client's monthly service count and bundle eligibility  
**Use:** Verify clients are receiving adequate services; plan future sessions

**Example Output:**
| Client Name | Month | Service Count | Bundle? |
|-------------|-------|---------------|---------|
| Jane Doe | Dec 2025 | 4 | ✅ BUN |
| John Smith | Dec 2025 | 2 | ❌ NO |
| Sarah Lee | Dec 2025 | 6 | ✅ BUN |

### 2. Division-Level Report
**Who:** Finance, executive leadership  
**Shows:** How many clients qualified for bundles each month (aggregated)  
**Use:** Revenue forecasting, program performance

**Example Output:**
| Month | Clients with Bundles | Clients without Bundles | Total Clients |
|-------|---------------------|------------------------|---------------|
| Dec 2025 | 87 | 23 | 110 |
| Nov 2025 | 92 | 18 | 110 |

---

## Common Questions

### Q: What happens if a client gets exactly 3 services?
**A:** They don't qualify for bundle billing that month. Services are billed individually.

### Q: Does it matter which days the services happen?
**A:** No, as long as all services are within the same calendar month. Four services on 4 different days vs. all in one week both count the same.

### Q: What if a client receives 10 services in a month?
**A:** They still qualify as "bundle" (just exceeded the minimum). The count is tracked for internal reporting.

### Q: Can the threshold change?
**A:** Yes, Medicaid rules can change. Currently it's 4+ services. If the rule changes, we update the data pipeline.

### Q: Where can I see this data?
**A:** 
- **Program Managers:** Power BI dashboard (Bundle Eligibility Report)
- **Finance Team:** Monthly billing summary spreadsheets
- **IT/Data Team:** Direct database access to `client-bundle-counter` table

---

## What If Something Looks Wrong?

### Data Issues to Watch For

**🚩 Client shows 0 services but you know they attended:**
- Check if services were entered in Credible
- Verify service type is BUN-eligible

**🚩 Count seems too low:**
- Confirm all BUN service types are being tracked
- Check if services span two different months (month boundaries)

**🚩 Client flagged "NO" but should be "BUN":**
- Verify primary vs. secondary service flags in Credible
- Check if all services are properly documented

### Who to Contact
- **For Credible data entry issues:** Clinical documentation team
- **For report errors or questions:** Data Analytics team
- **For billing questions:** Finance/Billing department

---

## Behind the Scenes (Simplified)

This is what happens automatically every day:

```
📊 Credible EHR System
   └─> Services data exported
       └─> Data Pipeline runs
           ├─> Filters to BUN services
           ├─> Groups by client + month
           ├─> Counts primary + secondary
           ├─> Applies >= 4 rule
           └─> Outputs to reports
               └─> You see results in dashboards!
```

**Runtime:** ~5 minutes  
**Frequency:** Daily at 6:00 AM  
**Historical Data:** Available back to January 2024

---

## Key Takeaways

✅ **4+ qualifying services per month** = Bundle billing eligible  
✅ **Primary + secondary services** both count toward the total  
✅ **Automatic counting** reduces manual work and errors  
✅ **Updated daily** so reports are always current  
✅ **Medicaid compliance** is built into the logic  

---

## Version History

| Date | Change |
|------|--------|
| Jan 2026 | Initial documentation created |
| | Added client-level and division-level reporting |
| | Implemented 4-service threshold rule |

---

**Questions?** Contact the Data Analytics Team  
**Need access to reports?** Submit IT ticket for Power BI access  
**Found a data issue?** Email data-team@voa-northla.org with details
