# Archive - January 2026 Cleanup

**Date:** January 15, 2026  
**Reason:** Lakehouse consolidation - removing unused/low-risk dataflows

## Archived Dataflows

### Zero Risk
1. **lh_attend_gold.Dataflow**
   - Located in test folder
   - No output table defined
   - No references found
   - Safe to delete

### Very Low Risk (No Output Tables)
2. **raw_credible_employees.Dataflow**
   - No output table defined
   - No dataflow references
   
3. **agg_ssvf_gl_month.Dataflow**
   - No output table defined
   - No dataflow references

4. **division-bundle-counter-flow.Dataflow**
   - No output table defined
   - No dataflow references

5. **transform_hmis_export.Dataflow**
   - No output table defined
   - No dataflow references

6. **raw_mcadoo_room_list.Dataflow**
   - No output table defined
   - No dataflow references

## Restoration

If any of these are needed, simply:
1. Move the .Dataflow folder back to original location
2. Commit and push to git
3. Update from Git in Fabric workspace

## Original Locations

- `lh_attend_gold.Dataflow` → `servicepoint/`
- `raw_credible_employees.Dataflow` → `Credible/raw/` (or root)
- `agg_ssvf_gl_month.Dataflow` → `staging/`
- `division-bundle-counter-flow.Dataflow` → `staging/Credible/`
- `transform_hmis_export.Dataflow` → root
- `raw_mcadoo_room_list.Dataflow` → root
