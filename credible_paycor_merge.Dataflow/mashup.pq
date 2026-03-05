[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_credible_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_services", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"client_id", "client_first_name", "client_last_name", "rate", "appr", "cotherapist", "comb_primary", "comb_secondary", "comb_units", "comb_rate", "episode_id", "delayed_reason_code", "manual_redx_note", "manual_redx", "auth_id", "manual_update_to_modifier", "is_late", "ins_due", "ins_paid_amount", "contract_rate", "non_billable", "appr_date"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed columns", "13", "6401", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "9", "Office Svc", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "12", "6021", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "14", "FEP", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "15", "IBHH", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 4", "6", "6020", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 6", "16", "CHRP", Replacer.ReplaceValue, {"program_id"}),
  #"Filtered rows" = Table.SelectRows(#"Replaced value 5", each ([program_id] <> "10" and [program_id] <> "11")),
  #"Filtered rows 6" = Table.SelectRows(#"Filtered rows", each [service_date] > #date(2025, 1, 1)),
  #"Filtered rows 5" = Table.SelectRows(#"Filtered rows 6", each ([service_type] = "Assessment CPST" or [service_type] = "BUN: ARST" or [service_type] = "BUN: ARST-G" or [service_type] = "BUN: Case Management" or [service_type] = "BUN: IST" or [service_type] = "CPST" or [service_type] = "Crisis Follow-Up" or [service_type] = "Crisis Intervention" or [service_type] = "PSR" or [service_type] = "PSR Group" or [service_type] = "Psychotherapy - Cris" or [service_type] = "Psychotherapy - Ind")),
  Custom = #"Filtered rows 5",
  #"Filtered rows 1" = Table.SelectRows(Custom, each [emp_name] = "Allyson Ebey" or [emp_name] = "Brittney Dukes" or [emp_name] = "Elizabeth Luent" or [emp_name] = "Jacinta Lafleur" or [emp_name] = "Kelley Baker" or [emp_name] = "Kristal Poland" or [emp_name] = "Michelle Merello" or [emp_name] = "Tina Hall" or [emp_name] = "W. Justin Davis" or Text.Contains([emp_name], "Claire")),
  #"Grouped rows" = Table.Group(#"Filtered rows 1", {"emp_name", "program_id", "service_date"}, {{"Count", each List.Sum([duration]), type nullable number}}),
  #"Divided column" = Table.TransformColumns(#"Grouped rows", {{"Count", each _ / 60, type number}}),
  #"Renamed columns" = Table.RenameColumns(#"Divided column", {{"service_date", "date"}, {"program_id", "dept"}, {"Count", "service_hours"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"dept"}),
  #"Added custom" = Table.AddColumn(#"Removed columns 1", "dept", each "EPICenter"),
  #"Grouped rows 1" = Table.Group(#"Added custom", {"emp_name", "date", "dept"}, {{"service_hours", each List.Sum([service_hours]), type nullable number}})
in
  #"Grouped rows 1";
shared raw_fep_punches = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_fep_punches", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each [date] > #date(2025, 1, 1)),
  #"Removed columns" = Table.RemoveColumns(#"Filtered rows", {"exceptions", "report_timeframe"}),
  #"Filtered rows 1" = Table.SelectRows(#"Removed columns", each ([earning_code] = "OT" or [earning_code] = "Reg")),
  #"Sorted rows" = Table.Sort(#"Filtered rows 1", {{"date", Order.Descending}, {"employee_name", Order.Ascending}}),
  #"Removed duplicates" = Table.Distinct(#"Sorted rows"),
  #"Removed columns 1" = Table.RemoveColumns(#"Removed duplicates", {"manager"}),
  #"Removed duplicates 1" = Table.Distinct(#"Removed columns 1"),
  #"Grouped rows" = Table.Group(#"Removed duplicates 1", {"employee_name", "dept", "date", "earning_code"}, {{"Count", each List.Sum([hours]), type nullable number}}),
  #"Renamed columns" = Table.RenameColumns(#"Grouped rows", {{"employee_name", "emp_name"}})
in
  #"Renamed columns";
shared fep_employees = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_fep_punches", ItemKind = "Table"]}[Data],
  #"Removed other columns" = Table.SelectColumns(#"Navigation 2", {"employee_name"}),
  #"Removed duplicates" = Table.Distinct(#"Removed other columns", {"employee_name"}),
  #"Inserted conditional column" = Table.AddColumn(#"Removed duplicates", "hourly_rate", each if Text.Contains([employee_name], "Tina") then 32.51 else if Text.Contains([employee_name], "Tammy") then 19.8 else if Text.Contains([employee_name], "Russell") then 26.97 else if Text.Contains([employee_name], "Allyson") then 31.25 else if Text.Contains([employee_name], "Kristal") then 32.51 else if Text.Contains([employee_name], "Luent") then 25 else if Text.Contains([employee_name], "Kati") then 22.07 else if Text.Contains([employee_name], "Jacinta") then 30.29 else if Text.Contains([employee_name], "Claire") then 22.11 else "")
in
  #"Inserted conditional column";
shared #"Sep 14 to Sep 27" = let
  Source = Table.Combine({raw_fep_punches, raw_credible_services}),
  #"Replaced value" = Table.ReplaceValue(Source, null, 0, Replacer.ReplaceValue, {"Count"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", null, 0, Replacer.ReplaceValue, {"service_hours"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "Management and General", "EPICenter", Replacer.ReplaceValue, {"dept"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "BehavioralHealth-Youth(Desoto)", "EPICenter", Replacer.ReplaceValue, {"dept"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "Behavioral Health-Youth(Caddo)", "EPICenter", Replacer.ReplaceValue, {"dept"}),
  #"Grouped rows" = Table.Group(#"Replaced value 4", {"emp_name", "date", "dept"}, {{"punch_hours", each List.Sum([Count]), type nullable number}, {"service_hours", each List.Sum([service_hours]), type nullable number}}),
  Custom = #"Grouped rows",
  #"Filtered rows" = Table.SelectRows(Custom, each [date] >= #date(2025, 9, 14) and [date] <= #date(2025, 9, 27)),
  #"Grouped rows 1" = Table.Group(#"Filtered rows", {"emp_name"}, {{"punch_hours", each List.Sum([punch_hours]), type nullable number}, {"service_hours", each List.Sum([service_hours]), type nullable number}}),
  #"Filtered rows 1" = Table.SelectRows(#"Grouped rows 1", each ([emp_name] <> "Justin Davis")),
  #"Inserted conditional column" = Table.AddColumn(#"Filtered rows 1", "fep_flag", each if [punch_hours] = 0 then "CHECK DEPT" else "FEP"),
  #"Inserted subtraction" = Table.AddColumn(#"Inserted conditional column", "Subtraction", each [punch_hours] - [service_hours], type number),
  #"Reordered columns" = Table.ReorderColumns(#"Inserted subtraction", {"emp_name", "punch_hours", "service_hours", "Subtraction", "fep_flag"}),
  #"Renamed columns" = Table.RenameColumns(#"Reordered columns", {{"Subtraction", "invoice_hours"}}),
  #"Filtered rows 2" = Table.SelectRows(#"Renamed columns", each ([emp_name] <> "W. Justin Davis")),
  #"Inserted conditional column 1" = Table.AddColumn(#"Filtered rows 2", "admin_adjust", each if Text.Contains([emp_name], "Tammy") then 9 else if Text.Contains([emp_name], "Russell") then 9 else ""),
  #"Inserted conditional column 2" = Table.AddColumn(#"Inserted conditional column 1", "inv", each if [admin_adjust] = 9 then 9 else [invoice_hours]),
  #"Removed columns" = Table.RemoveColumns(#"Inserted conditional column 2", {"admin_adjust", "invoice_hours"}),
  #"Reordered columns 1" = Table.ReorderColumns(#"Removed columns", {"emp_name", "punch_hours", "service_hours", "inv", "fep_flag"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Reordered columns 1", {{"inv", "invoice_hours"}}),
  #"Added custom" = Table.AddColumn(#"Renamed columns 1", "pay_period", each "9/14 to 9/27")
in
  #"Added custom";
shared #"Sep 28 to Oct 10" = let
  Source = Table.Combine({raw_fep_punches, raw_credible_services}),
  #"Replaced value" = Table.ReplaceValue(Source, null, 0, Replacer.ReplaceValue, {"Count"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", null, 0, Replacer.ReplaceValue, {"service_hours"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "Management and General", "EPICenter", Replacer.ReplaceValue, {"dept"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "BehavioralHealth-Youth(Desoto)", "EPICenter", Replacer.ReplaceValue, {"dept"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "Behavioral Health-Youth(Caddo)", "EPICenter", Replacer.ReplaceValue, {"dept"}),
  #"Grouped rows" = Table.Group(#"Replaced value 4", {"emp_name", "date", "dept"}, {{"punch_hours", each List.Sum([Count]), type nullable number}, {"service_hours", each List.Sum([service_hours]), type nullable number}}),
  Custom = #"Grouped rows",
  #"Filtered rows" = Table.SelectRows(Custom, each [date] >= #date(2025, 9, 28) and [date] <= #date(2025, 10, 11)),
  #"Grouped rows 1" = Table.Group(#"Filtered rows", {"emp_name"}, {{"punch_hours", each List.Sum([punch_hours]), type nullable number}, {"service_hours", each List.Sum([service_hours]), type nullable number}}),
  #"Filtered rows 1" = Table.SelectRows(#"Grouped rows 1", each ([emp_name] <> "Justin Davis")),
  #"Inserted conditional column" = Table.AddColumn(#"Filtered rows 1", "fep_flag", each if [punch_hours] = 0 then "CHECK DEPT" else "FEP"),
  #"Inserted subtraction" = Table.AddColumn(#"Inserted conditional column", "Subtraction", each [punch_hours] - [service_hours], type number),
  #"Reordered columns" = Table.ReorderColumns(#"Inserted subtraction", {"emp_name", "punch_hours", "service_hours", "Subtraction", "fep_flag"}),
  #"Renamed columns" = Table.RenameColumns(#"Reordered columns", {{"Subtraction", "invoice_hours"}}),
  #"Filtered rows 2" = Table.SelectRows(#"Renamed columns", each ([emp_name] <> "W. Justin Davis")),
  #"Inserted conditional column 1" = Table.AddColumn(#"Filtered rows 2", "admin_adjust", each if Text.Contains([emp_name], "Tammy") then 9 else if Text.Contains([emp_name], "Russell") then 9 else ""),
  #"Inserted conditional column 2" = Table.AddColumn(#"Inserted conditional column 1", "inv", each if [admin_adjust] = 9 then 9 else [invoice_hours]),
  #"Removed columns" = Table.RemoveColumns(#"Inserted conditional column 2", {"admin_adjust", "invoice_hours"}),
  #"Reordered columns 1" = Table.ReorderColumns(#"Removed columns", {"emp_name", "punch_hours", "service_hours", "inv", "fep_flag"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Reordered columns 1", {{"inv", "invoice_hours"}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Renamed columns 1", "pay_period", each "9/28 to 10/11"), {{"pay_period", type text}}),
  #"Filtered rows 3" = Table.SelectRows(#"Added custom", each ([emp_name] <> "Michelle Merello"))
in
  #"Filtered rows 3";
shared #"Oct 12 to Oct 25" = let
  Source = Table.Combine({raw_fep_punches, raw_credible_services}),
  #"Replaced value" = Table.ReplaceValue(Source, null, 0, Replacer.ReplaceValue, {"Count"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", null, 0, Replacer.ReplaceValue, {"service_hours"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "Management and General", "EPICenter", Replacer.ReplaceValue, {"dept"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "BehavioralHealth-Youth(Desoto)", "EPICenter", Replacer.ReplaceValue, {"dept"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "Behavioral Health-Youth(Caddo)", "EPICenter", Replacer.ReplaceValue, {"dept"}),
  #"Grouped rows" = Table.Group(#"Replaced value 4", {"emp_name", "date", "dept"}, {{"punch_hours", each List.Sum([Count]), type nullable number}, {"service_hours", each List.Sum([service_hours]), type nullable number}}),
  Custom = #"Grouped rows",
  #"Filtered rows" = Table.SelectRows(Custom, each [date] >= #date(2025, 10, 12) and [date] <= #date(2025, 10, 25)),
  #"Grouped rows 1" = Table.Group(#"Filtered rows", {"emp_name"}, {{"punch_hours", each List.Sum([punch_hours]), type nullable number}, {"service_hours", each List.Sum([service_hours]), type nullable number}}),
  #"Filtered rows 1" = Table.SelectRows(#"Grouped rows 1", each ([emp_name] <> "Justin Davis")),
  #"Inserted conditional column" = Table.AddColumn(#"Filtered rows 1", "fep_flag", each if [punch_hours] = 0 then "CHECK DEPT" else "FEP"),
  #"Inserted subtraction" = Table.AddColumn(#"Inserted conditional column", "Subtraction", each [punch_hours] - [service_hours], type number),
  #"Reordered columns" = Table.ReorderColumns(#"Inserted subtraction", {"emp_name", "punch_hours", "service_hours", "Subtraction", "fep_flag"}),
  #"Renamed columns" = Table.RenameColumns(#"Reordered columns", {{"Subtraction", "invoice_hours"}}),
  #"Filtered rows 2" = Table.SelectRows(#"Renamed columns", each ([emp_name] <> "W. Justin Davis")),
  #"Inserted conditional column 1" = Table.AddColumn(#"Filtered rows 2", "admin_adjust", each if Text.Contains([emp_name], "Tammy") then 9 else if Text.Contains([emp_name], "Russell") then 9 else ""),
  #"Inserted conditional column 2" = Table.AddColumn(#"Inserted conditional column 1", "inv", each if [admin_adjust] = 9 then 9 else [invoice_hours]),
  #"Removed columns" = Table.RemoveColumns(#"Inserted conditional column 2", {"admin_adjust", "invoice_hours"}),
  #"Reordered columns 1" = Table.ReorderColumns(#"Removed columns", {"emp_name", "punch_hours", "service_hours", "inv", "fep_flag"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Reordered columns 1", {{"inv", "invoice_hours"}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Renamed columns 1", "pay_period", each "10/12 to 10/25"), {{"pay_period", type text}}),
  #"Filtered rows 3" = Table.SelectRows(#"Added custom", each ([emp_name] <> "Michelle Merello"))
in
  #"Filtered rows 3";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Appended FEP Billing_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Appended FEP Billing" = let
  Source = Table.Combine({#"Sep 14 to Sep 27", #"Sep 28 to Oct 10", #"Oct 12 to Oct 25"}),
  #"Reordered columns" = Table.ReorderColumns(Source, {"emp_name", "pay_period", "punch_hours", "service_hours", "invoice_hours", "fep_flag"}),
  #"Inserted conditional column" = Table.AddColumn(#"Reordered columns", "period_index", each if [pay_period] = "9/14 to 9/27" then 1 else if [pay_period] = "9/28 to 10/11" then 2 else if [pay_period] = "10/12 to 10/25" then 3 else ""),
  #"Sorted rows" = Table.Sort(#"Inserted conditional column", {{"period_index", Order.Ascending}, {"emp_name", Order.Ascending}}),
  #"Removed columns" = Table.RemoveColumns(#"Sorted rows", {"fep_flag", "period_index"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns", {{"invoice_hours", type number}, {"service_hours", type number}, {"pay_period", type text}})
in
  #"Changed column type";
shared #"Appended FEP Billing_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "fccbdcf0-4c42-431d-8e07-ae4b00bda780"]}[Data],
  TableNavigation = Navigation_2{[Id = "october_fep_billing", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared puch_ref_table = let
  Source = Table.Combine({raw_fep_punches, raw_credible_services})
in
  Source;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dynamic_fep_admin_time_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dynamic_fep_admin_time = let
  // ===== Pay period ranges from Accounting (add an index for sorting) =====
  PayPeriodsBase =
    #table(
      type table [pay_period_start = date, pay_period_end = date, pay_period = text],
      {
        { #date(2025,12,21), #date(2026, 1, 3), "12/21/2025-1/03/2026" },
        { #date(2026, 1, 4), #date(2026, 1,17), "1/4/2026-1/17/2026" },
        { #date(2026, 1,18), #date(2026, 1,31), "1/18/2026-1/31/2026" },
        { #date(2026, 2, 1), #date(2026, 2,14), "2/1/2026-2/14/2026" }
      }
    ),

  PayPeriods =
    Table.AddIndexColumn(
      PayPeriodsBase,
      "pay_period_idx",
      1,
      1,
      Int64.Type
    ),

  // ===== Your existing combine + cleanup =====
  Source = Table.Combine({raw_fep_punches, raw_credible_services}),
  #"Replaced value" = Table.ReplaceValue(Source, null, 0, Replacer.ReplaceValue, {"Count"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", null, 0, Replacer.ReplaceValue, {"service_hours"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "Management and General", "EPICenter", Replacer.ReplaceValue, {"dept"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "BehavioralHealth-Youth(Desoto)", "EPICenter", Replacer.ReplaceValue, {"dept"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "Behavioral Health-Youth(Caddo)", "EPICenter", Replacer.ReplaceValue, {"dept"}),

  // Ensure date is date
  #"Typed date" = Table.TransformColumns(#"Replaced value 4", {{"date", each Date.From(_), type date}}),

  // ===== Assign pay period by range match (return record with pay_period + idx) =====
  #"Added pay period record" =
    Table.AddColumn(
      #"Typed date",
      "pp",
      (r) =>
        let
          d = r[date],
          hit = Table.SelectRows(PayPeriods, each d >= [pay_period_start] and d <= [pay_period_end])
        in
          if Table.RowCount(hit) = 0 then
            null
          else
            [
              pay_period = hit{0}[pay_period],
              pay_period_idx = hit{0}[pay_period_idx]
            ],
      type record
    ),

  #"Expanded pp" = Table.ExpandRecordColumn(#"Added pay period record", "pp", {"pay_period", "pay_period_idx"}, {"pay_period", "pay_period_idx"}),

  // Optional: drop rows that aren't inside your provided periods
  #"Filtered to pay periods" = Table.SelectRows(#"Expanded pp", each [pay_period] <> null),

  // ===== Your grouping, but include pay_period_idx so sort is stable =====
  #"Grouped rows" =
    Table.Group(
      #"Filtered to pay periods",
      {"emp_name", "pay_period", "pay_period_idx", "date", "dept"},
      {
        {"punch_hours", each List.Sum([Count]), type nullable number},
        {"service_hours", each List.Sum([service_hours]), type nullable number}
      }
    ),

  #"Grouped rows 1" =
    Table.Group(
      #"Grouped rows",
      {"emp_name", "pay_period", "pay_period_idx"},
      {
        {"punch_hours", each List.Sum([punch_hours]), type nullable number},
        {"service_hours", each List.Sum([service_hours]), type nullable number}
      }
    ),

  #"Filtered rows 1" = Table.SelectRows(#"Grouped rows 1", each ([emp_name] <> "Justin Davis")),
  #"Inserted conditional column" = Table.AddColumn(#"Filtered rows 1", "fep_flag", each if [punch_hours] = 0 then "CHECK DEPT" else "FEP"),
  #"Inserted subtraction" = Table.AddColumn(#"Inserted conditional column", "invoice_hours", each [punch_hours] - [service_hours], type number),

  #"Filtered rows 2" = Table.SelectRows(#"Inserted subtraction", each ([emp_name] <> "W. Justin Davis")),

  #"Inserted conditional column 1" =
    Table.AddColumn(
      #"Filtered rows 2",
      "admin_adjust",
      each if Text.Contains([emp_name], "Tammy") then 9 else if Text.Contains([emp_name], "Russell") then 9 else null
    ),

  #"Inserted conditional column 2" =
    Table.AddColumn(
      #"Inserted conditional column 1",
      "inv",
      each if [admin_adjust] = 9 then 9 else [invoice_hours]
    ),

  #"Removed columns" = Table.RemoveColumns(#"Inserted conditional column 2", {"admin_adjust", "invoice_hours"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"inv", "invoice_hours"}}),

  #"Reordered columns" =
    Table.ReorderColumns(
      #"Renamed columns 1",
      {"emp_name", "pay_period", "pay_period_idx", "punch_hours", "service_hours", "invoice_hours", "fep_flag"}
    ),

  #"Filtered rows" = Table.SelectRows(#"Reordered columns", each ([emp_name] <> "Michelle Merello")),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows", {"emp_name"}, fep_employees, {"employee_name"}, "fep_employees", JoinKind.LeftOuter),
  #"Expanded fep_employees" = Table.ExpandTableColumn(#"Merged queries", "fep_employees", {"hourly_rate"}, {"hourly_rate"}),

  #"Reordered columns 1" =
    Table.ReorderColumns(
      #"Expanded fep_employees",
      {"emp_name", "pay_period", "pay_period_idx", "punch_hours", "service_hours", "invoice_hours", "hourly_rate", "fep_flag"}
    ),

  // ===== Final: sort by employee then pay period (by idx) =====
  #"Sorted rows" = Table.Sort(#"Reordered columns 1", {{"emp_name", Order.Ascending}, {"pay_period_idx", Order.Ascending}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Sorted rows", {"fep_flag"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"hourly_rate", Currency.Type}, {"invoice_hours", type number}, {"pay_period_idx", Int64.Type}, {"pay_period", type text}})
in
  #"Changed column type";
shared dynamic_fep_admin_time_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "fccbdcf0-4c42-431d-8e07-ae4b00bda780"]}[Data],
  TableNavigation = Navigation_2{[Id = "dynamic_fep_admin_time", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
