[StagingDefinition = [Kind = "FastCopy"]]
section Section1;

// ===== CONFIGURATION PARAMETERS =====
// Dataflow last updated: 2026-01-05
shared WorkspaceId = "4241c9c7-5818-4c56-9220-faf632741770" meta [Description = "Fabric workspace ID"];
shared LakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910" meta [Description = "ServicePoint lakehouse ID"];

// Reporting period configuration
shared ReportingMonths = {"August", "September", "October"} meta [Description = "Months to include in attendance calculations"];
shared ReportingPeriodName = "1st 9 Weeks" meta [Description = "Name of the reporting period"];
shared MinimumDaysEnrolled = 7 meta [Description = "Minimum days enrolled to be included in calculations"];
shared EntryExitDateFilter = #date(2025, 9, 1) meta [Description = "Only include entries/exits after this date"];

// Attendance threshold configuration
shared HighAttendanceThreshold = 0.80 meta [Description = "Attendance threshold for 80% schools"];
shared StandardAttendanceThreshold = 0.60 meta [Description = "Attendance threshold for non-80% schools"];
shared HighAttendanceSchools = {"Bethune/Oak Park", "Broadmoor STEM", "Creswell Elementary"} meta [Description = "Schools requiring 80% attendance"];

// ===== SOURCE DATA QUERIES =====
shared #"2526_lh_clients" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = WorkspaceId]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = LakehouseId]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "2526_lh_clients", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each [days_enrolled] > MinimumDaysEnrolled) // Filter to clients enrolled more than minimum days
in
  #"Filtered rows";

shared raw_lighthouse_registration = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = WorkspaceId]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = LakehouseId]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lighthouse_registration", ItemKind = "Table"]}[Data] // Load lighthouse registration data with location and school info
in
  #"Navigation 2";

shared raw_sp_demographics = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = WorkspaceId]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = LakehouseId]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_demographics", ItemKind = "Table"]}[Data] // Load ServicePoint demographics data
in
  #"Navigation 2";

shared raw_sp_entries = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = WorkspaceId]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = LakehouseId]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each [exit_date] > EntryExitDateFilter or [exit_date] = null), // Filter to recent entries/exits
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each ([program] = "VOANLA 4002 -The LightHouse - School Sites" or [program] = "VOANLA 4080 -The LightHouse - Community Sites" or [program] = "VOANLA 4080a - Teen Club")), // Filter to Lighthouse programs only
  #"Grouped rows" = Table.Group(#"Filtered rows 1", {"client_id"}, {{"Count", each List.Max([entry_date]), type nullable date}, {"exit_date", each List.Max([exit_date]), type nullable date}, {"program", each List.Max([program]), type nullable text}}) // Get most recent entry per client
in
  #"Grouped rows";

shared #"silver_lighthouse Lighthouse Matched with JCampus" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = WorkspaceId]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = LakehouseId]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "silver_lighthouse.Lighthouse Matched with JCampus", ItemKind = "Table"]}[Data] // Load matched data with JCampus school names
in
  #"Navigation 2";

// ===== BASE ATTENDANCE CALCULATION =====
shared base_client_attendance = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = WorkspaceId]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = LakehouseId]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "v_client_month_rollups", ItemKind = "View"]}[Data],
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Navigation 2", "today", each DateTime.Date(DateTimeZone.LocalNow())), {{"today", type datetime}}), // Add current date for tracking
  #"Filtered rows" = Table.SelectRows(#"Added custom", each List.Contains(ReportingMonths, [month_name])), // Filter to configured reporting months
  #"Merged queries" = Table.NestedJoin(#"Filtered rows", {"client_id"}, raw_lighthouse_registration, {"client_id"}, "raw_lighthouse_registration", JoinKind.LeftOuter), // Join to get registration details
  #"Added custom 1" = Table.AddColumn(#"Merged queries", "reporting_period", each ReportingPeriodName), // Add reporting period name
  #"Expanded raw_lighthouse_registration" = Table.ExpandTableColumn(#"Added custom 1", "raw_lighthouse_registration", {"app_date", "lighthouse_location_enrolling", "school_name"}, {"app_date", "lighthouse_location_enrolling", "school_name"}), // Expand registration fields
  #"Removed columns" = Table.RemoveColumns(#"Expanded raw_lighthouse_registration", {"max830", "max92", "max98"}), // Remove unused max columns
  #"Removed duplicates" = Table.Distinct(#"Removed columns", {"client_id"}), // Deduplicate by client
  #"Grouped rows" = Table.Group(#"Removed duplicates", {"client_id", "client_last_name", "client_first_name", "lighthouse_location_enrolling"}, {{"sessions_attended", each List.Sum([Count]), type nullable number}, {"sessions_possible", each List.Sum([max99]), type nullable number}}), // Sum attended and possible sessions
  #"Merged queries 1" = Table.NestedJoin(#"Grouped rows", {"client_id"}, #"silver_lighthouse Lighthouse Matched with JCampus", {"client_id"}, "jcampus_match", JoinKind.LeftOuter), // Join to get JCampus school name
  #"Expanded jcampus_match" = Table.ExpandTableColumn(#"Merged queries 1", "jcampus_match", {"School Name"}, {"jcampus_school_name"}), // Expand JCampus school name
  #"Added school_name" = Table.AddColumn(#"Expanded jcampus_match", "school_name", each 
    if [jcampus_school_name] <> null and [jcampus_school_name] <> "" then 
      [jcampus_school_name]
    else if [lighthouse_location_enrolling] = "School Site - Broadmoor" then 
      "Broadmoor STEM"
    else if [lighthouse_location_enrolling] = "School Site - Creswell" then 
      "Creswell Elementary"
    else if [lighthouse_location_enrolling] = "School Site - Oak Park" then 
      "Bethune/Oak Park"
    else 
      [lighthouse_location_enrolling]), // Normalize school site names to match JCampus, otherwise use lighthouse location
  #"Added is_80_school" = Table.AddColumn(#"Added school_name", "is_80_school", each List.Contains(HighAttendanceSchools, [school_name])), // Flag if school requires 80% attendance (based on final school_name)
  #"Added required_threshold" = Table.AddColumn(#"Added is_80_school", "required_threshold", each if [is_80_school] then HighAttendanceThreshold else StandardAttendanceThreshold), // Set required threshold based on school type
  #"Added attendance_rate" = Table.AddColumn(#"Added required_threshold", "attendance_rate", each [sessions_attended] / [sessions_possible], type number), // Calculate attendance rate
  #"Added meets_threshold" = Table.AddColumn(#"Added attendance_rate", "meets_threshold", each [attendance_rate] >= [required_threshold]), // Flag if client meets their threshold
  #"Added attendance_flag" = Table.AddColumn(#"Added meets_threshold", "attendance_flag", each 
    if [attendance_rate] >= HighAttendanceThreshold then "80%+ GOOD"
    else if [attendance_rate] >= StandardAttendanceThreshold then "60%+ GOOD"
    else "BELOW THRESHOLD"), // Create readable flag
  #"Changed column type" = Table.TransformColumnTypes(#"Added attendance_flag", {{"attendance_rate", Percentage.Type}}) // Format attendance rate as percentage
in
  #"Changed column type";

// ===== OUTPUT: CLIENT-LEVEL DETAIL =====
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "client_attendance_detail_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared client_attendance_detail = base_client_attendance;

shared client_attendance_detail_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = WorkspaceId]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = LakehouseId]}[Data],
  TableNavigation = Navigation_2{[Id = "lighthouse_client_attendance_detail", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;

// ===== OUTPUT: 80% SCHOOLS SUMMARY =====
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "attendance_summary_80_schools_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared attendance_summary_80_schools = let
  Source = base_client_attendance,
  #"Filtered to 80 schools" = Table.SelectRows(Source, each [is_80_school] = true), // Filter to only 80% schools
  #"Removed duplicates" = Table.Distinct(#"Filtered to 80 schools", {"client_id"}), // Deduplicate clients
  #"Count meeting threshold" = Table.Group(#"Removed duplicates", {"lighthouse_location_enrolling"}, {
    {"students_meeting_80", each List.Count(List.Select([meets_threshold], each _ = true)), Int64.Type},
    {"total_students", each List.Count([client_id]), Int64.Type}
  }), // Count students meeting 80% threshold per lighthouse location
  #"Added percentage" = Table.AddColumn(#"Count meeting threshold", "pct_meeting_threshold", each [students_meeting_80] / [total_students], Percentage.Type) // Calculate percentage meeting threshold
in
  #"Added percentage";

shared attendance_summary_80_schools_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = WorkspaceId]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = LakehouseId]}[Data],
  TableNavigation = Navigation_2{[Id = "lighthouse_80_schools_summary", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;

// ===== OUTPUT: NON-80% SCHOOLS SUMMARY =====
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "attendance_summary_other_schools_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared attendance_summary_other_schools = let
  Source = base_client_attendance,
  #"Filtered to non-80 schools" = Table.SelectRows(Source, each [is_80_school] = false), // Filter to locations requiring 60% attendance
  #"Filter out specific locations" = Table.SelectRows(#"Filtered to non-80 schools", each [lighthouse_location_enrolling] <> "Highland" and [lighthouse_location_enrolling] <> "Teen Club"), // Exclude certain locations from summary
  #"Removed duplicates" = Table.Distinct(#"Filter out specific locations", {"client_id"}), // Deduplicate clients
  #"Count meeting threshold" = Table.Group(#"Removed duplicates", {"lighthouse_location_enrolling"}, {
    {"students_meeting_60", each List.Count(List.Select([meets_threshold], each _ = true)), Int64.Type},
    {"total_students", each List.Count([client_id]), Int64.Type}
  }), // Count students meeting 60% threshold per lighthouse location
  #"Added percentage" = Table.AddColumn(#"Count meeting threshold", "pct_meeting_threshold", each [students_meeting_60] / [total_students], Percentage.Type) // Calculate percentage meeting threshold
in
  #"Added percentage";

shared attendance_summary_other_schools_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = WorkspaceId]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = LakehouseId]}[Data],
  TableNavigation = Navigation_2{[Id = "lighthouse_other_schools_summary", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;

// ===== OUTPUT: AVERAGE SESSIONS BY LIGHTHOUSE LOCATION =====
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "attendance_averages_by_location_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared attendance_averages_by_location = let
  Source = base_client_attendance,
  #"Removed duplicates" = Table.Distinct(Source, {"client_id"}), // Deduplicate clients
  #"Grouped by location" = Table.Group(#"Removed duplicates", {"lighthouse_location_enrolling"}, {
    {"avg_sessions_attended", each List.Average([sessions_attended]), type nullable number},
    {"avg_sessions_possible", each List.Average([sessions_possible]), type nullable number},
    {"student_count", each List.Count([client_id]), Int64.Type}
  }), // Calculate average attended and possible sessions per lighthouse location
  #"Added avg_attendance_rate" = Table.AddColumn(#"Grouped by location", "avg_attendance_rate", each [avg_sessions_attended] / [avg_sessions_possible], Percentage.Type), // Calculate average attendance rate
  #"Sorted by location" = Table.Sort(#"Added avg_attendance_rate", {{"lighthouse_location_enrolling", Order.Ascending}}) // Sort alphabetically by location
in
  #"Sorted by location";

shared attendance_averages_by_location_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = WorkspaceId]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = LakehouseId]}[Data],
  TableNavigation = Navigation_2{[Id = "lighthouse_attendance_averages_by_location", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;

// ===== OUTPUT: UNMATCHED CLIENTS =====
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "unmatched_clients_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared unmatched_clients = let
  Source = base_client_attendance,
  #"Filtered to unmatched" = Table.SelectRows(Source, each [jcampus_school_name] = null or [jcampus_school_name] = ""), // Filter to clients without JCampus school match
  #"Removed duplicates" = Table.Distinct(#"Filtered to unmatched", {"client_id"}), // Deduplicate clients
  #"Selected columns" = Table.SelectColumns(#"Removed duplicates", {"client_id", "client_first_name", "client_last_name", "lighthouse_location_enrolling", "sessions_attended", "sessions_possible", "attendance_rate"}), // Keep relevant columns for investigation
  #"Grouped by location" = Table.Group(#"Selected columns", {"lighthouse_location_enrolling"}, {{"unmatched_count", each List.Count([client_id]), Int64.Type}}), // Count unmatched clients per lighthouse location
  #"Sorted by count" = Table.Sort(#"Grouped by location", {{"unmatched_count", Order.Descending}}) // Sort by highest unmatched count
in
  #"Sorted by count";

shared unmatched_clients_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = WorkspaceId]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = LakehouseId]}[Data],
  TableNavigation = Navigation_2{[Id = "lighthouse_unmatched_clients_summary", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
