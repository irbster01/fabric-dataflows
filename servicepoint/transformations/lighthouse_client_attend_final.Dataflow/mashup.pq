[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
WorkspaceId = "4241c9c7-5818-4c56-9220-faf632741770" meta [Description = "Fabric workspace ID"];
LakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910" meta [Description = "ServicePoint lakehouse ID"];
MinimumDaysEnrolled = 7 meta [Description = "Minimum days enrolled to be included in calculations"];
EntryExitDateFilter = #date(2025, 9, 1) meta [Description = "Only include entries/exits after this date"];
HighAttendanceThreshold = 0.80 meta [Description = "Attendance threshold for 80% schools"];
StandardAttendanceThreshold = 0.60 meta [Description = "Attendance threshold for non-80% schools"];
HighAttendanceSchools = {"Bethune/Oak Park", "Broadmoor STEM", "Creswell Elementary"} meta [Description = "Schools requiring 80% attendance"];
shared #"2526_lh_clients" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"2526_lh_clients" = #"Navigation 1"{[Id = "2526_lh_clients", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"2526_lh_clients", each [days_enrolled] > MinimumDaysEnrolled) // Filter to clients enrolled more than minimum days
in
  #"Filtered rows";
shared raw_lighthouse_registration = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  raw_lighthouse_registration = #"Navigation 1"{[Id = "raw_lighthouse_registration", ItemKind = "Table"]}[Data] // Load lighthouse registration data with location and school info
in
  raw_lighthouse_registration;
shared raw_sp_demographics = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  raw_sp_demographics = #"Navigation 1"{[Id = "raw_sp_demographics", ItemKind = "Table"]}[Data] // Load ServicePoint demographics data
in
  raw_sp_demographics;
shared raw_sp_entries = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  raw_sp_entries = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(raw_sp_entries, each [exit_date] > EntryExitDateFilter or [exit_date] = null), // Filter to recent entries/exits
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each ([program] = "VOANLA 4002 -The LightHouse - School Sites" or [program] = "VOANLA 4080 -The LightHouse - Community Sites" or [program] = "VOANLA 4080a - Teen Club")), // Filter to Lighthouse programs only
  #"Grouped rows" = Table.Group(#"Filtered rows 1", {"client_id"}, {{"Count", each List.Max([entry_date]), type nullable date}, {"exit_date", each List.Max([exit_date]), type nullable date}, {"program", each List.Max([program]), type nullable text}}) // Get most recent entry per client
in
  #"Grouped rows";
shared #"silver_lighthouse Lighthouse Matched with JCampus" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Lighthouse Matched with JCampus" = #"Navigation 1"{[Id = "silver_lighthouse.Lighthouse Matched with JCampus", ItemKind = "Table"]}[Data] // Load matched data with JCampus school names
in
  #"Lighthouse Matched with JCampus";
shared raw_sp_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  raw_sp_services = #"Navigation 1"{[Id = "raw_sp_services", ItemKind = "Table"]}[Data],
  #"Added nine_week_period" = Table.AddColumn(raw_sp_services, "nine_week_period", each 
    if [service_date] >= #date(2025, 8, 13) and [service_date] <= #date(2025, 10, 17) then "1st 9 Weeks"
    else if [service_date] >= #date(2025, 10, 20) and [service_date] <= #date(2025, 12, 17) then "2nd 9 Weeks"
    else if [service_date] >= #date(2026, 1, 6) and [service_date] <= #date(2026, 3, 6) then "3rd 9 Weeks"
    else if [service_date] >= #date(2026, 3, 16) and [service_date] <= #date(2026, 5, 21) then "4th 9 Weeks"
    else null, type text), // Assign each service to a 9-week period based on service_date
  #"Filtered to nine weeks" = Table.SelectRows(#"Added nine_week_period", each [nine_week_period] <> null) // Keep only services within defined 9-week periods
in
  #"Filtered to nine weeks";
shared lh_dates = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  lighthouse_dates = #"Navigation 1"{[Id = "lighthouse_dates", ItemKind = "Table"]}[Data] // Load lighthouse program operating dates calendar
in
  lighthouse_dates;
shared base_client_attendance = let
  Source = raw_sp_services,
  #"Merged with entries to filter" = Table.NestedJoin(Source, {"client_id"}, raw_sp_entries, {"client_id"}, "entry_check", JoinKind.Inner), // Inner join to keep only Lighthouse clients
  #"Removed entry_check" = Table.RemoveColumns(#"Merged with entries to filter", {"entry_check"}), // Remove the nested table, we just needed it for filtering
  #"Grouped services" = Table.Group(#"Removed entry_check", {"client_id", "nine_week_period"}, {{"sessions_attended", each Table.RowCount(_), Int64.Type}}), // Count services per client per 9-week period
  #"Merged with demographics" = Table.NestedJoin(#"Grouped services", {"client_id"}, raw_sp_demographics, {"client_id"}, "demographics", JoinKind.LeftOuter), // Join to get client names
  #"Expanded demographics" = Table.ExpandTableColumn(#"Merged with demographics", "demographics", {"last_name", "first_name"}, {"last_name", "first_name"}),
  #"Merged with registration" = Table.NestedJoin(#"Expanded demographics", {"client_id"}, raw_lighthouse_registration, {"client_id"}, "registration", JoinKind.LeftOuter), // Join to get lighthouse location
  #"Expanded registration" = Table.ExpandTableColumn(#"Merged with registration", "registration", {"lighthouse_location_enrolling"}, {"lighthouse_location_enrolling"}),
  #"Merged with entries" = Table.NestedJoin(#"Expanded registration", {"client_id"}, raw_sp_entries, {"client_id"}, "entry_data", JoinKind.LeftOuter), // Join to get entry and exit dates
  #"Expanded entry_data" = Table.ExpandTableColumn(#"Merged with entries", "entry_data", {"Count", "exit_date"}, {"entry_date", "exit_date"}), // Count field has entry_date from grouped entries
  #"Merged with dates" = Table.NestedJoin(#"Expanded entry_data", {"nine_week_period"}, lh_dates, {"nine_week_period"}, "program_dates", JoinKind.LeftOuter), // Join to program dates calendar
  #"Added sessions_possible" = Table.AddColumn(#"Merged with dates", "sessions_possible", each 
    let
      dates_table = [program_dates],
      client_entry = [entry_date],
      client_exit = [exit_date]
    in
      if dates_table = null then 0
      else 
        Table.RowCount(
          Table.SelectRows(dates_table, each 
            [date] <> null and 
            [IsSessionDay] = true and
            (client_entry = null or [date] >= client_entry) and
            (client_exit = null or [date] <= client_exit)
          )
        ), Int64.Type), // Count session days between client's entry and exit dates within this 9-week period
  #"Removed program_dates" = Table.RemoveColumns(#"Added sessions_possible", {"program_dates"}), // Clean up nested table
  #"Merged queries 1" = Table.NestedJoin(#"Removed program_dates", {"client_id"}, #"silver_lighthouse Lighthouse Matched with JCampus", {"client_id"}, "jcampus_match", JoinKind.LeftOuter), // Join to get JCampus school name
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
  #"Added is_80_school" = Table.AddColumn(#"Added school_name", "is_80_school", each List.Contains(HighAttendanceSchools, [school_name]), type logical), // Flag if school requires 80% attendance (based on final school_name)
  #"Added required_threshold" = Table.AddColumn(#"Added is_80_school", "required_threshold", each if [is_80_school] then HighAttendanceThreshold else StandardAttendanceThreshold, type number), // Set required threshold based on school type
  #"Added attendance_rate" = Table.AddColumn(#"Added required_threshold", "attendance_rate", each if [sessions_possible] = 0 then null else [sessions_attended] / [sessions_possible], type number), // Calculate attendance rate, null if no possible sessions
  #"Added meets_threshold" = Table.AddColumn(#"Added attendance_rate", "meets_threshold", each if [attendance_rate] = null then false else [attendance_rate] >= [required_threshold], type logical), // Flag if client meets their threshold
  #"Added attendance_flag" = Table.AddColumn(#"Added meets_threshold", "attendance_flag", each 
    if [attendance_rate] = null then "NO SESSIONS POSSIBLE"
    else if [attendance_rate] >= HighAttendanceThreshold then "80%+ GOOD"
    else if [attendance_rate] >= StandardAttendanceThreshold then "60%+ GOOD"
    else "BELOW THRESHOLD", type text), // Create readable flag
  #"Changed column type" = Table.TransformColumnTypes(#"Added attendance_flag", {
    {"client_id", Int64.Type},
    {"nine_week_period", type text},
    {"sessions_attended", Int64.Type},
    {"last_name", type text},
    {"first_name", type text},
    {"lighthouse_location_enrolling", type text},
    {"entry_date", type date},
    {"exit_date", type date},
    {"sessions_possible", Int64.Type},
    {"jcampus_school_name", type text},
    {"school_name", type text},
    {"attendance_rate", Percentage.Type}
  }) // Ensure all columns have correct types
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "client_attendance_detail_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared client_attendance_detail = 
  Table.RemoveColumns(base_client_attendance, {"last_name", "first_name"});
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "attendance_summary_80_schools_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared attendance_summary_80_schools = let
  Source = base_client_attendance,
  #"Filtered to 80 schools" = Table.SelectRows(Source, each [is_80_school] = true), // Filter to only 80% schools
  #"Removed duplicates" = Table.Distinct(#"Filtered to 80 schools", {"client_id", "nine_week_period"}), // Deduplicate clients per period
  #"Count meeting threshold" = Table.Group(#"Removed duplicates", {"lighthouse_location_enrolling", "nine_week_period"}, {
    {"students_meeting_80", each List.Count(List.Select([meets_threshold], each _ = true)), Int64.Type},
    {"total_students", each List.Count([client_id]), Int64.Type}
  }), // Count students meeting 80% threshold per lighthouse location and period
  #"Added percentage" = Table.AddColumn(#"Count meeting threshold", "pct_meeting_threshold", each [students_meeting_80] / [total_students], Percentage.Type) // Calculate percentage meeting threshold
in
  #"Added percentage";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "attendance_summary_other_schools_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared attendance_summary_other_schools = let
  Source = base_client_attendance,
  #"Filtered to non-80 schools" = Table.SelectRows(Source, each [is_80_school] = false), // Filter to locations requiring 60% attendance
  #"Filter out specific locations" = Table.SelectRows(#"Filtered to non-80 schools", each [lighthouse_location_enrolling] <> "Highland" and [lighthouse_location_enrolling] <> "Teen Club"), // Exclude certain locations from summary
  #"Removed duplicates" = Table.Distinct(#"Filter out specific locations", {"client_id", "nine_week_period"}), // Deduplicate clients per period
  #"Count meeting threshold" = Table.Group(#"Removed duplicates", {"lighthouse_location_enrolling", "nine_week_period"}, {
    {"students_meeting_60", each List.Count(List.Select([meets_threshold], each _ = true)), Int64.Type},
    {"total_students", each List.Count([client_id]), Int64.Type}
  }), // Count students meeting 60% threshold per lighthouse location and period
  #"Added percentage" = Table.AddColumn(#"Count meeting threshold", "pct_meeting_threshold", each [students_meeting_60] / [total_students], Percentage.Type) // Calculate percentage meeting threshold
  ,
  #"Filtered rows" = Table.SelectRows(#"Added percentage", each ([lighthouse_location_enrolling] <> null and [lighthouse_location_enrolling] <> "School Site - Judson"))
in
  #"Filtered rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "attendance_averages_by_location_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared attendance_averages_by_location = let
  Source = base_client_attendance,
  #"Removed duplicates" = Table.Distinct(Source, {"client_id", "nine_week_period"}), // Deduplicate clients per period
  #"Grouped by location" = Table.Group(#"Removed duplicates", {"lighthouse_location_enrolling", "nine_week_period"}, {
    {"avg_sessions_attended", each List.Average([sessions_attended]), type nullable number},
    {"avg_sessions_possible", each List.Average([sessions_possible]), type nullable number},
    {"student_count", each List.Count([client_id]), Int64.Type}
  }), // Calculate average attended and possible sessions per lighthouse location and period
  #"Added avg_attendance_rate" = Table.AddColumn(#"Grouped by location", "avg_attendance_rate", each [avg_sessions_attended] / [avg_sessions_possible], Percentage.Type), // Calculate average attendance rate
  #"Sorted by location" = Table.Sort(#"Added avg_attendance_rate", {{"lighthouse_location_enrolling", Order.Ascending}, {"nine_week_period", Order.Ascending}}) // Sort alphabetically by location and period
  ,
  #"Filtered rows" = Table.SelectRows(#"Sorted by location", each ([lighthouse_location_enrolling] <> null and [lighthouse_location_enrolling] <> "School Site - Judson"))
in
  #"Filtered rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "unmatched_clients_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared unmatched_clients = let
  Source = base_client_attendance,
  #"Filtered to unmatched" = Table.SelectRows(Source, each [jcampus_school_name] = null or [jcampus_school_name] = ""), // Filter to clients without JCampus school match
  #"Removed duplicates" = Table.Distinct(#"Filtered to unmatched", {"client_id", "nine_week_period"}), // Deduplicate clients per period
  #"Selected columns" = Table.SelectColumns(#"Removed duplicates", {"client_id", "first_name", "last_name", "lighthouse_location_enrolling", "nine_week_period", "sessions_attended", "sessions_possible", "attendance_rate"}), // Keep relevant columns for investigation
  #"Grouped by location and period" = Table.Group(#"Selected columns", {"lighthouse_location_enrolling", "nine_week_period"}, {{"unmatched_count", each List.Count([client_id]), Int64.Type}}), // Count unmatched clients per lighthouse location and period
  #"Sorted by count" = Table.Sort(#"Grouped by location and period", {{"unmatched_count", Order.Descending}}) // Sort by highest unmatched count
in
  #"Sorted by count";
