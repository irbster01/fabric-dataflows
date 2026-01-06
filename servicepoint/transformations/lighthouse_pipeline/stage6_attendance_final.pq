[StagingDefinition = [Kind = "FastCopy"]]
section Section1;

// Constants
shared HighAttendanceThreshold = 0.80;
shared StandardAttendanceThreshold = 0.60;
shared HighAttendanceSchools = {"Bethune/Oak Park", "Broadmoor STEM", "Creswell Elementary"};

// Source: VOA_Nexus lakehouse
shared lighthouse_sessions_attended = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  lighthouse_sessions_attended = #"Navigation 1"{[Id = "lighthouse_sessions_attended", ItemKind = "Table"]}[Data]
in
  lighthouse_sessions_attended;

shared lighthouse_sessions_possible = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  lighthouse_sessions_possible = #"Navigation 1"{[Id = "lighthouse_sessions_possible", ItemKind = "Table"]}[Data]
in
  lighthouse_sessions_possible;

shared lighthouse_client_master = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  lighthouse_client_master = #"Navigation 1"{[Id = "lighthouse_client_master", ItemKind = "Table"]}[Data]
in
  lighthouse_client_master;

// Source: ServicePoint lakehouse
shared jcampus_match = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  jcampus_match = #"Navigation 1"{[Id = "silver_lighthouse.Lighthouse Matched with JCampus", ItemKind = "Table"]}[Data]
in
  jcampus_match;

// Main attendance detail table
shared lighthouse_attendance_detail = let
  // Join attended and possible
  #"Joined attended and possible" = Table.Join(lighthouse_sessions_attended, {"client_id", "nine_week_period"}, lighthouse_sessions_possible, {"client_id", "nine_week_period"}, JoinKind.FullOuter),
  // Join with client master
  #"Merged with client master" = Table.Join(#"Joined attended and possible", "client_id", lighthouse_client_master, "client_id", JoinKind.LeftOuter),
  // Join with JCampus
  #"Merged with jcampus" = Table.Join(#"Merged with client master", "client_id", jcampus_match, "client_id", JoinKind.LeftOuter),
  // Add school_name (normalize locations)
  #"Added school_name" = Table.AddColumn(#"Merged with jcampus", "school_name", each
    if [School Name] <> null and [School Name] <> "" then
      [School Name]
    else if [lighthouse_location_enrolling] = "School Site - Broadmoor" then
      "Broadmoor STEM"
    else if [lighthouse_location_enrolling] = "School Site - Creswell" then
      "Creswell Elementary"
    else if [lighthouse_location_enrolling] = "School Site - Oak Park" then
      "Bethune/Oak Park"
    else
      [lighthouse_location_enrolling], type nullable text),
  // Add is_80_school flag
  #"Added is_80_school" = Table.AddColumn(#"Added school_name", "is_80_school", each
    List.Contains(HighAttendanceSchools, [school_name]), type logical),
  // Add required_threshold
  #"Added required_threshold" = Table.AddColumn(#"Added is_80_school", "required_threshold", each
    if [is_80_school] then HighAttendanceThreshold else StandardAttendanceThreshold, type number),
  // Add attendance_rate
  #"Added attendance_rate" = Table.AddColumn(#"Added required_threshold", "attendance_rate", each
    if [sessions_possible] = null or [sessions_possible] = 0 then null
    else [sessions_attended] / [sessions_possible], Percentage.Type),
  // Add meets_threshold flag
  #"Added meets_threshold" = Table.AddColumn(#"Added attendance_rate", "meets_threshold", each
    if [attendance_rate] = null then false
    else [attendance_rate] >= [required_threshold], type logical),
  // Add attendance_flag
  #"Added attendance_flag" = Table.AddColumn(#"Added meets_threshold", "attendance_flag", each
    if [attendance_rate] = null then "NO SESSIONS POSSIBLE"
    else if [attendance_rate] >= HighAttendanceThreshold then "80%+ GOOD"
    else if [attendance_rate] >= StandardAttendanceThreshold then "60%+ GOOD"
    else "BELOW THRESHOLD", type text),
  // Select final columns
  #"Select columns" = Table.SelectColumns(#"Added attendance_flag", {
    "client_id",
    "nine_week_period",
    "first_name",
    "last_name",
    "lighthouse_location_enrolling",
    "entry_date",
    "exit_date",
    "sessions_attended",
    "sessions_possible",
    "school_name",
    "is_80_school",
    "required_threshold",
    "attendance_rate",
    "meets_threshold",
    "attendance_flag"
  })
in
  #"Select columns";

// Summary for 80% schools
shared attendance_summary_80_schools = let
  Source = lighthouse_attendance_detail,
  #"Filtered to 80 schools" = Table.SelectRows(Source, each [is_80_school] = true),
  #"Removed duplicates" = Table.Distinct(#"Filtered to 80 schools", {"client_id", "nine_week_period"}),
  #"Grouped by location and period" = Table.Group(#"Removed duplicates", {"lighthouse_location_enrolling", "nine_week_period"}, {
    {"students_meeting_80", each List.Count(List.Select([meets_threshold], each _ = true)), Int64.Type},
    {"total_students", each Table.RowCount(_), Int64.Type}
  }),
  #"Added percentage" = Table.AddColumn(#"Grouped by location and period", "pct_meeting_threshold", each
    [students_meeting_80] / [total_students], Percentage.Type)
in
  #"Added percentage";

// Summary for other schools
shared attendance_summary_other_schools = let
  Source = lighthouse_attendance_detail,
  #"Filtered to non-80 schools" = Table.SelectRows(Source, each [is_80_school] = false),
  #"Filtered out specific" = Table.SelectRows(#"Filtered to non-80 schools", each
    [lighthouse_location_enrolling] <> "Highland" and
    [lighthouse_location_enrolling] <> "Teen Club" and
    [lighthouse_location_enrolling] <> "School Site - Judson"),
  #"Removed duplicates" = Table.Distinct(#"Filtered out specific", {"client_id", "nine_week_period"}),
  #"Grouped by location and period" = Table.Group(#"Removed duplicates", {"lighthouse_location_enrolling", "nine_week_period"}, {
    {"students_meeting_60", each List.Count(List.Select([meets_threshold], each _ = true)), Int64.Type},
    {"total_students", each Table.RowCount(_), Int64.Type}
  }),
  #"Added percentage" = Table.AddColumn(#"Grouped by location and period", "pct_meeting_threshold", each
    [students_meeting_60] / [total_students], Percentage.Type)
in
  #"Added percentage";

// Average attendance by location
shared attendance_averages_by_location = let
  Source = lighthouse_attendance_detail,
  #"Filtered out specific" = Table.SelectRows(Source, each
    [lighthouse_location_enrolling] <> null and
    [lighthouse_location_enrolling] <> "School Site - Judson"),
  #"Removed duplicates" = Table.Distinct(#"Filtered out specific", {"client_id", "nine_week_period"}),
  #"Grouped by location and period" = Table.Group(#"Removed duplicates", {"lighthouse_location_enrolling", "nine_week_period"}, {
    {"avg_sessions_attended", each List.Average([sessions_attended]), type nullable number},
    {"avg_sessions_possible", each List.Average([sessions_possible]), type nullable number},
    {"student_count", each Table.RowCount(_), Int64.Type}
  }),
  #"Added avg_attendance_rate" = Table.AddColumn(#"Grouped by location and period", "avg_attendance_rate", each
    [avg_sessions_attended] / [avg_sessions_possible], Percentage.Type),
  #"Sorted" = Table.Sort(#"Added avg_attendance_rate", {
    {"lighthouse_location_enrolling", Order.Ascending},
    {"nine_week_period", Order.Ascending}
  })
in
  #"Sorted";
