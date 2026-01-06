[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared HighAttendanceThreshold = 0.80;
shared StandardAttendanceThreshold = 0.60;
shared HighAttendanceSchools = {"Bethune/Oak Park", "Broadmoor STEM", "Creswell Elementary"};
shared lighthouse_sessions_attended = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "lighthouse_sessions_attended", ItemKind = "Table"]}[Data],
  #"Changed types" = Table.TransformColumnTypes(#"Navigation 3", {{"client_id", Int64.Type}})
in
  #"Changed types";
shared lighthouse_sessions_possible = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "lighthouse_sessions_possible", ItemKind = "Table"]}[Data],
  #"Changed types" = Table.TransformColumnTypes(#"Navigation 3", {{"client_id", Int64.Type}})
in
  #"Changed types";
shared lighthouse_client_master = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "lighthouse_client_master", ItemKind = "Table"]}[Data],
  #"Changed types" = Table.TransformColumnTypes(#"Navigation 3", {{"client_id", Int64.Type}})
in
  #"Changed types";
shared jcampus_match = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  jcampus_match = #"Navigation 1"{[Id = "silver_lighthouse.Lighthouse Matched with JCampus", ItemKind = "Table"]}[Data],
  #"Changed types" = Table.TransformColumnTypes(jcampus_match, {{"client_id", Int64.Type}})
in
  #"Changed types";
[BindToDefaultDestination = true]
shared lighthouse_attendance_detail = let
  // Join attended and possible using NestedJoin with FullOuter
  #"Merged attended and possible" = Table.NestedJoin(lighthouse_sessions_attended, {"client_id", "nine_week_period"}, lighthouse_sessions_possible, {"client_id", "nine_week_period"}, "possible", JoinKind.FullOuter),
  #"Expanded possible" = Table.ExpandTableColumn(#"Merged attended and possible", "possible", {"client_id", "nine_week_period", "sessions_possible"}, {"possible_client_id", "possible_nine_week_period", "sessions_possible"}),
  // Coalesce client_id and nine_week_period from both sides of FullOuter join
  #"Added coalesced_client_id" = Table.AddColumn(#"Expanded possible", "coalesced_client_id", each if [client_id] <> null then [client_id] else [possible_client_id], type nullable number),
  #"Added coalesced_period" = Table.AddColumn(#"Added coalesced_client_id", "coalesced_period", each if [nine_week_period] <> null then [nine_week_period] else [possible_nine_week_period], type nullable text),
  #"Removed original keys" = Table.RemoveColumns(#"Added coalesced_period", {"client_id", "nine_week_period", "possible_client_id", "possible_nine_week_period"}),
  #"Renamed coalesced" = Table.RenameColumns(#"Removed original keys", {{"coalesced_client_id", "client_id"}, {"coalesced_period", "nine_week_period"}}),
  // Join with client master using NestedJoin
  #"Merged with client master" = Table.NestedJoin(#"Renamed coalesced", {"client_id"}, lighthouse_client_master, {"client_id"}, "client_master", JoinKind.LeftOuter),
  #"Expanded client master" = Table.ExpandTableColumn(#"Merged with client master", "client_master", {"first_name", "last_name", "entry_date", "exit_date", "lighthouse_location_enrolling", "program"}, {"first_name", "last_name", "entry_date", "exit_date", "lighthouse_location_enrolling", "program"}),
  // Join with JCampus using NestedJoin
  #"Merged with jcampus" = Table.NestedJoin(#"Expanded client master", {"client_id"}, jcampus_match, {"client_id"}, "jcampus", JoinKind.LeftOuter),
  #"Expanded jcampus" = Table.ExpandTableColumn(#"Merged with jcampus", "jcampus", {"School Name"}, {"School Name"}),
  // Add school_name (normalize locations)
  #"Added school_name" = Table.AddColumn(#"Expanded jcampus", "school_name", each
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
  #"Select columns" = Table.SelectColumns(#"Added attendance_flag", {"sessions_attended", "sessions_possible", "client_id", "nine_week_period", "first_name", "last_name", "entry_date", "exit_date", "lighthouse_location_enrolling", "program", "school_name", "is_80_school", "required_threshold", "attendance_rate", "meets_threshold", "attendance_flag"})
in
  #"Select columns";
[BindToDefaultDestination = true]
shared attendance_summary_80_schools = let
  Source = lighthouse_attendance_detail,
  #"Filtered to 80 schools" = Table.SelectRows(Source, each [is_80_school] = true),
  // Filter to only students enrolled during each specific 9-week period
  #"Filtered to enrolled" = Table.SelectRows(#"Filtered to 80 schools", each 
    let
      period = [nine_week_period],
      entry = [entry_date],
      exit_date = [exit_date],
      period_start = if period = "1st 9 Weeks" then #date(2025, 8, 13)
                     else if period = "2nd 9 Weeks" then #date(2025, 10, 20)
                     else if period = "3rd 9 Weeks" then #date(2026, 1, 6)
                     else if period = "4th 9 Weeks" then #date(2026, 3, 16)
                     else null,
      period_end = if period = "1st 9 Weeks" then #date(2025, 10, 17)
                   else if period = "2nd 9 Weeks" then #date(2025, 12, 17)
                   else if period = "3rd 9 Weeks" then #date(2026, 3, 6)
                   else if period = "4th 9 Weeks" then #date(2026, 5, 21)
                   else null
    in
      period_start <> null and
      (entry = null or entry <= period_end) and
      (exit_date = null or exit_date >= period_start)),
  #"Removed duplicates" = Table.Distinct(#"Filtered to enrolled", {"client_id", "nine_week_period"}),
  #"Grouped by location and period" = Table.Group(#"Removed duplicates", {"lighthouse_location_enrolling", "nine_week_period"}, {
    {"students_meeting_80", each List.Count(List.Select([meets_threshold], each _ = true)), Int64.Type},
    {"total_students", each Table.RowCount(_), Int64.Type}
  }),
  #"Added percentage" = Table.AddColumn(#"Grouped by location and period", "pct_meeting_threshold", each
    [students_meeting_80] / [total_students], Percentage.Type)
in
  #"Added percentage";
[BindToDefaultDestination = true]
shared attendance_summary_other_schools = let
  Source = lighthouse_attendance_detail,
  #"Filtered to non-80 schools" = Table.SelectRows(Source, each [is_80_school] = false),
  #"Filtered out specific" = Table.SelectRows(#"Filtered to non-80 schools", each
    [lighthouse_location_enrolling] <> "Highland" and
    [lighthouse_location_enrolling] <> "Teen Club" and
    [lighthouse_location_enrolling] <> "School Site - Judson"),
  // Filter to only students enrolled during each specific 9-week period
  #"Filtered to enrolled" = Table.SelectRows(#"Filtered out specific", each 
    let
      period = [nine_week_period],
      entry = [entry_date],
      exit_date = [exit_date],
      period_start = if period = "1st 9 Weeks" then #date(2025, 8, 13)
                     else if period = "2nd 9 Weeks" then #date(2025, 10, 20)
                     else if period = "3rd 9 Weeks" then #date(2026, 1, 6)
                     else if period = "4th 9 Weeks" then #date(2026, 3, 16)
                     else null,
      period_end = if period = "1st 9 Weeks" then #date(2025, 10, 17)
                   else if period = "2nd 9 Weeks" then #date(2025, 12, 17)
                   else if period = "3rd 9 Weeks" then #date(2026, 3, 6)
                   else if period = "4th 9 Weeks" then #date(2026, 5, 21)
                   else null
    in
      period_start <> null and
      (entry = null or entry <= period_end) and
      (exit_date = null or exit_date >= period_start)),
  #"Removed duplicates" = Table.Distinct(#"Filtered to enrolled", {"client_id", "nine_week_period"}),
  #"Grouped by location and period" = Table.Group(#"Removed duplicates", {"lighthouse_location_enrolling", "nine_week_period"}, {
    {"students_meeting_60", each List.Count(List.Select([meets_threshold], each _ = true)), Int64.Type},
    {"total_students", each Table.RowCount(_), Int64.Type}
  }),
  #"Added percentage" = Table.AddColumn(#"Grouped by location and period", "pct_meeting_threshold", each
    [students_meeting_60] / [total_students], Percentage.Type),
  #"Filtered rows" = Table.SelectRows(#"Added percentage", each ([lighthouse_location_enrolling] <> null))
in
  #"Filtered rows";
[BindToDefaultDestination = true]
shared attendance_averages_by_location = let
  Source = lighthouse_attendance_detail,
  #"Filtered out specific" = Table.SelectRows(Source, each
    [lighthouse_location_enrolling] <> null and
    [lighthouse_location_enrolling] <> "School Site - Judson"),
  // Filter to only students enrolled during each specific 9-week period
  #"Filtered to enrolled" = Table.SelectRows(#"Filtered out specific", each 
    let
      period = [nine_week_period],
      entry = [entry_date],
      exit_date = [exit_date],
      period_start = if period = "1st 9 Weeks" then #date(2025, 8, 13)
                     else if period = "2nd 9 Weeks" then #date(2025, 10, 20)
                     else if period = "3rd 9 Weeks" then #date(2026, 1, 6)
                     else if period = "4th 9 Weeks" then #date(2026, 3, 16)
                     else null,
      period_end = if period = "1st 9 Weeks" then #date(2025, 10, 17)
                   else if period = "2nd 9 Weeks" then #date(2025, 12, 17)
                   else if period = "3rd 9 Weeks" then #date(2026, 3, 6)
                   else if period = "4th 9 Weeks" then #date(2026, 5, 21)
                   else null
    in
      period_start <> null and
      (entry = null or entry <= period_end) and
      (exit_date = null or exit_date >= period_start)),
  #"Removed duplicates" = Table.Distinct(#"Filtered to enrolled", {"client_id", "nine_week_period"}),
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
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data];
