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
  #"Changed types" = Table.TransformColumnTypes(#"Navigation 3", {{"client_id", Int64.Type}}),
  // Ensure one row per client_id
  #"Deduped" = Table.Distinct(#"Changed types", {"client_id"})
in
  #"Deduped";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "jcampus_match_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared jcampus_match = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  jcampus_match = #"Navigation 1"{[Id = "silver_lighthouse.Lighthouse Matched with JCampus", ItemKind = "Table"]}[Data],
  #"Changed types" = Table.TransformColumnTypes(jcampus_match, {{"client_id", Int64.Type}}),
  // Dedupe to one row per client_id (take first School Name if multiple)
  #"Grouped by client" = Table.Group(#"Changed types", {"client_id"}, {
    {"School Name", each List.First(_[#"School Name"]), type nullable text}
  })
in
  #"Grouped by client";
[BindToDefaultDestination = true]
shared lighthouse_attendance_detail = let
  // Rename columns in sessions_possible to avoid conflicts
  #"Renamed possible cols" = Table.RenameColumns(lighthouse_sessions_possible, {{"client_id", "possible_client_id"}, {"nine_week_period", "possible_nine_week_period"}}),
  // Join attended and possible using Table.Join with FullOuter
  #"Joined attended and possible" = Table.Join(lighthouse_sessions_attended, {"client_id", "nine_week_period"}, #"Renamed possible cols", {"possible_client_id", "possible_nine_week_period"}, JoinKind.FullOuter),
  // Coalesce client_id and nine_week_period from both sides of FullOuter join
  #"Added coalesced_client_id" = Table.AddColumn(#"Joined attended and possible", "coalesced_client_id", each if [client_id] <> null then [client_id] else [possible_client_id], Int64.Type),
  #"Added coalesced_period" = Table.AddColumn(#"Added coalesced_client_id", "coalesced_period", each if [nine_week_period] <> null then [nine_week_period] else [possible_nine_week_period], type nullable text),
  #"Removed original keys" = Table.RemoveColumns(#"Added coalesced_period", {"client_id", "nine_week_period", "possible_client_id", "possible_nine_week_period"}),
  #"Renamed coalesced" = Table.RenameColumns(#"Removed original keys", {{"coalesced_client_id", "client_id"}, {"coalesced_period", "nine_week_period"}}),
  // Join with client master using Table.Join
  #"Joined with client master" = Table.Join(#"Renamed coalesced", {"client_id"}, lighthouse_client_master, {"client_id"}, JoinKind.LeftOuter),
  // Join with JCampus using Table.Join
  #"Joined with jcampus" = Table.Join(#"Joined with client master", {"client_id"}, jcampus_match, {"client_id"}, JoinKind.LeftOuter),
  // Add school_name (normalize locations)
  #"Added school_name" = Table.AddColumn(#"Joined with jcampus", "school_name", each
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
  #"Select columns" = Table.SelectColumns(#"Added attendance_flag", {"sessions_attended", "sessions_possible", "client_id", "nine_week_period", "first_name", "last_name", "entry_date", "exit_date", "lighthouse_location_enrolling", "program", "school_name", "is_80_school", "required_threshold", "attendance_rate", "meets_threshold", "attendance_flag"}),
  // Ensure numeric types
  #"Changed types" = Table.TransformColumnTypes(#"Select columns", {
    {"sessions_attended", Int64.Type},
    {"sessions_possible", Int64.Type}
  })
in
  #"Changed types";
[BindToDefaultDestination = true]
shared attendance_summary_80_schools = let
  Source = lighthouse_attendance_detail,
  #"Filtered to 80 schools" = Table.SelectRows(Source, each [is_80_school] = true),
  #"Removed duplicates" = Table.Distinct(#"Filtered to 80 schools", {"client_id", "nine_week_period", "lighthouse_location_enrolling"}),
  #"Grouped" = Table.Group(#"Removed duplicates", {"lighthouse_location_enrolling", "nine_week_period"}, {
    {"total_students", each Table.RowCount(_), Int64.Type},
    {"students_meeting_80", each Table.RowCount(Table.SelectRows(_, each [meets_threshold] = true)), Int64.Type}
  }),
  #"Added percentage" = Table.AddColumn(#"Grouped", "pct_meeting_threshold", each
    if [total_students] = 0 then 0 else [students_meeting_80] / [total_students], Percentage.Type)
in
  #"Added percentage";
[BindToDefaultDestination = true]
shared attendance_summary_other_schools = let
  Source = lighthouse_attendance_detail,
  #"Filtered to non-80 schools" = Table.SelectRows(Source, each [is_80_school] = false),
  #"Filtered out specific" = Table.SelectRows(#"Filtered to non-80 schools", each
    [lighthouse_location_enrolling] <> "Highland" and
    [lighthouse_location_enrolling] <> "Teen Club" and
    [lighthouse_location_enrolling] <> "School Site - Judson" and
    [lighthouse_location_enrolling] <> null),
  #"Removed duplicates" = Table.Distinct(#"Filtered out specific", {"client_id", "nine_week_period", "lighthouse_location_enrolling"}),
  #"Grouped" = Table.Group(#"Removed duplicates", {"lighthouse_location_enrolling", "nine_week_period"}, {
    {"total_students", each Table.RowCount(_), Int64.Type},
    {"students_meeting_60", each Table.RowCount(Table.SelectRows(_, each [meets_threshold] = true)), Int64.Type}
  }),
  #"Added percentage" = Table.AddColumn(#"Grouped", "pct_meeting_threshold", each
    if [total_students] = 0 then 0 else [students_meeting_60] / [total_students], Percentage.Type)
in
  #"Added percentage";
[BindToDefaultDestination = true]
shared attendance_averages_by_location = let
  Source = lighthouse_attendance_detail,
  #"Filtered out specific" = Table.SelectRows(Source, each
    [lighthouse_location_enrolling] <> null and
    [lighthouse_location_enrolling] <> "School Site - Judson"),
  #"Removed duplicates" = Table.Distinct(#"Filtered out specific", {"client_id", "nine_week_period", "lighthouse_location_enrolling"}),
  #"Grouped by location and period" = Table.Group(#"Removed duplicates", {"lighthouse_location_enrolling", "nine_week_period"}, {
    {"avg_sessions_attended", each List.Average(_[sessions_attended]), type nullable number},
    {"avg_sessions_possible", each List.Average(_[sessions_possible]), type nullable number},
    {"student_count", each Table.RowCount(_), Int64.Type}
  }),
  #"Added avg_attendance_rate" = Table.AddColumn(#"Grouped by location and period", "avg_attendance_rate", each
    if [avg_sessions_possible] = 0 or [avg_sessions_possible] = null then null else [avg_sessions_attended] / [avg_sessions_possible], Percentage.Type),
  #"Sorted" = Table.Sort(#"Added avg_attendance_rate", {
    {"lighthouse_location_enrolling", Order.Ascending},
    {"nine_week_period", Order.Ascending}
  })
in
  #"Sorted";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data];
shared jcampus_match_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "jcampus_match", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
