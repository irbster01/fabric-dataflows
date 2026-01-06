[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;

// === SOURCE TABLES ===
shared lighthouse_sessions_attended = let
  Source = Lakehouse.Contents(null),
  Nav1 = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Nav2 = Nav1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  Nav3 = Nav2{[Id = "lighthouse_sessions_attended", ItemKind = "Table"]}[Data]
in
  Nav3;

shared lighthouse_sessions_possible = let
  Source = Lakehouse.Contents(null),
  Nav1 = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Nav2 = Nav1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  Nav3 = Nav2{[Id = "lighthouse_sessions_possible", ItemKind = "Table"]}[Data]
in
  Nav3;

shared lighthouse_client_master = let
  Source = Lakehouse.Contents(null),
  Nav1 = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Nav2 = Nav1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  Nav3 = Nav2{[Id = "lighthouse_client_master", ItemKind = "Table"]}[Data]
in
  Nav3;

// === DETAIL TABLE ===
[BindToDefaultDestination = true]
shared lighthouse_attendance_detail = let
  Source = lighthouse_sessions_possible,
  
  // Join sessions_attended
  #"Merged attended" = Table.NestedJoin(Source, {"client_id", "nine_week_period"}, lighthouse_sessions_attended, {"client_id", "nine_week_period"}, "attended_data", JoinKind.LeftOuter),
  #"Expanded attended" = Table.ExpandTableColumn(#"Merged attended", "attended_data", {"sessions_attended"}, {"sessions_attended"}),
  #"Fill nulls" = Table.ReplaceValue(#"Expanded attended", null, 0, Replacer.ReplaceValue, {"sessions_attended"}),
  
  // Join client_master
  #"Merged client" = Table.NestedJoin(#"Fill nulls", {"client_id"}, lighthouse_client_master, {"client_id"}, "client_data", JoinKind.LeftOuter),
  #"Expanded client" = Table.ExpandTableColumn(#"Merged client", "client_data", {"first_name", "last_name", "entry_date", "exit_date", "lighthouse_location_enrolling", "program"}, {"first_name", "last_name", "entry_date", "exit_date", "lighthouse_location_enrolling", "program"}),
  
  // Add calculated columns
  #"Add rate" = Table.AddColumn(#"Expanded client", "attendance_rate", each if [sessions_possible] = 0 or [sessions_possible] = null then null else [sessions_attended] / [sessions_possible], Percentage.Type),
  
  #"Add is_80" = Table.AddColumn(#"Add rate", "is_80_school", each 
    [lighthouse_location_enrolling] = "Bethune/Oak Park" or 
    [lighthouse_location_enrolling] = "Broadmoor STEM" or 
    [lighthouse_location_enrolling] = "Creswell Elementary" or
    [lighthouse_location_enrolling] = "School Site - Broadmoor" or
    [lighthouse_location_enrolling] = "School Site - Creswell" or
    [lighthouse_location_enrolling] = "School Site - Oak Park", type logical),
  
  #"Add meets" = Table.AddColumn(#"Add is_80", "meets_threshold", each 
    if [attendance_rate] = null then false
    else if [is_80_school] then [attendance_rate] >= 0.80
    else [attendance_rate] >= 0.60, type logical)
in
  #"Add meets";

// === SUMMARY: 80% SCHOOLS ===
[BindToDefaultDestination = true]
shared attendance_summary_80_schools = let
  Source = lighthouse_attendance_detail,
  #"Filtered" = Table.SelectRows(Source, each [is_80_school] = true and [lighthouse_location_enrolling] <> null),
  #"Grouped" = Table.Group(#"Filtered", {"lighthouse_location_enrolling", "nine_week_period"}, {
    {"total_students", each Table.RowCount(_), Int64.Type},
    {"students_meeting_80", each Table.RowCount(Table.SelectRows(_, each [meets_threshold] = true)), Int64.Type}
  }),
  #"Add pct" = Table.AddColumn(#"Grouped", "pct_meeting_threshold", each if [total_students] = 0 then 0 else [students_meeting_80] / [total_students], Percentage.Type)
in
  #"Add pct";

// === SUMMARY: OTHER SCHOOLS ===
[BindToDefaultDestination = true]
shared attendance_summary_other_schools = let
  Source = lighthouse_attendance_detail,
  #"Filtered" = Table.SelectRows(Source, each 
    [is_80_school] = false and 
    [lighthouse_location_enrolling] <> null and
    [lighthouse_location_enrolling] <> "Highland" and
    [lighthouse_location_enrolling] <> "Teen Club" and
    [lighthouse_location_enrolling] <> "School Site - Judson"),
  #"Grouped" = Table.Group(#"Filtered", {"lighthouse_location_enrolling", "nine_week_period"}, {
    {"total_students", each Table.RowCount(_), Int64.Type},
    {"students_meeting_60", each Table.RowCount(Table.SelectRows(_, each [meets_threshold] = true)), Int64.Type}
  }),
  #"Add pct" = Table.AddColumn(#"Grouped", "pct_meeting_threshold", each if [total_students] = 0 then 0 else [students_meeting_60] / [total_students], Percentage.Type)
in
  #"Add pct";

// === AVERAGES BY LOCATION ===
[BindToDefaultDestination = true]
shared attendance_averages_by_location = let
  Source = lighthouse_attendance_detail,
  #"Filtered" = Table.SelectRows(Source, each [lighthouse_location_enrolling] <> null and [lighthouse_location_enrolling] <> "School Site - Judson"),
  #"Grouped" = Table.Group(#"Filtered", {"lighthouse_location_enrolling", "nine_week_period"}, {
    {"avg_sessions_attended", each List.Average(_[sessions_attended]), type number},
    {"avg_sessions_possible", each List.Average(_[sessions_possible]), type number},
    {"student_count", each Table.RowCount(_), Int64.Type}
  }),
  #"Add avg rate" = Table.AddColumn(#"Grouped", "avg_attendance_rate", each if [avg_sessions_possible] = 0 or [avg_sessions_possible] = null then null else [avg_sessions_attended] / [avg_sessions_possible], Percentage.Type)
in
  #"Add avg rate";

// === DESTINATION ===
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data];
