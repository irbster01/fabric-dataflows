[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;

// === CONSTANTS ===
shared HighAttendanceThreshold = 0.80;
shared StandardAttendanceThreshold = 0.60;
shared HighAttendanceSchools = {"Bethune/Oak Park", "Broadmoor STEM", "Creswell Elementary"};

// === SOURCE TABLES ===
shared lighthouse_sessions_attended = let
  Source = Lakehouse.Contents(null){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data]{[Id = "lighthouse_sessions_attended", ItemKind = "Table"]}[Data]
in
  Source;

shared lighthouse_sessions_possible = let
  Source = Lakehouse.Contents(null){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data]{[Id = "lighthouse_sessions_possible", ItemKind = "Table"]}[Data]
in
  Source;

shared lighthouse_client_master = let
  Source = Lakehouse.Contents(null){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data]{[Id = "lighthouse_client_master", ItemKind = "Table"]}[Data]
in
  Source;

// === DETAIL TABLE ===
// One row per client per 9-week period with attendance stats
[BindToDefaultDestination = true]
shared lighthouse_attendance_detail = let
  // Start with sessions_possible (has all client/period combos)
  Base = lighthouse_sessions_possible,
  
  // Add sessions_attended
  AddAttended = Table.AddColumn(Base, "sessions_attended", each 
    let 
      match = Table.SelectRows(lighthouse_sessions_attended, (r) => r[client_id] = [client_id] and r[nine_week_period] = [nine_week_period])
    in 
      if Table.RowCount(match) > 0 then match{0}[sessions_attended] else 0, Int64.Type),
  
  // Add client info from client_master
  AddClientInfo = Table.AddColumn(AddAttended, "client_info", each
    let
      match = Table.SelectRows(lighthouse_client_master, (r) => r[client_id] = [client_id])
    in
      if Table.RowCount(match) > 0 then match{0} else null),
  
  // Expand client info
  Expanded = Table.ExpandRecordColumn(AddClientInfo, "client_info", 
    {"first_name", "last_name", "entry_date", "exit_date", "lighthouse_location_enrolling", "program"}),
  
  // Add school_name (normalize)
  AddSchoolName = Table.AddColumn(Expanded, "school_name", each
    if [lighthouse_location_enrolling] = "School Site - Broadmoor" then "Broadmoor STEM"
    else if [lighthouse_location_enrolling] = "School Site - Creswell" then "Creswell Elementary"
    else if [lighthouse_location_enrolling] = "School Site - Oak Park" then "Bethune/Oak Park"
    else [lighthouse_location_enrolling], type text),
  
  // Add is_80_school
  AddIs80 = Table.AddColumn(AddSchoolName, "is_80_school", each
    List.Contains(HighAttendanceSchools, [school_name]), type logical),
  
  // Add attendance_rate
  AddRate = Table.AddColumn(AddIs80, "attendance_rate", each
    if [sessions_possible] = null or [sessions_possible] = 0 then null
    else [sessions_attended] / [sessions_possible], Percentage.Type),
  
  // Add meets_threshold
  AddMeets = Table.AddColumn(AddRate, "meets_threshold", each
    if [attendance_rate] = null then false
    else if [is_80_school] then [attendance_rate] >= HighAttendanceThreshold
    else [attendance_rate] >= StandardAttendanceThreshold, type logical)
in
  AddMeets;

// === SUMMARY: 80% SCHOOLS ===
[BindToDefaultDestination = true]
shared attendance_summary_80_schools = let
  Source = lighthouse_attendance_detail,
  Filtered = Table.SelectRows(Source, each [is_80_school] = true and [lighthouse_location_enrolling] <> null),
  Grouped = Table.Group(Filtered, {"lighthouse_location_enrolling", "nine_week_period"}, {
    {"total_students", each Table.RowCount(_), Int64.Type},
    {"students_meeting_80", each Table.RowCount(Table.SelectRows(_, each [meets_threshold] = true)), Int64.Type}
  }),
  AddPct = Table.AddColumn(Grouped, "pct_meeting_threshold", each 
    if [total_students] = 0 then 0 else [students_meeting_80] / [total_students], Percentage.Type)
in
  AddPct;

// === SUMMARY: OTHER SCHOOLS (60% threshold) ===
[BindToDefaultDestination = true]
shared attendance_summary_other_schools = let
  Source = lighthouse_attendance_detail,
  Filtered = Table.SelectRows(Source, each 
    [is_80_school] = false and 
    [lighthouse_location_enrolling] <> null and
    [lighthouse_location_enrolling] <> "Highland" and
    [lighthouse_location_enrolling] <> "Teen Club" and
    [lighthouse_location_enrolling] <> "School Site - Judson"),
  Grouped = Table.Group(Filtered, {"lighthouse_location_enrolling", "nine_week_period"}, {
    {"total_students", each Table.RowCount(_), Int64.Type},
    {"students_meeting_60", each Table.RowCount(Table.SelectRows(_, each [meets_threshold] = true)), Int64.Type}
  }),
  AddPct = Table.AddColumn(Grouped, "pct_meeting_threshold", each
    if [total_students] = 0 then 0 else [students_meeting_60] / [total_students], Percentage.Type)
in
  AddPct;

// === AVERAGES BY LOCATION ===
[BindToDefaultDestination = true]
shared attendance_averages_by_location = let
  Source = lighthouse_attendance_detail,
  Filtered = Table.SelectRows(Source, each 
    [lighthouse_location_enrolling] <> null and 
    [lighthouse_location_enrolling] <> "School Site - Judson"),
  Grouped = Table.Group(Filtered, {"lighthouse_location_enrolling", "nine_week_period"}, {
    {"avg_sessions_attended", each List.Average(_[sessions_attended]), type number},
    {"avg_sessions_possible", each List.Average(_[sessions_possible]), type number},
    {"student_count", each Table.RowCount(_), Int64.Type}
  }),
  AddAvgRate = Table.AddColumn(Grouped, "avg_attendance_rate", each
    if [avg_sessions_possible] = 0 or [avg_sessions_possible] = null then null 
    else [avg_sessions_attended] / [avg_sessions_possible], Percentage.Type)
in
  AddAvgRate;

// === DESTINATION ===
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data];
