// SIMPLIFIED VERSION - Clean, simple approach
section Section1;

// Parameters
MinimumDaysEnrolled = 7;
EntryExitDateFilter = #date(2025, 9, 1);
HighAttendanceThreshold = 0.80;
StandardAttendanceThreshold = 0.60;
HighAttendanceSchools = {"Bethune/Oak Park", "Broadmoor STEM", "Creswell Elementary"};

// Helper function to safely get lakehouse table
GetLakehouseTable = (tableName as text) as table =>
  let
    Source = Lakehouse.Contents(null),
    Workspace = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
    Lakehouse = Workspace{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
    Result = Lakehouse{[Id = tableName, ItemKind = "Table"]}[Data]
  in
    Result;

// 1. Get services and assign 9-week periods
shared services_with_periods = let
  Source = GetLakehouseTable("raw_sp_services"),
  ValidDates = Table.SelectRows(Source, each [service_date] <> null),
  AddPeriod = Table.AddColumn(ValidDates, "nine_week_period", each 
    if [service_date] >= #date(2025, 8, 13) and [service_date] <= #date(2025, 10, 17) then "1st 9 Weeks"
    else if [service_date] >= #date(2025, 10, 20) and [service_date] <= #date(2025, 12, 17) then "2nd 9 Weeks"
    else if [service_date] >= #date(2026, 1, 6) and [service_date] <= #date(2026, 3, 6) then "3rd 9 Weeks"
    else if [service_date] >= #date(2026, 3, 16) and [service_date] <= #date(2026, 5, 21) then "4th 9 Weeks"
    else null, type nullable text),
  FilterPeriod = Table.SelectRows(AddPeriod, each [nine_week_period] <> null),
  SelectCols = Table.SelectColumns(FilterPeriod, {"client_id", "nine_week_period"})
in
  SelectCols;

// 2. Get lighthouse entries (grouped by client)
shared lighthouse_entries = let
  Source = GetLakehouseTable("raw_sp_entries"),
  FilterPrograms = Table.SelectRows(Source, each 
    [program] = "VOANLA 4002 -The LightHouse - School Sites" or 
    [program] = "VOANLA 4080 -The LightHouse - Community Sites" or 
    [program] = "VOANLA 4080a - Teen Club"),
  GroupByClient = Table.Group(FilterPrograms, {"client_id"}, {
    {"entry_date", each List.Max([entry_date]), type nullable date},
    {"exit_date", each List.Max([exit_date]), type nullable date}
  }),
  FilterDate = Table.SelectRows(GroupByClient, each [exit_date] = null or [exit_date] > EntryExitDateFilter)
in
  FilterDate;

// 3. Count sessions per client per period (only for lighthouse clients)
shared sessions_counted = let
  Services = services_with_periods,
  Entries = lighthouse_entries,
  // Inner join to keep only lighthouse clients
  Joined = Table.Join(Services, "client_id", Entries, "client_id", JoinKind.Inner),
  // Group and count
  Grouped = Table.Group(Joined, {"client_id", "nine_week_period", "entry_date", "exit_date"}, {
    {"sessions_attended", each Table.RowCount(_), Int64.Type}
  })
in
  Grouped;

// 4. Get registration info (deduplicated)
shared registration_lookup = let
  Source = GetLakehouseTable("raw_lighthouse_registration"),
  Dedup = Table.Distinct(Source, {"client_id"}),
  SelectCols = Table.SelectColumns(Dedup, {"client_id", "lighthouse_location_enrolling"})
in
  SelectCols;

// 5. Get JCampus matches (deduplicated)
shared jcampus_lookup = let
  Source = GetLakehouseTable("silver_lighthouse.Lighthouse Matched with JCampus"),
  Dedup = Table.Distinct(Source, {"client_id"}),
  SelectCols = Table.SelectColumns(Dedup, {"client_id", "School Name"})
in
  SelectCols;

// 6. Get calendar dates grouped by period
shared calendar_by_period = let
  Source = GetLakehouseTable("lighthouse_dates"),
  SessionDays = Table.SelectRows(Source, each [IsSessionDay] = true),
  Grouped = Table.Group(SessionDays, {"nine_week_period"}, {
    {"session_dates", each [date], type list}
  })
in
  Grouped;

// 7. Final output - simple left joins
shared client_attendance_simple = let
  Base = sessions_counted,
  
  // Join registration
  WithReg = Table.NestedJoin(Base, {"client_id"}, registration_lookup, {"client_id"}, "reg", JoinKind.LeftOuter),
  ExpandReg = Table.AddColumn(WithReg, "lighthouse_location_enrolling", each 
    if [reg] = null or Table.IsEmpty([reg]) then null else [reg]{0}[lighthouse_location_enrolling], type nullable text),
  DropReg = Table.RemoveColumns(ExpandReg, {"reg"}),
  
  // Join JCampus
  WithJC = Table.NestedJoin(DropReg, {"client_id"}, jcampus_lookup, {"client_id"}, "jc", JoinKind.LeftOuter),
  ExpandJC = Table.AddColumn(WithJC, "jcampus_school_name", each 
    if [jc] = null or Table.IsEmpty([jc]) then null else [jc]{0}[School Name], type nullable text),
  DropJC = Table.RemoveColumns(ExpandJC, {"jc"}),
  
  // Join calendar
  WithCal = Table.NestedJoin(DropJC, {"nine_week_period"}, calendar_by_period, {"nine_week_period"}, "cal", JoinKind.LeftOuter),
  
  // Calculate sessions possible
  AddPossible = Table.AddColumn(WithCal, "sessions_possible", each
    let
      cal = [cal],
      dates = if cal = null or Table.IsEmpty(cal) then {} else cal{0}[session_dates],
      entry = [entry_date],
      exit = [exit_date]
    in
      List.Count(List.Select(dates, each 
        (entry = null or _ >= entry) and (exit = null or _ <= exit)
      )), Int64.Type),
  DropCal = Table.RemoveColumns(AddPossible, {"cal"}),
  
  // Add school name (normalized)
  AddSchool = Table.AddColumn(DropCal, "school_name", each
    if [jcampus_school_name] <> null then [jcampus_school_name]
    else if [lighthouse_location_enrolling] = "School Site - Broadmoor" then "Broadmoor STEM"
    else if [lighthouse_location_enrolling] = "School Site - Creswell" then "Creswell Elementary"
    else if [lighthouse_location_enrolling] = "School Site - Oak Park" then "Bethune/Oak Park"
    else [lighthouse_location_enrolling], type nullable text),
  
  // Add is_80_school
  Add80 = Table.AddColumn(AddSchool, "is_80_school", each List.Contains(HighAttendanceSchools, [school_name]), type logical),
  
  // Add threshold
  AddThresh = Table.AddColumn(Add80, "required_threshold", each if [is_80_school] then 0.80 else 0.60, type number),
  
  // Add attendance rate
  AddRate = Table.AddColumn(AddThresh, "attendance_rate", each 
    if [sessions_possible] = 0 then null else [sessions_attended] / [sessions_possible], type nullable number),
  
  // Add meets threshold
  AddMeets = Table.AddColumn(AddRate, "meets_threshold", each 
    if [attendance_rate] = null then false else [attendance_rate] >= [required_threshold], type logical),
  
  // Final column selection and typing
  Final = Table.SelectColumns(AddMeets, {
    "client_id", "nine_week_period", "sessions_attended", "lighthouse_location_enrolling",
    "entry_date", "exit_date", "sessions_possible", "jcampus_school_name", "school_name",
    "is_80_school", "required_threshold", "attendance_rate", "meets_threshold"
  }),
  
  Typed = Table.TransformColumnTypes(Final, {
    {"client_id", Int64.Type},
    {"nine_week_period", type text},
    {"sessions_attended", Int64.Type},
    {"lighthouse_location_enrolling", type text},
    {"entry_date", type date},
    {"exit_date", type date},
    {"sessions_possible", Int64.Type},
    {"jcampus_school_name", type text},
    {"school_name", type text},
    {"is_80_school", type logical},
    {"required_threshold", type number},
    {"attendance_rate", type number},
    {"meets_threshold", type logical}
  })
in
  Typed;
