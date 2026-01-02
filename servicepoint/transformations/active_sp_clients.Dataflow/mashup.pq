[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared active_sp_entries = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([exit_date] = null)),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows", {{"client_id", type text}}),
  #"Sorted rows" = Table.Sort(#"Changed column type", {{"entry_date", Order.Descending}})
in
  #"Sorted rows";
[BindToDefaultDestination = true]
shared active_now_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([exit_date] = null) and ([program] = "VOANLA 2043 - NOW Waiver")),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows", {{"client_id", type text}}),
  #"Removed duplicates" = Table.Distinct(#"Changed column type", {"client_id"}),
  #"Removed duplicates 1" = Table.Distinct(#"Removed duplicates", {"client_id"})
in
  #"Removed duplicates 1";
[BindToDefaultDestination = true]
shared active_lh_school_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([exit_date] = null)),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows", {{"client_id", type text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type", each ([program] = "VOANLA 4002 -The LightHouse - School Sites")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows 1", {"client_id"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared active_lh_comm_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([exit_date] = null)),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows", {{"client_id", type text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type", each ([program] = "VOANLA 4080 -The LightHouse - Community Sites")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows 1", {"client_id"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared active_choice_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([exit_date] = null)),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows", {{"client_id", type text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type", each ([program] = "VOANLA 8110 - Choice Neighborhoods")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows 1", {"client_id"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared active_greenwood_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([exit_date] = null)),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows", {{"client_id", type text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type", each ([program] = "VOANLA 9060 - SC Property - Greenwood Lodge")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows 1", {"client_id"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared active_corbitt_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([exit_date] = null)),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows", {{"client_id", type text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type", each ([program] = "VOANLA 9061 - SC Property - Corbitt Lodge")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows 1", {"client_id"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared active_independence_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([exit_date] = null)),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows", {{"client_id", type text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type", each ([program] = "VOANLA 9062 - SC Property - Independence Lodge")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows 1", {"client_id"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared active_southPointe_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([exit_date] = null)),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows", {{"client_id", type text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type", each ([program] = "VOANLA 9062 - SC Property - South Pointe")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows 1", {"client_id"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared active_newHaven_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([exit_date] = null)),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows", {{"client_id", type text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type", each ([program] = "VOANLA 9163 - SC Property - New Haven")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows 1", {"client_id"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared active_indMead_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([exit_date] = null)),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows", {{"client_id", type text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type", each ([program] = "VOANLA 9264 - SC Property - Indpendence Meadows")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows 1", {"client_id"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared active_pineHaven = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([exit_date] = null)),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows", {{"client_id", type text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type", each ([program] = "VOANLA 9301 - SC Property - Pine Haven")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows 1", {"client_id"})
in
  #"Removed duplicates";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "2526_lh_clients_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"2526_lh_clients" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each [exit_date] = null or [exit_date] > #date(2025, 8, 30)),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows", {{"client_id", type text}}),
  #"Sorted rows" = Table.Sort(#"Changed column type", {{"entry_date", Order.Descending}}),
  Custom = #"Sorted rows",

  // -------------------------
  // Helper functions & config
  // -------------------------

  // safe conversion: handles null, empty text, or date types
  ToDateOrNull = (v) =>
    if v = null then null
    else
      let
        conv =
          try
            if Value.Is(v, type text)
            then if Text.Trim(v) = "" then null else Date.From(v)
            else Date.From(v)
          otherwise null
      in
        conv,

  // blackout/excluded dates
  ExcludedDates = {
    #date(2025,10,6), #date(2025,10,7), #date(2025,10,8), #date(2025,10,9), #date(2025,10,10),
    #date(2025,11,24), #date(2025,11,25), #date(2025,11,26), #date(2025,11,27), #date(2025,11,28),
    #date(2025,12,18), #date(2025,12,19), #date(2025,12,20), #date(2025,12,21), #date(2025,12,22),
    #date(2025,12,23), #date(2025,12,24), #date(2025,12,25), #date(2025,12,26), #date(2025,12,27),
    #date(2025,12,28), #date(2025,12,29), #date(2025,12,30), #date(2025,12,31),
    #date(2026,1,1), #date(2026,1,2),
    #date(2026,2,13), #date(2026,2,14), #date(2026,2,15), #date(2026,2,16),
    #date(2026,3,9),  #date(2026,3,10), #date(2026,3,11), #date(2026,3,12), #date(2026,3,13),
    #date(2026,4,3),  #date(2026,4,4),  #date(2026,4,5),  #date(2026,4,6)
  },

  // anchors
  AnchorSept2 = #date(2025,9,2),
  AnchorSept8 = #date(2025,9,8),
  AnchorSept9 = #date(2025,9,9),

  // using Day.Monday so Mon=0, Tue=1, Wed=2, Thu=3
  AllowedWeekdays = {1,2,3},

  // returns list of Tue/Wed/Thu between startDate and endDate inclusive, minus exclusions
  ValidSessionDates = (startDate as date, endDate as date) as list =>
    let
      days = if endDate >= startDate then Duration.Days(endDate - startDate) + 1 else 0,
      dates = if days > 0 then List.Dates(startDate, days, #duration(1,0,0,0)) else {},
      onlyTueWedThu = List.Select(dates, (d) => List.Contains(AllowedWeekdays, Date.DayOfWeek(d, Day.Monday))),
      removed = List.RemoveMatchingItems(onlyTueWedThu, ExcludedDates)
    in
      removed,

  // Row-level function producing counts & date-lists
  RowSessionRecord = (r as record) =>
    let
      entry = ToDateOrNull(Record.FieldOrDefault(r, "entry_date", null)),
      exitRaw = Record.FieldOrDefault(r, "exit_date", null),
      exit = if exitRaw <> null then ToDateOrNull(exitRaw) else Date.From(DateTime.LocalNow()),

      // entry->exit
      DatesFromEntry = if (entry <> null and exit <> null and exit >= entry) then ValidSessionDates(entry, exit) else {},
      CountFromEntry = List.Count(DatesFromEntry),

      // anchors capped at entry (later of entry and anchor)
      Start2 = if entry <> null then List.Max({entry, AnchorSept2}) else AnchorSept2,
      Start8 = if entry <> null then List.Max({entry, AnchorSept8}) else AnchorSept8,
      Start9 = if entry <> null then List.Max({entry, AnchorSept9}) else AnchorSept9,

      DatesFrom2 = if exit <> null and exit >= Start2 then ValidSessionDates(Start2, exit) else {},
      DatesFrom8 = if exit <> null and exit >= Start8 then ValidSessionDates(Start8, exit) else {},
      DatesFrom9 = if exit <> null and exit >= Start9 then ValidSessionDates(Start9, exit) else {},

      CountFrom2 = List.Count(DatesFrom2),
      CountFrom8 = List.Count(DatesFrom8),
      CountFrom9 = List.Count(DatesFrom9)
    in
      [
        // keep both a count column and a Days_Between column for compatibility
        SessionCount = CountFromEntry,
        Days_Between_Entry_And_Exit = CountFromEntry,

        // anchor starts & results (lists + counts)
        Start2 = Start2,
        SessionDatesFromSept2 = DatesFrom2,
        SessionCountFromSept2 = CountFrom2,

        Start8 = Start8,
        SessionDatesFromSept8 = DatesFrom8,
        SessionCountFromSept8 = CountFrom8,

        Start9 = Start9,
        SessionDatesFromSept9 = DatesFrom9,
        SessionCountFromSept9 = CountFrom9,

        // also expose the list for debugging/inspection if needed
        SessionDates = DatesFromEntry
      ],

  // Add a record column and expand it into separate columns
  #"Added Session Record" = Table.AddColumn(Custom, "SessionInfo", each RowSessionRecord(_), type record),

  #"Expanded SessionInfo" = Table.ExpandRecordColumn(
    #"Added Session Record",
    "SessionInfo",
    {
      "SessionDates", "SessionCount", "Days_Between_Entry_And_Exit",
      "Start2", "SessionDatesFromSept2", "SessionCountFromSept2",
      "Start8", "SessionDatesFromSept8", "SessionCountFromSept8",
      "Start9", "SessionDatesFromSept9", "SessionCountFromSept9"
    },
    {
      "SessionDates", "SessionCount", "Days_Between_Entry_And_Exit",
      "Start2", "SessionDatesFromSept2", "SessionCountFromSept2",
      "Start8", "SessionDatesFromSept8", "SessionCountFromSept8",
      "Start9", "SessionDatesFromSept9", "SessionCountFromSept9"
    }
  ),

  // ensure numeric types for counts (preserve nulls)
  #"Changed Type - Counts" = Table.TransformColumnTypes(
    #"Expanded SessionInfo",
    {
      {"SessionCount", Int64.Type},
      {"Days_Between_Entry_And_Exit", Int64.Type},
      {"SessionCountFromSept2", Int64.Type},
      {"SessionCountFromSept8", Int64.Type},
      {"SessionCountFromSept9", Int64.Type}
    }, "en-US"),

  // optional: convert the lists to text for easier glance (comment out if you prefer lists)
  #"SessionDates Text" = Table.TransformColumns(#"Changed Type - Counts",
    {
      {"SessionDates", each Text.Combine(List.Transform(_, each Date.ToText(_, "yyyy-MM-dd")), ","), type nullable text},
      {"SessionDatesFromSept2", each Text.Combine(List.Transform(_, each Date.ToText(_, "yyyy-MM-dd")), ","), type nullable text},
      {"SessionDatesFromSept8", each Text.Combine(List.Transform(_, each Date.ToText(_, "yyyy-MM-dd")), ","), type nullable text},
      {"SessionDatesFromSept9", each Text.Combine(List.Transform(_, each Date.ToText(_, "yyyy-MM-dd")), ","), type nullable text}
    }),

  // match your later steps: insert month name, rename days, filter program
  #"Inserted month name" = Table.AddColumn(#"SessionDates Text", "Month name", each Date.MonthName([entry_date]), type nullable text),
  #"Renamed columns" = Table.RenameColumns(#"Inserted month name", {{"Days_Between_Entry_And_Exit", "days_enrolled"}}),
  #"Filtered rows 1" = Table.SelectRows(#"Renamed columns", each ([program] = "VOANLA 4002 -The LightHouse - School Sites" or [program] = "VOANLA 4080 -The LightHouse - Community Sites" or [program] = "VOANLA 4080a - Teen Club")),
  #"Renamed columns 1" = Table.RenameColumns(#"Filtered rows 1", {{"SessionDates", "session_dates"}, {"SessionCount", "session_count"}, {"Start2", "start2"}, {"SessionDatesFromSept2", "dates_from_sept2"}, {"SessionCountFromSept2", "session_count_from_sept2"}, {"Start8", "start8"}, {"SessionDatesFromSept8", "session_dates_from_sept8"}, {"SessionCountFromSept8", "session_count_from_sept8"}, {"Start9", "start9"}, {"SessionDatesFromSept9", "session_dates_from_sept9"}, {"SessionCountFromSept9", "session_count_from_sept9"}, {"Month name", "entry_month_name"}}),
  #"Removed duplicates" = Table.Distinct(#"Renamed columns 1", {"entry_id"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed duplicates", {{"start2", type date}, {"start8", type date}, {"start9", type date}}),
  #"Removed duplicates 1" = Table.Distinct(#"Changed column type 1", {"client_id"})
in
  #"Removed duplicates 1";
shared #"2526_lh_clients_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  TableNavigation = Navigation_2{[Id = "2526_lh_clients", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
