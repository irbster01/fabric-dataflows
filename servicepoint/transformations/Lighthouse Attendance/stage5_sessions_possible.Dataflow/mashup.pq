[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared lighthouse_client_master = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "lighthouse_client_master", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
shared lighthouse_dates = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  lighthouse_dates = #"Navigation 1"{[Id = "lighthouse_dates", ItemKind = "Table"]}[Data]
in
  lighthouse_dates;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "lighthouse_sessions_possible_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared lighthouse_sessions_possible = let
  Source = lighthouse_client_master,
  // Cross join with distinct periods
  #"Distinct periods" = Table.Distinct(lighthouse_dates, {"nine_week_period"}),
  #"Added period column" = Table.AddColumn(Source, "periods", each #"Distinct periods"),
  #"Expanded periods" = Table.ExpandTableColumn(#"Added period column", "periods", {"nine_week_period"}, {"nine_week_period"}),
  // Join with lighthouse_dates to get session days for this period
  #"Merged with dates" = Table.NestedJoin(#"Expanded periods", {"nine_week_period"}, lighthouse_dates, {"nine_week_period"}, "dates", JoinKind.LeftOuter),
  // Calculate sessions_possible
  #"Added sessions_possible" = Table.AddColumn(#"Merged with dates", "sessions_possible", each
    let
      dates_table = [dates],
      client_entry = [entry_date],
      client_exit = [exit_date]
    in
      if dates_table = null or Table.IsEmpty(dates_table) then 0
      else
        Table.RowCount(
          Table.SelectRows(dates_table, (row) =>
            row[date] <> null and
            row[IsSessionDay] = true and
            (client_entry = null or row[date] >= client_entry) and
            (client_exit = null or row[date] <= client_exit)
          )
        ), Int64.Type),
  // Remove nested table and select final columns
  #"Removed dates" = Table.RemoveColumns(#"Added sessions_possible", {"dates"}),
  #"Select columns" = Table.SelectColumns(#"Removed dates", {
    "client_id",
    "nine_week_period",
    "sessions_possible"
  }),
  // Set types
  #"Changed types" = Table.TransformColumnTypes(#"Select columns", {
    {"client_id", Int64.Type},
    {"nine_week_period", type text},
    {"sessions_possible", Int64.Type}
  }),
  // Remove rows where student wasn't enrolled (sessions_possible = 0)
  #"Filtered enrolled" = Table.SelectRows(#"Changed types", each [sessions_possible] > 0)
in
  #"Filtered enrolled";
shared lighthouse_sessions_possible_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "lighthouse_sessions_possible", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
