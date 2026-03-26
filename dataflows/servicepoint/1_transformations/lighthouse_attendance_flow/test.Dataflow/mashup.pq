[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_sp_services = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "sp_services_table", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
shared clean_lighthouse_entries = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "stage1_clean_lighthouse_entries", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 3", {{"client_id", type text}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "stage2_clean_lighthouse_services_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "client_id", DestinationColumnName = "client_id"], [SourceColumnName = "service_date", DestinationColumnName = "service_date"], [SourceColumnName = "nine_week_period", DestinationColumnName = "nine_week_period"], [SourceColumnName = "service_id", DestinationColumnName = "service_id"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared stage2_clean_lighthouse_services = let
  Source = raw_sp_services,
  // Filter to valid service dates only
  #"Filtered to valid dates" = Table.SelectRows(Source, each 
    [service_date] <> null and 
    [service_date] >= #date(2025, 8, 13) and 
    [service_date] <= #date(2026, 5, 21)),
  // Filter to only lighthouse clients (inner join with clean entries)
  #"Merged with lighthouse clients" = Table.NestedJoin(#"Filtered to valid dates", {"client_id"}, clean_lighthouse_entries, {"client_id"}, "lighthouse_check", JoinKind.Inner),
  #"Removed lighthouse_check" = Table.RemoveColumns(#"Merged with lighthouse clients", {"lighthouse_check"}),
  // Add nine_week_period based on service_date
  #"Added nine_week_period" = Table.AddColumn(#"Removed lighthouse_check", "nine_week_period", each 
    let svc_date = [service_date] in
    if svc_date >= #date(2025, 8, 13) and svc_date <= #date(2025, 10, 17) then "1st 9 Weeks"
    else if svc_date >= #date(2025, 10, 20) and svc_date <= #date(2025, 12, 17) then "2nd 9 Weeks"
    else if svc_date >= #date(2026, 1, 6) and svc_date <= #date(2026, 3, 6) then "3rd 9 Weeks"
    else if svc_date >= #date(2026, 3, 16) and svc_date <= #date(2026, 5, 21) then "4th 9 Weeks"
    else null, type text),
  // Keep only records within defined periods
  #"Filtered to nine weeks" = Table.SelectRows(#"Added nine_week_period", each [nine_week_period] <> null),
  // Select only needed columns
  #"Selected columns" = Table.SelectColumns(#"Filtered to nine weeks", {
    "client_id",
    "service_date", 
    "nine_week_period",
    "service_id"
  }),
  // Set explicit column types
  #"Changed types" = Table.TransformColumnTypes(#"Selected columns", {
    {"client_id", Int64.Type},
    {"service_date", type date},
    {"nine_week_period", type text},
    {"service_id", Int64.Type}
  }),
  #"Sorted rows" = Table.Sort(#"Changed types", {{"service_date", Order.Descending}})
in
  #"Sorted rows";
shared stage2_clean_lighthouse_services_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
  TableNavigation = Navigation_2{[Id = "stage2_clean_lighthouse_services", ItemKind = "Table"]}[Data]
in
  TableNavigation;
