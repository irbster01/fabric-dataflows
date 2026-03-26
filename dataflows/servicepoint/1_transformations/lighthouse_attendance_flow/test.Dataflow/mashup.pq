[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_sp_entries = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "raw_sp_entries_new", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "test1_clean_lighthouse_entries_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared test1_clean_lighthouse_entries = let
  Source = raw_sp_entries,
  #"Filtered rows" = Table.SelectRows(Source, each ([program] = "VOANLA 4002 -The LightHouse - School Sites" or [program] = "VOANLA 4080 -The LightHouse - Community Sites" or [program] = "VOANLA 4080a - Teen Club")),
  // Group by client_id to get one row per client with max entry/exit dates
  #"Grouped by client" = Table.Group(#"Filtered rows", {"client_id"}, {
    {"entry_date", each List.Max(_[entry_date]), type nullable date},
    {"exit_date", each List.Max(_[exit_date]), type nullable date},
    {"program", each List.First(_[program]), type nullable text}
  }),
  // Set explicit column types
  #"Changed types" = Table.TransformColumnTypes(#"Grouped by client", {
    {"client_id", Int64.Type},
    {"entry_date", type nullable date},
    {"exit_date", type nullable date},
    {"program", type nullable text}
  }),
  #"Sorted rows" = Table.Sort(#"Changed types", {{"entry_date", Order.Descending}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Sorted rows", {{"client_id", type text}})
in
  #"Changed column type";
shared test1_clean_lighthouse_entries_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
  TableNavigation = Navigation_2{[Id = "test1_clean_lighthouse_entries", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
