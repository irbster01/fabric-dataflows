[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_sp_entries = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  raw_sp_entries = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data]
in
  raw_sp_entries;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "clean_lighthouse_entries_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared clean_lighthouse_entries = let
  Source = raw_sp_entries,
  // Filter to only Lighthouse programs
  #"Filtered to lighthouse" = Table.SelectRows(Source, each 
    Text.Contains([program], "Lighthouse", Comparer.OrdinalIgnoreCase)),
  // Group by client_id to get one row per client with max entry/exit dates
  #"Grouped by client" = Table.Group(#"Filtered to lighthouse", {"client_id"}, {
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
  })
in
  #"Changed types";
shared clean_lighthouse_entries_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "clean_lighthouse_entries", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
