section Section1;

shared raw_sp_entries = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  raw_sp_entries = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data]
in
  raw_sp_entries;

shared clean_lighthouse_entries = let
  Source = raw_sp_entries,
  // Filter to only Lighthouse programs
  #"Filtered to lighthouse" = Table.SelectRows(Source, each 
    Text.Contains([program_name], "Lighthouse", Comparer.OrdinalIgnoreCase)),
  // Group by client_id to get one row per client with max entry/exit dates
  #"Grouped by client" = Table.Group(#"Filtered to lighthouse", {"client_id"}, {
    {"entry_date", each List.Max([entry_date]), type nullable date},
    {"exit_date", each List.Max([exit_date]), type nullable date},
    {"program_name", each List.First([program_name]), type nullable text}
  }),
  // Set explicit column types
  #"Changed types" = Table.TransformColumnTypes(#"Grouped by client", {
    {"client_id", Int64.Type},
    {"entry_date", type nullable date},
    {"exit_date", type nullable date},
    {"program_name", type nullable text}
  })
in
  #"Changed types";
