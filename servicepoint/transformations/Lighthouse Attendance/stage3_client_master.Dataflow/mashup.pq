[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared clean_lighthouse_entries = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "clean_lighthouse_entries", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
shared raw_sp_demographics = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  raw_sp_demographics = #"Navigation 1"{[Id = "raw_sp_demographics", ItemKind = "Table"]}[Data]
in
  raw_sp_demographics;
shared raw_lighthouse_registration = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  raw_lighthouse_registration = #"Navigation 1"{[Id = "raw_lighthouse_registration", ItemKind = "Table"]}[Data]
in
  raw_lighthouse_registration;
shared demographics_deduped = let
  Source = raw_sp_demographics,
  #"Grouped by client" = Table.Group(Source, {"client_id"}, {
    {"first_name", each List.First([client_first_name]), type nullable text},
    {"last_name", each List.First([client_last_name]), type nullable text}
  })
in
  #"Grouped by client";
shared registration_deduped = let
  Source = raw_lighthouse_registration,
  #"Grouped by client" = Table.Group(Source, {"client_id"}, {
    {"lighthouse_location_enrolling", each List.First([lighthouse_location_enrolling]), type nullable text}
  })
in
  #"Grouped by client";
shared lighthouse_client_master = let
  Source = clean_lighthouse_entries,
  // Left join demographics using NestedJoin
  #"Merged with demographics" = Table.NestedJoin(Source, {"client_id"}, demographics_deduped, {"client_id"}, "demographics", JoinKind.LeftOuter),
  #"Expanded demographics" = Table.ExpandTableColumn(#"Merged with demographics", "demographics", {"first_name", "last_name"}, {"first_name", "last_name"}),
  // Left join registration using NestedJoin
  #"Merged with registration" = Table.NestedJoin(#"Expanded demographics", {"client_id"}, registration_deduped, {"client_id"}, "registration", JoinKind.LeftOuter),
  #"Expanded registration" = Table.ExpandTableColumn(#"Merged with registration", "registration", {"lighthouse_location_enrolling"}, {"lighthouse_location_enrolling"}),
  // Select final columns
  #"Select columns" = Table.SelectColumns(#"Expanded registration", {
    "client_id",
    "first_name",
    "last_name",
    "entry_date",
    "exit_date",
    "lighthouse_location_enrolling",
    "program"
  }),
  // Set types
  #"Changed types" = Table.TransformColumnTypes(#"Select columns", {
    {"client_id", Int64.Type},
    {"first_name", type nullable text},
    {"last_name", type nullable text},
    {"entry_date", type nullable date},
    {"exit_date", type nullable date},
    {"lighthouse_location_enrolling", type nullable text},
    {"program", type nullable text}
  })
in
  #"Changed types";
