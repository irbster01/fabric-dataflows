[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared clean_lighthouse_entries = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "stage1_clean_lighthouse_entries", ItemKind = "Table"]}[Data],
  #"Changed types" = Table.TransformColumnTypes(#"Navigation 3", {{"client_id", Int64.Type}})
in
  #"Changed types";
shared raw_sp_demographics = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "raw_sp_demographics", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
shared raw_lighthouse_registration = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "raw_lighthouse_registration", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 3", {{"client_id", Int64.Type}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared stage3_demographics_deduped = let
  Source = raw_sp_demographics,
  #"Grouped by client" = Table.Group(Source, {"client_id"}, {
    {"first_name", each List.First(_[client_first_name]), type nullable text},
    {"last_name", each List.First(_[client_last_name]), type nullable text}
  })
in
  #"Grouped by client";
[BindToDefaultDestination = true]
shared stage3_registration_deduped = let
  Source = raw_lighthouse_registration,
  #"Grouped by client" = Table.Group(Source, {"client_id"}, {
    {"lighthouse_location_enrolling", each List.First(_[lighthouse_location_enrolling]), type nullable text}
  })
in
  #"Grouped by client";
[BindToDefaultDestination = true]
shared stage3_lighthouse_client_master = let
  Source = clean_lighthouse_entries,
  // Left join demographics using NestedJoin
  #"Merged with demographics" = Table.NestedJoin(Source, {"client_id"}, stage3_demographics_deduped, {"client_id"}, "demographics", JoinKind.LeftOuter),
  #"Expanded demographics" = Table.ExpandTableColumn(#"Merged with demographics", "demographics", {"first_name", "last_name"}, {"first_name", "last_name"}),
  // Left join registration using NestedJoin
  #"Merged with registration" = Table.NestedJoin(#"Expanded demographics", {"client_id"}, stage3_registration_deduped, {"client_id"}, "registration", JoinKind.LeftOuter),
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
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data];
