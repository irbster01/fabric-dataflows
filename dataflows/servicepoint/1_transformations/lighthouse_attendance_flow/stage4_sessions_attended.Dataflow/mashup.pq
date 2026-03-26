[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared clean_lighthouse_services = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "stage2_clean_lighthouse_services", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
[BindToDefaultDestination = true]
shared stage4_lighthouse_sessions_attended = let
  Source = clean_lighthouse_services,
  #"Grouped by client and period" = Table.Group(Source, {"client_id", "nine_week_period"}, {
    {"sessions_attended", each Table.RowCount(_), Int64.Type}
  }),
  // Set types
  #"Changed types" = Table.TransformColumnTypes(#"Grouped by client and period", {
    {"client_id", Int64.Type},
    {"nine_week_period", type text},
    {"sessions_attended", Int64.Type}
  })
in
  #"Changed types";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data];
