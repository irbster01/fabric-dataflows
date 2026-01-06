[StagingDefinition = [Kind = "FastCopy"]]
section Section1;

// Source: VOA_Nexus lakehouse (reading from Stage 2 output)
shared clean_lighthouse_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  clean_lighthouse_services = #"Navigation 1"{[Id = "clean_lighthouse_services", ItemKind = "Table"]}[Data]
in
  clean_lighthouse_services;

// Count sessions attended per client per period
shared lighthouse_sessions_attended = let
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
