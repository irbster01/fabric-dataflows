[StagingDefinition = [Kind = "FastCopy"]]
section Section1;

// Source: ServicePoint lakehouse
shared clean_lighthouse_entries = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  clean_lighthouse_entries = #"Navigation 1"{[Id = "clean_lighthouse_entries", ItemKind = "Table"]}[Data]
in
  clean_lighthouse_entries;

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

// Deduplicate lookup tables
shared demographics_deduped = let
  Source = raw_sp_demographics,
  #"Grouped by client" = Table.Group(Source, {"client_id"}, {
    {"first_name", each List.First([first_name]), type nullable text},
    {"last_name", each List.First([last_name]), type nullable text}
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

// Main client master table
shared lighthouse_client_master = let
  Source = clean_lighthouse_entries,
  // Left join demographics
  #"Merged with demographics" = Table.Join(Source, "client_id", demographics_deduped, "client_id", JoinKind.LeftOuter),
  // Left join registration
  #"Merged with registration" = Table.Join(#"Merged with demographics", "client_id", registration_deduped, "client_id", JoinKind.LeftOuter),
  // Select final columns
  #"Select columns" = Table.SelectColumns(#"Merged with registration", {
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
