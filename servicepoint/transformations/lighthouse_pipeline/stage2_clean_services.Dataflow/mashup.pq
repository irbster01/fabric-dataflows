section Section1;

shared raw_sp_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  raw_sp_services = #"Navigation 1"{[Id = "raw_sp_services", ItemKind = "Table"]}[Data]
in
  raw_sp_services;

shared clean_lighthouse_entries = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  clean_lighthouse_entries = #"Navigation 1"{[Id = "clean_lighthouse_entries", ItemKind = "Table"]}[Data]
in
  clean_lighthouse_entries;

shared clean_lighthouse_services = let
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
  })
in
  #"Changed types";
