[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared clean_lighthouse_entries = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "clean_lighthouse_entries", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"client_id", type text}})
in
  #"Changed column type";
shared clean_lighthouse_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "clean_lighthouse_services", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"client_id", type text}})
in
  #"Changed column type";
shared raw_sp_clients_wIntID = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_clients_wIntID", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared raw_lighthouse_registration = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lighthouse_registration", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared entry_client_merge = let
  Source = Table.NestedJoin(clean_lighthouse_entries, {"client_id"}, raw_sp_clients, {"client_id"}, "raw_sp_clients", JoinKind.LeftOuter),
  #"Expanded raw_sp_clients" = Table.ExpandTableColumn(Source, "raw_sp_clients", {"client_first_name", "client_last_name", "client_dob", "int_id"}, {"client_first_name", "client_last_name", "client_dob", "int_id"})
in
  #"Expanded raw_sp_clients";
shared active_clients_last_service_date = let
  Source = entry_client_merge,
  #"Filtered rows" = Table.SelectRows(Source, each ([exit_date] = null)),
  #"Reordered columns" = Table.ReorderColumns(#"Filtered rows", {"client_id", "client_last_name", "client_first_name", "entry_date", "exit_date", "program", "client_dob", "int_id"}),
  #"Merged queries" = Table.NestedJoin(#"Reordered columns", {"client_id"}, stage_last_service_date, {"client_id"}, "stage_last_service_date", JoinKind.LeftOuter),
  #"Expanded stage_last_service_date" = Table.ExpandTableColumn(#"Merged queries", "stage_last_service_date", {"last_service_date"}, {"last_service_date"}),
  #"Sorted rows" = Table.Sort(#"Expanded stage_last_service_date", {{"last_service_date", Order.Ascending}})
in
  #"Sorted rows";
shared stage_last_service_date = let
  Source = clean_lighthouse_services,
  #"Grouped rows" = Table.Group(Source, {"client_id"}, {{"last_service_date", each List.Max([service_date]), type nullable date}})
in
  #"Grouped rows";
shared raw_sp_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_clients", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
