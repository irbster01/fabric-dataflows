[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "all_lsndc_clients_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared all_lsndc_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_clients_new", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"client_id"}, raw_lsndc_entries_new, {"client_id"}, "raw_lsndc_entries_new", JoinKind.LeftOuter),
  #"Expanded raw_lsndc_entries_new" = Table.ExpandTableColumn(#"Merged queries", "raw_lsndc_entries_new", {"entry_date", "entry_id", "exit_date", "program"}, {"entry_date", "entry_id", "exit_date", "program"})
in
  #"Expanded raw_lsndc_entries_new";
shared raw_lsndc_entries_new = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_entries_new", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "active_lsndc_clients_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared active_lsndc_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_clients_new", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"client_id"}, raw_lsndc_entries_new, {"client_id"}, "raw_lsndc_entries_new", JoinKind.LeftOuter),
  #"Expanded raw_lsndc_entries_new" = Table.ExpandTableColumn(#"Merged queries", "raw_lsndc_entries_new", {"entry_date", "entry_id", "exit_date", "program"}, {"entry_date", "entry_id", "exit_date", "program"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded raw_lsndc_entries_new", each ([exit_date] = null))
in
  #"Filtered rows";
shared active_lsndc_clients_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c46fe4a6-e106-4e77-993b-efc9ce22e551"]}[Data],
  TableNavigation = Navigation_2{[Id = "active_lsndc_clients", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared all_lsndc_clients_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c46fe4a6-e106-4e77-993b-efc9ce22e551"]}[Data],
  TableNavigation = Navigation_2{[Id = "all_lsndc_clients", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
