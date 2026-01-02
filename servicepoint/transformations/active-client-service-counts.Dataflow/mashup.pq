[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "active_lh_comm_clients_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared active_lh_comm_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "active_lh_comm_clients", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"client_id"}, raw_sp_services, {"client_id"}, "raw_sp_services", JoinKind.LeftOuter),
  #"Expanded raw_sp_services" = Table.ExpandTableColumn(#"Merged queries", "raw_sp_services", {"Count"}, {"Count"}),
  #"Sorted rows" = Table.Sort(#"Expanded raw_sp_services", {{"Count", Order.Ascending}}),
  #"Replaced value" = Table.ReplaceValue(#"Sorted rows", null, 0, Replacer.ReplaceValue, {"Count"}),
  #"Renamed columns" = Table.RenameColumns(#"Replaced value", {{"Count", "service_count"}})
in
  #"Renamed columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "active_lh_school_clients_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared active_lh_school_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "active_lh_school_clients", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"client_id"}, raw_sp_services, {"client_id"}, "raw_sp_services", JoinKind.LeftOuter),
  #"Expanded raw_sp_services" = Table.ExpandTableColumn(#"Merged queries", "raw_sp_services", {"Count"}, {"Count"}),
  #"Sorted rows" = Table.Sort(#"Expanded raw_sp_services", {{"Count", Order.Ascending}}),
  #"Replaced value" = Table.ReplaceValue(#"Sorted rows", null, 0, Replacer.ReplaceValue, {"Count"}),
  #"Renamed columns" = Table.RenameColumns(#"Replaced value", {{"Count", "service_count"}}),
  #"Sorted rows 1" = Table.Sort(#"Renamed columns", {{"entry_date", Order.Ascending}})
in
  #"Sorted rows 1";
shared raw_sp_services = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "raw_sp_services", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 3", each [service_month] > #date(2025, 8, 1)),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each ([program_creating] = "VOANLA 4002 -The LightHouse - School Sites" or [program_creating] = "VOANLA 4080 -The LightHouse - Community Sites")),
  #"Grouped rows" = Table.Group(#"Filtered rows 1", {"client_id"}, {{"Count", each Table.RowCount(_), Int64.Type}})
in
  #"Grouped rows";
shared raw_sp_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_clients", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "active_4002_wNo_services_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared active_4002_wNo_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "active_lh_school_clients", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"client_id"}, raw_sp_services, {"client_id"}, "raw_sp_services", JoinKind.LeftOuter),
  #"Expanded raw_sp_services" = Table.ExpandTableColumn(#"Merged queries", "raw_sp_services", {"Count"}, {"Count"}),
  #"Sorted rows" = Table.Sort(#"Expanded raw_sp_services", {{"Count", Order.Ascending}}),
  #"Replaced value" = Table.ReplaceValue(#"Sorted rows", null, 0, Replacer.ReplaceValue, {"Count"}),
  #"Renamed columns" = Table.RenameColumns(#"Replaced value", {{"Count", "service_count"}}),
  #"Filtered rows" = Table.SelectRows(#"Renamed columns", each ([service_count] = 0)),
  #"Sorted rows 1" = Table.Sort(#"Filtered rows", {{"entry_date", Order.Ascending}})
in
  #"Sorted rows 1";
shared active_lh_school_clients_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  TableNavigation = Navigation_2{[Id = "active_lh_school_serv_count", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared active_lh_comm_clients_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  TableNavigation = Navigation_2{[Id = "active_lh_comm_sev_count", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "active_4080_wNo_services_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared active_4080_wNo_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "active_lh_comm_clients", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"client_id"}, raw_sp_services, {"client_id"}, "raw_sp_services", JoinKind.LeftOuter),
  #"Expanded raw_sp_services" = Table.ExpandTableColumn(#"Merged queries", "raw_sp_services", {"Count"}, {"Count"}),
  #"Sorted rows" = Table.Sort(#"Expanded raw_sp_services", {{"Count", Order.Ascending}}),
  #"Replaced value" = Table.ReplaceValue(#"Sorted rows", null, 0, Replacer.ReplaceValue, {"Count"}),
  #"Renamed columns" = Table.RenameColumns(#"Replaced value", {{"Count", "service_count"}}),
  #"Filtered rows" = Table.SelectRows(#"Renamed columns", each ([service_count] = 0))
in
  #"Filtered rows";
shared active_4080_wNo_services_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  TableNavigation = Navigation_2{[Id = "active_4080_wNo_services", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared active_4002_wNo_services_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  TableNavigation = Navigation_2{[Id = "analysis.active_4002_wNo_services", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
