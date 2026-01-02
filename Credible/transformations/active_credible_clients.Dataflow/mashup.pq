[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bundle_count_byClient_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared bundle_count_byClient = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_services", ItemKind = "Table"]}[Data],
  #"Inserted start of month" = Table.AddColumn(#"Navigation 2", "Start of month", each Date.StartOfMonth([service_date]), type nullable date),
  #"Renamed columns" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}}),
  #"Filtered rows" = Table.SelectRows(#"Renamed columns", each ([service_type] = "BUN: ARST" or [service_type] = "BUN: ARST-G" or [service_type] = "BUN: Case Management" or [service_type] = "BUN: IST" or [service_type] = "CPST" or [service_type] = "PSR" or [service_type] = "PSR Group")),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"client_id", "service_month"}, {{"Count", each Table.RowCount(_), Int64.Type}}),
  #"Sorted rows" = Table.Sort(#"Grouped rows", {{"service_month", Order.Descending}})
in
  #"Sorted rows";
shared bundle_count_byClient_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "bundle_count_byClient", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
