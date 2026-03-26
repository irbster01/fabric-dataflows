[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_credible_outreach_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared raw_credible_outreach = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "API - Outreach Service", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 3", {"service_id"}, raw_credible_services, {"service_id"}, "raw_credible_services", JoinKind.LeftOuter),
  #"Expanded raw_credible_services 1" = Table.ExpandTableColumn(#"Merged queries", "raw_credible_services", {"client_id", "service_type", "duration", "emp_name", "service_date"}, {"client_id", "service_type", "duration", "emp_name", "service_date"}),
  #"Divided column" = Table.TransformColumns(#"Expanded raw_credible_services 1", {{"duration", each _ / 60, type number}}),
  #"Removed duplicates" = Table.Distinct(#"Divided column", {"service_id"}),
  #"Filtered rows" = Table.SelectRows(#"Removed duplicates", each ([service_type] = "FEP: Outreach" or [service_type] = "FEP: SEE/Outreach" or [service_type] = "Marketing Outreach") and ([client_id] = "6984")),
  #"Removed columns" = Table.RemoveColumns(#"Filtered rows", {"supportedED_or_Employment", "contact_type", "client_present"})
in
  #"Removed columns";
shared raw_credible_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_services", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared raw_credible_outreach_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "credible_outreach_transform", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
