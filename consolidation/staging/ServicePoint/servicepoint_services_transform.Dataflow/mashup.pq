[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_sp_services = let
  Source = Fabric.Warehouse(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[warehouseId = "ea227a8a-7257-406d-9d09-8fb6f312cadc"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Schema = "dbo", Item = "sp_services_table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 3", {{"Client ID", type text}, {"Service ID", type text}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "lighthouse_services_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Last Name", DestinationColumnName = "Last Name"], [SourceColumnName = "First Name", DestinationColumnName = "First Name"], [SourceColumnName = "Client ID", DestinationColumnName = "Client ID"], [SourceColumnName = "Service Start Date", DestinationColumnName = "Service Start Date"], [SourceColumnName = "Provider Specific Service", DestinationColumnName = "Provider Specific Service"], [SourceColumnName = "Number of Units", DestinationColumnName = "Number of Units"], [SourceColumnName = "User Creating", DestinationColumnName = "User Creating"], [SourceColumnName = "Provider Creating", DestinationColumnName = "Provider Creating"], [SourceColumnName = "Service ID", DestinationColumnName = "Service ID"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared lighthouse_services = let
  Source = Fabric.Warehouse(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[warehouseId = "ea227a8a-7257-406d-9d09-8fb6f312cadc"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Schema = "dbo", Item = "sp_services_table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 3", {{"Client ID", type text}, {"Service ID", type text}}),
  #"Filtered rows" = Table.SelectRows(#"Changed column type", each ([Provider Creating] = "VOANLA 4002 -The LightHouse - School Sites" or [Provider Creating] = "VOANLA 4080 -The LightHouse - Community Sites"))
in
  #"Filtered rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "choice_services_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Last Name", DestinationColumnName = "Last Name"], [SourceColumnName = "First Name", DestinationColumnName = "First Name"], [SourceColumnName = "Client ID", DestinationColumnName = "Client ID"], [SourceColumnName = "Service Start Date", DestinationColumnName = "Service Start Date"], [SourceColumnName = "Provider Specific Service", DestinationColumnName = "Provider Specific Service"], [SourceColumnName = "Number of Units", DestinationColumnName = "Number of Units"], [SourceColumnName = "User Creating", DestinationColumnName = "User Creating"], [SourceColumnName = "Provider Creating", DestinationColumnName = "Provider Creating"], [SourceColumnName = "Service ID", DestinationColumnName = "Service ID"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared choice_services = let
  Source = Fabric.Warehouse(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[warehouseId = "ea227a8a-7257-406d-9d09-8fb6f312cadc"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Schema = "dbo", Item = "sp_services_table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 3", {{"Client ID", type text}, {"Service ID", type text}}),
  #"Filtered rows" = Table.SelectRows(#"Changed column type", each ([Provider Creating] = "VOANLA 8110 - Choice Neighborhoods"))
in
  #"Filtered rows";
shared lighthouse_services_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ea227a8a-7257-406d-9d09-8fb6f312cadc"]}[Data],
  TableNavigation = Navigation_2{[Item = "silver.lighthouse_services", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared choice_services_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ea227a8a-7257-406d-9d09-8fb6f312cadc"]}[Data],
  TableNavigation = Navigation_2{[Item = "silver.choice_services", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
