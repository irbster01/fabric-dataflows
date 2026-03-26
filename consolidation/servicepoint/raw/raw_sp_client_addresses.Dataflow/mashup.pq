[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_client_addresses_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared raw_client_addresses = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "ServicePoint"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Client Addresses.xlsx"]}[Content],
  #"Imported Excel workbook" = Excel.Workbook(#"Navigation 4", null, true),
  #"Expanded Data" = Table.ExpandTableColumn(#"Imported Excel workbook", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9"}),
  #"Promoted headers" = Table.PromoteHeaders(#"Expanded Data", [PromoteAllScalars = true]),
  #"Removed columns" = Table.RemoveColumns(#"Promoted headers", {"Start Date", "hi.Start Date", "Column9", "Column10", "Column8", "Table1_1", "Sheet", "false", "Table1"}),
  #"Inserted text after delimiter" = Table.AddColumn(#"Removed columns", "Text after delimiter", each Text.AfterDelimiter(Text.From([Full Address]), " ", {0, RelativePosition.FromEnd}), type text),
  #"Renamed columns" = Table.RenameColumns(#"Inserted text after delimiter", {{"Text after delimiter", "zip"}, {"Full Address", "full_address"}, {"First Name", "client_first_name"}, {"Last Name", "client_last_name"}, {"Client ID", "client_id"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"client_id", type text}, {"client_last_name", type text}, {"client_first_name", type text}, {"full_address", type text}})
in
  #"Changed column type";
shared raw_client_addresses_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_sp_client_addresses", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
