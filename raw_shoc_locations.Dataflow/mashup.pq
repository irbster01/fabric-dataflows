[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Housing Locations - Matt Turek xlsx_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Housing Locations - Matt Turek xlsx" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "3505 SHOC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Housing Locations - Matt Turek.xlsx"]}[Content],
  #"Imported Excel workbook" = Excel.Workbook(#"Navigation 4", null, true),
  #"Expanded Data" = Table.ExpandTableColumn(#"Imported Excel workbook", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27"}),
  #"Promoted headers" = Table.PromoteHeaders(#"Expanded Data", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"LOC#", Int64.Type}, {"Bldg #", Int64.Type}, {"Prgm #", Int64.Type}, {"Address", type text}, {"City, State, Zip", type text}, {"Area", Int64.Type}, {"Use", type text}, {"Client #", Int64.Type}, {"Constr", type text}, {"# Stories", Int64.Type}, {"Yr Built", Int64.Type}, {"BIEE", Int64.Type}, {"Building", Int64.Type}, {"Business Prop", Int64.Type}, {"PPO", Int64.Type}, {"Sprinkler", type text}, {"Boiler", type text}, {"Owner", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"Column22", "Column23", "Column24", "Column25", "Column26", "Column27", "Column28", "Housing_1", "Sheet", "false"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed columns", {{"Location", type text}, {"Parish", type text}})
in
  #"Changed column type 1";
shared #"Housing Locations - Matt Turek xlsx_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "fccbdcf0-4c42-431d-8e07-ae4b00bda780"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_shoc_location", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Housing Locations - Matt Turek xlsx (2)_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Housing Locations - Matt Turek xlsx (2)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "3505 SHOC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Housing Locations - Matt Turek.xlsx"]}[Content],
  #"Imported Excel workbook" = Excel.Workbook(#"Navigation 4", null, true),
  #"Expanded Data" = Table.ExpandTableColumn(#"Imported Excel workbook", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27"}),
  #"Promoted headers" = Table.PromoteHeaders(#"Expanded Data", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"LOC#", Int64.Type}, {"Bldg #", Int64.Type}, {"Prgm #", Int64.Type}, {"Address", type text}, {"City, State, Zip", type text}, {"Area", Int64.Type}, {"Use", type text}, {"Client #", Int64.Type}, {"Constr", type text}, {"# Stories", Int64.Type}, {"Yr Built", Int64.Type}, {"BIEE", Int64.Type}, {"Building", Int64.Type}, {"Business Prop", Int64.Type}, {"PPO", Int64.Type}, {"Sprinkler", type text}, {"Boiler", type text}, {"Owner", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"Column22", "Column23", "Column24", "Column25", "Column26", "Column27", "Column28", "Housing_1", "Sheet", "false"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed columns", {{"Location", type text}, {"Parish", type text}})
in
  #"Changed column type 1";
shared #"Housing Locations - Matt Turek xlsx (2)_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_shoc_locations", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
