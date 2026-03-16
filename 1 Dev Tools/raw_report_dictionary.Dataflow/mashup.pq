[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_report_dictionary_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared raw_report_dictionary = let
  Source = Excel.Workbook(Web.Contents("https://voanorthlaorg.sharepoint.com/sites/TheVOAHub/Management/Operational%20Improvement%20Team/QA%20Manager/!%20Dev%20tools/report%20dictionary.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "Sheet1", Kind = "Sheet"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Column1", type text}, {"Column2", type text}, {"Column3", type text}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}}),
  #"Promoted headers" = Table.PromoteHeaders(#"Changed column type", [PromoteAllScalars = true])
in
  #"Promoted headers";
shared raw_report_dictionary_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_report_dictionary", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
