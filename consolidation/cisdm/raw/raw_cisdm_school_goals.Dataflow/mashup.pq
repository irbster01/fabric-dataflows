[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "school_goals_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared school_goals = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "school_goals"]}[Content],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", let latest = List.Max(#"Navigation 4"[Date modified]) in each [Date modified] = latest),
  #"Drill down" = #"Filtered rows"{0}[Content],
  #"Imported Excel workbook" = Excel.Workbook(#"Drill down", null, true),
  #"Drill down 1" = #"Imported Excel workbook"{0}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Drill down 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Organization Name", type text}, {"Organization ID", Int64.Type}, {"District Name", type text}, {"School", type text}, {"School ID", Int64.Type}, {"NCES ID", Int64.Type}, {"Model Status", type text}, {"School Type", type text}, {"Total Enrollment", Int64.Type}, {"Goal", type text}, {"Metric", type text}, {"Baseline Value", type number}, {"Target Value", type number}, {"First Review", type number}, {"Latest Progress", type text}, {"Date of Last Review", type date}, {"Second Review", type number}, {"Third Review", type number}, {"Fourth Review", type number}, {"Fifth Review", type number}, {"Sixth Review", type number}, {"Achieved Value", type text}, {"Goal Achievement", type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"Organization Name", "org"}, {"Organization ID", "org_id"}, {"District Name", "district"}, {"School", "school_name"}, {"School ID", "school_id"}, {"NCES ID", "nces_id"}})
in
  #"Renamed columns";
shared school_goals_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_cisdm_school_goals", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
