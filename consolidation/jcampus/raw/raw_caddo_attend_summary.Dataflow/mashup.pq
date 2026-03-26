[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "caddo_attend_summary_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared caddo_attend_summary = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "caddo_attend_summary"]}[Content],
  #"Filtered rows 2" = Table.SelectRows(#"Navigation 4", let latest = List.Max(#"Navigation 4"[Date modified]) in each [Date modified] = latest),
  #"Navigation 5" = #"Filtered rows 2"{[Name = "1.8 SUMMARY.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 5", [Delimiter = ",", Columns = 12, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Renamed columns" = Table.RenameColumns(#"Promoted headers", {{"Name", "name"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"ND", "OP", "H", "U", "S", "E"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"SIDNO", "sidno"}, {"Grade", "grade"}, {"School", "scho"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns 1", {"scho", "grade"}),
  #"Removed duplicates" = Table.Distinct(#"Removed columns 1", {"sidno"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed duplicates", {{"F", Int64.Type}, {"O", Int64.Type}}),
  #"Merged queries" = Table.NestedJoin(#"Changed column type", {"sidno"}, raw_caddo_student_locator, {"sidno"}, "raw_caddo_student_locator", JoinKind.LeftOuter),
  #"Expanded raw_caddo_student_locator" = Table.ExpandTableColumn(#"Merged queries", "raw_caddo_student_locator", {"sasid"}, {"sasid"}),
  #"Sorted rows" = Table.Sort(#"Expanded raw_caddo_student_locator", {{"sasid", Order.Ascending}}),
  #"Filtered rows" = Table.SelectRows(#"Sorted rows", each ([sasid] <> null))
in
  #"Filtered rows";
shared caddo_attend_summary_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "caddo_attend_summary", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared raw_caddo_student_locator = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_caddo_student_locator", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
