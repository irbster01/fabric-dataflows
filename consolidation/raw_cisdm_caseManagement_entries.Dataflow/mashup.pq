[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "case_management_entries_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared case_management_entries = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "case_management_entries"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Organization", type text}, {"School", type text}, {"Student ID", Int64.Type}, {"Student Name", type text}, {"Grade Level", type text}, {" CM Intensity Level ", type text}, {"Support Category", type text}, {"Support", type text}, {"Individual or Group", type text}, {"Number of Hours", type number}, {"Provider Type", type text}, {"Primary Provider", type text}, {"Enrollment Begin Date", type date}, {"Enrollment End Date", type date}, {"School Year", type text}}),
  #"Added index" = Table.AddIndexColumn(#"Changed column type", "Index", 0, 1, Int64.Type),
  #"Renamed columns" = Table.RenameColumns(#"Added index", {{"Index", "service_id"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"service_id", type text}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type 1", {{"Organization", "org"}, {"School", "school_name"}, {"Student ID", "sasid"}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Renamed columns 1", {{"sasid", type text}, {"Activity", type text}}),
  #"Renamed columns 2" = Table.RenameColumns(#"Changed column type 2", {{"Student Name", "client_full_name"}, {"Grade Level", "grade"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns 2", {" CM Intensity Level "}),
  #"Renamed columns 3" = Table.RenameColumns(#"Removed columns", {{"Support Category", "service_type"}, {"Support", "notes"}}),
  #"Renamed columns 4" = Table.RenameColumns(#"Renamed columns 3", {{"Number of Hours", "hours"}, {"Provider Type", "provider"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns 4", "CIS Staff", "CIS", Replacer.ReplaceValue, {"provider"}),
  #"Renamed columns 5" = Table.RenameColumns(#"Replaced value", {{"Primary Provider", "coordinator_name"}, {"Enrollment Begin Date", "entry_date"}, {"Enrollment End Date", "exit_date"}, {"School Year", "school_year"}, {"Activity", "activity"}, {"Individual or Group", "ind_or_group"}})
in
  #"Renamed columns 5";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "case_management_entries"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Excel.Workbook(Parameter, null, true),
  Navigation = Source{[Item = "Sheet1", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Excel.Workbook(Parameter, null, true),
  Navigation = Source{[Item = "Sheet1", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared case_management_entries_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_cisdm_case_management_entries", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
