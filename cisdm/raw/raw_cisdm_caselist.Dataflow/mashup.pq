[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "cis_clients_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared cis_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "cis_clients"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Column1", type text}, {"Column2", type text}, {"Column3", type text}, {"Column4", type text}, {"Column5", type text}, {"Column7", type text}, {"Column8", type text}, {"Column9", type text}, {"Column10", type text}, {"Column11", type text}, {"Column12", type text}, {"Column13", type text}, {"Column14", type text}, {"Column15", type text}, {"Column17", type text}, {"Column18", type text}, {"Column19", type text}, {"Column20", type text}, {"Column21", type text}, {"Column26", type text}, {"Column27", type text}, {"Column28", type text}, {"Column29", type text}, {"Column30", type text}, {"Column31", type text}, {"Column32", type text}, {"Column35", type text}}),
  #"Removed top rows" = Table.Skip(#"Changed column type", 1),
  #"Promoted headers" = Table.PromoteHeaders(#"Removed top rows", [PromoteAllScalars = true]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Promoted headers", {{"Student System ID", type text}, {"Student ID", Int64.Type}, {"Birth Date", type date}, {"Enrollment #(lf)Begin Date", type date}, {"First CIS Enrollment Date", type date}, {"Organization ID", Int64.Type}, {"Site ID", Int64.Type}, {"NCES ID", Int64.Type}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type 1", {"Organization ID", "Site ID", "NCES ID", "Initiatives"}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Removed columns", {{"Student ID", type text}}),
  #"Removed other columns 1" = Table.SelectColumns(#"Changed column type 2", {"Home School", "Student System ID", "Student ID", "Student", "Birth Date", "Enrollment #(lf)Begin Date", "Enrollment#(lf)Exit Date", "First CIS Enrollment Date"}),
  #"Removed duplicates" = Table.Distinct(#"Removed other columns 1", {"Student System ID"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed duplicates", {{"Student System ID", "client_id"}, {"Student ID", "sidno"}, {"Student", "client_full_name"}}),
  #"Removed duplicates 1" = Table.Distinct(#"Renamed columns", {"sidno"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed duplicates 1", {{"Home School", "school_name"}, {"Birth Date", "client_dob"}, {"Enrollment #(lf)Begin Date", "entry_start_date"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns 1", {"Enrollment#(lf)Exit Date"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"First CIS Enrollment Date", "first_cis_enrollment_date"}})
in
  #"Renamed columns 2";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "cis_clients"]}[Content],
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
  Navigation = Source{[Item = "Caselist Details", Kind = "Sheet"]}[Data]
in
  Navigation;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Excel.Workbook(Parameter, null, true),
  Navigation = Source{[Item = "Caselist Details", Kind = "Sheet"]}[Data]
in
  Navigation;
shared cis_clients_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_cisdm_clients", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
