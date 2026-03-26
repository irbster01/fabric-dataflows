[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "screener_upload_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared screener_upload = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "raw-data"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "screener_upload"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"SubmissionDate", type text}, {"ClientData.firstName", type text}, {"ClientData.lastName", type text}, {"ClientData.phone", type text}, {"ClientData.email", type text}, {"ClientData.dateOfBirth", type date}, {"ClientData.address", type text}, {"ClientData.city", type text}, {"ClientData.state", type text}, {"ClientData.zipCode", Int64.Type}, {"ClientData.q1_housing", Int64.Type}, {"ClientData.q2_transportation", Int64.Type}, {"ClientData.q3_utilities", Int64.Type}, {"ClientData.q4_food", Int64.Type}, {"ClientData.q5_healthcare", Int64.Type}, {"ClientData.notes", type text}, {"SubmittedBy", type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"ClientData.dateOfBirth", "dob"}, {"ClientData.address", "client_street_address"}, {"ClientData.city", "client_city"}, {"ClientData.state", "cliebt_state"}, {"ClientData.zipCode", "client_zip"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"SubmissionDate", type datetimezone}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Changed column type 1", {{"SubmissionDate", type date}})
in
  #"Changed column type 2";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "raw-data"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "screener_upload"]}[Content],
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
  Source = Json.Document(Parameter),
  #"Converted to table" = Table.FromRecords({Source}),
  #"Expanded ClientData" = Table.ExpandRecordColumn(#"Converted to table", "ClientData", {"firstName", "lastName", "phone", "email", "dateOfBirth", "address", "city", "state", "zipCode", "q1_housing", "q2_transportation", "q3_utilities", "q4_food", "q5_healthcare", "notes"}, {"ClientData.firstName", "ClientData.lastName", "ClientData.phone", "ClientData.email", "ClientData.dateOfBirth", "ClientData.address", "ClientData.city", "ClientData.state", "ClientData.zipCode", "ClientData.q1_housing", "ClientData.q2_transportation", "ClientData.q3_utilities", "ClientData.q4_food", "ClientData.q5_healthcare", "ClientData.notes"})
in
  #"Expanded ClientData";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Json.Document(Parameter),
  #"Converted to table" = Table.FromRecords({Source}),
  #"Expanded ClientData" = Table.ExpandRecordColumn(#"Converted to table", "ClientData", {"firstName", "lastName", "phone", "email", "dateOfBirth", "address", "city", "state", "zipCode", "q1_housing", "q2_transportation", "q3_utilities", "q4_food", "q5_healthcare", "notes"}, {"ClientData.firstName", "ClientData.lastName", "ClientData.phone", "ClientData.email", "ClientData.dateOfBirth", "ClientData.address", "ClientData.city", "ClientData.state", "ClientData.zipCode", "ClientData.q1_housing", "ClientData.q2_transportation", "ClientData.q3_utilities", "ClientData.q4_food", "ClientData.q5_healthcare", "ClientData.notes"})
in
  #"Expanded ClientData";
shared screener_upload_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_screener_upload", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
