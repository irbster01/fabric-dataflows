[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "sp_services_table_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared sp_services_table = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "ServicePoint"]}[Content],
  #"Navigation 5" = #"Navigation 4"{[Name = "Services"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 5", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Last Name", type text}, {"First Name", type text}, {"Client ID", Int64.Type}, {"Service Start Date", type date}, {"Provider Specific Service", type text}, {"Number of Units", type number}, {"User Creating", type text}, {"Provider Creating", type text}, {"Service ID", type text}}),
  #"Removed duplicates" = Table.Distinct(#"Changed column type", {"Service ID"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed duplicates", {{"Last Name", "client_last_name"}, {"First Name", "client_first_name"}, {"Client ID", "client_id"}, {"Service Start Date", "service_date"}, {"Provider Specific Service", "service_type"}, {"Number of Units", "units"}, {"User Creating", "user"}, {"Provider Creating", "program_creating"}, {"Service ID", "service_id"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"client_id", type text}}),
  #"Inserted start of month" = Table.AddColumn(#"Changed column type 1", "Start of month", each Date.StartOfMonth([service_date]), type nullable date),
  #"Renamed columns 1" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}})
in
  #"Renamed columns 1";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "ServicePoint"]}[Content],
  #"Navigation 5" = #"Navigation 4"{[Name = "Services"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 5", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  Navigation = #"Filtered hidden files"{0}[Content]
in
  Navigation;
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 9, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 9, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared sp_services_table_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_sp_services", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
