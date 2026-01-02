[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "to 1111 turnover csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"to 1111 turnover csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "turnover_reports"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "to 1111 turnover.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 23, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Work Location", type text}, {"Client ID", Int64.Type}, {"Client Name", type text}, {"Employee Name", type text}, {"Department Name", type text}, {"Job Title", type text}, {"Manager", type text}, {"Status", type text}, {"Termination Date", type date}, {"Voluntary", type text}, {"Eligible for Rehire", type text}, {"Termination Reason", type text}, {"Hire Date", type date}, {"Rehire Date", type date}, {"Length of Service", type text}, {"Separations", Int64.Type}, {"Average Headcount", type number}, {"Turnover Rate", Percentage.Type}, {"Department Code", Int64.Type}, {"Employee Number", Int64.Type}, {"FLSA", type text}, {"Status Type", type text}, {"Worker Type", type text}}),
  #"Removed top rows" = Table.Skip(#"Changed column type", 1),
  #"Renamed columns" = Table.RenameColumns(#"Removed top rows", {{"Work Location", "work_location"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"Client ID", "Client Name"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"Department Name", "dept"}, {"Job Title", "job_title"}, {"Manager", "manager"}, {"Status", "status"}, {"Termination Date", "termination_date"}, {"Voluntary", "voluntary"}, {"Eligible for Rehire", "rehire_eligible"}, {"Termination Reason", "term_reason"}, {"Hire Date", "hire_date"}, {"Rehire Date", "rehire_date"}, {"Length of Service", "service_length"}, {"Separations", "separations"}, {"Average Headcount", "avg_headcount"}, {"Turnover Rate", "turnover_rate"}, {"Department Code", "dept_code"}, {"Employee Number", "emp_num"}, {"FLSA", "flsa"}, {"Status Type", "emp_type"}, {"Worker Type", "worker_type"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns 1", {{"emp_num", type text}, {"dept_code", type text}})
in
  #"Changed column type 1";
shared #"to 1111 turnover csv_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "fccbdcf0-4c42-431d-8e07-ae4b00bda780"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_paycor_turnover", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "turnover_by_dept_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared turnover_by_dept = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "Paycor"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "turnover_by_dept"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Department Name", type text}, {"Department Code", type text}, {"Client ID", type text}, {"Client Name", type text}, {"Employee Name", type text}, {"Work Location", type text}, {"Job Title", type text}, {"Manager", type text}, {"Status", type text}, {"Termination Date", type date}, {"Voluntary", type text}, {"Eligible for Rehire", type text}, {"Termination Reason", type text}, {"Hire Date", type date}, {"Rehire Date", type date}, {"Length of Service", type text}, {"Separations", Int64.Type}, {"Average Headcount", type number}, {"Turnover Rate", Percentage.Type}, {"Employee Number", Int64.Type}, {"FLSA", type text}, {"Status Type", type text}, {"Worker Type", type text}}),
  #"Filtered rows" = Table.SelectRows(#"Changed column type", each ([Employee Name] <> "")),
  #"Removed columns" = Table.RemoveColumns(#"Filtered rows", {"Client ID", "Client Name"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Department Name", "dept"}, {"Department Code", "dept_code"}, {"Employee Name", "emp_name"}, {"Work Location", "work_location"}, {"Job Title", "job_title"}, {"Manager", "manager"}, {"Status", "status"}, {"Termination Date", "term_date"}, {"Voluntary", "voluntary"}, {"Eligible for Rehire", "rehire_eligible"}, {"Termination Reason", "term_reason"}, {"Hire Date", "hire_date"}, {"Rehire Date", "rehire_date"}, {"Length of Service", "service_length"}, {"Separations", "separations"}, {"Average Headcount", "avg_headcount"}, {"Turnover Rate", "turnover_rate"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"Employee Number", type text}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type 1", {{"Employee Number", "emp_num"}, {"FLSA", "flsa"}, {"Status Type", "emp_type"}, {"Worker Type", "worker_type"}})
in
  #"Renamed columns 1";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "Paycor"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "turnover_by_dept"]}[Content],
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
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 23, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 23, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared turnover_by_dept_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "fccbdcf0-4c42-431d-8e07-ae4b00bda780"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_turnover_by_dept", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
