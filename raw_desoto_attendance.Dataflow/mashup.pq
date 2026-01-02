[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "desoto_daily_attend_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared desoto_daily_attend = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "desoto_daily_attend"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Date modified", "Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"<span ", type text}, {"Name", type text}, {"SIDNO", type text}, {"SASID", type text}, {"Sch", type text}, {"School Name", type text}, {"SIS School", Int64.Type}, {"GD", Int64.Type}, {"Ethnic", Int64.Type}, {"Sex", type text}, {"Special Codes", type text}, {"Custom Codes", type text}, {"Team", type text}, {"Class", type text}, {"Homeroom", type text}, {"Date", type date}, {"OTime", type time}, {"OCode", type text}, {"OReason", type text}, {"ITime", type time}, {"ICode", type text}, {"IReason", type text}, {"ECode", type text}, {"EDate", type date}, {"EReason", type text}, {"HLT", type text}, {"HLT Description", type text}, {"F", type text}, {"H", type text}, {"O", type text}, {"U", type text}, {"S", type text}, {"E", type text}, {"ND", type text}, {"OP", type text}, {"PTD Absent", type number}, {"YTD Absent", type number}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"<span "}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Name", "student_name"}, {"SIDNO", "sidno"}, {"SASID", "sasid"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"School Name", "Sch"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns 1", {{"SIS School", "sis_school"}, {"GD", "grade"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns 1", {"Ethnic"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 2", {{"Sex", "gender"}}),
  #"Removed columns 3" = Table.RemoveColumns(#"Renamed columns 2", {"Special Codes", "Custom Codes", "Team", "Class", "Homeroom"}),
  #"Renamed columns 3" = Table.RenameColumns(#"Removed columns 3", {{"Date", "date"}}),
  #"Removed columns 4" = Table.RemoveColumns(#"Renamed columns 3", {"OTime", "OCode", "ITime", "ICode", "EDate", "EReason", "HLT", "HLT Description"}),
  #"Renamed columns 4" = Table.RenameColumns(#"Removed columns 4", {{"F", "full"}, {"H", "half"}, {"U", "unexcused"}, {"O", "tardy_out"}}),
  #"Removed columns 5" = Table.RemoveColumns(#"Renamed columns 4", {"S", "E", "ND", "OP", "PTD Absent"}),
  #"Renamed columns 5" = Table.RenameColumns(#"Removed columns 5", {{"YTD Absent", "ytd_absent"}}),
  #"Removed columns 6" = Table.RemoveColumns(#"Renamed columns 5", {"ECode"}),
  #"Renamed columns 6" = Table.RenameColumns(#"Removed columns 6", {{"OReason", "out_reason"}, {"IReason", "in_reason"}}),
  #"Removed duplicates" = Table.Distinct(#"Renamed columns 6", {"student_name", "sidno", "sasid", "sis_school", "grade", "gender", "date", "out_reason", "in_reason", "full", "half", "tardy_out", "unexcused", "ytd_absent"}),
  #"Renamed columns 7" = Table.RenameColumns(#"Removed duplicates", {{"Date modified", "date_report_generated"}})
in
  #"Renamed columns 7";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "desoto_daily_attend"]}[Content],
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
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 37, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 37, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared desoto_daily_attend_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_desoto_daily_attend", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
