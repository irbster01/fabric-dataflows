[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "caddo_grades_export_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared caddo_grades_export = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "caddo_grades_export"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Filtered rows" = Table.SelectRows(#"Filtered hidden files", let latest = List.Max(#"Filtered hidden files"[Date modified]) in each [Date modified] = latest),
  #"Invoke custom function" = Table.AddColumn(#"Filtered rows", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Student Name", type text}, {"ID", Int64.Type}, {"State ID", Int64.Type}, {"Gd", Int64.Type}, {"Gender", type text}, {"Ethnic", Int64.Type}, {"DOB", type date}, {"School", Int64.Type}, {"School Name", type text}, {"Homeroom Teacher", type text}, {"Course", type text}, {"Section", Int64.Type}, {"ST Code", Int64.Type}, {"Teacher", type text}, {"Crds Ps", type number}, {"1st 9wks", type text}, {"2nd 9wks", type text}, {"3rd 9wks", type text}, {"4th 9wks", type text}, {"Exam 1", type text}, {"Exam 2", type text}, {"Sem 1", type text}, {"Sem 2", type text}, {"Final", type text}, {"Srt Sem", Int64.Type}, {"Num of Sem", Int64.Type}, {"Num of Prd", Int64.Type}, {"Group", Int64.Type}, {"IEP status", type text}, {"504 status", type text}, {"LEP status", type text}, {"Last Retained Grade", Int64.Type}, {"Teacher Email", type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"Student Name", "student_name"}, {"ID", "sidno"}, {"State ID", "sasid"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"sidno", type text}, {"sasid", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type 1", {"School"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"Gd", "grade"}, {"Gender", "gender"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns 1", {"Ethnic"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"DOB", "dob"}, {"School Name", "school_name"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns 2", {"Homeroom Teacher"}),
  #"Renamed columns 3" = Table.RenameColumns(#"Removed columns 2", {{"Course", "course"}}),
  #"Removed columns 3" = Table.RemoveColumns(#"Renamed columns 3", {"Section", "ST Code", "Teacher", "Crds Ps"}),
  #"Renamed columns 4" = Table.RenameColumns(#"Removed columns 3", {{"1st 9wks", "1st_9_weeks"}, {"2nd 9wks", "2nd_9_weeks"}, {"3rd 9wks", "3rd_9_weeks"}, {"4th 9wks", "4th_9_weeks"}}),
  #"Removed columns 4" = Table.RemoveColumns(#"Renamed columns 4", {"Exam 1", "Exam 2", "Sem 1", "Sem 2", "Final", "Srt Sem", "Num of Sem", "Num of Prd", "Group", "Last Retained Grade", "Teacher Email"}),
  #"Renamed columns 5" = Table.RenameColumns(#"Removed columns 4", {{"LEP status", "lep_status"}, {"504 status", "504_status"}, {"IEP status", "iep_status"}})
in
  #"Renamed columns 5";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "caddo_grades_export"]}[Content],
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
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 33, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 33, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared caddo_grades_export_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_caddo_grades_export", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
