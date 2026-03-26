[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared desoto_grades_export = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "desoto_grades_export"]}[Content],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", let latest = List.Max(#"Navigation 4"[Date modified]) in each [Date modified] = latest),
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Filtered rows", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Student Name", type text}, {"ID", type text}, {"State ID", type text}, {"Gd", Int64.Type}, {"Gender", type text}, {"Ethnic", Int64.Type}, {"DOB", type date}, {"School", Int64.Type}, {"School Name", type text}, {"Homeroom Teacher", type text}, {"Course", type text}, {"Section", Int64.Type}, {"ST Code", Int64.Type}, {"Teacher", type text}, {"Crds Ps", type number}, {"1st 9wks", type text}, {"Per P1", Int64.Type}, {"2nd 9wks", type text}, {"Per P2", Int64.Type}, {"3rd 9wks", type text}, {"Per P3", type text}, {"4th 9wks", type text}, {"Per P4", type text}, {"Exam 1", type text}, {"Per E1", Int64.Type}, {"Exam 2", type text}, {"Per E2", type text}, {"Sem 1", type text}, {"Per S1", Int64.Type}, {"Sem 2", type text}, {"Per S2", Int64.Type}, {"Final", type text}, {"Per Fin", Int64.Type}, {"Quality Points", Int64.Type}, {"Srt Sem", Int64.Type}, {"Num of Sem", Int64.Type}, {"Num of Prd", Int64.Type}, {"Group", Int64.Type}, {"IEP status", type text}, {"504 status", type text}, {"LEP status", type text}, {"Last Retained Grade", Int64.Type}, {"Teacher Email", type text}, {"Year", Int64.Type}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"ID", "sidno"}, {"State ID", "sasid"}, {"Gd", "grade"}, {"Gender", "gender"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"Ethnic", "School"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"School Name", "school _name"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns 1", {"Homeroom Teacher"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"Course", "course"}, {"Section", "sectio"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns 2", {"sectio", "ST Code", "Teacher", "Quality Points", "Srt Sem", "Num of Sem", "Num of Prd", "Group", "IEP status", "504 status", "LEP status", "Last Retained Grade", "Teacher Email"})
in
  #"Removed columns 2";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "desoto_grades_export"]}[Content],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", let latest = List.Max(#"Navigation 4"[Date modified]) in each [Date modified] = latest),
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Filtered rows", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 44, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 44, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
