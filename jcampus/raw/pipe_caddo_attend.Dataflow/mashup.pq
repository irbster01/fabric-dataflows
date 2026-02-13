[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "caddo_daily_attend_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared caddo_daily_attend = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "caddo_daily_attend"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Name", type text}, {"SIDNO", Int64.Type}, {"SASID", Int64.Type}, {"Sch", type text}, {"School Name", type text}, {"SIS School", Int64.Type}, {"GD", Int64.Type}, {"Ethnic", Int64.Type}, {"Sex", type text}, {"Special Codes", type text}, {"Custom Codes", type text}, {"Team", type text}, {"Class", type text}, {"Homeroom", type text}, {"Date", type date}, {"OTime", type time}, {"OCode", type text}, {"OReason", type text}, {"ITime", type time}, {"ICode", type text}, {"IReason", type text}, {"ECode", type text}, {"EDate", type date}, {"EReason", type text}, {"HLT", type text}, {"HLT Description", type text}, {"F", type text}, {"H", type text}, {"O", type text}, {"U", type text}, {"S", type text}, {"E", type text}, {"ND", type text}, {"OP", type text}, {"PTD Absent", Int64.Type}, {"YTD Absent", type number}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"YTD Absent", "ytd_absent"}, {"PTD Absent", "ptd_absent"}, {"Name", "student_name"}, {"SIDNO", "sidno"}, {"SASID", "sasid"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"School Name", "Sch", "SIS School", "GD", "Ethnic", "Sex", "Homeroom", "Class"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"Custom Codes", "custom_codes"}, {"Special Codes", "special_codes"}, {"Team", "team"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns 1", {"ICode", "IReason", "ECode", "EDate"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"Date", "attend_date"}, {"F", "full"}, {"H", "half"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns 2", {"HLT", "EReason", "ITime", "OTime", "OCode"}),
  #"Renamed columns 3" = Table.RenameColumns(#"Removed columns 2", {{"U", "unexcused"}, {"E", "excused"}, {"OReason", "out_reason"}}),
  #"Removed columns 3" = Table.RemoveColumns(#"Renamed columns 3", {"HLT Description"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed columns 3", {{"sidno", type text}, {"sasid", type text}}),
  #"Sorted rows" = Table.Sort(#"Changed column type 1", {{"attend_date", Order.Descending}}),
  #"Removed duplicates" = Table.Distinct(#"Sorted rows")
in
  #"Removed duplicates";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "caddo_daily_attend"]}[Content],
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
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 36, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 36, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared caddo_daily_attend_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_caddo_daily_attend", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "caddo_chronic_absentee_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared caddo_chronic_absentee = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "caddo_chronic_absentee"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (2)", each #"Transform file (2)"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (2)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (2)", Table.ColumnNames(#"Transform file (2)"(#"Sample file (2)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Column1", type text}, {"Column2", type text}, {"Column3", type text}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", type text}, {"Column8", type text}, {"Column9", type text}, {"Column10", type text}, {"Column11", type text}, {"Column12", type text}, {"Column13", type text}}),
  #"Promoted headers" = Table.PromoteHeaders(#"Changed column type", [PromoteAllScalars = true]),
  #"Removed columns" = Table.RemoveColumns(#"Promoted headers", {"School"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"School Name", "school_name"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"SIS School"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns 1", {{"Last Name", "last_name"}, {"First Name", "first_name"}, {"Middle Name", "middle_name"}, {"SIDNO", "sidno"}, {"SASID", "sasid"}, {"Grade", "grade"}, {"Sex", "sex"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns 1", {"Ethnic"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 2", {{"D Enroll", "days_enrolled"}, {"D Missed", "days_missed"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns 2", {{"days_enrolled", type number}, {"days_missed", type number}})
in
  #"Changed column type 1";
shared #"Sample file (2)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "caddo_chronic_absentee"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (2)" = let
  #"Parameter (2)" = #"Sample file (2)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (2)"]
in
  #"Parameter (2)";
shared #"Transform Sample file (2)" = let
  Source = Csv.Document(#"Parameter (2)", [Delimiter = ",", Columns = 13, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (2)""}"]
shared #"Transform file (2)" = (#"Parameter (2)" as binary) => let
  Source = Csv.Document(#"Parameter (2)", [Delimiter = ",", Columns = 13, QuoteStyle = QuoteStyle.None])
in
  Source;
shared caddo_chronic_absentee_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_caddo_chronic_absentee", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
