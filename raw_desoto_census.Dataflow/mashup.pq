[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "desoto_census_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared desoto_census = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "desoto_census"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Date modified", "Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Renamed columns" = Table.RenameColumns(#"Expanded table column", {{"Date modified", "date_report_generated"}}),
  #"Replaced value 1" = Table.ReplaceValue(#"Renamed columns", "-", "", Replacer.ReplaceValue, {"Student Name"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "", "", Replacer.ReplaceValue, {"Student Name"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", " ", "", Replacer.ReplaceValue, {"Student Name"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", ".", "", Replacer.ReplaceValue, {"Student Name"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 4", {{"Student Name", type text}, {"SIDNO", type text}, {"Birth Date", type date}, {"Sex", type text}, {"Eth", Int64.Type}, {"ECode", type text}, {"EDate", type date}, {"LCode", type text}, {"LDate", type text}, {"School", Int64.Type}, {"Grade", Int64.Type}, {"Phone", Int64.Type}, {"Father", type text}, {"Mother", type text}, {"Address 1", type text}, {"Apt/Lot/Ste", type text}, {"City", type text}, {"State", type text}, {"Zip", Int64.Type}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type", {{"School", "school_id"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns 1", {"LDate", "LCode", "Eth"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns", {{"Student Name", "student_name"}, {"SIDNO", "sidno"}, {"Birth Date", "dob"}, {"Sex", "gender"}, {"ECode", "entry_code"}, {"EDate", "entry_date"}, {"Grade", "grade"}, {"Phone", "phone"}, {"Father", "father"}, {"Mother", "mother"}, {"Address 1", "address"}, {"Apt/Lot/Ste", "apt"}, {"City", "city"}, {"State", "state"}, {"Zip", "zip"}}),
  #"Capitalized each word" = Table.TransformColumns(#"Renamed columns 2", {{"student_name", each Text.Proper(_), type nullable text}}),
  #"Inserted text before delimiter" = Table.AddColumn(#"Capitalized each word", "Text before delimiter", each Text.BeforeDelimiter([student_name], ",", 0), type text),
  #"Duplicated column" = Table.DuplicateColumn(#"Inserted text before delimiter", "dob", "dob - Copy"),
  #"Inserted text after delimiter" = Table.AddColumn(#"Duplicated column", "Text after delimiter", each Text.AfterDelimiter([student_name], ",", 0), type text),
  #"Replaced value" = Table.ReplaceValue(#"Inserted text after delimiter", "`", "", Replacer.ReplaceText, {"Text after delimiter"}),
  #"Extracted text range" = Table.TransformColumns(#"Replaced value", {{"Text after delimiter", each Text.Middle(_, 0, 4), type text}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Extracted text range", {{"dob - Copy", Int64.Type}}),
  #"Uppercased text" = Table.TransformColumns(#"Changed column type 1", {{"Text before delimiter", each Text.Upper(_), type text}}),
  #"Uppercased text 1" = Table.TransformColumns(#"Uppercased text", {{"Text after delimiter", each Text.Upper(_), type text}}),
  #"Inserted conditional column" = Table.AddColumn(#"Uppercased text 1", "school_name", each if [school_id] = 4 then "Logansport High School" else if [school_id] = 7 then "Mansfield High School" else if [school_id] = 10 then "Stanley High School" else if [school_id] = 12 then "North Desoto High School" else if [school_id] = 14 then "North Desoto Lower Elementary" else if [school_id] = 19 then "Mansfield Elementary" else if [school_id] = 17 then "North Desoto Middle School" else if [school_id] = 23 then "North Desoto Upper Elementary" else if [school_id] = 20 then "Mansfield Middle School" else ""),
  #"Merged columns" = Table.CombineColumns(Table.TransformColumnTypes(#"Inserted conditional column", {{"dob - Copy", type text}}), {"Text before delimiter", "Text after delimiter", "dob - Copy"}, Combiner.CombineTextByDelimiter("", QuoteStyle.None), "int_id"),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Merged columns", {{"school_name", type text}}),
  #"Sorted rows" = Table.Sort(#"Changed column type 2", {{"student_name", Order.Descending}})
in
  #"Sorted rows";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "desoto_census"]}[Content],
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
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 19, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 19, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared desoto_census_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_jcampus_desoto_census", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "modified_census_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared modified_census = let
  Source = Excel.Workbook(Web.Contents("https://voanorthlaorg.sharepoint.com/sites/TheVOAHub/Behavioral%20Health%20Data/KPI/4.%206401/CensusRpt_2024-02-12.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "Sheet1", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Student Name", type text}, {"SIDNO", Int64.Type}, {"Birth Date", type date}, {"Gen", type text}, {"School", Int64.Type}, {"Grade", Int64.Type}, {"Phone", Int64.Type}, {"Address 1", type text}, {"Apt/Lot/Ste", type text}, {"City", type text}, {"State", type text}, {"Zip", Int64.Type}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Changed column type", {{"SIDNO", type text}}),
  #"Inserted text before delimiter" = Table.AddColumn(#"Changed column type 1", "Text before delimiter", each Text.BeforeDelimiter([Student Name], ",", 0), type text),
  #"Duplicated column" = Table.DuplicateColumn(#"Inserted text before delimiter", "Birth Date", "Birth Date - Copy"),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Duplicated column", {{"Birth Date - Copy", Int64.Type}}),
  #"Inserted merged column" = Table.AddColumn(#"Changed column type 2", "Int ID", each Text.Combine({[Text before delimiter], Text.From([#"Birth Date - Copy"])}, ""), type text),
  #"Inserted conditional column" = Table.AddColumn(#"Inserted merged column", "School Name", each if [School] = 4 then "Logansport High School" else if [School] = 7 then "Mansfield High School" else if [School] = 10 then "Stanley High School" else if [School] = 12 then "North Desoto High School" else if [School] = 14 then "North Desoto Lower Elementary" else if [School] = 19 then "Mansfield Elementary" else if [School] = 17 then "North Desoto Middle School" else if [School] = 23 then "North Desoto Upper Elementary" else if [School] = 20 then "Mansfield Middle School" else ""),
  #"Transform columns" = Table.TransformColumnTypes(#"Inserted conditional column", {{"School Name", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"School Name", null}}),
  #"Inserted merged column 1" = Table.AddColumn(#"Replace errors", "Full Address", each Text.Combine({[Address 1], [City], [State], Text.From([Zip])}, " "), type text),
  #"Inserted text after delimiter" = Table.AddColumn(#"Inserted merged column 1", "Text after delimiter", each Text.AfterDelimiter([Student Name], ",", 0), type text),
  #"Inserted text before delimiter 1" = Table.AddColumn(#"Inserted text after delimiter", "Text before delimiter (2)", each Text.BeforeDelimiter([Student Name], ",", 0), type text),
  #"Renamed columns" = Table.RenameColumns(#"Inserted text before delimiter 1", {{"Text before delimiter (2)", "Last Name"}, {"Text after delimiter", "First Name"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns", " ", "", Replacer.ReplaceText, {"First Name"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "'", "", Replacer.ReplaceText, {"First Name"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "`", "", Replacer.ReplaceText, {"First Name"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", " ", "", Replacer.ReplaceText, {"Last Name"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "'", "", Replacer.ReplaceText, {"Last Name"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "`", "", Replacer.ReplaceText, {"Last Name"}),
  #"Duplicated column 1" = Table.DuplicateColumn(#"Replaced value 5", "Birth Date", "Birth Date - Copy (2)"),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Duplicated column 1", {{"Birth Date - Copy (2)", Int64.Type}}),
  #"Extracted text range" = Table.TransformColumns(#"Changed column type 3", {{"First Name", each Text.Middle(_, 0, 2), type text}}),
  #"Inserted merged column 2" = Table.AddColumn(#"Extracted text range", "INT ID (2)", each Text.Combine({[Last Name], [First Name], Text.From([#"Birth Date - Copy (2)"])}, ""), type text),
  #"Removed columns" = Table.RemoveColumns(#"Inserted merged column 2", {"Student Name", "Phone", "Address 1", "Apt/Lot/Ste", "Text before delimiter", "Birth Date - Copy", "Full Address"}),
  #"Filtered rows" = Table.SelectRows(#"Removed columns", each ([School year] = 2526)),
  #"Transform columns 1" = Table.TransformColumnTypes(#"Filtered rows", {{"School year", type text}}),
  #"Replace errors 1" = Table.ReplaceErrorValues(#"Transform columns 1", {{"School year", null}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Replace errors 1", {"First Name", "Last Name", "Birth Date - Copy (2)"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns 1", {{"Int ID", "Int ID old"}, {"INT ID (2)", "INT ID"}})
in
  #"Renamed columns 1";
shared modified_census_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "modified_desoto_census", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "test_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared test = let
  Source = Excel.Workbook(Web.Contents("https://voanorthlaorg.sharepoint.com/sites/TheVOAHub/Behavioral%20Health%20Data/KPI/4.%206401/CensusRpt_2024-02-12.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "Sheet1", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Student Name", type text}, {"SIDNO", Int64.Type}, {"Birth Date", type date}, {"Gen", type text}, {"School", Int64.Type}, {"Grade", Int64.Type}, {"Phone", Int64.Type}, {"Address 1", type text}, {"Apt/Lot/Ste", type text}, {"City", type text}, {"State", type text}, {"Zip", Int64.Type}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Changed column type", {{"SIDNO", type text}}),
  #"Inserted text before delimiter" = Table.AddColumn(#"Changed column type 1", "Text before delimiter", each Text.BeforeDelimiter([Student Name], ",", 0), type text),
  #"Duplicated column" = Table.DuplicateColumn(#"Inserted text before delimiter", "Birth Date", "Birth Date - Copy"),
  #"Inserted text after delimiter" = Table.AddColumn(#"Duplicated column", "Text after delimiter", each Text.AfterDelimiter([Student Name], ",", 0), type text),
  #"Replaced value 2" = Table.ReplaceValue(#"Inserted text after delimiter", " ", "", Replacer.ReplaceText, {"Text after delimiter"}),
  #"Replaced value" = Table.ReplaceValue(#"Replaced value 2", "`", "", Replacer.ReplaceText, {"Text after delimiter"}),
  #"Extracted text range" = Table.TransformColumns(#"Replaced value", {{"Text after delimiter", each Text.Middle(_, 0, 4), type text}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Extracted text range", {{"Birth Date - Copy", Int64.Type}}),
  #"Inserted conditional column" = Table.AddColumn(#"Changed column type 2", "School Name", each if [School] = 4 then "Logansport High School" else if [School] = 7 then "Mansfield High School" else if [School] = 10 then "Stanley High School" else if [School] = 12 then "North Desoto High School" else if [School] = 14 then "North Desoto Lower Elementary" else if [School] = 19 then "Mansfield Elementary" else if [School] = 17 then "North Desoto Middle School" else if [School] = 23 then "North Desoto Upper Elementary" else if [School] = 20 then "Mansfield Middle School" else ""),
  #"Inserted merged column" = Table.AddColumn(#"Inserted conditional column", "Int ID", each Text.Combine({[Text before delimiter], [Text after delimiter], Text.From([#"Birth Date - Copy"])}, ""), type text),
  #"Transform columns" = Table.TransformColumnTypes(#"Inserted merged column", {{"School Name", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"School Name", null}}),
  #"Inserted merged column 1" = Table.AddColumn(#"Replace errors", "Full Address", each Text.Combine({[Address 1], [City], [State], Text.From([Zip])}, " "), type text),
  #"Removed columns" = Table.RemoveColumns(#"Inserted merged column 1", {"Phone", "Address 1", "Apt/Lot/Ste", "Text before delimiter", "Birth Date - Copy", "Full Address", "Text after delimiter"}),
  #"Transform columns 1" = Table.TransformColumnTypes(#"Removed columns", {{"School year", type text}}),
  #"Replace errors 1" = Table.ReplaceErrorValues(#"Transform columns 1", {{"School year", null}}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replace errors 1", "CHERRY-JACKSONZE40798", "CHERRY JACKSZE40798", Replacer.ReplaceValue, {"Int ID"})
in
  #"Replaced value 1";
shared test_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "test", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
