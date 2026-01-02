[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "exclusions_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared exclusions = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "OIG"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exclusions"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Inserted text range" = Table.AddColumn(#"Filtered hidden files", "Text range", each Text.Middle([Name], 0, 4), type text),
  #"Duplicated column" = Table.DuplicateColumn(#"Inserted text range", "Text range", "Text range - Copy"),
  #"Extracted last characters" = Table.TransformColumns(#"Duplicated column", {{"Text range", each Text.End(_, 2), type text}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Extracted last characters", {{"Text range", Int64.Type}}),
  #"Extracted first characters" = Table.TransformColumns(#"Changed column type 2", {{"Text range - Copy", each Text.Start(_, 2), type text}}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Extracted first characters", {{"Text range - Copy", Int64.Type}}),
  #"Renamed columns 2" = Table.RenameColumns(#"Changed column type 3", {{"Text range - Copy", "year"}, {"Text range", "month"}}),
  #"Inserted conditional column" = Table.AddColumn(#"Renamed columns 2", "month_name", each if [month] = 1 then "January" else if [month] = 2 then "February" else if [month] = 3 then "March" else if [month] = 4 then "April" else if [month] = 5 then "May" else if [month] = 6 then "June" else if [month] = 7 then "July" else if [month] = 8 then "August" else if [month] = 9 then "September" else if [month] = 10 then "October" else if [month] = 11 then "November" else if [month] = 12 then "December" else ""),
  #"Invoke custom function" = Table.AddColumn(#"Inserted conditional column", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Date modified", "year", "month_name", "Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Column1", type text}, {"Column2", type text}, {"Column3", type text}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", type text}, {"Column8", type text}, {"Column9", type text}, {"Column10", type text}, {"Column11", type text}, {"Column12", type text}, {"Column13", type text}, {"Column14", type text}, {"Column15", type text}, {"Column16", type text}, {"Column17", type text}, {"Column18", type text}}),
  #"Promoted headers" = Table.PromoteHeaders(#"Changed column type", [PromoteAllScalars = true]),
  #"Renamed columns" = Table.RenameColumns(#"Promoted headers", {{"LASTNAME", "last_name"}, {"FIRSTNAME", "first_name"}, {"MIDNAME", "middle_name"}, {"BUSNAME", "business_name"}}),
  #"Sorted rows" = Table.Sort(#"Renamed columns", {{"business_name", Order.Descending}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Sorted rows", {{"GENERAL", "general"}, {"SPECIALTY", "speciality"}, {"UPIN", "upin"}, {"NPI", "npi"}, {"DOB", "dob"}, {"ADDRESS", "street_address"}, {"CITY", "city"}, {"STATE", "state"}, {"ZIP", "zip"}, {"EXCLTYPE", "excl_type"}, {"EXCLDATE", "excl_date"}, {"REINDATE", "rein_date"}, {"WAIVERDATE", "waiver_date"}, {"WVRSTATE", "waiver_state"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns 1", "00000000", "", Replacer.ReplaceValue, {"rein_date"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "00000000", "", Replacer.ReplaceValue, {"waiver_date"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Replaced value 1", {{"excl_date", type date}, {"rein_date", type date}, {"waiver_date", type date}}),
  #"Inserted merged column" = Table.AddColumn(#"Changed column type 1", "full_name", each Text.Combine({[last_name], [first_name]}, ", "), type text),
  #"Reordered columns" = Table.ReorderColumns(#"Inserted merged column", {"full_name", "last_name", "first_name", "middle_name", "business_name", "general", "speciality", "upin", "npi", "dob", "street_address", "city", "state", "zip", "excl_type", "excl_date", "rein_date", "waiver_date", "waiver_state"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Reordered columns", ", ", "", Replacer.ReplaceValue, {"full_name"}),
  #"Changed column type 4" = Table.TransformColumnTypes(#"Replaced value 2", {{"January", type text}}),
  #"Added a Flag to Catch Errors" = Table.AddColumn(#"Changed column type 4", "error_flag", each try [excl_date] otherwise "Error"),
  #"Filtered out Header Rows" = Table.SelectRows(#"Added a Flag to Catch Errors", each ([error_flag] <> "Error")),
  #"Filtered rows" = Table.SelectRows(#"Filtered out Header Rows", each Text.Contains([full_name], "LAST")),
  #"Cleaned up Error Flag Column" = Table.RemoveColumns(#"Filtered rows", {"error_flag"})
in
  #"Cleaned up Error Flag Column";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "OIG"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exclusions"]}[Content],
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
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 18, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 18, QuoteStyle = QuoteStyle.None])
in
  Source;
shared exclusions_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_oig_exclusions", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
