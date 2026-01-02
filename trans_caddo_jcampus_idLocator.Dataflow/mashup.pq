[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_caddo_census_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Student Name", DestinationColumnName = "Student Name"], [SourceColumnName = "SIDNO", DestinationColumnName = "SIDNO"], [SourceColumnName = "Birth Date", DestinationColumnName = "Birth Date"], [SourceColumnName = "Sex", DestinationColumnName = "Sex"], [SourceColumnName = "Eth", DestinationColumnName = "Eth"], [SourceColumnName = "ECode", DestinationColumnName = "ECode"], [SourceColumnName = "EDate", DestinationColumnName = "EDate"], [SourceColumnName = "LCode", DestinationColumnName = "LCode"], [SourceColumnName = "LDate", DestinationColumnName = "LDate"], [SourceColumnName = "School", DestinationColumnName = "School"], [SourceColumnName = "Grade", DestinationColumnName = "Grade"], [SourceColumnName = "Phone", DestinationColumnName = "Phone"], [SourceColumnName = "Father", DestinationColumnName = "Father"], [SourceColumnName = "Mother", DestinationColumnName = "Mother"], [SourceColumnName = "Address 1", DestinationColumnName = "Address 1"], [SourceColumnName = "Apt/Lot/Ste", DestinationColumnName = "Apt/Lot/Ste"], [SourceColumnName = "City", DestinationColumnName = "City"], [SourceColumnName = "State", DestinationColumnName = "State"], [SourceColumnName = "Zip", DestinationColumnName = "Zip"], [SourceColumnName = "Year", DestinationColumnName = "Year"], [SourceColumnName = "SASID", DestinationColumnName = "SASID"], [SourceColumnName = "Int ID", DestinationColumnName = "Int ID"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared raw_caddo_census = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_caddo_census", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([Year] = 2526)),
  #"Removed columns" = Table.RemoveColumns(#"Filtered rows", {"Sheet1_1", "Sheet", "false", "Sheet1"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns", {{"SIDNO", type text}}),
  #"Merged queries" = Table.NestedJoin(#"Changed column type", {"SIDNO"}, #"Student Locator", {"SIDNO"}, "Student Locator", JoinKind.LeftOuter),
  #"Expanded Student Locator" = Table.ExpandTableColumn(#"Merged queries", "Student Locator", {"SASID"}, {"SASID"}),
  #"Inserted text before delimiter" = Table.AddColumn(#"Expanded Student Locator", "Text before delimiter", each Text.BeforeDelimiter([Student Name], ",", 0), type text),
  #"Inserted text after delimiter" = Table.AddColumn(#"Inserted text before delimiter", "Text after delimiter", each Text.AfterDelimiter([Student Name], ",", 0), type text),
  #"Replaced value" = Table.ReplaceValue(#"Inserted text after delimiter", "`", "", Replacer.ReplaceText, {"Text after delimiter"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "-", "", Replacer.ReplaceText, {"Text after delimiter"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "'", "", Replacer.ReplaceText, {"Text after delimiter"}),
  #"Trimmed text 1" = Table.TransformColumns(#"Replaced value 2", {{"Text after delimiter", each Text.Trim(_), type nullable text}}),
  #"Cleaned text 1" = Table.TransformColumns(#"Trimmed text 1", {{"Text after delimiter", each Text.Clean(_), type nullable text}}),
  #"Replaced value 3" = Table.ReplaceValue(#"Cleaned text 1", " ", "", Replacer.ReplaceText, {"Text after delimiter"}),
  #"Extracted text range" = Table.TransformColumns(#"Replaced value 3", {{"Text after delimiter", each Text.Middle(_, 0, 4), type text}}),
  #"Trimmed text" = Table.TransformColumns(#"Extracted text range", {{"Text before delimiter", each Text.Trim(_), type text}}),
  #"Cleaned text" = Table.TransformColumns(#"Trimmed text", {{"Text before delimiter", each Text.Clean(_), type text}}),
  #"Duplicated column" = Table.DuplicateColumn(#"Cleaned text", "Birth Date", "Birth Date - Copy"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Duplicated column", {{"Birth Date - Copy", Int64.Type}}),
  #"Merged columns" = Table.CombineColumns(Table.TransformColumnTypes(#"Changed column type 1", {{"Birth Date - Copy", type text}}), {"Text before delimiter", "Text after delimiter", "Birth Date - Copy"}, Combiner.CombineTextByDelimiter("", QuoteStyle.None), "Int ID")
in
  #"Merged columns";
shared #"Student Locator" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Student Locator.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 22, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Sch", Int64.Type}, {"School Abbr", type text}, {"Student Name", type text}, {"Last Name", type text}, {"First Name", type text}, {"Sex", type text}, {"GD", Int64.Type}, {"SIDNO", Int64.Type}, {"SASID", Int64.Type}, {"Special Codes", type text}, {"Custom Codes", type text}, {"Period 1", type text}, {"Period 2", type text}, {"Period 3", type text}, {"Period 4", type text}, {"Period 5", type text}, {"Period 6", type text}, {"Period 7", type text}, {"Period 8", type text}, {"Homeroom Teacher", type text}, {"Advisor", type text}, {"Counselor", type text}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Changed column type", {{"SIDNO", type text}, {"SASID", type text}}),
  #"Removed other columns" = Table.SelectColumns(#"Changed column type 1", {"SIDNO", "SASID"})
in
  #"Removed other columns";
shared raw_caddo_census_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "e4d2de35-5f5d-457f-8e5b-43f758299057"]}[Data],
  TableNavigation = Navigation_2{[Item = "silver.caddo_census", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
