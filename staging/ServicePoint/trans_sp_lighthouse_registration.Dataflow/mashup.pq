[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_lighthouse_registration = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "ea227a8a-7257-406d-9d09-8fb6f312cadc"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "dbo", Item = "raw_lighthouse_registration"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "sp_entry_table_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Client ID", DestinationColumnName = "Client ID"], [SourceColumnName = "Entry Date", DestinationColumnName = "Entry Date"], [SourceColumnName = "Entry Exit ID", DestinationColumnName = "Entry Exit ID"], [SourceColumnName = "Exit Date", DestinationColumnName = "Exit Date"], [SourceColumnName = "Provider", DestinationColumnName = "Provider"], [SourceColumnName = "First Name", DestinationColumnName = "First Name"], [SourceColumnName = "Last Name", DestinationColumnName = "Last Name"], [SourceColumnName = "Date of Birth", DestinationColumnName = "Date of Birth"], [SourceColumnName = "Int ID", DestinationColumnName = "Int ID"], [SourceColumnName = "SIDNO", DestinationColumnName = "SIDNO"], [SourceColumnName = "School", DestinationColumnName = "School"], [SourceColumnName = "Grade", DestinationColumnName = "Grade"], [SourceColumnName = "Year", DestinationColumnName = "Year"], [SourceColumnName = "SASID", DestinationColumnName = "SASID"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared sp_entry_table = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "ea227a8a-7257-406d-9d09-8fb6f312cadc"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "dbo", Item = "sp_entry_table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Client ID", type text}}),
  #"Filtered rows" = Table.SelectRows(#"Changed column type", each ([Exit Date] = null) and ([Provider] = "VOANLA 4002 -The LightHouse - School Sites" or [Provider] = "VOANLA 4080 -The LightHouse - Community Sites" or [Provider] = "VOANLA 4080a - Teen Club")),
  #"Merged queries 1" = Table.NestedJoin(#"Filtered rows", {"Client ID"}, clients, {"Client ID"}, "clients", JoinKind.LeftOuter),
  #"Expanded clients" = Table.ExpandTableColumn(#"Merged queries 1", "clients", {"First Name", "Last Name", "Date of Birth"}, {"First Name", "Last Name", "Date of Birth"}),
  #"Replaced value" = Table.ReplaceValue(#"Expanded clients", "'", "", Replacer.ReplaceText, {"First Name"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "-", "", Replacer.ReplaceText, {"Last Name"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 1", ".", "", Replacer.ReplaceText, {"Last Name"}),
  #"Cleaned text 1" = Table.TransformColumns(#"Replaced value 3", {{"Last Name", each Text.Clean(_), type nullable text}}),
  #"Trimmed text 1" = Table.TransformColumns(#"Cleaned text 1", {{"Last Name", each Text.Trim(_), type nullable text}}),
  #"Replaced value 2" = Table.ReplaceValue(#"Trimmed text 1", "’", "", Replacer.ReplaceText, {"First Name"}),
  #"Duplicated column" = Table.DuplicateColumn(#"Replaced value 2", "Last Name", "Last Name - Copy"),
  #"Uppercased text" = Table.TransformColumns(#"Duplicated column", {{"Last Name - Copy", each Text.Upper(_), type nullable text}}),
  #"Inserted text range" = Table.AddColumn(#"Uppercased text", "Text range", each Text.Middle([First Name], 0, 4), type text),
  #"Uppercased text 1" = Table.TransformColumns(#"Inserted text range", {{"Text range", each Text.Upper(_), type text}}),
  #"Duplicated column 1" = Table.DuplicateColumn(#"Uppercased text 1", "Date of Birth", "Date of Birth - Copy"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Duplicated column 1", {{"Date of Birth - Copy", Int64.Type}}),
  #"Merged columns" = Table.CombineColumns(Table.TransformColumnTypes(#"Changed column type 1", {{"Date of Birth - Copy", type text}}), {"Last Name - Copy", "Text range", "Date of Birth - Copy"}, Combiner.CombineTextByDelimiter("", QuoteStyle.None), "Int ID"),
  #"Replaced value 5" = Table.ReplaceValue(#"Merged columns", "VEALNICK39512", "VEALNICH39512", Replacer.ReplaceValue, {"Int ID"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "'", "", Replacer.ReplaceText, {"Int ID"}),
  #"Trimmed text" = Table.TransformColumns(#"Replaced value 6", {{"Int ID", each Text.Trim(_), type nullable text}}),
  #"Cleaned text" = Table.TransformColumns(#"Trimmed text", {{"Int ID", each Text.Clean(_), type nullable text}}),
  #"Replaced value 7" = Table.ReplaceValue(#"Cleaned text", " ", "", Replacer.ReplaceText, {"Int ID"}),
  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 7", "RANDOLPHMADI41985", "RANDOLPHMADI41990", Replacer.ReplaceValue, {"Int ID"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 8", "MOSLEYTYRI43343", "MOSLEYTYRI43342", Replacer.ReplaceValue, {"Int ID"}),
  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 9", "TORRESGARCESDANI42623", "TORRES-GARCESDANI42623", Replacer.ReplaceValue, {"Int ID"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", "MULLONEALEX39401", "MULLONEALEX39401", Replacer.ReplaceValue, {"Int ID"}),
  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", "MOOREJAAE42032", "MOOREJAAE42032", Replacer.ReplaceValue, {"Int ID"}),
  #"Replaced value 13" = Table.ReplaceValue(#"Replaced value 12", "GOODRUMKYLA41979", "GOODRUMKYLA41979", Replacer.ReplaceValue, {"Int ID"}),
  #"Replaced value 14" = Table.ReplaceValue(#"Replaced value 13", "KEMP3TY43415", "KEMPTYDE43415", Replacer.ReplaceValue, {"Int ID"}),
  #"Merged queries" = Table.NestedJoin(#"Replaced value 14", {"Int ID"}, #"silver caddo_census", {"Int ID"}, "silver caddo_census", JoinKind.LeftOuter),
  #"Expanded silver caddo_census" = Table.ExpandTableColumn(#"Merged queries", "silver caddo_census", {"SIDNO", "School", "Grade", "Year", "SASID"}, {"SIDNO", "School", "Grade", "Year", "SASID"}),
  #"Merged queries 2" = Table.NestedJoin(#"Expanded silver caddo_census", {"Client ID"}, raw_lighthouse_registration, {"Client ID"}, "raw_lighthouse_registration", JoinKind.LeftOuter),
  #"Expanded raw_lighthouse_registration" = Table.ExpandTableColumn(#"Merged queries 2", "raw_lighthouse_registration", {"Current Grade Level"}, {"Current Grade Level"}),
  #"Inserted conditional column" = Table.AddColumn(#"Expanded raw_lighthouse_registration", "Custom", each if [Current Grade Level] = "" then [Grade] else [Current Grade Level]),
  #"Replaced value 4" = Table.ReplaceValue(#"Inserted conditional column", "K", 0, Replacer.ReplaceValue, {"Custom"}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Replaced value 4", {{"Custom", Int64.Type}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type 2", {"Current Grade Level", "Grade"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Custom", "Grade"}})
in
  #"Renamed columns";
shared #"SP Clients" = let
  Source = PowerBI.Datamarts("nzrmcoc5w6tebmq3bkxbdeohga-pptkz72kh6xe7ddoxtqo5fhs64.datamart.fabric.microsoft.com"),
  #"Navigation 1" = Source{[Schema = "model", Item = "SP Clients"]}[Data]
in
  #"Navigation 1";
shared clients = let
  Source = Csv.Document(Web.Contents("https://voanorthlaorg-my.sharepoint.com/personal/russell_irby_voanorthla_org/Documents/Apps/Microsoft Power Query/Uploaded Files/clients.csv"), [Delimiter = ",", Columns = 4, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Client ID", Int64.Type}, {"First Name", type text}, {"Last Name", type text}, {"Date of Birth", type date}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Changed column type", {{"Client ID", type text}})
in
  #"Changed column type 1";
shared #"silver caddo_census" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "e4d2de35-5f5d-457f-8e5b-43f758299057"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "dbo", Item = "silver.caddo_census"]}[Data],
  #"Removed duplicates" = Table.Distinct(#"Navigation 2", {"SIDNO"}),
  #"Sorted rows" = Table.Sort(#"Removed duplicates", {{"Student Name", Order.Ascending}}),
  #"Filtered rows" = Table.SelectRows(#"Sorted rows", each Text.Contains([Student Name], "ONEAL"))
in
  #"Filtered rows";
shared sp_entry_table_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "e4d2de35-5f5d-457f-8e5b-43f758299057"]}[Data],
  TableNavigation = Navigation_2{[Item = "silver.lighthouse_studentData", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared #"sp_entry_table (2)" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "ea227a8a-7257-406d-9d09-8fb6f312cadc"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "dbo", Item = "sp_entry_table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Client ID", type text}}),
  #"Filtered rows" = Table.SelectRows(#"Changed column type", each ([Exit Date] = null) and ([Provider] = "VOANLA 4002 -The LightHouse - School Sites" or [Provider] = "VOANLA 4080 -The LightHouse - Community Sites" or [Provider] = "VOANLA 4080a - Teen Club")),
  #"Merged queries 1" = Table.NestedJoin(#"Filtered rows", {"Client ID"}, clients, {"Client ID"}, "clients", JoinKind.LeftOuter),
  #"Expanded clients" = Table.ExpandTableColumn(#"Merged queries 1", "clients", {"First Name", "Last Name", "Date of Birth"}, {"First Name", "Last Name", "Date of Birth"}),
  #"Replaced value" = Table.ReplaceValue(#"Expanded clients", "'", "", Replacer.ReplaceText, {"First Name"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "-", "", Replacer.ReplaceText, {"Last Name"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 1", ".", "", Replacer.ReplaceText, {"Last Name"}),
  #"Cleaned text 1" = Table.TransformColumns(#"Replaced value 3", {{"Last Name", each Text.Clean(_), type nullable text}}),
  #"Trimmed text 1" = Table.TransformColumns(#"Cleaned text 1", {{"Last Name", each Text.Trim(_), type nullable text}}),
  #"Replaced value 2" = Table.ReplaceValue(#"Trimmed text 1", "’", "", Replacer.ReplaceText, {"First Name"}),
  #"Duplicated column" = Table.DuplicateColumn(#"Replaced value 2", "Last Name", "Last Name - Copy"),
  #"Uppercased text" = Table.TransformColumns(#"Duplicated column", {{"Last Name - Copy", each Text.Upper(_), type nullable text}}),
  #"Inserted text range" = Table.AddColumn(#"Uppercased text", "Text range", each Text.Middle([First Name], 0, 4), type text),
  #"Uppercased text 1" = Table.TransformColumns(#"Inserted text range", {{"Text range", each Text.Upper(_), type text}}),
  #"Duplicated column 1" = Table.DuplicateColumn(#"Uppercased text 1", "Date of Birth", "Date of Birth - Copy"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Duplicated column 1", {{"Date of Birth - Copy", Int64.Type}}),
  #"Merged columns" = Table.CombineColumns(Table.TransformColumnTypes(#"Changed column type 1", {{"Date of Birth - Copy", type text}}), {"Last Name - Copy", "Text range", "Date of Birth - Copy"}, Combiner.CombineTextByDelimiter("", QuoteStyle.None), "Int ID"),
  #"Merged queries" = Table.NestedJoin(#"Merged columns", {"Int ID"}, #"silver caddo_census", {"Int ID"}, "silver caddo_census", JoinKind.LeftAnti),
  #"Expanded silver caddo_census" = Table.ExpandTableColumn(#"Merged queries", "silver caddo_census", {"SIDNO", "School", "Grade", "Year", "SASID"}, {"SIDNO", "School", "Grade", "Year", "SASID"}),
  #"Merged queries 2" = Table.NestedJoin(#"Expanded silver caddo_census", {"Client ID"}, raw_lighthouse_registration, {"Client ID"}, "raw_lighthouse_registration", JoinKind.LeftOuter),
  #"Expanded raw_lighthouse_registration" = Table.ExpandTableColumn(#"Merged queries 2", "raw_lighthouse_registration", {"Current Grade Level"}, {"Current Grade Level"}),
  #"Inserted conditional column" = Table.AddColumn(#"Expanded raw_lighthouse_registration", "Custom", each if [Current Grade Level] = "" then [Grade] else [Current Grade Level]),
  #"Replaced value 4" = Table.ReplaceValue(#"Inserted conditional column", "K", 0, Replacer.ReplaceValue, {"Custom"}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Replaced value 4", {{"Custom", Int64.Type}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type 2", {"Current Grade Level", "Grade"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Custom", "Grade"}})
in
  #"Renamed columns";
