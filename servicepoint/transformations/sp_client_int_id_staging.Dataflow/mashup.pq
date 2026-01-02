[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_sp_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_clients", ItemKind = "Table"]}[Data],
  #"Inserted text range" = Table.AddColumn(#"Navigation 2", "Text range", each Text.Middle([client_first_name], 0, 4), type text),
  #"Duplicated column" = Table.DuplicateColumn(#"Inserted text range", "client_dob", "client_dob - Copy"),
  #"Changed column type" = Table.TransformColumnTypes(#"Duplicated column", {{"client_dob - Copy", Int64.Type}}),
  #"Uppercased text" = Table.TransformColumns(#"Changed column type", {{"Text range", each Text.Upper(_), type text}}),
  #"Trimmed text" = Table.TransformColumns(#"Uppercased text", {{"Text range", each Text.Trim(_), type text}}),
  #"Cleaned text" = Table.TransformColumns(#"Trimmed text", {{"Text range", each Text.Clean(_), type text}}),
  #"Duplicated column 1" = Table.DuplicateColumn(#"Cleaned text", "client_last_name", "client_last_name - Copy"),
  #"Uppercased text 1" = Table.TransformColumns(#"Duplicated column 1", {{"client_last_name - Copy", each Text.Upper(_), type nullable text}}),
  #"Merged columns" = Table.CombineColumns(Table.TransformColumnTypes(#"Uppercased text 1", {{"client_dob - Copy", type text}}), {"client_last_name - Copy", "Text range", "client_dob - Copy"}, Combiner.CombineTextByDelimiter("", QuoteStyle.None), "int_id"),
  #"Removed duplicates" = Table.Distinct(#"Merged columns", {"client_id", "int_id"})
in
  #"Removed duplicates";
shared #"Caddo Census 2526" = let
  Source = PowerPlatform.Dataflows([]),
  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[workspaceId = "023c7b37-b171-4bd0-8016-0381e269e31c"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[dataflowId = "5b765452-f6b3-4605-b333-afd40f119d06"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[entity = "Caddo Census 2526", version = ""]}[Data],
  #"Inserted text before delimiter" = Table.AddColumn(#"Navigation 4", "Text before delimiter", each Text.BeforeDelimiter([Student Name], ",", 0), type text),
  #"Inserted text after delimiter" = Table.AddColumn(#"Inserted text before delimiter", "Text after delimiter (2)", each Text.AfterDelimiter([Student Name], ",", 0), type text),
  #"Replaced value" = Table.ReplaceValue(#"Inserted text after delimiter", " ", "", Replacer.ReplaceText, {"Text after delimiter (2)"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "-", "", Replacer.ReplaceText, {"Text after delimiter (2)"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "'", "", Replacer.ReplaceText, {"Text after delimiter (2)"}),
  #"Extracted text range" = Table.TransformColumns(#"Replaced value 2", {{"Text after delimiter (2)", each Text.Middle(_, 0, 4), type text}}),
  #"Duplicated column" = Table.DuplicateColumn(#"Extracted text range", "Birth Date", "Birth Date - Copy (2)"),
  #"Changed column type" = Table.TransformColumnTypes(#"Duplicated column", {{"Birth Date - Copy (2)", Int64.Type}}),
  #"Merged columns" = Table.CombineColumns(Table.TransformColumnTypes(#"Changed column type", {{"Birth Date - Copy (2)", type text}}), {"Text before delimiter", "Text after delimiter (2)", "Birth Date - Copy (2)"}, Combiner.CombineTextByDelimiter("", QuoteStyle.None), "int_id")
in
  #"Merged columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Lighthouse Matched with JCampus_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Lighthouse Matched with JCampus" = let
  Source = Table.NestedJoin(raw_sp_clients, {"int_id"}, #"Caddo Census 2526", {"int_id"}, "Caddo Census 2526", JoinKind.LeftOuter),
  #"Expanded Caddo Census 2527" = Table.ExpandTableColumn(Source, "Caddo Census 2526", {"Student Name", "SIDNO", "School", "Year", "School Name"}, {"Student Name", "SIDNO", "School", "Year", "School Name"}),
  #"Merged queries" = Table.NestedJoin(#"Expanded Caddo Census 2527", {"client_id"}, raw_sp_entries, {"client_id"}, "raw_sp_entries", JoinKind.LeftOuter),
  #"Expanded raw_sp_entries" = Table.ExpandTableColumn(#"Merged queries", "raw_sp_entries", {"entry_date", "exit_date", "program"}, {"entry_date", "exit_date", "program"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded raw_sp_entries", each [exit_date] > #date(2025, 8, 1) or [exit_date] = null),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each ([program] = "VOANLA 4002 -The LightHouse - School Sites" or [program] = "VOANLA 4080 -The LightHouse - Community Sites" or [program] = "VOANLA 4080a - Teen Club")),
  #"Removed other columns" = Table.SelectColumns(#"Filtered rows 1", {"client_id", "client_first_name", "client_last_name", "client_dob", "int_id", "SIDNO", "School Name", "entry_date", "exit_date", "program"}),
  #"Removed columns" = Table.RemoveColumns(#"Removed other columns", {"program"}),
  #"Removed duplicates" = Table.Distinct(#"Removed columns", {"client_id"})
in
  #"Removed duplicates";
shared raw_sp_entries = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared #"Lighthouse Matched with JCampus_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  TableNavigation = Navigation_2{[Id = "Lighthouse Matched with JCampus", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
