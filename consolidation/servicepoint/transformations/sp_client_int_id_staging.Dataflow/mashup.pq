[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_sp_clients_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared raw_sp_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_clients", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 2", {{"internal_id", "int_id"}}),
  #"Removed duplicates" = Table.Distinct(#"Renamed columns", {"int_id"}),
  #"Duplicated column 1" = Table.DuplicateColumn(#"Removed duplicates", "client_last_name", "client_last_name - Copy"),
  #"Uppercased text 1" = Table.TransformColumns(#"Duplicated column 1", {{"client_last_name - Copy", each Text.Upper(_), type nullable text}}),
  #"Extracted text range" = Table.TransformColumns(#"Uppercased text 1", {{"int_id", each Text.Middle(_, 4, 50), type text}}),
  #"Duplicated column" = Table.DuplicateColumn(#"Extracted text range", "client_first_name", "client_first_name - Copy"),
  #"Extracted text range 1" = Table.TransformColumns(#"Duplicated column", {{"client_first_name - Copy", each Text.Middle(_, 0, 4), type text}}),
  #"Uppercased text" = Table.TransformColumns(#"Extracted text range 1", {{"client_first_name - Copy", each Text.Upper(_), type text}}),
  #"Duplicated column 2" = Table.DuplicateColumn(#"Uppercased text", "dob", "dob - Copy"),
  #"Changed column type" = Table.TransformColumnTypes(#"Duplicated column 2", {{"dob - Copy", Int64.Type}}),
  #"Merged columns" = Table.CombineColumns(Table.TransformColumnTypes(#"Changed column type", {{"dob - Copy", type text}}), {"client_last_name - Copy", "client_first_name - Copy", "dob - Copy"}, Combiner.CombineTextByDelimiter("", QuoteStyle.None), "int_id (2)"),
  #"Removed columns" = Table.RemoveColumns(#"Merged columns", {"int_id"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"int_id (2)", "int_id"}})
in
  #"Renamed columns 1";
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
  #"Merged queries" = Table.NestedJoin(#"Expanded Caddo Census 2527", {"client_id"}, sp_matched_wJCampus, {"client_id"}, "raw_sp_entries", JoinKind.LeftOuter),
  #"Expanded raw_sp_entries" = Table.ExpandTableColumn(#"Merged queries", "raw_sp_entries", {"entry_date", "exit_date", "program"}, {"entry_date", "exit_date", "program"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded raw_sp_entries", each [exit_date] > #date(2025, 8, 1) or [exit_date] = null),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each ([program] = "VOANLA 4002 -The LightHouse - School Sites" or [program] = "VOANLA 4080 -The LightHouse - Community Sites" or [program] = "VOANLA 4080a - Teen Club")),
  #"Removed other columns" = Table.SelectColumns(#"Filtered rows 1", {"client_id", "client_first_name", "client_last_name", "dob", "int_id", "SIDNO", "School Name", "entry_date", "exit_date", "program"}),
  #"Removed columns" = Table.RemoveColumns(#"Removed other columns", {"program"}),
  #"Removed duplicates" = Table.Distinct(#"Removed columns", {"client_id"}),
  #"Sorted rows" = Table.Sort(#"Removed duplicates", {{"SIDNO", Order.Descending}})
in
  #"Sorted rows";
shared sp_matched_wJCampus = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"client_id"}, raw_sp_clients_wIntID, {"client_id"}, "raw_sp_clients_wIntID", JoinKind.Inner),
  #"Expanded raw_sp_clients_wIntID 1" = Table.ExpandTableColumn(#"Merged queries", "raw_sp_clients_wIntID", {"client_first_name", "client_last_name", "client_dob", "int_id"}, {"client_first_name", "client_last_name", "client_dob", "int_id"}),
  #"Merged queries 1" = Table.NestedJoin(#"Expanded raw_sp_clients_wIntID 1", {"int_id"}, #"Caddo Census 2526", {"int_id"}, "Caddo Census 2526", JoinKind.Inner),
  #"Expanded Caddo Census 2526" = Table.ExpandTableColumn(#"Merged queries 1", "Caddo Census 2526", {"SIDNO", "Grade", "Year", "School Name"}, {"SIDNO", "Grade", "Year", "School Name"})
in
  #"Expanded Caddo Census 2526";
shared #"Lighthouse Matched with JCampus_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  TableNavigation = Navigation_2{[Id = "Lighthouse Matched with JCampus", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared raw_sp_clients_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_sp_clients_wIntID", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared raw_sp_clients_wIntID = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_clients_wIntID", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared choice_wJcampus_data = let
  Source = sp_matched_wJCampus,
  #"Filtered rows" = Table.SelectRows(Source, each ([program] = "VOANLA 8110 - Choice Neighborhoods"))
in
  #"Filtered rows";
shared DefaultDestination = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data]
in
  #"Navigation 2";
