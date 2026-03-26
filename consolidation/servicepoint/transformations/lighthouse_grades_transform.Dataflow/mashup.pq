[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared clean_lighthouse_entries = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "clean_lighthouse_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each [exit_date] > #date(2025, 8, 1) or [exit_date] = null)
in
  #"Filtered rows";
shared lighthouse_client_master = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "lighthouse_client_master", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"client_id"}, raw_ligthouse_grades, {"client_id"}, "raw_ligthouse_grades", JoinKind.LeftOuter),
  #"Expanded raw_ligthouse_grades" = Table.ExpandTableColumn(#"Merged queries", "raw_ligthouse_grades", {"1Q_grades", "2Q_grades", "3Q_grades"}, {"1Q_grades", "2Q_grades", "3Q_grades"}),
  #"Inserted conditional column" = Table.AddColumn(#"Expanded raw_ligthouse_grades", "1Q_grade_check", each if [1Q_grades] >= 2.5 then "GOOD" else if [1Q_grades] = null then "NOGRADE" else "BAD"),
  #"Reordered columns" = Table.ReorderColumns(#"Inserted conditional column", {"client_id", "first_name", "last_name", "entry_date", "exit_date", "lighthouse_location_enrolling", "program", "1Q_grades", "1Q_grade_check", "2Q_grades", "3Q_grades"}),
  #"Inserted conditional column 1" = Table.AddColumn(#"Reordered columns", "1q_counter", each if [1Q_grade_check] = "GOOD" then 1 else if [1Q_grade_check] = "BAD" then 0 else 0),
  #"Added custom" = Table.AddColumn(#"Inserted conditional column 1", "Custom", each 1),
  #"Renamed columns" = Table.RenameColumns(#"Added custom", {{"Custom", "control"}}),
  #"Filtered rows 2" = Table.SelectRows(#"Renamed columns", each ([1Q_grades] <> null)),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows 2", {{"1Q_grade_check", type text}, {"1q_counter", type number}, {"control", type number}})
in
  #"Changed column type";
shared raw_ligthouse_grades = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_ligthouse_grades", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([school_year] = "2025-26"))
in
  #"Filtered rows";
[BindToDefaultDestination = true]
shared #"1q_lh_grades" = let
  Source = lighthouse_client_master,
  #"Filtered rows" = Table.SelectRows(Source, each [entry_date] >= #date(2025, 8, 1) and [entry_date] <= #date(2025, 10, 17) or [entry_date] < #date(2025, 8, 1)),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"lighthouse_location_enrolling"}, {{"counter", each List.Sum([1q_counter]), type any}, {"control", each List.Sum([control]), type any}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Grouped rows", {{"counter", type number}, {"control", type number}}),
  #"Inserted division" = Table.AddColumn(#"Changed column type", "Division", each [counter] / [control], type number),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted division", {{"Division", Percentage.Type}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type 1", {{"control", "total_counted"}, {"counter", "meeting_target"}})
in
  #"Renamed columns";
[BindToDefaultDestination = true]
shared #"2q_lh_grades" = let
  Source = lighthouse_client_master,
  #"Filtered rows" = Table.SelectRows(Source, each [entry_date] >= #date(2025, 10, 17) and [entry_date] <= #date(2025, 12, 20) or [entry_date] < #date(2025, 8, 1)),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"lighthouse_location_enrolling"}, {{"counter", each List.Sum([1q_counter]), type any}, {"control", each List.Sum([control]), type any}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Grouped rows", {{"counter", type number}, {"control", type number}}),
  #"Inserted division" = Table.AddColumn(#"Changed column type", "Division", each [counter] / [control], type number),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted division", {{"Division", Percentage.Type}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type 1", {{"control", "total_counted"}, {"counter", "meeting_target"}})
in
  #"Renamed columns";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data];
