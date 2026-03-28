[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_lighthouse_grades = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lighthouse_grades", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared stage6_lighthouse_attendance_detail = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "stage6_lighthouse_attendance_detail", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared grades_staging_table_wFlag = let
  Source = stage6_lighthouse_attendance_detail,
  #"Grouped rows" = Table.Group(Source, {"client_id"}, {{"avg_rate", each List.Average([attendance_rate]), type nullable number}, {"max_rate", each List.Max([attendance_rate]), type nullable number}}),
  #"Inserted conditional column" = Table.AddColumn(#"Grouped rows", "good_flag", each if [max_rate] >= 0.6 then "GOOD" else "NOT MET"),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted conditional column", {{"client_id", type text}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared lighthouse_grant_aggregated_attendance_metric = let
  Source = grades_staging_table_wFlag,
  #"Grouped rows" = Table.Group(Source, {"good_flag"}, {{"Count", each Table.RowCount(_), Int64.Type}}),
  #"Pivoted column" = Table.Pivot(Table.TransformColumnTypes(#"Grouped rows", {{"good_flag", type text}}), List.Distinct(Table.TransformColumnTypes(#"Grouped rows", {{"good_flag", type text}})[good_flag]), "good_flag", "Count"),
  #"Inserted sum" = Table.AddColumn(#"Pivoted column", "Sum", each List.Sum({[NOT MET], [GOOD]}), type nullable Int64.Type),
  #"Renamed columns" = Table.RenameColumns(#"Inserted sum", {{"Sum", "total"}}),
  #"Inserted division" = Table.AddColumn(#"Renamed columns", "Division", each [GOOD] / [total], type number),
  #"Renamed columns 1" = Table.RenameColumns(#"Inserted division", {{"Division", "good_perc"}})
in
  #"Renamed columns 1";
[BindToDefaultDestination = true]
shared lighthouse_grant_aggregated_grades_metric = let
  Source = grades_staging_table_wFlag,
  #"Filtered rows" = Table.SelectRows(Source, each ([good_flag] = "GOOD")),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows", {"client_id"}, raw_lighthouse_grades, {"client_id"}, "raw_lighthouse_grades", JoinKind.Inner),
  #"Expanded raw_lighthouse_grades" = Table.ExpandTableColumn(#"Merged queries", "raw_lighthouse_grades", {"1Q_grades", "2Q_grades", "3Q_grades"}, {"1Q_grades", "2Q_grades", "3Q_grades"}),
  #"Inserted maximum" = Table.AddColumn(#"Expanded raw_lighthouse_grades", "Max", each List.Max({[1Q_grades], [2Q_grades], [3Q_grades]}), type nullable number),
  #"Renamed columns" = Table.RenameColumns(#"Inserted maximum", {{"Max", "max_gpa"}}),
  #"Filtered rows 1" = Table.SelectRows(#"Renamed columns", each ([max_gpa] <> null)),
  #"Inserted conditional column" = Table.AddColumn(#"Filtered rows 1", "gpa_flag", each if [max_gpa] >= 2.5 then "GOOD" else "NOT MET"),
  #"Grouped rows" = Table.Group(#"Inserted conditional column", {"gpa_flag"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}),
  #"Pivoted column" = Table.Pivot(Table.TransformColumnTypes(#"Grouped rows", {{"gpa_flag", type text}}), List.Distinct(Table.TransformColumnTypes(#"Grouped rows", {{"gpa_flag", type text}})[gpa_flag]), "gpa_flag", "Count"),
  #"Inserted sum" = Table.AddColumn(#"Pivoted column", "Sum", each List.Sum({[GOOD], [NOT MET]}), type nullable Int64.Type),
  #"Renamed columns 1" = Table.RenameColumns(#"Inserted sum", {{"Sum", "total"}}),
  #"Inserted division" = Table.AddColumn(#"Renamed columns 1", "Division", each [GOOD] / [total], type number)
in
  #"Inserted division";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data];
