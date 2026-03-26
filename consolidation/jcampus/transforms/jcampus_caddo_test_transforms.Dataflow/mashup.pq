[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared jcampus_caddo_tests = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "jcampus_caddo_tests", ItemKind = "Table"]}[Data],
  #"Inserted text before delimiter" = Table.AddColumn(#"Navigation 2", "Text before delimiter", each Text.BeforeDelimiter([literacy_boy], " ", 0), type text),
  #"Renamed columns" = Table.RenameColumns(#"Inserted text before delimiter", {{"Text before delimiter", "literacy_boy_score"}}),
  #"Inserted text before delimiter 1" = Table.AddColumn(#"Renamed columns", "Text before delimiter", each Text.BeforeDelimiter([literacy_moy], " ", 0), type text),
  #"Renamed columns 1" = Table.RenameColumns(#"Inserted text before delimiter 1", {{"Text before delimiter", "literacy_moy_score"}}),
  #"Inserted text before delimiter 2" = Table.AddColumn(#"Renamed columns 1", "Text before delimiter", each Text.BeforeDelimiter([literacy_eoy], " ", 0), type text),
  #"Renamed columns 2" = Table.RenameColumns(#"Inserted text before delimiter 2", {{"Text before delimiter", "literacy_eoy_score"}}),
  #"Trimmed text" = Table.TransformColumns(#"Renamed columns 2", {{"literacy_boy_score", each Text.Trim(_), type text}}),
  #"Cleaned text" = Table.TransformColumns(#"Trimmed text", {{"literacy_boy_score", each Text.Clean(_), type text}}),
  #"Trimmed text 1" = Table.TransformColumns(#"Cleaned text", {{"literacy_moy_score", each Text.Trim(_), type text}}),
  #"Cleaned text 1" = Table.TransformColumns(#"Trimmed text 1", {{"literacy_moy_score", each Text.Clean(_), type text}}),
  #"Trimmed text 2" = Table.TransformColumns(#"Cleaned text 1", {{"literacy_eoy_score", each Text.Trim(_), type text}}),
  #"Cleaned text 2" = Table.TransformColumns(#"Trimmed text 2", {{"literacy_eoy_score", each Text.Clean(_), type text}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Cleaned text 2", {{"literacy_eoy_score", type number}, {"literacy_moy_score", type number}, {"literacy_boy_score", type number}})
in
  #"Changed column type";
shared cis_client_list = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "cis_client_list", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "cis_dibels_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared cis_dibels = let
  Source = Table.NestedJoin(jcampus_caddo_tests, {"sasid"}, cis_client_list, {"sasid"}, "cis_client_list", JoinKind.Inner),
  #"Expanded cis_client_list" = Table.ExpandTableColumn(Source, "cis_client_list", {"client_id", "client_full_name", "school_name"}, {"client_id", "client_full_name", "school_name"}),
  #"Removed other columns" = Table.SelectColumns(#"Expanded cis_client_list", {"student_name", "sasid", "sidno", "grade", "literacy_boy_score", "literacy_moy_score", "literacy_eoy_score", "client_id", "school_name"}),
  #"Filtered rows" = Table.SelectRows(#"Removed other columns", each ([literacy_boy_score] <> null))
in
  #"Filtered rows";
shared cis_dibels_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "cis_dibels", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared caddo_tests = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "caddo_tests", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "cis_wDibels_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared cis_wDibels = let
  Source = caddo_tests,
  #"Removed other columns" = Table.SelectColumns(Source, {"Student Name", "SASID", "SIDNO", "Grade", "WK 2.0 - Overall", "BOY", "MOY", "EOY", "ELPT"}),
  #"Filtered rows" = Table.SelectRows(#"Removed other columns", each ([Grade] <> "09" and [Grade] <> "10" and [Grade] <> "11" and [Grade] <> "12" and [Grade] <> "99" and [Grade] <> "T9")),
  #"Sorted rows" = Table.Sort(#"Filtered rows", {{"Grade", Order.Ascending}}),
  #"Filtered rows 1" = Table.SelectRows(#"Sorted rows", each ([Grade] <> "-1")),
  #"Inserted merged column" = Table.AddColumn(#"Filtered rows 1", "empty_controller", each Text.Combine({[BOY], [MOY], [EOY]}, ""), type text),
  #"Filtered rows 2" = Table.SelectRows(#"Inserted merged column", each ([empty_controller] <> "")),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows 2", {"SASID"}, raw_cisdm_progress_monitoring, {"sasid"}, "raw_cisdm_progress_monitoring", JoinKind.Inner),
  #"Expanded raw_cisdm_progress_monitoring 1" = Table.ExpandTableColumn(#"Merged queries", "raw_cisdm_progress_monitoring", {"school_name", "client_id", "client_full_name", "grade", "goal", "metric", "baseline", "target"}, {"school_name", "client_id", "client_full_name", "grade.1", "goal", "metric", "baseline", "target"}),
  #"Reordered columns" = Table.ReorderColumns(#"Expanded raw_cisdm_progress_monitoring 1", {"client_id", "client_full_name", "Student Name", "SASID", "SIDNO", "Grade", "WK 2.0 - Overall", "BOY", "MOY", "EOY", "ELPT", "empty_controller", "goal", "metric", "baseline", "target"}),
  #"Removed columns" = Table.RemoveColumns(#"Reordered columns", {"Student Name", "empty_controller"}),

  // Break query folding before DISTINCT/SORT to avoid SQL Server DISTINCT+ORDER BY restriction
  #"Buffered" = Table.Buffer(#"Removed columns"),

  #"Removed duplicates" = Table.Distinct(#"Buffered"),
  #"Sorted rows 1" = Table.Sort(#"Removed duplicates", {{"metric", Order.Ascending}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Sorted rows 1", {"EOY"}),
  #"Inserted text before delimiter" = Table.AddColumn(#"Removed columns 1", "Text before delimiter", each Text.BeforeDelimiter([MOY], " ", 0), type text),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted text before delimiter", {{"Text before delimiter", type number}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"Text before delimiter", "moy_jcampus_value"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns", {"WK 2.0 - Overall"}),
  #"Filtered rows 3" = Table.SelectRows(#"Removed columns 2", each ([goal] = "Improve Academics") and ([metric] = "Reading Level" or [metric] = "Standardized test score: English / Language Arts")),
  #"Reordered columns 1" = Table.ReorderColumns(#"Filtered rows 3", {"school_name", "client_id", "client_full_name", "SASID", "SIDNO", "Grade", "BOY", "MOY", "ELPT", "grade.1", "goal", "metric", "baseline", "target", "moy_jcampus_value"})
in
  #"Reordered columns 1";
shared raw_cisdm_progress_monitoring = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared cis_wDibels_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "cis_wDibels", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
