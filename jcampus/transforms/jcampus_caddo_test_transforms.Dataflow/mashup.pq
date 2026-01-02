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
