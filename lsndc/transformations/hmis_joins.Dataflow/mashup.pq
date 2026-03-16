[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared blank_sex = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "blank_sex", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"PersonalID"}, client_control_table, {"PersonalID"}, "client_control_table", JoinKind.LeftOuter),
  #"Expanded client_control_table" = Table.ExpandTableColumn(#"Merged queries", "client_control_table", {"client_id"}, {"client_id"}),
  #"Reordered columns" = Table.ReorderColumns(#"Expanded client_control_table", {"PersonalID", "client_id", "FirstName", "MiddleName", "LastName", "deficiency"}),
  #"Removed columns" = Table.RemoveColumns(#"Reordered columns", {"PersonalID"})
in
  #"Removed columns";
shared client_control_table = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "client_control_table", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared employment_blank = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "employment_blank", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared last_grade_blank = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "last_grade_blank", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared ssn_blank = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "ssn_blank", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"PersonalID"}, client_control_table, {"PersonalID"}, "client_control_table", JoinKind.LeftOuter),
  #"Expanded client_control_table" = Table.ExpandTableColumn(#"Merged queries", "client_control_table", {"client_id"}, {"client_id"}),
  #"Reordered columns" = Table.ReorderColumns(#"Expanded client_control_table", {"client_id", "PersonalID", "FirstName", "MiddleName", "LastName", "deficiency"}),
  #"Removed columns" = Table.RemoveColumns(#"Reordered columns", {"PersonalID"})
in
  #"Removed columns";
shared ssn_incomplete = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "ssn_incomplete", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"PersonalID"}, client_control_table, {"PersonalID"}, "client_control_table", JoinKind.LeftOuter),
  #"Expanded client_control_table" = Table.ExpandTableColumn(#"Merged queries", "client_control_table", {"client_id"}, {"client_id"}),
  #"Reordered columns" = Table.ReorderColumns(#"Expanded client_control_table", {"client_id", "PersonalID", "FirstName", "MiddleName", "LastName", "deficiency"}),
  #"Removed columns" = Table.RemoveColumns(#"Reordered columns", {"PersonalID"})
in
  #"Removed columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "appended_deficiency_table_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared appended_deficiency_table = let
  Source = Table.Combine({blank_sex, employment_blank, last_grade_blank, ssn_blank, ssn_incomplete, blank_coc, blank_dv, blank_income, blank_vamc, blank_vet_status})
in
  Source;
shared appended_deficiency_table_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  TableNavigation = Navigation_2{[Id = "appended_deficiency_table", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared blank_vamc = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "blank_vamc", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared blank_vet_status = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "blank_vet_status", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"PersonalID"}, client_control_table, {"PersonalID"}, "client_control_table", JoinKind.LeftOuter),
  #"Expanded client_control_table" = Table.ExpandTableColumn(#"Merged queries", "client_control_table", {"client_id"}, {"client_id"}),
  #"Removed other columns" = Table.SelectColumns(#"Expanded client_control_table", {"client_id", "FirstName", "MiddleName", "LastName"}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Removed other columns", "deficiency", each "Blank Vet Status"), {{"deficiency", type text}})
in
  #"Added custom";
shared blank_coc = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "blank_coc", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"EnrollmentCoC"})
in
  #"Removed columns";
shared blank_dv = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "blank_dv", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 2", {{"Custom", "deficiency"}})
in
  #"Renamed columns";
shared blank_income = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "blank_income", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
