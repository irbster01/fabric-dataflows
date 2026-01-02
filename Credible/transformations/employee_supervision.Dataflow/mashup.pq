[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_credible_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_services", ItemKind = "Table"]}[Data],
  #"Sorted rows" = Table.Sort(#"Navigation 2", {{"service_date", Order.Descending}}),
  #"Merged queries" = Table.NestedJoin(#"Sorted rows", {"episode_id"}, raw_credible_episodes, {"entry_id"}, "raw_credible_episodes", JoinKind.LeftOuter),
  #"Expanded raw_credible_episodes" = Table.ExpandTableColumn(#"Merged queries", "raw_credible_episodes", {"program"}, {"program"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded raw_credible_episodes", each ([program] = "Supervision")),
  #"Extracted text before delimiter" = Table.TransformColumns(#"Filtered rows", {{"client_last_name", each Text.BeforeDelimiter(_, ",", 0), type text}}),
  #"Inserted merged column" = Table.AddColumn(#"Extracted text before delimiter", "client_full_name", each Text.Combine({[client_last_name], [client_first_name]}, ", "), type text),
  #"Inserted start of month" = Table.AddColumn(#"Inserted merged column", "Start of month", each Date.StartOfMonth([service_date]), type nullable date),
  #"Renamed columns" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}})
in
  #"Renamed columns";
shared raw_credible_episodes = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_episodes", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([entry_status] = "ACTIVE" or [entry_status] = "Enrolled") and ([program] = "Supervision"))
in
  #"Filtered rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "individual supervision_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"individual supervision" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_services", ItemKind = "Table"]}[Data],
  #"Sorted rows" = Table.Sort(#"Navigation 2", {{"service_date", Order.Descending}}),
  #"Merged queries" = Table.NestedJoin(#"Sorted rows", {"episode_id"}, raw_credible_episodes, {"entry_id"}, "raw_credible_episodes", JoinKind.LeftOuter),
  #"Expanded raw_credible_episodes" = Table.ExpandTableColumn(#"Merged queries", "raw_credible_episodes", {"program"}, {"program"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded raw_credible_episodes", each ([program] = "Supervision")),
  #"Extracted text before delimiter" = Table.TransformColumns(#"Filtered rows", {{"client_last_name", each Text.BeforeDelimiter(_, ",", 0), type text}}),
  #"Inserted merged column" = Table.AddColumn(#"Extracted text before delimiter", "client_full_name", each Text.Combine({[client_last_name], [client_first_name]}, ", "), type text),
  #"Inserted start of month" = Table.AddColumn(#"Inserted merged column", "Start of month", each Date.StartOfMonth([service_date]), type nullable date),
  #"Renamed columns" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}}),
  #"Filtered rows 1" = Table.SelectRows(#"Renamed columns", each ([service_type] = "BH Supervision: Ind"))
in
  #"Filtered rows 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "group_supervision_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared group_supervision = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_services", ItemKind = "Table"]}[Data],
  #"Sorted rows" = Table.Sort(#"Navigation 2", {{"service_date", Order.Descending}}),
  #"Merged queries" = Table.NestedJoin(#"Sorted rows", {"episode_id"}, raw_credible_episodes, {"entry_id"}, "raw_credible_episodes", JoinKind.LeftOuter),
  #"Expanded raw_credible_episodes" = Table.ExpandTableColumn(#"Merged queries", "raw_credible_episodes", {"program"}, {"program"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded raw_credible_episodes", each ([program] = "Supervision")),
  #"Extracted text before delimiter" = Table.TransformColumns(#"Filtered rows", {{"client_last_name", each Text.BeforeDelimiter(_, ",", 0), type text}}),
  #"Inserted merged column" = Table.AddColumn(#"Extracted text before delimiter", "client_full_name", each Text.Combine({[client_last_name], [client_first_name]}, ", "), type text),
  #"Inserted start of month" = Table.AddColumn(#"Inserted merged column", "Start of month", each Date.StartOfMonth([service_date]), type nullable date),
  #"Renamed columns" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}}),
  #"Filtered rows 1" = Table.SelectRows(#"Renamed columns", each ([service_type] = "BHSupervision: Group"))
in
  #"Filtered rows 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "group_supervision_hours_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared group_supervision_hours = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_services", ItemKind = "Table"]}[Data],
  #"Sorted rows" = Table.Sort(#"Navigation 2", {{"service_date", Order.Descending}}),
  #"Merged queries" = Table.NestedJoin(#"Sorted rows", {"episode_id"}, raw_credible_episodes, {"entry_id"}, "raw_credible_episodes", JoinKind.LeftOuter),
  #"Expanded raw_credible_episodes" = Table.ExpandTableColumn(#"Merged queries", "raw_credible_episodes", {"program"}, {"program"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded raw_credible_episodes", each ([program] = "Supervision")),
  #"Extracted text before delimiter" = Table.TransformColumns(#"Filtered rows", {{"client_last_name", each Text.BeforeDelimiter(_, ",", 0), type text}}),
  #"Inserted merged column" = Table.AddColumn(#"Extracted text before delimiter", "client_full_name", each Text.Combine({[client_last_name], [client_first_name]}, ", "), type text),
  #"Inserted start of month" = Table.AddColumn(#"Inserted merged column", "Start of month", each Date.StartOfMonth([service_date]), type nullable date),
  #"Renamed columns" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}}),
  #"Filtered rows 1" = Table.SelectRows(#"Renamed columns", each ([service_type] = "BHSupervision: Group")),
  #"Grouped rows" = Table.Group(#"Filtered rows 1", {"client_full_name", "service_month"}, {{"duration", each List.Sum([duration]), type nullable number}}),
  #"Divided column" = Table.TransformColumns(#"Grouped rows", {{"duration", each _ / 60, type number}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Divided column", {{"duration", "hours"}}),
  #"Added custom" = Table.AddColumn(#"Renamed columns 1", "type", each "Group"),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom", {{"type", type text}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "individual supervision_hours_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"individual supervision_hours" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_services", ItemKind = "Table"]}[Data],
  #"Sorted rows" = Table.Sort(#"Navigation 2", {{"service_date", Order.Descending}}),
  #"Merged queries" = Table.NestedJoin(#"Sorted rows", {"episode_id"}, raw_credible_episodes, {"entry_id"}, "raw_credible_episodes", JoinKind.LeftOuter),
  #"Expanded raw_credible_episodes" = Table.ExpandTableColumn(#"Merged queries", "raw_credible_episodes", {"program"}, {"program"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded raw_credible_episodes", each ([program] = "Supervision")),
  #"Extracted text before delimiter" = Table.TransformColumns(#"Filtered rows", {{"client_last_name", each Text.BeforeDelimiter(_, ",", 0), type text}}),
  #"Inserted merged column" = Table.AddColumn(#"Extracted text before delimiter", "client_full_name", each Text.Combine({[client_last_name], [client_first_name]}, ", "), type text),
  #"Inserted start of month" = Table.AddColumn(#"Inserted merged column", "Start of month", each Date.StartOfMonth([service_date]), type nullable date),
  #"Renamed columns" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}}),
  #"Filtered rows 1" = Table.SelectRows(#"Renamed columns", each ([service_type] = "BH Supervision: Ind")),
  #"Grouped rows" = Table.Group(#"Filtered rows 1", {"client_full_name", "service_month"}, {{"duration", each List.Sum([duration]), type nullable number}}),
  #"Divided column" = Table.TransformColumns(#"Grouped rows", {{"duration", each _ / 60, type number}}),
  #"Added custom" = Table.AddColumn(#"Divided column", "type", each "Individual"),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom", {{"type", type text}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type", {{"duration", "hours"}})
in
  #"Renamed columns 1";
shared #"individual supervision_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "individual_supervision_services", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared group_supervision_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "group_supervision_services", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"individual supervision_hours_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "individual supervision_hours", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared group_supervision_hours_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "group_supervision_hours", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Append_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared combined_supervision_hours = let
  Source = Table.Combine({group_supervision_hours, #"individual supervision_hours"})
in
  Source;
shared Append_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "combined_supervision_hours", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "active_employees_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared active_employees = let
  Source = Table.NestedJoin(raw_credible_episodes, {"entry_id"}, raw_credible_services, {"episode_id"}, "raw_credible_services", JoinKind.LeftOuter),
  #"Expanded raw_credible_services" = Table.ExpandTableColumn(Source, "raw_credible_services", {"client_full_name"}, {"client_full_name"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded raw_credible_services", each ([client_full_name] <> null and [client_full_name] <> "Acevedo, Emily" and [client_full_name] <> "Baker, Kelley" and [client_full_name] <> "Boykins, JaShunna" and [client_full_name] <> "Caesar, Tieranee'" and [client_full_name] <> "Caesar, Tieranee‘" and [client_full_name] <> "Chkhichekh, Holly" and [client_full_name] <> "Davis, Jasmine" and [client_full_name] <> "Dukes, Brittney" and [client_full_name] <> "Houston, Fanetta" and [client_full_name] <> "Irby, Russell" and [client_full_name] <> "Johnson, Kayla" and [client_full_name] <> "Lewis, Clara" and [client_full_name] <> "McFarland, Kati" and [client_full_name] <> "Myers, Kasey" and [client_full_name] <> "Piazza, Elizabeth" and [client_full_name] <> "Rainwater, Brandon" and [client_full_name] <> "Thomas, Demarquane" and [client_full_name] <> "Williams, Oriell" and [client_full_name] <> "Winn, Brittany")),
  #"Removed other columns" = Table.SelectColumns(#"Filtered rows", {"client_id", "client_full_name"}),
  #"Removed duplicates" = Table.Distinct(#"Removed other columns", {"client_id"})
in
  #"Removed duplicates";
shared active_employees_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "active_supervision_employees", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "combined_supervision_totals_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared combined_supervision_totals = let
  Source = Table.Combine({group_supervision_hours, #"individual supervision_hours"}),
  #"Removed columns" = Table.RemoveColumns(Source, {"type"}),
  #"Grouped rows" = Table.Group(#"Removed columns", {"client_full_name", "service_month"}, {{"hours", each List.Sum([hours]), type nullable number}}),
  #"Inserted conditional column" = Table.AddColumn(#"Grouped rows", "achieved", each if [hours] >= 4 then "GOOD" else "NOT GOOD"),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted conditional column", {{"achieved", type text}})
in
  #"Changed column type";
shared combined_supervision_totals_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "combined_supervision_totals", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "monthly_supervision_totals_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared monthly_supervision_totals = let
  Source = Table.Combine({group_supervision_hours, #"individual supervision_hours"}),
  #"Removed columns" = Table.RemoveColumns(Source, {"type"}),
  #"Grouped rows" = Table.Group(#"Removed columns", {"client_full_name", "service_month"}, {{"hours", each List.Sum([hours]), type nullable number}}),
  #"Inserted conditional column" = Table.AddColumn(#"Grouped rows", "achieved", each if [hours] >= 4 then "GOOD" else "NOT GOOD"),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted conditional column", {{"achieved", type text}}),
  #"Grouped rows 1" = Table.Group(#"Changed column type", {"service_month", "achieved"}, {{"count", each Table.RowCount(_), Int64.Type}})
in
  #"Grouped rows 1";
shared monthly_supervision_totals_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "monthly_supervision_totals", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
