[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_cisdm_progress_monitoring = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared cis_prog_count_bySchool = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}[Data],
  #"Removed other columns" = Table.SelectColumns(#"Navigation 2", {"school_name", "sasid", "client_id", "client_full_name", "grading_period"}),
  #"Grouped rows" = Table.Group(#"Removed other columns", {"school_name", "grading_period"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}})
in
  #"Grouped rows";
shared cis_positive_progress = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([latest_progress] = "No Progress/Positive" or [latest_progress] = "Progressing")),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"school_name", "grading_period"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}})
in
  #"Grouped rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "positive_prog_withRaw_%_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"positive_prog_withRaw_%" = let
  Source = Table.NestedJoin(cis_positive_progress, {"school_name", "grading_period"}, cis_prog_count_bySchool, {"school_name", "grading_period"}, "cis_prog_count_bySchool", JoinKind.Inner),
  #"Expanded cis_prog_count_bySchool" = Table.ExpandTableColumn(Source, "cis_prog_count_bySchool", {"Count"}, {"Count.1"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded cis_prog_count_bySchool", {{"Count.1", "all_monitored"}, {"Count", "positive_progress"}}),
  #"Inserted division" = Table.AddColumn(#"Renamed columns", "Division", each [positive_progress] / [all_monitored], type number),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted division", {{"Division", Percentage.Type}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type", {{"Division", "cis_progressing"}})
in
  #"Renamed columns 1";
shared cis_attendance = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([latest_progress] = "No Progress/Positive" or [latest_progress] = "Progressing") and ([goal] = "Improve Attendance")),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"school_name", "grading_period"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}})
in
  #"Grouped rows";
shared cis_attendance_count = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each [goal] = "Improve Attendance"),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"school_name", "grading_period"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}})
in
  #"Grouped rows";
shared cis_attendance_positie_count = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([latest_progress] = "No Progress/Positive" or [latest_progress] = "Progressing") and ([goal] = "Improve Attendance")),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"school_name", "grading_period"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}})
in
  #"Grouped rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "attendance_pos_wRaw_%_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"attendance_pos_wRaw_%" = let
  Source = Table.NestedJoin(cis_attendance_count, {"school_name", "grading_period"}, cis_attendance_positie_count, {"school_name", "grading_period"}, "cis_attendance_positie_count", JoinKind.Inner),
  #"Expanded cis_attendance_positie_count" = Table.ExpandTableColumn(Source, "cis_attendance_positie_count", {"Count"}, {"cis_attendance_positie_count.Count"}),
  #"Reordered columns" = Table.ReorderColumns(#"Expanded cis_attendance_positie_count", {"school_name", "cis_attendance_positie_count.Count", "Count"}),
  #"Renamed columns" = Table.RenameColumns(#"Reordered columns", {{"cis_attendance_positie_count.Count", "positive_progress"}, {"Count", "all_monitored"}}),
  #"Inserted division" = Table.AddColumn(#"Renamed columns", "Division", each [positive_progress] / [all_monitored], type number),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted division", {{"Division", Percentage.Type}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type", {{"Division", "positive_percentage"}})
in
  #"Renamed columns 1";
shared cis_behavior = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([goal] = "Improve School Behavior")),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"school_name", "grading_period"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}})
in
  #"Grouped rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "cis_behavior_positive_count_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared cis_behavior_positive_count = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([goal] = "Improve School Behavior") and ([latest_progress] = "No Progress/Positive" or [latest_progress] = "Progressing")),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"school_name", "grading_period"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}),
  #"Merged queries" = Table.NestedJoin(#"Grouped rows", {"school_name", "grading_period"}, cis_behavior, {"school_name", "grading_period"}, "cis_behavior", JoinKind.Inner),
  #"Expanded cis_behavior" = Table.ExpandTableColumn(#"Merged queries", "cis_behavior", {"Count"}, {"cis_behavior.Count"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded cis_behavior", {{"cis_behavior.Count", "all_monitored"}, {"Count", "positive_progress"}}),
  #"Inserted division" = Table.AddColumn(#"Renamed columns", "Division", each [positive_progress] / [all_monitored], type number),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted division", {{"Division", Percentage.Type}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type", {{"Division", "positive_percentage"}}),
  #"Removed duplicates" = Table.Distinct(#"Renamed columns 1")
in
  #"Removed duplicates";
shared cis_academics = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([goal] = "Improve Academics")),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"school_name", "grading_period"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}})
in
  #"Grouped rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "cis_academics_positive_count_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared cis_academics_positive_count = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([goal] = "Improve Academics") and ([latest_progress] = "No Progress/Positive" or [latest_progress] = "Progressing")),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"school_name", "grading_period"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}),
  #"Merged queries" = Table.NestedJoin(#"Grouped rows", {"school_name", "grading_period"}, cis_academics, {"school_name", "grading_period"}, "cis_academics", JoinKind.Inner),
  #"Expanded cis_academics" = Table.ExpandTableColumn(#"Merged queries", "cis_academics", {"Count"}, {"cis_academics.Count"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded cis_academics", {{"cis_academics.Count", "all_monitored"}, {"Count", "positive_progress"}}),
  #"Inserted division" = Table.AddColumn(#"Renamed columns", "Division", each [positive_progress] / [all_monitored], type number),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted division", {{"Division", Percentage.Type}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type", {{"Division", "positive_percentage"}}),
  #"Filtered rows 1" = Table.SelectRows(#"Renamed columns 1", each [positive_percentage] <= 1)
in
  #"Filtered rows 1";
shared #"positive_prog_withRaw_%_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "cis_positive_prog_withRaw_%", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"attendance_pos_wRaw_%_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "cis_attendance_pos_wRaw_%", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared cis_behavior_positive_count_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "cis_behavior_positive_count", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared cis_academics_positive_count_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "cis_academics_positive_count", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "cis_org_all_goals_count_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared cis_org_all_goals_count = let
  Source = Table.NestedJoin(cis_positive_progress, {"school_name", "grading_period"}, cis_prog_count_bySchool, {"school_name", "grading_period"}, "cis_prog_count_bySchool", JoinKind.Inner),
  #"Expanded cis_prog_count_bySchool" = Table.ExpandTableColumn(Source, "cis_prog_count_bySchool", {"Count"}, {"Count.1"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded cis_prog_count_bySchool", {{"Count.1", "all_monitored"}, {"Count", "positive_progress"}}),
  #"Inserted division" = Table.AddColumn(#"Renamed columns", "Division", each [positive_progress] / [all_monitored], type number),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted division", {{"Division", Percentage.Type}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type", {{"Division", "cis_progressing"}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Renamed columns 1", "org_controller", each "VOA"), {{"org_controller", type text}}),
  #"Grouped rows" = Table.Group(#"Added custom", {"org_controller", "grading_period"}, {{"positive_progress", each List.Sum([positive_progress]), type nullable number}, {"all_monitored", each List.Sum([all_monitored]), type nullable number}}),
  #"Inserted division 1" = Table.AddColumn(#"Grouped rows", "Division", each [positive_progress] / [all_monitored], type number),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted division 1", {{"Division", Percentage.Type}})
in
  #"Changed column type 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "cis_org_attendance_count_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared cis_org_attendance_count = let
  Source = Table.NestedJoin(cis_attendance_count, {"school_name", "grading_period"}, cis_attendance_positie_count, {"school_name", "grading_period"}, "cis_attendance_positie_count", JoinKind.Inner),
  #"Expanded cis_attendance_positie_count" = Table.ExpandTableColumn(Source, "cis_attendance_positie_count", {"Count"}, {"cis_attendance_positie_count.Count"}),
  #"Reordered columns" = Table.ReorderColumns(#"Expanded cis_attendance_positie_count", {"school_name", "cis_attendance_positie_count.Count", "Count"}),
  #"Renamed columns" = Table.RenameColumns(#"Reordered columns", {{"cis_attendance_positie_count.Count", "positive_progress"}, {"Count", "all_monitored"}}),
  #"Inserted division" = Table.AddColumn(#"Renamed columns", "Division", each [positive_progress] / [all_monitored], type number),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted division", {{"Division", Percentage.Type}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type", {{"Division", "positive_percentage"}}),
  #"Added custom" = Table.AddColumn(#"Renamed columns 1", "org_controller", each "VOA"),
  #"Grouped rows" = Table.Group(#"Added custom", {"org_controller", "grading_period"}, {{"positive_progress", each List.Sum([positive_progress]), type nullable number}, {"all_monitored", each List.Sum([all_monitored]), type nullable number}}),
  #"Inserted division 1" = Table.AddColumn(#"Grouped rows", "Division", each [positive_progress] / [all_monitored], type number),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted division 1", {{"Division", Percentage.Type}, {"org_controller", type text}})
in
  #"Changed column type 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "cis_org_behavior_count_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared cis_org_behavior_count = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([goal] = "Improve School Behavior") and ([latest_progress] = "No Progress/Positive" or [latest_progress] = "Progressing")),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"school_name", "grading_period"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}),
  #"Merged queries" = Table.NestedJoin(#"Grouped rows", {"school_name", "grading_period"}, cis_behavior, {"school_name", "grading_period"}, "cis_behavior", JoinKind.Inner),
  #"Expanded cis_behavior" = Table.ExpandTableColumn(#"Merged queries", "cis_behavior", {"Count"}, {"cis_behavior.Count"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded cis_behavior", {{"cis_behavior.Count", "all_monitored"}, {"Count", "positive_progress"}}),
  #"Inserted division" = Table.AddColumn(#"Renamed columns", "Division", each [positive_progress] / [all_monitored], type number),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted division", {{"Division", Percentage.Type}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type", {{"Division", "positive_percentage"}}),
  #"Added custom" = Table.AddColumn(#"Renamed columns 1", "org_controller", each "VOA"),
  #"Grouped rows 1" = Table.Group(#"Added custom", {"org_controller", "grading_period"}, {{"positive_progress", each List.Sum([positive_progress]), type nullable number}, {"all_monitored", each List.Sum([all_monitored]), type nullable number}}),
  #"Inserted division 1" = Table.AddColumn(#"Grouped rows 1", "Division", each [positive_progress] / [all_monitored], type number),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted division 1", {{"Division", Percentage.Type}}),
  #"Renamed columns 2" = Table.RenameColumns(#"Changed column type 1", {{"Division", "positive_percentage"}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Renamed columns 2", {{"org_controller", type text}})
in
  #"Changed column type 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "cis_org_academics_count_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared cis_org_academics_count = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([goal] = "Improve Academics") and ([latest_progress] = "No Progress/Positive" or [latest_progress] = "Progressing")),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"school_name", "grading_period"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}),
  #"Merged queries" = Table.NestedJoin(#"Grouped rows", {"school_name", "grading_period"}, cis_academics, {"school_name", "grading_period"}, "cis_academics", JoinKind.Inner),
  #"Expanded cis_academics" = Table.ExpandTableColumn(#"Merged queries", "cis_academics", {"Count"}, {"cis_academics.Count"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded cis_academics", {{"cis_academics.Count", "all_monitored"}, {"Count", "positive_progress"}}),
  #"Inserted division" = Table.AddColumn(#"Renamed columns", "Division", each [positive_progress] / [all_monitored], type number),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted division", {{"Division", Percentage.Type}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type", {{"Division", "positive_percentage"}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Renamed columns 1", "org_controller", each "VOA"), {{"org_controller", type text}}),
  #"Grouped rows 1" = Table.Group(#"Added custom", {"org_controller", "grading_period"}, {{"positive_progress", each List.Sum([positive_progress]), type nullable number}, {"all_monitored", each List.Sum([all_monitored]), type nullable number}}),
  #"Inserted division 1" = Table.AddColumn(#"Grouped rows 1", "Division", each [positive_progress] / [all_monitored], type number),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted division 1", {{"Division", Percentage.Type}}),
  #"Renamed columns 2" = Table.RenameColumns(#"Changed column type 1", {{"Division", "positive_percentage"}})
in
  #"Renamed columns 2";
shared cis_org_attendance_count_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "cis_org_attendance_count", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared cis_org_behavior_count_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "cis_org_behavior_count", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared cis_org_academics_count_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "cis_org_academics_count", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "cis_academics_goal_clients_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared cis_academics_goal_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each [goal] = "Improve Academics")
in
  #"Filtered rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "cis_attendance_goal_clients_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared cis_attendance_goal_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each [goal] = "Improve Attendance")
in
  #"Filtered rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "cis_behavior_goal_clients_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared cis_behavior_goal_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([goal] = "Improve School Behavior"))
in
  #"Filtered rows";
shared cis_behavior_goal_clients_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "cis_behavior_goal_clients", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared cis_attendance_goal_clients_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "cis_attendance_goal_clients", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared cis_academics_goal_clients_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "cis_academics_goal_clients", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared cis_org_all_goals_count_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "cis_org_all_goals_count", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
