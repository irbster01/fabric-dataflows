[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"caddo_jcampus cleaned_caddo_grades" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "caddo_jcampus.cleaned_caddo_grades", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "cis_grades_wJcampus_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared cis_grades_wJcampus = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "raw_cisdm_clients", ItemKind = "Table"]}[Data],

  #"Merged queries 1" = Table.NestedJoin(
    #"Navigation 3",
    {"sasid"},
    #"caddo_jcampus cleaned_caddo_grades",
    {"sasid"},
    "caddo_jcampus cleaned_caddo_grades",
    JoinKind.LeftOuter
  ),

  #"Expanded caddo_jcampus cleaned_caddo_grades" = Table.ExpandTableColumn(
    #"Merged queries 1",
    "caddo_jcampus cleaned_caddo_grades",
    {
      "grade", "gender", "dob", "school_name", "course",
      "1st_9_weeks", "1st_9_GPA", "2nd_9_weeks", "2nd_9_GPA",
      "3rd_9_weeks", "3rd_9_gpa", "4th_9_weeks",
      "iep_status", "504_status", "lep_status", "short_name_academics",
      "1st_9_grade_value", "2nd_9_grade_value", "1st_9_letter_grade"
    },
    {
      "grade", "gender", "dob", "school_name.1", "course",
      "1st_9_weeks", "1st_9_GPA", "2nd_9_weeks", "2nd_9_GPA",
      "3rd_9_weeks", "3rd_9_gpa", "4th_9_weeks",
      "iep_status", "504_status", "lep_status", "short_name_academics",
      "1st_9_grade_value", "2nd_9_grade_value", "1st_9_letter_grade"
    }
  ),

  // Entry nine-weeks bucket (based on entry_start_date)
  #"Added Nine Weeks (Entry)" = Table.AddColumn(
    #"Expanded caddo_jcampus cleaned_caddo_grades",
    "entry_nine_weeks",
    each
      let d = try Date.From([entry_start_date]) otherwise null
      in
        if d = null then null
        else if d >= #date(2025, 8, 13) and d <= #date(2025, 10, 17) then "1st Nine Weeks"
        else if d >= #date(2025, 10, 20) and d <= #date(2025, 12, 17) then "2nd Nine Weeks"
        else if d >= #date(2026, 1, 6) and d <= #date(2026, 3, 6) then "3rd Nine Weeks"
        else if d >= #date(2026, 3, 16) and d <= #date(2026, 5, 21) then "4th Nine Weeks"
        else null,
    type text
  ),

  // Exit nine-weeks bucket (based on Enrollment_Exit Date; null => Active)
  #"Added Nine Weeks (Exit)" = Table.AddColumn(
    #"Added Nine Weeks (Entry)",
    "exit_nine_weeks",
    each
      let d = try Date.From([Enrollment_Exit Date]) otherwise null
      in
        if d = null then "Active"
        else if d >= #date(2025, 8, 13) and d <= #date(2025, 10, 17) then "1st Nine Weeks"
        else if d >= #date(2025, 10, 20) and d <= #date(2025, 12, 17) then "2nd Nine Weeks"
        else if d >= #date(2026, 1, 6) and d <= #date(2026, 3, 6) then "3rd Nine Weeks"
        else if d >= #date(2026, 3, 16) and d <= #date(2026, 5, 21) then "4th Nine Weeks"
        else null,
    type text
  )
in
  #"Added Nine Weeks (Exit)";
shared cis_grades_wJcampus_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "cis_grades_wJcampus", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[BindToDefaultDestination = true]
shared cis_attend_wJcampus = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "raw_cisdm_clients", ItemKind = "Table"]}[Data],

  #"Merged queries 1" = Table.NestedJoin(#"Navigation 3", {"sasid"}, caddo_attend_summary, {"sasid"}, "caddo_attend_summary", JoinKind.LeftOuter),
  #"Expanded caddo_attend_summary" = Table.ExpandTableColumn(#"Merged queries 1", "caddo_attend_summary", {"F", "O"}, {"F", "O"}),

  // Entry nine-weeks bucket (based on entry_start_date)
  #"Added Nine Weeks (Entry)" = Table.AddColumn(
    #"Expanded caddo_attend_summary",
    "entry_nine_weeks",
    each
      let d = try Date.From([entry_start_date]) otherwise null
      in
        if d = null then null
        else if d >= #date(2025, 8, 13) and d <= #date(2025, 10, 17) then "1st Nine Weeks"
        else if d >= #date(2025, 10, 20) and d <= #date(2025, 12, 17) then "2nd Nine Weeks"
        else if d >= #date(2026, 1, 6) and d <= #date(2026, 3, 6) then "3rd Nine Weeks"
        else if d >= #date(2026, 3, 16) and d <= #date(2026, 5, 21) then "4th Nine Weeks"
        else null,
    type text
  ),

  // Exit nine-weeks bucket (based on Enrollment_Exit Date; null => Active)
  #"Added Nine Weeks (Exit)" = Table.AddColumn(
    #"Added Nine Weeks (Entry)",
    "exit_nine_weeks",
    each
      let d = try Date.From([Enrollment_Exit Date]) otherwise null
      in
        if d = null then "Active"
        else if d >= #date(2025, 8, 13) and d <= #date(2025, 10, 17) then "1st Nine Weeks"
        else if d >= #date(2025, 10, 20) and d <= #date(2025, 12, 17) then "2nd Nine Weeks"
        else if d >= #date(2026, 1, 6) and d <= #date(2026, 3, 6) then "3rd Nine Weeks"
        else if d >= #date(2026, 3, 16) and d <= #date(2026, 5, 21) then "4th Nine Weeks"
        else null,
    type text
  )
in
  #"Added Nine Weeks (Exit)";
shared raw_caddo_daily_attend = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_caddo_daily_attend", ItemKind = "Table"]}[Data],
  #"Inserted conditional column" = Table.AddColumn(#"Navigation 2", "tardy", each if Text.Contains([out_reason], "Tardy/") then 1 else ""),
  #"Inserted conditional column 1" = Table.AddColumn(#"Inserted conditional column", "abs", each if [unexcused] = "O" then 1 else ""),
  #"Sorted rows 3" = Table.Sort(#"Inserted conditional column 1", {{"unexcused", Order.Descending}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Sorted rows 3", {{"tardy", Int64.Type}, {"abs", Int64.Type}}),
  #"Sorted rows 2" = Table.Sort(#"Changed column type", {{"abs", Order.Descending}}),
  #"Grouped rows" = Table.Group(#"Sorted rows 2", {"sidno", "sasid", "student_name"}, {{"ytd_abs", each List.Sum([abs]), type nullable number}, {"tardies", each List.Sum([tardy]), type nullable number}}),
  #"Sorted rows" = Table.Sort(#"Grouped rows", {{"tardies", Order.Descending}})
in
  #"Sorted rows";
shared raw_caddo_referrals = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_caddo_referrals", ItemKind = "Table"]}[Data],
  #"Grouped rows" = Table.Group(#"Navigation 2", {"sidno", "sasid", "student_name"}, {{"referrals", each Table.RowCount(Table.Distinct(_)), Int64.Type}})
in
  #"Grouped rows";
[BindToDefaultDestination = true]
shared cis_referrals_wJcampus = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "cis_client_list", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"sasid"}, raw_caddo_referrals, {"sasid"}, "raw_caddo_referrals", JoinKind.LeftOuter),
  #"Expanded raw_caddo_referrals" = Table.ExpandTableColumn(#"Merged queries", "raw_caddo_referrals", {"referrals"}, {"referrals"}),
  #"Replaced value" = Table.ReplaceValue(#"Expanded raw_caddo_referrals", null, 0, Replacer.ReplaceValue, {"referrals"}),
  #"Merged queries 1" = Table.NestedJoin(#"Replaced value", {"client_id"}, raw_cisdm_clients, {"client_id"}, "raw_cisdm_clients", JoinKind.Inner),
  #"Expanded raw_cisdm_clients" = Table.ExpandTableColumn(#"Merged queries 1", "raw_cisdm_clients", {"entry_start_date", "Enrollment_Exit Date"}, {"entry_start_date", "Enrollment_Exit Date"}),

  #"Added Nine Weeks (Entry)" = Table.AddColumn(
    #"Expanded raw_cisdm_clients",
    "entry_nine_weeks",
    each
      let d = try Date.From([entry_start_date]) otherwise null
      in
        if d = null then null
        else if d >= #date(2025, 8, 13) and d <= #date(2025, 10, 17) then "1st Nine Weeks"
        else if d >= #date(2025, 10, 20) and d <= #date(2025, 12, 17) then "2nd Nine Weeks"
        else if d >= #date(2026, 1, 6) and d <= #date(2026, 3, 6) then "3rd Nine Weeks"
        else if d >= #date(2026, 3, 16) and d <= #date(2026, 5, 21) then "4th Nine Weeks"
        else null,
    type text
  ),

  #"Added Nine Weeks (Exit)" = Table.AddColumn(
    #"Added Nine Weeks (Entry)",
    "exit_nine_weeks",
    each
      let d = try Date.From([Enrollment_Exit Date]) otherwise null
      in
        if d = null then "Active"
        else if d >= #date(2025, 8, 13) and d <= #date(2025, 10, 17) then "1st Nine Weeks"
        else if d >= #date(2025, 10, 20) and d <= #date(2025, 12, 17) then "2nd Nine Weeks"
        else if d >= #date(2026, 1, 6) and d <= #date(2026, 3, 6) then "3rd Nine Weeks"
        else if d >= #date(2026, 3, 16) and d <= #date(2026, 5, 21) then "4th Nine Weeks"
        else null,
    type text
  )
in
  #"Added Nine Weeks (Exit)";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data];
shared raw_cisdm_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_clients", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared lighthouse_dates = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "lighthouse_dates", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "cis_grades_wJcampus_goalStudents_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared cis_grades_wJcampus_goalStudents = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "raw_cisdm_clients", ItemKind = "Table"]}[Data],

  #"Merged queries 1" = Table.NestedJoin(
    #"Navigation 3",
    {"sasid"},
    #"caddo_jcampus cleaned_caddo_grades",
    {"sasid"},
    "caddo_jcampus cleaned_caddo_grades",
    JoinKind.LeftOuter
  ),

  #"Expanded caddo_jcampus cleaned_caddo_grades" = Table.ExpandTableColumn(
    #"Merged queries 1",
    "caddo_jcampus cleaned_caddo_grades",
    {
      "grade", "gender", "dob", "school_name", "course",
      "1st_9_weeks", "1st_9_GPA", "2nd_9_weeks", "2nd_9_GPA",
      "3rd_9_weeks", "3rd_9_gpa", "4th_9_weeks",
      "iep_status", "504_status", "lep_status", "short_name_academics",
      "1st_9_grade_value", "2nd_9_grade_value", "1st_9_letter_grade"
    },
    {
      "grade", "gender", "dob", "school_name.1", "course",
      "1st_9_weeks", "1st_9_GPA", "2nd_9_weeks", "2nd_9_GPA",
      "3rd_9_weeks", "3rd_9_gpa", "4th_9_weeks",
      "iep_status", "504_status", "lep_status", "short_name_academics",
      "1st_9_grade_value", "2nd_9_grade_value", "1st_9_letter_grade"
    }
  ),

  // Entry nine-weeks bucket (based on entry_start_date)
  #"Added Nine Weeks (Entry)" = Table.AddColumn(
    #"Expanded caddo_jcampus cleaned_caddo_grades",
    "entry_nine_weeks",
    each
      let d = try Date.From([entry_start_date]) otherwise null
      in
        if d = null then null
        else if d >= #date(2025, 8, 13) and d <= #date(2025, 10, 17) then "1st Nine Weeks"
        else if d >= #date(2025, 10, 20) and d <= #date(2025, 12, 17) then "2nd Nine Weeks"
        else if d >= #date(2026, 1, 6) and d <= #date(2026, 3, 6) then "3rd Nine Weeks"
        else if d >= #date(2026, 3, 16) and d <= #date(2026, 5, 21) then "4th Nine Weeks"
        else null,
    type text
  ),

  // Exit nine-weeks bucket (based on Enrollment_Exit Date; null => Active)
  #"Added Nine Weeks (Exit)" = Table.AddColumn(
    #"Added Nine Weeks (Entry)",
    "exit_nine_weeks",
    each
      let d = try Date.From([Enrollment_Exit Date]) otherwise null
      in
        if d = null then "Active"
        else if d >= #date(2025, 8, 13) and d <= #date(2025, 10, 17) then "1st Nine Weeks"
        else if d >= #date(2025, 10, 20) and d <= #date(2025, 12, 17) then "2nd Nine Weeks"
        else if d >= #date(2026, 1, 6) and d <= #date(2026, 3, 6) then "3rd Nine Weeks"
        else if d >= #date(2026, 3, 16) and d <= #date(2026, 5, 21) then "4th Nine Weeks"
        else null,
    type text
  ),
  #"Merged queries" = Table.NestedJoin(#"Added Nine Weeks (Exit)", {"client_id", "short_name_academics"}, raw_cisdm_progress_monitoring, {"client_id", "short_name_academics"}, "raw_cisdm_progress_monitoring", JoinKind.Inner),
  #"Expanded raw_cisdm_progress_monitoring" = Table.ExpandTableColumn(#"Merged queries", "raw_cisdm_progress_monitoring", {"case_manager", "goal", "metric", "baseline", "target", "baseline_date"}, {"case_manager", "goal", "metric", "baseline", "target", "baseline_date"})
in
  #"Expanded raw_cisdm_progress_monitoring";
shared raw_cisdm_progress_monitoring = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared cis_grades_wJcampus_goalStudents_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "cis_grades_wJcampus_goalStudents", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[BindToDefaultDestination = true]
shared cis_referrals_wJcampus_goalStudents = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "cis_client_list", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"sasid"}, raw_caddo_referrals, {"sasid"}, "raw_caddo_referrals", JoinKind.LeftOuter),
  #"Expanded raw_caddo_referrals" = Table.ExpandTableColumn(#"Merged queries", "raw_caddo_referrals", {"referrals"}, {"referrals"}),
  #"Replaced value" = Table.ReplaceValue(#"Expanded raw_caddo_referrals", null, 0, Replacer.ReplaceValue, {"referrals"}),
  #"Merged queries 1" = Table.NestedJoin(#"Replaced value", {"client_id"}, raw_cisdm_clients, {"client_id"}, "raw_cisdm_clients", JoinKind.Inner),
  #"Expanded raw_cisdm_clients" = Table.ExpandTableColumn(#"Merged queries 1", "raw_cisdm_clients", {"entry_start_date", "Enrollment_Exit Date"}, {"entry_start_date", "Enrollment_Exit Date"}),

  #"Added Nine Weeks (Entry)" = Table.AddColumn(
    #"Expanded raw_cisdm_clients",
    "entry_nine_weeks",
    each
      let d = try Date.From([entry_start_date]) otherwise null
      in
        if d = null then null
        else if d >= #date(2025, 8, 13) and d <= #date(2025, 10, 17) then "1st Nine Weeks"
        else if d >= #date(2025, 10, 20) and d <= #date(2025, 12, 17) then "2nd Nine Weeks"
        else if d >= #date(2026, 1, 6) and d <= #date(2026, 3, 6) then "3rd Nine Weeks"
        else if d >= #date(2026, 3, 16) and d <= #date(2026, 5, 21) then "4th Nine Weeks"
        else null,
    type text
  ),

  #"Added Nine Weeks (Exit)" = Table.AddColumn(
    #"Added Nine Weeks (Entry)",
    "exit_nine_weeks",
    each
      let d = try Date.From([Enrollment_Exit Date]) otherwise null
      in
        if d = null then "Active"
        else if d >= #date(2025, 8, 13) and d <= #date(2025, 10, 17) then "1st Nine Weeks"
        else if d >= #date(2025, 10, 20) and d <= #date(2025, 12, 17) then "2nd Nine Weeks"
        else if d >= #date(2026, 1, 6) and d <= #date(2026, 3, 6) then "3rd Nine Weeks"
        else if d >= #date(2026, 3, 16) and d <= #date(2026, 5, 21) then "4th Nine Weeks"
        else null,
    type text
  ),
  #"Merged queries 2" = Table.NestedJoin(#"Added Nine Weeks (Exit)", {"sasid"}, behavior_prog_monitor, {"sasid"}, "behavior_prog_monitor", JoinKind.Inner),
  #"Expanded behavior_prog_monitor" = Table.ExpandTableColumn(#"Merged queries 2", "behavior_prog_monitor", {"case_manager", "goal", "metric", "baseline", "target", "baseline_date", "baseline_report_period", "target_set_date", "grading_period", "grading_period_progress", "nonNumeric_value", "metric_value", "days_in_report_prd", "latest_progress", "progress_notes", "goal_narrative", "entry_date", "exit_date", "enrollment_status", "school_year", "attendance_type"}, {"case_manager", "goal", "metric", "baseline", "target", "baseline_date", "baseline_report_period", "target_set_date", "grading_period", "grading_period_progress", "nonNumeric_value", "metric_value", "days_in_report_prd", "latest_progress", "progress_notes", "goal_narrative", "entry_date", "exit_date", "enrollment_status", "school_year", "attendance_type"}),
  #"Reordered columns" = Table.ReorderColumns(#"Expanded behavior_prog_monitor", {"sasid", "client_id", "client_full_name", "school_name", "goal", "metric", "baseline", "target", "referrals", "entry_start_date", "Enrollment_Exit Date", "entry_nine_weeks", "exit_nine_weeks", "case_manager", "baseline_date", "baseline_report_period", "target_set_date", "grading_period", "grading_period_progress", "nonNumeric_value", "metric_value", "days_in_report_prd", "latest_progress", "progress_notes", "goal_narrative", "entry_date", "exit_date", "enrollment_status", "school_year", "attendance_type"}),
  #"Removed duplicates" = Table.Distinct(#"Reordered columns", {"client_id"})
in
  #"Removed duplicates";
shared behavior_prog_monitor = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([goal] = "Improve School Behavior"))
in
  #"Filtered rows";
shared attend_prog_monitor = let
  Source = raw_cisdm_progress_monitoring,
  #"Filtered rows" = Table.SelectRows(Source, each ([goal] = "Improve Attendance"))
in
  #"Filtered rows";
[BindToDefaultDestination = true]
shared cis_attend_wJcampus_goalStudents = let
  Source = cis_attend_wJcampus,
  #"Merged queries" = Table.NestedJoin(Source, {"client_id"}, attend_prog_monitor, {"client_id"}, "attend_prog_monitor", JoinKind.Inner),
  #"Expanded attend_prog_monitor" = Table.ExpandTableColumn(#"Merged queries", "attend_prog_monitor", {"case_manager", "goal", "metric", "baseline", "target", "baseline_date", "baseline_report_period", "target_set_date", "grading_period", "grading_period_progress", "nonNumeric_value", "metric_value", "days_in_report_prd", "excused_absences", "unexcused_absences", "review_date", "latest_progress", "progress_notes", "goal_narrative", "entry_date", "exit_date", "enrollment_status", "school_year", "short_name_academics", "attendance_type"}, {"case_manager", "goal", "metric", "baseline", "target", "baseline_date", "baseline_report_period", "target_set_date", "grading_period", "grading_period_progress", "nonNumeric_value", "metric_value", "days_in_report_prd", "excused_absences", "unexcused_absences", "review_date", "latest_progress", "progress_notes", "goal_narrative", "entry_date", "exit_date", "enrollment_status", "school_year", "short_name_academics", "attendance_type"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded attend_prog_monitor", {"baseline_date", "baseline_report_period", "target_set_date", "grading_period", "grading_period_progress", "nonNumeric_value", "metric_value", "days_in_report_prd", "excused_absences", "unexcused_absences", "review_date", "latest_progress", "progress_notes"}),
  #"Removed duplicates" = Table.Distinct(#"Removed columns", {"client_id", "metric"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed duplicates", {{"F", "unexcused"}, {"O", "tardies"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"client_dob", "entry_start_date", "first_cis_enrollment_date", "Enrollment_Exit Date", "entry_nine_weeks", "exit_nine_weeks", "case_manager", "entry_date", "exit_date", "enrollment_status", "school_year", "short_name_academics", "attendance_type"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed columns 1", {"school_name", "client_id", "client_full_name", "sasid", "goal", "metric", "baseline", "target", "unexcused", "tardies", "goal_narrative"})
in
  #"Reordered columns";
[BindToDefaultDestination = true]
shared caddo_attend_summary = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "caddo_attend_summary", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
