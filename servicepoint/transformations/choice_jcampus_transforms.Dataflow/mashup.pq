[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared choice_wJcampus_data = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "choice_wJcampus_data", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"SIDNO"}, cleaned_caddo_grades, {"sidno"}, "cleaned_caddo_grades", JoinKind.Inner),
  #"Expanded cleaned_caddo_grades" = Table.ExpandTableColumn(#"Merged queries", "cleaned_caddo_grades", {"school_name", "course", "1st_9_GPA", "2nd_9_GPA", "short_name_academics", "1st_9_grade_value", "2nd_9_grade_value"}, {"school_name", "course", "1st_9_GPA", "2nd_9_GPA", "short_name_academics", "1st_9_grade_value", "2nd_9_grade_value"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded cleaned_caddo_grades", {"client_dob", "int_id", "Year", "school_name", "short_name_academics"})
in
  #"Removed columns";
shared cleaned_caddo_grades = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "cleaned_caddo_grades", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared choice_client_gpas = let
  Source = choice_wJcampus_data,
  #"Grouped rows" = Table.Group(Source, {"client_id", "client_last_name", "client_first_name"}, {{"1st_9_gpa", each List.Average([1st_9_GPA]), type nullable number}, {"2nd_9_gpa", each List.Average([2nd_9_GPA]), type nullable number}})
in
  #"Grouped rows";
[BindToDefaultDestination = true]
shared choice_client_grade_breakout = let
  Source = choice_wJcampus_data,
  #"Removed columns" = Table.RemoveColumns(Source, {"entry_date", "entry_id", "exit_date", "program"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed columns", {"client_id", "client_first_name", "client_last_name", "SIDNO", "Grade", "School Name", "course", "1st_9_grade_value", "2nd_9_grade_value", "1st_9_GPA", "2nd_9_GPA"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Reordered columns", {"1st_9_GPA", "2nd_9_GPA"})
in
  #"Removed columns 1";
shared DefaultDestination = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data]
in
  #"Navigation 2";
