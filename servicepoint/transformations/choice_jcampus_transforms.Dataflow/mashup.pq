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
[BindToDefaultDestination = true]
shared caddo_tests = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "caddo_tests", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared choice_refTests = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "choice_wJcampus_data", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"SIDNO"}, caddo_tests, {"SIDNO"}, "caddo_tests", JoinKind.Inner),
  #"Expanded caddo_tests" = Table.ExpandTableColumn(#"Merged queries", "caddo_tests", {"ALG I Init LEAP", "GEO Init LEAP", "ENG I Init LEAP", "ENG II Init LEAP", "BIO Init LEAP", "AM HIS Init LEAP", "CIV Init LEAP", "ALG I High LEAP", "GEO High LEAP", "ENG I High LEAP", "ENG II High LEAP", "BIO High LEAP", "AM HIS High LEAP", "CIV High LEAP", "Math Pair Flag Passed Requirement", "English Pair Flag Passed Requirement", "History-Science Pair Flag Passed Requirement", "Civics-Science Pair Flag Passed Requirement_", "ELA Init 3-8", "Math Init 3-8", "Sci Init 3-8", "Soc Init 3-8", "Civ Init 3-8", "ELA High 3-8", "Math High 3-8", "Sci High 3-8", "Soc High 3-8", "Civ High 3-8", "ELA Init Connect", "Math Init Connect", "Sci Init Connect", "ELA High Connect", "Math High Connect", "Sci High Connect", "Plan Score", "Explore Score", "ACT - E", "ACT - M", "ACT - R", "ACT - S", "ACT Composite", "BOY", "MOY", "EOY", "Pre Engl", "Pre Math", "Pre Read", "Pre Scie", "Pre Comp", "ASVAB"}, {"ALG I Init LEAP", "GEO Init LEAP", "ENG I Init LEAP", "ENG II Init LEAP", "BIO Init LEAP", "AM HIS Init LEAP", "CIV Init LEAP", "ALG I High LEAP", "GEO High LEAP", "ENG I High LEAP", "ENG II High LEAP", "BIO High LEAP", "AM HIS High LEAP", "CIV High LEAP", "Math Pair Flag Passed Requirement", "English Pair Flag Passed Requirement", "History-Science Pair Flag Passed Requirement", "Civics-Science Pair Flag Passed Requirement_", "ELA Init 3-8", "Math Init 3-8", "Sci Init 3-8", "Soc Init 3-8", "Civ Init 3-8", "ELA High 3-8", "Math High 3-8", "Sci High 3-8", "Soc High 3-8", "Civ High 3-8", "ELA Init Connect", "Math Init Connect", "Sci Init Connect", "ELA High Connect", "Math High Connect", "Sci High Connect", "Plan Score", "Explore Score", "ACT - E", "ACT - M", "ACT - R", "ACT - S", "ACT Composite", "BOY", "MOY", "EOY", "Pre Engl", "Pre Math", "Pre Read", "Pre Scie", "Pre Comp", "ASVAB"}),
  #"Inserted text before delimiter" = Table.AddColumn(#"Expanded caddo_tests", "boy_value", each Text.BeforeDelimiter([BOY], " ", 0), type text),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted text before delimiter", {{"boy_value", type number}}),
  #"Inserted text before delimiter 1" = Table.AddColumn(#"Changed column type", "moy_value", each Text.BeforeDelimiter([MOY], " ", 0), type number),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted text before delimiter 1", {{"moy_value", type number}}),
  #"Inserted text before delimiter 2" = Table.AddColumn(#"Changed column type 1", "eoy_value", each Text.BeforeDelimiter([EOY], " ", 0), type number),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Inserted text before delimiter 2", {{"eoy_value", type number}})
in
  #"Changed column type 2";
shared #"choice_wJcampus_data (4)" = let
  Source = choice_refTests,
  #"Filtered rows" = Table.SelectRows(Source, each ([boy_value] <> null)),
  #"Reordered columns" = Table.ReorderColumns(#"Filtered rows", {"client_id", "entry_date", "entry_id", "exit_date", "program", "client_first_name", "client_last_name", "client_dob", "int_id", "SIDNO", "Grade", "Year", "School Name", "ALG I Init LEAP", "GEO Init LEAP", "ENG I Init LEAP", "ENG II Init LEAP", "BIO Init LEAP", "AM HIS Init LEAP", "CIV Init LEAP", "ALG I High LEAP", "GEO High LEAP", "ENG I High LEAP", "ENG II High LEAP", "BIO High LEAP", "AM HIS High LEAP", "CIV High LEAP", "boy_value", "moy_value", "eoy_value", "Math Pair Flag Passed Requirement", "English Pair Flag Passed Requirement", "History-Science Pair Flag Passed Requirement", "Civics-Science Pair Flag Passed Requirement_", "ELA Init 3-8", "Math Init 3-8", "Sci Init 3-8", "Soc Init 3-8", "Civ Init 3-8", "ELA High 3-8", "Math High 3-8", "Sci High 3-8", "Soc High 3-8", "Civ High 3-8", "ELA Init Connect", "Math Init Connect", "Sci Init Connect", "ELA High Connect", "Math High Connect", "Sci High Connect", "Plan Score", "Explore Score", "ACT - E", "ACT - M", "ACT - R", "ACT - S", "ACT Composite", "Pre Engl", "Pre Math", "Pre Read", "Pre Scie", "Pre Comp", "ASVAB"})
in
  #"Reordered columns";
shared choice_dibels = let
  Source = choice_refTests,
  #"Removed other columns" = Table.SelectColumns(Source, {"client_id", "entry_date", "entry_id", "exit_date", "program", "client_first_name", "client_last_name", "ELA Init 3-8", "Math Init 3-8", "ELA High 3-8", "Math High 3-8", "BOY", "MOY", "EOY", "boy_value", "moy_value", "eoy_value"}),
  #"Filtered rows" = Table.SelectRows(#"Removed other columns", each ([eoy_value] <> null) and ([EOY] = "420 At Benchmark" or [EOY] = "435 At Benchmark" or [EOY] = "445 At Benchmark" or [EOY] = "450 At Benchmark" or [EOY] = "451 At Benchmark" or [EOY] = "455 At Benchmark" or [EOY] = "457 At Benchmark" or [EOY] = "458 At Benchmark" or [EOY] = "466 At Benchmark" or [EOY] = "473 Above Benchmark" or [EOY] = "474 Above Benchmark" or [EOY] = "475 Above Benchmark" or [EOY] = "475 At Benchmark" or [EOY] = "481 Above Benchmark" or [EOY] = "485 Above Benchmark" or [EOY] = "522 Above Benchmark" or [EOY] = "528 Above Benchmark"))
in
  #"Filtered rows";
shared choice_leap_ela = let
  Source = choice_refTests,
  #"Filtered rows" = Table.SelectRows(Source, each ([#"ELA Init 3-8"] <> "")),
  #"Removed other columns" = Table.SelectColumns(#"Filtered rows", {"ELA Init 3-8", "Math Init 3-8", "ELA High 3-8", "Math High 3-8", "boy_value", "moy_value", "eoy_value", "client_id", "client_first_name", "client_last_name", "Grade", "School Name"}),
  #"Filtered rows 1" = Table.SelectRows(#"Removed other columns", each ([#"ELA Init 3-8"] = "729 Basic" or [#"ELA Init 3-8"] = "730 Basic" or [#"ELA Init 3-8"] = "732 Basic" or [#"ELA Init 3-8"] = "733 Basic" or [#"ELA Init 3-8"] = "737 Basic" or [#"ELA Init 3-8"] = "742 Basic" or [#"ELA Init 3-8"] = "745 Basic" or [#"ELA Init 3-8"] = "749 Basic" or [#"ELA Init 3-8"] = "752 Mastery" or [#"ELA Init 3-8"] = "753 Mastery" or [#"ELA Init 3-8"] = "757 Mastery" or [#"ELA Init 3-8"] = "776 Mastery" or [#"ELA Init 3-8"] = "834 Advanced"))
in
  #"Filtered rows 1";
shared choice_leap_math = let
  Source = choice_refTests,
  #"Filtered rows" = Table.SelectRows(Source, each ([#"ELA Init 3-8"] <> "")),
  #"Removed other columns" = Table.SelectColumns(#"Filtered rows", {"ELA Init 3-8", "Math Init 3-8", "ELA High 3-8", "Math High 3-8", "boy_value", "moy_value", "eoy_value", "client_id", "client_first_name", "client_last_name", "Grade", "School Name"})
in
  #"Removed other columns";
shared choice_actRef = let
  Source = choice_refTests,
  #"Filtered rows" = Table.SelectRows(Source, each ([Pre Engl] <> "")),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows", {{"Pre Engl", type number}, {"Pre Math", type number}, {"Pre Read", type number}, {"Pre Scie", type number}, {"Pre Comp", type number}}),
  #"Inserted conditional column" = Table.AddColumn(#"Changed column type", "eng_flag", each if [Pre Engl] >= 18 then "GOOD" else "NOT GOOD"),
  #"Inserted conditional column 1" = Table.AddColumn(#"Inserted conditional column", "read_flag", each if [Pre Read] >= 22 then "GOOD" else "NOT GOOD"),
  #"Inserted conditional column 2" = Table.AddColumn(#"Inserted conditional column 1", "math_flag", each if [Pre Math] >= 22 then "GOOD" else "NOT GOOD"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted conditional column 2", {{"eng_flag", type text}, {"read_flag", type text}, {"math_flag", type text}})
in
  #"Changed column type 1";
shared choice_seniors = let
  Source = choice_wJcampus_data,
  #"Filtered rows" = Table.SelectRows(Source, each ([Grade] = 12))
in
  #"Filtered rows";
shared choice_chronic_absentee_matches = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "choice_wJcampus_data", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"SIDNO"}, raw_caddo_chronic_absentee, {"sidno"}, "raw_caddo_chronic_absentee", JoinKind.Inner),
  #"Expanded raw_caddo_chronic_absentee" = Table.ExpandTableColumn(#"Merged queries", "raw_caddo_chronic_absentee", {"days_enrolled", "days_missed"}, {"days_enrolled", "days_missed"})
in
  #"Expanded raw_caddo_chronic_absentee";
[BindToDefaultDestination = true]
shared raw_caddo_chronic_absentee = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_caddo_chronic_absentee", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
