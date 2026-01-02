[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_choice_intake = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_choice_intake", ItemKind = "Table"]}[Data],
  #"Merged columns" = Table.CombineColumns(#"Navigation 2", {"last_name", "first_name"}, Combiner.CombineTextByDelimiter(", ", QuoteStyle.None), "client_name"),
  #"Replaced value" = Table.ReplaceValue(#"Merged columns", "Pre-K", "-1", Replacer.ReplaceValue, {"current_grade"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "K", "0", Replacer.ReplaceValue, {"current_grade"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 1", {{"current_grade", Int64.Type}, {"employment_end_date", type date}, {"employment_start_date", type date}, {"movein_date", type date}, {"homelessness_start", type date}, {"last_dental", type date}}),
  #"Replaced value 2" = Table.ReplaceValue(#"Changed column type", "No (HUD)", "Unemployed", Replacer.ReplaceValue, {"employed_flag"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "Yes (HUD)", "Employed", Replacer.ReplaceValue, {"employed_flag"})
in
  #"Replaced value 3";
shared active_intakes = let
  Source = raw_choice_intake,
  #"Filtered rows" = Table.SelectRows(Source, each ([exit_date] = null))
in
  #"Filtered rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "active_choice_with_intake_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared active_choice_with_intake = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "active_choice_clients", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"client_id"}, raw_choice_intake, {"client_id"}, "raw_choice_intake", JoinKind.LeftOuter),
  #"Expanded raw_choice_intake" = Table.ExpandTableColumn(#"Merged queries", "raw_choice_intake", {"client_name", "early_learning", "math_grade_level", "reading_grade_level", "dob", "early_learning_name", "current_grade", "school_name", "positive_youth_dev", "household_id", "ssn", "miss_school", "religious_community", "behavioral_health", "behavioralHealth_name", "physical_date", "degree_field", "dv_survivor", "dentist", "emergencyContact_num", "emergency_contact", "employer_name", "employment_end_date", "employment_start_date", "education_attained", "how_heard", "enrollment_coc", "health_ins", "disability", "employment_type", "homelessness_start", "movein_date", "less_than_7_nights", "less_than_90_days", "looking_for_work", "nightBefore_onStreets", "total_months_homeless", "num_of_times_homeless", "race", "relationship", "total_monthly_income", "internet_access", "in_cis", "choice_tutoring", "home_computer", "workforce_dev", "jcampus_signed", "last_dental", "in_lighthouse", "marital_status", "has_pcp", "school_attending", "school_year", "If yes_ please specify:", "gender", "race_sec", "income_anySource", "noncash_anySource", "employed_flag", "frmr_jacksonHeights", "clinic_name", "lh_cis_location", "vocational_school", "spiritual_support"}, {"client_name", "early_learning", "math_grade_level", "reading_grade_level", "dob", "early_learning_name", "current_grade", "school_name", "positive_youth_dev", "household_id", "ssn", "miss_school", "religious_community", "behavioral_health", "behavioralHealth_name", "physical_date", "degree_field", "dv_survivor", "dentist", "emergencyContact_num", "emergency_contact", "employer_name", "employment_end_date", "employment_start_date", "education_attained", "how_heard", "enrollment_coc", "health_ins", "disability", "employment_type", "homelessness_start", "movein_date", "less_than_7_nights", "less_than_90_days", "looking_for_work", "nightBefore_onStreets", "total_months_homeless", "num_of_times_homeless", "race", "relationship", "total_monthly_income", "internet_access", "in_cis", "choice_tutoring", "home_computer", "workforce_dev", "jcampus_signed", "last_dental", "in_lighthouse", "marital_status", "has_pcp", "school_attending", "school_year", "If yes_ please specify:", "gender", "race_sec", "income_anySource", "noncash_anySource", "employed_flag", "frmr_jacksonHeights", "clinic_name", "lh_cis_location", "vocational_school", "spiritual_support"}),
  Custom = Table.Group(#"Expanded raw_choice_intake", {"client_id", "client_name", "exit_date", "early_learning", "math_grade_level", "reading_grade_level", "dob", "early_learning_name", "current_grade", "school_name", "positive_youth_dev", "household_id", "ssn", "miss_school", "religious_community", "behavioral_health", "behavioralHealth_name", "physical_date", "degree_field", "dv_survivor", "dentist", "emergencyContact_num", "emergency_contact", "employer_name", "employment_end_date", "employment_start_date", "education_attained", "how_heard", "enrollment_coc", "health_ins", "disability", "employment_type", "homelessness_start", "movein_date", "less_than_7_nights", "less_than_90_days", "looking_for_work", "nightBefore_onStreets", "total_months_homeless", "num_of_times_homeless", "race", "relationship", "total_monthly_income", "internet_access", "in_cis", "choice_tutoring", "home_computer", "workforce_dev", "jcampus_signed", "last_dental", "in_lighthouse", "marital_status", "has_pcp", "school_attending", "school_year",  "gender", "race_sec", "income_anySource", "noncash_anySource", "employed_flag", "frmr_jacksonHeights", "clinic_name", "lh_cis_location", "vocational_school", "spiritual_support"}, {{"entry_date", each List.Max([entry_date]), type nullable text}}),
  #"Changed column type" = Table.TransformColumnTypes(Custom, {{"entry_date", type date}}),
  #"Reordered columns" = Table.ReorderColumns(#"Changed column type", {"client_id", "client_name", "entry_date", "exit_date", "early_learning", "math_grade_level", "reading_grade_level", "dob", "early_learning_name", "current_grade", "school_name", "positive_youth_dev", "household_id", "ssn", "miss_school", "religious_community", "behavioral_health", "behavioralHealth_name", "physical_date", "degree_field", "dv_survivor", "dentist", "emergencyContact_num", "emergency_contact", "employer_name", "employment_end_date", "employment_start_date", "education_attained", "how_heard", "enrollment_coc", "health_ins", "disability", "employment_type", "homelessness_start", "movein_date", "less_than_7_nights", "less_than_90_days", "looking_for_work", "nightBefore_onStreets", "total_months_homeless", "num_of_times_homeless", "race", "relationship", "total_monthly_income", "internet_access", "in_cis", "choice_tutoring", "home_computer", "workforce_dev", "jcampus_signed", "last_dental", "in_lighthouse", "marital_status", "has_pcp", "school_attending", "school_year", "gender", "race_sec", "income_anySource", "noncash_anySource", "employed_flag", "frmr_jacksonHeights", "clinic_name", "lh_cis_location", "vocational_school", "spiritual_support"})
in
  #"Reordered columns";
shared active_choice_with_intake_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  TableNavigation = Navigation_2{[Id = "active_choice_with_intake", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
