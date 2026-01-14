[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
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
shared #"2025_intake_refTable" = let
    Source = raw_choice_intake,

    // Exit quarter/year from exit_date (null stays null)
    AddExitQuarterYear =
        Table.AddColumn(
            Source,
            "exit_qtr_year",
            each
                if [exit_date] = null then
                    null
                else
                    "Q" & Number.ToText(Date.QuarterOfYear(Date.From([exit_date])))
                    & " "
                    & Number.ToText(Date.Year(Date.From([exit_date]))),
            type text
        ),

    // Age (whole years) as of today, based on dob (null stays null)
    AddAge =
        Table.AddColumn(
            AddExitQuarterYear,
            "age",
            each
                if [dob] = null then
                    null
                else
                    let
                        d = Date.From([dob]),
                        today = Date.From(DateTime.LocalNow()),
                        years = Date.Year(today) - Date.Year(d),
                        hadBirthday =
                            Date.From(#date(Date.Year(today), Date.Month(d), Date.Day(d))) <= today
                    in
                        if hadBirthday then years else years - 1,
            Int64.Type
        ),

    // Flag for kids age 0-6 (inclusive). Null age => null flag.
    AddKid06Flag =
        Table.AddColumn(
            AddAge,
            "is_kid_0_6",
            each
                if [age] = null then
                    null
                else
                    ([age] >= 0 and [age] <= 6),
            type logical
        ),
  #"Removed other columns" = Table.SelectColumns(AddKid06Flag, {"client_id", "client_name", "exit_date", "early_learning", "dob", "early_learning_name", "positive_youth_dev", "household_id", "ssn", "employer_name", "employment_end_date", "employment_start_date", "education_attained", "health_ins", "disability", "employment_type", "homelessness_start", "race", "total_monthly_income", "internet_access", "in_cis", "choice_tutoring", "home_computer", "workforce_dev", "in_lighthouse", "marital_status", "has_pcp", "school_attending", "gender", "race_sec", "income_anySource", "noncash_anySource", "employed_flag", "frmr_jacksonHeights", "clinic_name", "vocational_school", "entry_date", "exit_qtr_year", "age", "is_kid_0_6"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed other columns", {"client_id", "client_name", "positive_youth_dev", "household_id", "employer_name", "employment_end_date", "employment_start_date", "education_attained", "health_ins", "disability", "employment_type", "homelessness_start", "total_monthly_income", "internet_access", "in_cis", "choice_tutoring", "home_computer", "workforce_dev", "in_lighthouse", "marital_status", "has_pcp", "school_attending", "gender", "race_sec", "income_anySource", "noncash_anySource", "employed_flag", "frmr_jacksonHeights", "clinic_name", "vocational_school", "entry_date", "exit_qtr_year", "age", "is_kid_0_6", "exit_date", "early_learning", "dob", "early_learning_name", "ssn"}),
  #"Filtered rows" = Table.SelectRows(#"Reordered columns", each ([exit_qtr_year] = null or [exit_qtr_year] = "Q1 2025" or [exit_qtr_year] = "Q2 2025" or [exit_qtr_year] = "Q3 2025" or [exit_qtr_year] = "Q4 2025"))
in
    #"Filtered rows";
[BindToDefaultDestination = true]
shared #"2025_household_internet" = let
    Source = raw_choice_intake,

    // Exit quarter/year from exit_date (null stays null)
    AddExitQuarterYear =
        Table.AddColumn(
            Source,
            "exit_qtr_year",
            each
                if [exit_date] = null then
                    null
                else
                    "Q" & Number.ToText(Date.QuarterOfYear(Date.From([exit_date])))
                    & " "
                    & Number.ToText(Date.Year(Date.From([exit_date]))),
            type text
        ),

    // Age (whole years) as of today, based on dob (null stays null)
    AddAge =
        Table.AddColumn(
            AddExitQuarterYear,
            "age",
            each
                if [dob] = null then
                    null
                else
                    let
                        d = Date.From([dob]),
                        today = Date.From(DateTime.LocalNow()),
                        years = Date.Year(today) - Date.Year(d),
                        hadBirthday =
                            Date.From(#date(Date.Year(today), Date.Month(d), Date.Day(d))) <= today
                    in
                        if hadBirthday then years else years - 1,
            Int64.Type
        ),

    // Flag for kids age 0-6 (inclusive). Null age => null flag.
    AddKid06Flag =
        Table.AddColumn(
            AddAge,
            "is_kid_0_6",
            each
                if [age] = null then
                    null
                else
                    ([age] >= 0 and [age] <= 6),
            type logical
        ),
  #"Filtered rows" = Table.SelectRows(AddKid06Flag, each ([exit_qtr_year] = null or [exit_qtr_year] = "Q1 2025" or [exit_qtr_year] = "Q2 2025" or [exit_qtr_year] = "Q3 2025" or [exit_qtr_year] = "Q4 2025")),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"internet_access"}, {{"Count", each List.Count(List.Distinct([household_id])), Int64.Type}}),
  #"Filtered rows 1" = Table.SelectRows(#"Grouped rows", each ([internet_access] <> ""))
in
    #"Filtered rows 1";
[BindToDefaultDestination = true]
shared #"2025_choice_pcp" = let
  Source = #"2025_intake_refTable",
  #"Grouped rows" = Table.Group(Source, {"has_pcp"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}),
  #"Filtered rows" = Table.SelectRows(#"Grouped rows", each ([has_pcp] <> "")),
  #"Transposed table" = Table.Transpose(#"Filtered rows"),
  #"Promoted headers" = Table.PromoteHeaders(#"Transposed table", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Yes", type number}, {"No", type number}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "category", each "Has PCP"), {{"category", type text}})
in
  #"Added custom";
[BindToDefaultDestination = true]
shared #"2025_choice_internet" = let
  Source = #"2025_intake_refTable",
  #"Grouped rows" = Table.Group(Source, {"internet_access"}, {{"Count", each List.Count(List.Distinct([household_id])), Int64.Type}}),
  #"Filtered rows" = Table.SelectRows(#"Grouped rows", each ([internet_access] <> "")),
  #"Transposed table" = Table.Transpose(#"Filtered rows"),
  #"Promoted headers" = Table.PromoteHeaders(#"Transposed table", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Yes", type number}, {"No", type number}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "category", each "Internet"), {{"category", type text}})
in
  #"Added custom";
[BindToDefaultDestination = true]
shared #"2025_choice_computer" = let
  Source = #"2025_intake_refTable",
  #"Grouped rows" = Table.Group(Source, {"home_computer"}, {{"Count", each List.Count(List.Distinct([household_id])), Int64.Type}}),
  #"Filtered rows" = Table.SelectRows(#"Grouped rows", each ([home_computer] <> "")),
  #"Transposed table" = Table.Transpose(#"Filtered rows"),
  #"Promoted headers" = Table.PromoteHeaders(#"Transposed table", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Yes", type number}, {"No", type number}}),
  #"Added custom" = Table.AddColumn(#"Changed column type", "category", each "Home Computer"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom", {{"category", type text}})
in
  #"Changed column type 1";
[BindToDefaultDestination = true]
shared #"2025_choice_race" = let
  Source = #"2025_intake_refTable",
  #"Grouped rows" = Table.Group(Source, {"race"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}})
in
  #"Grouped rows";
[BindToDefaultDestination = true]
shared #"2025_choice_age" = let
  Source = #"2025_intake_refTable",
  #"Grouped rows" = Table.Group(Source, {"age"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}),
  #"Sorted rows" = Table.Sort(#"Grouped rows", {{"age", Order.Ascending}})
in
  #"Sorted rows";
[BindToDefaultDestination = true]
shared #"2025_choice_disability" = let
  Source = #"2025_intake_refTable",
  #"Grouped rows" = Table.Group(Source, {"disability"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}})
in
  #"Grouped rows";
[BindToDefaultDestination = true]
shared #"2025_health_ins" = let
  Source = #"2025_intake_refTable",
  #"Grouped rows" = Table.Group(Source, {"health_ins"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}})
in
  #"Grouped rows";
shared DefaultDestination = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data]
in
  #"Navigation 2";
shared #"2025_employment_refTable" = let
    Source = #"2025_intake_refTable",
    #"Removed other columns" =
        Table.SelectColumns(
            Source,
            {
                "client_id", "client_name", "household_id", "employer_name",
                "employment_end_date", "employment_start_date", "disability",
                "employment_type", "employed_flag", "entry_date", "age", "exit_date"
            }
        ),
    #"Filtered rows 1" = Table.SelectRows(#"Removed other columns", each ([disability] = "No (HUD)")),
    #"Reordered columns" =
        Table.ReorderColumns(
            #"Filtered rows 1",
            {
                "client_id", "client_name", "employer_name", "employment_end_date",
                "household_id", "employment_start_date", "employed_flag",
                "entry_date", "exit_date", "age", "employment_type"
            }
        ),

    // Days from employment_start_date to (employment_end_date if present, else today)
    #"Added employment duration days" =
        Table.AddColumn(
            #"Reordered columns",
            "employment_days",
            each
                let
                    startDate = if [employment_start_date] = null then null else Date.From([employment_start_date]),
                    endDate =
                        if [employment_end_date] <> null then
                            Date.From([employment_end_date])
                        else
                            Date.From(DateTime.LocalNow())
                in
                    if startDate = null then
                        null
                    else
                        Duration.Days(endDate - startDate),
            Int64.Type
        ),
  #"Inserted division" = Table.AddColumn(#"Added employment duration days", "months_employed", each [employment_days] / 30, type number)
in
    #"Inserted division";
[BindToDefaultDestination = true]
shared #"2025_employment_types" = let
  Source = #"2025_employment_refTable",
  #"Grouped rows" = Table.Group(Source, {"employment_type"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}),
  #"Filtered rows" = Table.SelectRows(#"Grouped rows", each ([employment_type] <> ""))
in
  #"Filtered rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "2025_6mo_employed_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"2025_6mo_employed" = let
  Source = #"2025_employment_refTable",
  #"Filtered rows" = Table.SelectRows(Source, each [months_employed] >= 6),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"employment_type"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}})
in
  #"Grouped rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "2025_base_employment_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"2025_base_employment" = let
  Source = #"2025_employment_refTable",
  #"Grouped rows" = Table.Group(Source, {"employed_flag"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}),
  #"Filtered rows" = Table.SelectRows(#"Grouped rows", each ([employed_flag] <> ""))
in
  #"Filtered rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "2025_early_learning_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"2025_early_learning" = let
  Source = #"2025_intake_refTable",
  #"Removed other columns" = Table.SelectColumns(Source, {"client_id", "client_name", "positive_youth_dev", "choice_tutoring", "in_lighthouse", "school_attending", "age", "is_kid_0_6", "early_learning", "early_learning_name"}),
  #"Filtered rows" = Table.SelectRows(#"Removed other columns", each ([is_kid_0_6] = true)),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"early_learning"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}),
  #"Filtered rows 1" = Table.SelectRows(#"Grouped rows", each ([early_learning] <> ""))
in
  #"Filtered rows 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "2025_pos_development_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"2025_pos_development" = let
  Source = #"2025_intake_refTable",
  #"Removed other columns" = Table.SelectColumns(Source, {"client_id", "client_name", "positive_youth_dev", "choice_tutoring", "in_lighthouse", "school_attending", "age", "is_kid_0_6", "early_learning", "early_learning_name"}),
  #"Filtered rows" = Table.SelectRows(#"Removed other columns", each [age] <= 17),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"positive_youth_dev"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}})
in
  #"Grouped rows";
shared #"2025_pos_development_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "2025_pos_development", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"2025_early_learning_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "2025_early_learning", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"2025_base_employment_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "2025_base_employment", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"2025_6mo_employed_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "2025_6mo_employed", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
