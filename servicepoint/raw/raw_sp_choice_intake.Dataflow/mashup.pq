[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Choice Intake_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Choice Intake" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "ServicePoint"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Choice Intake"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", let latest = List.Max(#"Expanded Attributes"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Column1", type text}, {"Column2", type text}, {"Column3", type text}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", type text}, {"Column8", type text}, {"Column9", type text}, {"Column10", type text}, {"Column11", type text}, {"Column12", type text}, {"Column13", type text}, {"Column14", type text}, {"Column15", type text}, {"Column16", type text}, {"Column17", type text}, {"Column18", type text}, {"Column19", type text}, {"Column20", type text}, {"Column21", type text}, {"Column22", type text}, {"Column23", type text}, {"Column24", type text}, {"Column25", type text}, {"Column26", type text}, {"Column27", type text}, {"Column28", type text}, {"Column29", type text}, {"Column30", type text}, {"Column31", type text}, {"Column32", type text}, {"Column33", type text}, {"Column34", type text}, {"Column35", type text}, {"Column36", type text}, {"Column37", type text}, {"Column38", type text}, {"Column39", type text}, {"Column40", type text}, {"Column41", type text}, {"Column42", type text}, {"Column43", type text}, {"Column44", type text}, {"Column45", type text}, {"Column46", type text}, {"Column47", type text}, {"Column48", type text}, {"Column49", type text}, {"Column50", type text}, {"Column51", type text}, {"Column52", type text}, {"Column53", type text}, {"Column54", type text}, {"Column55", type text}, {"Column56", type text}, {"Column57", type text}, {"Column58", type text}, {"Column59", type text}, {"Column60", type text}, {"Column61", type text}, {"Column62", type text}, {"Column63", type text}, {"Column64", type text}, {"Column65", type text}, {"Column66", type text}, {"Column67", type text}, {"Column68", type text}, {"Column69", type text}, {"Column70", type text}}),
  #"Promoted headers" = Table.PromoteHeaders(#"Changed column type", [PromoteAllScalars = true]),
  #"Renamed columns" = Table.RenameColumns(#"Promoted headers", {{"Client ID", "client_id"}, {"First Name", "first_name"}, {"Last Name", "last_name"}, {"Entry Date", "entry_date"}, {"Exit Date", "exit_date"}, {"For children aged 0 to 5, is child enrolled in a formal early learning program?", "early_learning"}, {"Working At Grade Level In Math?", "math_grade_level"}, {"Working At Grade Level In Reading?", "reading_grade_level"}, {"Date of Birth", "dob"}, {"If yes, what is the name of the formal early learning program?", "early_learning_name"}, {"Current Grade Level", "current_grade"}, {"School Name", "school_name"}, {"Is the child involved in positive youth development?", "positive_youth_dev"}, {"Household ID", "household_id"}, {"Soc Sec No", "ssn"}, {"Did the student miss more than 10 percent of the school days last year?", "miss_school"}, {"Are you a member of a religious community (church, mosque, etc.)?", "religious_community"}, {"Do you have a behavioral health provider?", "behavioral_health"}, {"If so, name of provider/clinic:", "behavioralHealth_name"}, {"Date of your last physical?", "physical_date"}, {"Degree field:", "degree_field"}, {"Survivor of Domestic Violence", "dv_survivor"}, {"Do you have a regular dentist?", "dentist"}, {"Emergency Contact's Phone Number", "emergencyContact_num"}, {"Emergency Contact", "emergency_contact"}, {"Employer's Name", "employer_name"}, {"Employment End Date", "employment_end_date"}, {"Employment Start Date", "employment_start_date"}, {"Highest Level of Education Attained", "education_attained"}, {"How did you hear about us?", "how_heard"}, {"Enrollment CoC", "enrollment_coc"}, {"Covered by Health Insurance", "health_ins"}, {"Does the client have a disabling condition?", "disability"}, {"If Yes, Type of Employment", "employment_type"}, {"Approximate date this episode of homelessness started", "homelessness_start"}, {"Housing Move-in Date", "movein_date"}, {"Did you stay less than 7 nights?", "less_than_7_nights"}, {"Did you stay less than 90 days?", "less_than_90_days"}, {"If Unemployed, Looking for Work?", "looking_for_work"}, {"On the night before did you stay on the streets, ES or SH?", "nightBefore_onStreets"}, {"Total number of months homeless on the street, in ES or SH in the past three years", "total_months_homeless"}, {"Regardless of where they stayed last night - Number of times the client has been on the streets, in ES, or SH in the past three years including today", "num_of_times_homeless"}, {"Race and Ethnicity", "race"}, {"Relationship to Head of Household", "relationship"}, {"Total Monthly Income", "total_monthly_income"}, {"Do you have Internet access at home?", "internet_access"}, {"Is student enrolled in CIS?", "in_cis"}, {"Is the child participating in Choice Tutoring?", "choice_tutoring"}, {"Is there a computer in your home?", "home_computer"}, {"Are you enrolled in job training or other workforce development?", "workforce_dev"}, {"JCAMPUS signed permission on file?", "jcampus_signed"}, {"Last Dental Visit", "last_dental"}, {"Is the child enrolled in the Lighthouse program?", "in_lighthouse"}, {"Marital Status", "marital_status"}, {"Do you have a place where you get regular health care (i.e. PCP, clinic, Dr. office, NOT ER or Urgent Care))?", "has_pcp"}, {"School Attending", "school_attending"}, {"School Year", "school_year"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"Date of Birth Type"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"Gender", "gender"}, {"Secondary Race (Retired)", "race_sec"}, {"Income from Any Source", "income_anySource"}, {"Non-cash benefit from any source", "noncash_anySource"}, {"Employed?", "employed_flag"}, {"Are you a former resident of Jackson Heights?", "frmr_jacksonHeights"}, {"Clinic or Doctor's Name", "clinic_name"}, {"Which Lighthouse/CIS location is the student attending?", "lh_cis_location"}, {"Are you participating in a vocational school or higher education?", "vocational_school"}, {"Would you be interested in any sort of spiritual support?", "spiritual_support"}, {"Service ID", "service_id"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns 1", {{"total_monthly_income", type number}}),
  #"Replaced value" = Table.ReplaceValue(#"Changed column type 1", "Two times (HUD)", "2", Replacer.ReplaceValue, {"num_of_times_homeless"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "Three times (HUD)", "3", Replacer.ReplaceValue, {"num_of_times_homeless"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "One time (HUD)", "1", Replacer.ReplaceValue, {"num_of_times_homeless"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "Four or more times (HUD)", "4", Replacer.ReplaceValue, {"num_of_times_homeless"}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Replaced value 3", {{"num_of_times_homeless", Int64.Type}}),
  #"Replaced value 4" = Table.ReplaceValue(#"Changed column type 2", "More than 12 months (HUD)", "13", Replacer.ReplaceValue, {"total_months_homeless"}),
  #"Grouped rows" = Table.Group(#"Replaced value 4", {"client_id", "first_name", "last_name", "exit_date", "early_learning", "math_grade_level", "reading_grade_level", "dob", "early_learning_name", "current_grade", "school_name", "positive_youth_dev", "household_id", "ssn", "miss_school", "religious_community", "behavioral_health", "behavioralHealth_name", "physical_date", "degree_field", "dv_survivor", "dentist", "emergencyContact_num", "emergency_contact", "employer_name", "employment_end_date", "employment_start_date", "education_attained", "how_heard", "enrollment_coc", "health_ins", "disability", "employment_type", "homelessness_start", "movein_date", "less_than_7_nights", "less_than_90_days", "looking_for_work", "nightBefore_onStreets", "total_months_homeless", "num_of_times_homeless", "race", "relationship", "total_monthly_income", "internet_access", "in_cis", "choice_tutoring", "home_computer", "workforce_dev", "jcampus_signed", "last_dental", "in_lighthouse", "marital_status", "has_pcp", "school_attending", "school_year", "If yes, please specify:", "gender", "race_sec", "income_anySource", "noncash_anySource", "employed_flag", "frmr_jacksonHeights", "clinic_name", "lh_cis_location", "vocational_school", "spiritual_support"}, {{"entry_date", each List.Max([entry_date]), type nullable text}}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Grouped rows", {{"exit_date", type date}, {"dob", type date}, {"entry_date", type date}})
in
  #"Changed column type 3";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "ServicePoint"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Choice Intake"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", let latest = List.Max(#"Expanded Attributes"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 70, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 70, QuoteStyle = QuoteStyle.None])
in
  Source;
shared #"Choice Intake_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_choice_intake", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Choice Intake (2)" = let
  Source = #"Choice Intake"
in
  Source;
