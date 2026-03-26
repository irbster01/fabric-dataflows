[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_fep_assessment = let
  Source = Xml.Tables(Web.Contents("https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2r6Euxp1Wwd4yV!de324ACAw__&start_date=&end_date=&custom_param1=&custom_param2=&custom_param3=")),
  #"Navigation 1" = Source{1}[Table],
  #"Expanded Table" = Table.ExpandTableColumn(#"Navigation 1", "Table", {"Name", "Table"}, {"Name.1", "Table.1"}),
  #"Expanded Table.1" = Table.ExpandTableColumn(#"Expanded Table", "Table.1", {"Name", "Table"}, {"Name.2", "Table"}),
  #"Expanded Table 1" = Table.ExpandTableColumn(#"Expanded Table.1", "Table", {"Name", "Table"}, {"Name.3", "Table.1"}),
  #"Expanded Table.1 1" = Table.ExpandTableColumn(#"Expanded Table 1", "Table.1", {"arrests", "arrests_update", "cp_alcohol", "assess_dt", "assess_type", "cp_drugs", "disability_1", "disability_2", "disability_3", "disability_4", "disability_5", "drug_1", "drug_1_update", "drug_2", "drug_2_update", "drug_3", "drug_3_updat", "drug_1_age", "drug_2_age", "drug_3_ag", "drug_1_freq", "drug_1_freq_update", "drug_2_freq", "drug_2_freq_update", "drug_3_freq", "drug_3_freq_updat", "drug_1_rte", "drug_1_rte_update", "drug_2_rte", "drug_2_rte_update", "drug_3_rte_updat", "dx_primary", "dx_sec", "encounters", "encounters_update", "episode_uid", "school_absence", "school_absence_update", "school_enrollment", "school_enroll_update", "school_suspension", "school_susp_update", "sp_date", "sp_smi", "dx_3", "dx_4", "drug_3_rte", "rev_timein", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "region"}, {"arrests", "arrests_update", "cp_alcohol", "assess_dt", "assess_type", "cp_drugs", "disability_1", "disability_2", "disability_3", "disability_4", "disability_5", "drug_1", "drug_1_update", "drug_2", "drug_2_update", "drug_3", "drug_3_updat", "drug_1_age", "drug_2_age", "drug_3_ag", "drug_1_freq", "drug_1_freq_update", "drug_2_freq", "drug_2_freq_update", "drug_3_freq", "drug_3_freq_updat", "drug_1_rte", "drug_1_rte_update", "drug_2_rte", "drug_2_rte_update", "drug_3_rte_updat", "dx_primary", "dx_sec", "encounters", "encounters_update", "episode_uid", "school_absence", "school_absence_update", "school_enrollment", "school_enroll_update", "school_suspension", "school_susp_update", "sp_date", "sp_smi", "dx_3", "dx_4", "drug_3_rte", "rev_timein", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "region"}),
  #"Expanded cp_alcohol" = Table.ExpandTableColumn(#"Expanded Table.1 1", "cp_alcohol", {"Element:Text"}, {"Element:Text"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded cp_alcohol", {{"Element:Text", "cp_alcohol"}}),
  #"Expanded cp_drugs" = Table.ExpandTableColumn(#"Renamed columns", "cp_drugs", {"Element:Text"}, {"Element:Text.1"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Expanded cp_drugs", {{"Element:Text.1", "cp_drugs"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns 1", {"urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "Name", "Name.1", "Name.2", "Name.3"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns", {{"arrests_update", type date}, {"assess_dt", type date}, {"assess_type", Int64.Type}}),
  #"Replaced value" = Table.ReplaceValue(#"Changed column type", "Yes", 1, Replacer.ReplaceValue, {"cp_alcohol"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "No", 0, Replacer.ReplaceValue, {"cp_alcohol"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "Yes", 1, Replacer.ReplaceValue, {"cp_drugs"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "No", 0, Replacer.ReplaceValue, {"cp_drugs"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Replaced value 3", {{"disability_1", type text}, {"disability_2", type text}, {"disability_3", type text}, {"disability_4", type text}, {"disability_5", type text}, {"drug_1", Int64.Type}, {"drug_1_update", type date}, {"drug_2_update", type date}, {"drug_2", Int64.Type}, {"drug_3", type text}, {"drug_3_updat", type date}, {"drug_1_age", Int64.Type}, {"drug_2_age", Int64.Type}, {"drug_3_ag", Int64.Type}, {"drug_1_freq", Int64.Type}, {"drug_1_freq_update", type date}, {"drug_2_freq", Int64.Type}, {"drug_2_freq_update", type date}, {"drug_3_freq", Int64.Type}, {"drug_3_freq_updat", type date}, {"drug_1_rte", type text}, {"drug_1_rte_update", type date}, {"drug_2_rte", type text}, {"drug_2_rte_update", type date}, {"drug_3_rte_updat", type date}, {"dx_sec", type text}, {"dx_primary", type text}, {"encounters", Int64.Type}, {"encounters_update", type date}, {"episode_uid", type text}, {"school_absence", Int64.Type}, {"school_absence_update", type date}, {"school_enrollment", Int64.Type}, {"school_enroll_update", type date}, {"school_suspension", Int64.Type}, {"school_susp_update", type date}, {"sp_date", type date}, {"sp_smi", Int64.Type}, {"dx_3", type text}, {"dx_4", type text}, {"drug_3_rte", type text}}),
  #"Extracted text range" = Table.TransformColumns(#"Changed column type 1", {{"rev_timein", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Extracted text range", {{"rev_timein", type date}, {"region", type text}, {"arrests", Int64.Type}, {"cp_alcohol", Int64.Type}, {"cp_drugs", Int64.Type}})
in
  #"Changed column type 2";
shared raw_credible_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_clients", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"employeeid", "external_id", "Status", "Client Status Date", "Date Created", "Date Updated", "First Name", "Last Name"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed columns", {"Client ID", "Client Name", "programs", "DOB", "Age", "SSN", "Gender", "Zip", "City", "county", "preferred_language", "Admission Date", "Date Diagnosis Updated", "Diagnosis 1", "race_omb", "race_omb2", "ethnicity_omb", "Client Type", "dd1", "Phone", "address1", "state", "last_svc_date", "Profile Auth End Date", "teams", "show_pbm_medhistory", "Diagnosis 2", "mobile_phone", "client_email", "address2", "Diagnosis 3", "Case Manager Emp ID", "dd7", "dd6", "Discharge Date", "dd4", "Redzone", "axis_1e", "maritalstatus", "maritalstatupdate", "currentschool", "currentschoolupdate", "axis_1f", "Employee Flag", "6020 Flag", "IBHH Flag", "6021 Flag", "6401 Flag", "OfficeSvs Flag", "EpiCenter Flag", "Text before delimiter", "DOB - Copy", "Full Address", "Text after delimiter", "Int ID", "Client Name - Copy", "Long ID", "Today", "Days Between Auth", "Clinician Team"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Reordered columns", {"programs", "Age", "preferred_language", "Admission Date", "Date Diagnosis Updated", "Diagnosis 1", "race_omb", "race_omb2", "ethnicity_omb", "Client Type", "dd1", "Phone", "address1", "state", "last_svc_date", "Profile Auth End Date", "teams", "show_pbm_medhistory", "Diagnosis 2", "mobile_phone", "client_email", "address2", "Diagnosis 3", "Case Manager Emp ID", "dd7", "dd6", "Discharge Date", "dd4", "Redzone", "axis_1e", "maritalstatus", "maritalstatupdate", "currentschool", "currentschoolupdate", "axis_1f", "Employee Flag", "6020 Flag", "IBHH Flag", "6021 Flag", "6401 Flag", "OfficeSvs Flag", "EpiCenter Flag", "Text before delimiter", "DOB - Copy", "Full Address", "Text after delimiter", "Int ID", "Client Name - Copy", "Long ID", "Today", "Days Between Auth", "Clinician Team"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"City", "CITY"}, {"Client ID", "CLUID"}}),
  #"Inserted text after delimiter" = Table.AddColumn(#"Renamed columns 2", "Text after delimiter", each Text.AfterDelimiter([Client Name], ", ", 0), type text),
  #"Extracted text before delimiter" = Table.TransformColumns(#"Inserted text after delimiter", {{"Client Name", each Text.BeforeDelimiter(_, ",", 0), type text}}),
  #"Trimmed text" = Table.TransformColumns(#"Extracted text before delimiter", {{"Text after delimiter", each Text.Trim(_), type text}}),
  #"Cleaned text" = Table.TransformColumns(#"Trimmed text", {{"Client Name", each Text.Clean(_), type text}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Cleaned text", {{"Client Name", "NAME_F"}, {"Text after delimiter", "NAME_L"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns 1", "-", "", Replacer.ReplaceText, {"SSN"}),
  #"Trimmed text 1" = Table.TransformColumns(#"Replaced value", {{"SSN", each Text.Trim(_), type nullable text}}),
  #"Cleaned text 1" = Table.TransformColumns(#"Trimmed text 1", {{"SSN", each Text.Clean(_), type nullable text}}),
  #"Reordered columns 1" = Table.ReorderColumns(#"Cleaned text 1", {"CLUID", "NAME_F", "NAME_L", "DOB", "SSN", "Gender", "Zip", "CITY", "county"})
in
  #"Reordered columns 1";
shared FEP_EPISODES = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_episodes", ItemKind = "Table"]}[Data],
  #"FILTER ACTIVE/ENROLLED" = Table.SelectRows(#"Navigation 2", each ([program] = "Epicenter") and ([entry_status] = "ACTIVE" or [entry_status] = "Enrolled")),
  #"Merged queries" = Table.NestedJoin(#"FILTER ACTIVE/ENROLLED", {"client_id"}, raw_credible_clients, {"CLUID"}, "raw_credible_clients", JoinKind.LeftOuter),
  #"Expanded raw_credible_clients" = Table.ExpandTableColumn(#"Merged queries", "raw_credible_clients", {"CLUID", "NAME_F", "DOB", "SSN", "NAME_L"}, {"CLUID", "NAME_F", "DOB", "SSN", "NAME_L"}),
  #"REMOVE DUMMY" = Table.SelectRows(#"Expanded raw_credible_clients", each ([client_id] <> "1484") and ([NAME_L] <> "Fep")),
  #"Trimmed text" = Table.TransformColumns(#"REMOVE DUMMY", {{"SSN", each Text.Trim(_), type nullable text}}),
  #"Cleaned text" = Table.TransformColumns(#"Trimmed text", {{"SSN", each Text.Clean(_), type nullable text}}),
  #"Replaced value" = Table.ReplaceValue(#"Cleaned text", "-", "", Replacer.ReplaceText, {"SSN"})
in
  #"Replaced value";
shared raw_credible_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_services", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared CLIENT = let
  Source = Xml.Tables(Web.Contents("https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2rK1G7ilD1PHPmr7ZwCt9wqQ__&start_date=&end_date=&custom_param1=&custom_param2=&custom_param3="), null, 65001),
  #"Navigation 1" = Source{1}[Table],
  #"Expanded Table" = Table.ExpandTableColumn(#"Navigation 1", "Table", {"Name", "Table"}, {"Name.1", "Table.1"}),
  #"Expanded Table.1" = Table.ExpandTableColumn(#"Expanded Table", "Table.1", {"Name", "Table"}, {"Name.2", "Table"}),
  #"Expanded Table 1" = Table.ExpandTableColumn(#"Expanded Table.1", "Table", {"Name", "Table"}, {"Name.3", "Table.1"}),
  #"Expanded Table.1 1" = Table.ExpandTableColumn(#"Expanded Table 1", "Table.1", {"city", "irs_dep_num", "dob", "ethnicity", "gender", "inc_other", "inc_puba", "inc_ssrr", "language1", "name_f", "name_l", "name_s", "parish", "race", "sexual_orientation", "va_elig", "va_st", "zip", "region", "ss_from_profile", "cluid", "sex", "ssn", "preferred_language", "city1", "zip1", "county", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "inc_wage", "monthly_income", "health_ins", "hh_income_1", "name_m", "health_ins_sec", "hh_income_2", "hh_income_3", "hh_income_4", "hh_income_5", "hh_income_6", "hh_income_7", "hh_income_8", "race2", "race3", "race4", "pay_source_1", "pay_source_2", "pay_source_3"}, {"city", "irs_dep_num", "dob", "ethnicity", "gender", "inc_other", "inc_puba", "inc_ssrr", "language1", "name_f", "name_l", "name_s", "parish", "race", "sexual_orientation", "va_elig", "va_st", "zip", "region", "ss_from_profile", "cluid", "sex", "ssn", "preferred_language", "city1", "zip1", "county", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "inc_wage", "monthly_income", "health_ins", "hh_income_1", "name_m", "health_ins_sec", "hh_income_2", "hh_income_3", "hh_income_4", "hh_income_5", "hh_income_6", "hh_income_7", "hh_income_8", "race2", "race3", "race4", "pay_source_1", "pay_source_2", "pay_source_3"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Table.1 1", {"Name", "Name.1", "Name.2", "Name.3", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed columns", {"cluid", "city", "irs_dep_num", "dob", "ethnicity", "gender", "inc_other", "inc_puba", "inc_ssrr", "language1", "name_f", "name_l", "name_s", "parish", "race", "sexual_orientation", "va_elig", "va_st", "zip", "region", "ss_from_profile", "sex", "ssn", "preferred_language", "city1", "zip1", "county", "inc_wage", "monthly_income", "health_ins", "hh_income_1", "name_m", "health_ins_sec", "hh_income_2", "hh_income_3", "hh_income_4", "hh_income_5", "hh_income_6", "hh_income_7", "hh_income_8", "race2", "race3", "race4", "pay_source_1", "pay_source_2", "pay_source_3"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Reordered columns", {{"cluid", type text}, {"city", type text}}),
  #"Filtered rows" = Table.SelectRows(#"Changed column type", each ([name_f] <> "*****Testy")),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Filtered rows", {{"irs_dep_num", type text}}),
  #"Extracted text range" = Table.TransformColumns(#"Changed column type 1", {{"dob", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Extracted text range", {{"dob", type date}, {"ethnicity", Int64.Type}, {"gender", Int64.Type}, {"inc_other", type number}, {"inc_puba", type number}, {"inc_ssrr", type number}, {"language1", type text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type 2", each ([name_l] <> "Baker, PLPC")),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Filtered rows 1", {{"name_l", type text}, {"name_s", type text}, {"name_f", type text}, {"parish", type text}, {"race", type text}, {"sexual_orientation", type text}, {"va_elig", type text}, {"va_st", type text}, {"region", type text}, {"ss_from_profile", type text}, {"ssn", type text}}),
  #"Replaced value" = Table.ReplaceValue(#"Changed column type 3", "-", "", Replacer.ReplaceText, {"name_l", "name_s", "name_f", "parish", "race", "sexual_orientation", "va_elig", "va_st", "zip", "region", "ss_from_profile", "ssn"}),
  #"Changed column type 4" = Table.TransformColumnTypes(#"Replaced value", {{"sex", type text}, {"pay_source_3", type text}, {"pay_source_2", type text}, {"pay_source_1", type text}, {"race4", type text}, {"race3", type text}, {"race2", type text}, {"hh_income_8", type text}, {"hh_income_7", type text}, {"hh_income_6", type text}, {"hh_income_5", type text}, {"hh_income_4", type text}, {"hh_income_3", type text}, {"hh_income_2", type text}, {"health_ins_sec", type text}, {"name_m", type text}, {"hh_income_1", type text}, {"health_ins", type text}, {"monthly_income", type text}, {"inc_wage", type text}, {"county", type text}, {"zip1", type text}, {"city1", type text}, {"preferred_language", type text}}),
  #"Trimmed text" = Table.TransformColumns(#"Changed column type 4", {{"name_l", each Text.Trim(_), type nullable text}, {"name_s", each Text.Trim(_), type nullable text}, {"name_f", each Text.Trim(_), type nullable text}, {"parish", each Text.Trim(_), type nullable text}, {"race", each Text.Trim(_), type nullable text}, {"sexual_orientation", each Text.Trim(_), type nullable text}, {"va_elig", each Text.Trim(_), type nullable text}, {"va_st", each Text.Trim(_), type nullable text}, {"zip", each Text.Trim(_), type nullable text}, {"region", each Text.Trim(_), type nullable text}, {"ss_from_profile", each Text.Trim(_), type nullable text}, {"ssn", each Text.Trim(_), type nullable text}}),
  #"Cleaned text" = Table.TransformColumns(#"Trimmed text", {{"name_l", each Text.Clean(_), type nullable text}, {"name_s", each Text.Clean(_), type nullable text}, {"name_f", each Text.Clean(_), type nullable text}, {"parish", each Text.Clean(_), type nullable text}, {"race", each Text.Clean(_), type nullable text}, {"sexual_orientation", each Text.Clean(_), type nullable text}, {"va_elig", each Text.Clean(_), type nullable text}, {"va_st", each Text.Clean(_), type nullable text}, {"zip", each Text.Clean(_), type nullable text}, {"region", each Text.Clean(_), type nullable text}, {"ss_from_profile", each Text.Clean(_), type nullable text}, {"ssn", each Text.Clean(_), type nullable text}}),
  #"Uppercase Headers" = Table.TransformColumnNames(#"Cleaned text", each Text.Upper(_))

in
  #"Uppercase Headers";
