[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared CLIENTS = let
  Source = PowerPlatform.Dataflows([]),
  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[workspaceId = "2e078b2f-481a-42df-8fee-6d81b3c2dac1"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[dataflowId = "dc6ce368-c8e7-4d12-b1d7-880bde95c99c"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[entity = "CLIENTS", version = ""]}[Data],

  // Force ALL CAPS headers
  #"Uppercased column names" = Table.RenameColumns(
      #"Navigation 4",
      List.Transform(Table.ColumnNames(#"Navigation 4"), (c) => {c, Text.Upper(c)}),
      MissingField.Ignore
  ),

  // Filter by episode-controller (inner join)
  #"Merged queries" = Table.NestedJoin(#"Uppercased column names", {"CLUID"}, #"episode-controller", {"Client ID"}, "episode-controller", JoinKind.Inner),
  #"Removed join column" = Table.RemoveColumns(#"Merged queries", {"episode-controller"}),

  // Remove duplicates by CLUID (keep first occurrence)
  #"Removed duplicates" = Table.Distinct(#"Removed join column", {"CLUID"}),

  // Target schema + order (matching CLIE CSV exactly - 40 columns)
  TargetCols = {
      "CITY", "CLUID", "IRS_DEP_NUM", "DOB", "ETHNICITY", "GENDER", "HEALTH_INS", "HEALTH_INS_SEC",
      "HH_INCOME_1", "HH_INCOME_2", "HH_INCOME_3", "HH_INCOME_4", "HH_INCOME_5", "HH_INCOME_6", "HH_INCOME_7", "HH_INCOME_8",
      "INC_OTHER", "INC_PUBA", "INC_SSRR", "INC_WAGE", "LANGUAGE1", "MONTHLY_INCOME",
      "NAME_F", "NAME_L", "NAME_M", "NAME_S", "PARISH", "RACE", "RACE2", "RACE3", "RACE4",
      "SEXUAL_ORIENTATION", "PAY_SOURCE_1", "PAY_SOURCE_2", "PAY_SOURCE_3", "SSN", "VA_ELIG", "VA_ST", "ZIP", "REGION"
  },

  // Add any missing target columns as nulls
  #"Added missing cols" = List.Accumulate(
      TargetCols,
      #"Removed duplicates",
      (state, col) => if Table.HasColumns(state, col) then state else Table.AddColumn(state, col, each null)
  ),

  // Select only target columns in exact order
  #"Final columns" = Table.SelectColumns(#"Added missing cols", TargetCols, MissingField.UseNull),

  // Clean "null" text to actual null across all columns
  #"Cleaned nulls" = Table.TransformColumns(#"Final columns", 
      List.Transform(Table.ColumnNames(#"Final columns"), (col) => 
          {col, each if _ = "null" or _ = "" then null else _, type any}
      )
  ),

  // Map HEALTH_INS text values to codes
  #"Map HEALTH_INS" = Table.TransformColumns(#"Cleaned nulls", {
      {"HEALTH_INS", each if _ = "Medicaid" then "04" 
                          else if _ = "None" then "06" 
                          else if _ = "Private" then "01"
                          else if _ = "Medicare" then "02"
                          else if _ = "CHIP" then "03"
                          else if _ = "VA" then "05"
                          else if _ = "Other" then "07"
                          else _, type text}
  }),

  // Map HEALTH_INS_SEC text values to codes
  #"Map HEALTH_INS_SEC" = Table.TransformColumns(#"Map HEALTH_INS", {
      {"HEALTH_INS_SEC", each if _ = "Medicaid" then "04" 
                              else if _ = "None" then "06" 
                              else if _ = "Private" then "01"
                              else if _ = "Medicare" then "02"
                              else if _ = "CHIP" then "03"
                              else if _ = "VA" then "05"
                              else if _ = "Other" then "07"
                              else _, type text}
  }),

  // Map HH_INCOME_1 text values to codes
  #"Map HH_INCOME_1" = Table.TransformColumns(#"Map HEALTH_INS_SEC", {
      {"HH_INCOME_1", each if _ = "None" then "01" 
                           else if Text.Contains(Text.From(_ ?? ""), "Other") then "02"
                           else if Text.Contains(Text.From(_ ?? ""), "Wages") then "02"
                           else _, type text}
  }),

  // Pad ZIP to 9 digits with trailing zeros
  #"Pad ZIP" = Table.TransformColumns(#"Map HH_INCOME_1", {
      {"ZIP", each if _ <> null then Text.PadEnd(Text.From(_), 9, "0") else _, type text}
  }),

  // Apply defaults for required fields
  #"Default CITY" = Table.ReplaceValue(#"Pad ZIP", null, "Shreveport", Replacer.ReplaceValue, {"CITY"}),
  #"Default IRS_DEP_NUM" = Table.ReplaceValue(#"Default CITY", null, "01 ", Replacer.ReplaceValue, {"IRS_DEP_NUM"}),
  #"Default ETHNICITY" = Table.ReplaceValue(#"Default IRS_DEP_NUM", null, "6", Replacer.ReplaceValue, {"ETHNICITY"}),
  #"Default GENDER" = Table.ReplaceValue(#"Default ETHNICITY", null, "1", Replacer.ReplaceValue, {"GENDER"}),
  #"Default HEALTH_INS" = Table.ReplaceValue(#"Default GENDER", null, "04", Replacer.ReplaceValue, {"HEALTH_INS"}),
  #"Default HEALTH_INS_SEC" = Table.ReplaceValue(#"Default HEALTH_INS", null, "06", Replacer.ReplaceValue, {"HEALTH_INS_SEC"}),
  #"Default HH_INCOME_1" = Table.ReplaceValue(#"Default HEALTH_INS_SEC", null, "01", Replacer.ReplaceValue, {"HH_INCOME_1"}),
  #"Default HH_INCOME_2" = Table.ReplaceValue(#"Default HH_INCOME_1", null, "01", Replacer.ReplaceValue, {"HH_INCOME_2"}),
  #"Default HH_INCOME_3" = Table.ReplaceValue(#"Default HH_INCOME_2", null, "01", Replacer.ReplaceValue, {"HH_INCOME_3"}),
  #"Default HH_INCOME_4" = Table.ReplaceValue(#"Default HH_INCOME_3", null, "01", Replacer.ReplaceValue, {"HH_INCOME_4"}),
  #"Default HH_INCOME_5" = Table.ReplaceValue(#"Default HH_INCOME_4", null, "01", Replacer.ReplaceValue, {"HH_INCOME_5"}),
  #"Default HH_INCOME_6" = Table.ReplaceValue(#"Default HH_INCOME_5", null, "01", Replacer.ReplaceValue, {"HH_INCOME_6"}),
  #"Default HH_INCOME_7" = Table.ReplaceValue(#"Default HH_INCOME_6", null, "01", Replacer.ReplaceValue, {"HH_INCOME_7"}),
  #"Default HH_INCOME_8" = Table.ReplaceValue(#"Default HH_INCOME_7", null, "01", Replacer.ReplaceValue, {"HH_INCOME_8"}),
  #"Default INC_OTHER" = Table.ReplaceValue(#"Default HH_INCOME_8", null, "0", Replacer.ReplaceValue, {"INC_OTHER"}),
  #"Default INC_PUBA" = Table.ReplaceValue(#"Default INC_OTHER", null, "0", Replacer.ReplaceValue, {"INC_PUBA"}),
  #"Default INC_SSRR" = Table.ReplaceValue(#"Default INC_PUBA", null, "0", Replacer.ReplaceValue, {"INC_SSRR"}),
  #"Default INC_WAGE" = Table.ReplaceValue(#"Default INC_SSRR", null, "0", Replacer.ReplaceValue, {"INC_WAGE"}),
  #"Default LANGUAGE1" = Table.ReplaceValue(#"Default INC_WAGE", null, "01", Replacer.ReplaceValue, {"LANGUAGE1"}),
  #"Default MONTHLY_INCOME" = Table.ReplaceValue(#"Default LANGUAGE1", null, "0", Replacer.ReplaceValue, {"MONTHLY_INCOME"}),
  #"Default PARISH" = Table.ReplaceValue(#"Default MONTHLY_INCOME", null, "09", Replacer.ReplaceValue, {"PARISH"}),
  #"Default RACE" = Table.ReplaceValue(#"Default PARISH", null, "5", Replacer.ReplaceValue, {"RACE"}),
  #"Default SEXUAL_ORIENTATION" = Table.ReplaceValue(#"Default RACE", null, "4", Replacer.ReplaceValue, {"SEXUAL_ORIENTATION"}),
  #"Default VA_ELIG" = Table.ReplaceValue(#"Default SEXUAL_ORIENTATION", null, "2", Replacer.ReplaceValue, {"VA_ELIG"}),
  #"Default VA_ST" = Table.ReplaceValue(#"Default VA_ELIG", null, "1", Replacer.ReplaceValue, {"VA_ST"}),
  #"Default REGION" = Table.ReplaceValue(#"Default VA_ST", null, "22", Replacer.ReplaceValue, {"REGION"}),

  // Set column types
  #"Changed types" = Table.TransformColumnTypes(#"Default REGION", {
      {"CITY", type text}, {"CLUID", type text}, {"IRS_DEP_NUM", type text}, {"DOB", type date},
      {"ETHNICITY", type text}, {"GENDER", type text}, {"HEALTH_INS", type text}, {"HEALTH_INS_SEC", type text},
      {"HH_INCOME_1", type text}, {"HH_INCOME_2", type text}, {"HH_INCOME_3", type text}, {"HH_INCOME_4", type text},
      {"HH_INCOME_5", type text}, {"HH_INCOME_6", type text}, {"HH_INCOME_7", type text}, {"HH_INCOME_8", type text},
      {"INC_OTHER", type text}, {"INC_PUBA", type text}, {"INC_SSRR", type text}, {"INC_WAGE", type text},
      {"LANGUAGE1", type text}, {"MONTHLY_INCOME", type text}, {"NAME_F", type text}, {"NAME_L", type text},
      {"NAME_M", type text}, {"NAME_S", type text}, {"PARISH", type text}, {"RACE", type text},
      {"RACE2", type text}, {"RACE3", type text}, {"RACE4", type text}, {"SEXUAL_ORIENTATION", type text},
      {"PAY_SOURCE_1", type text}, {"PAY_SOURCE_2", type text}, {"PAY_SOURCE_3", type text},
      {"SSN", type text}, {"VA_ELIG", type text}, {"VA_ST", type text}, {"ZIP", type text}, {"REGION", type text}
  }),

  // Final null text cleanup
  #"Final null cleanup" = Table.ReplaceValue(#"Changed types", "null", null, Replacer.ReplaceValue, Table.ColumnNames(#"Changed types"))
in
  #"Final null cleanup";
[BindToDefaultDestination = true]
shared ASSESSMENT = let
  Source = Xml.Tables(Web.Contents("https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2r6Euxp1Wwd4yV!de324ACAw__&start_date=&end_date=&custom_param1=&custom_param2=&custom_param3=")),
  #"Navigation 1" = Source{1}[Table],
  #"Expanded Table" = Table.ExpandTableColumn(#"Navigation 1", "Table", {"Name", "Table"}, {"Name.1", "Table.1"}),
  #"Expanded Table.1" = Table.ExpandTableColumn(#"Expanded Table", "Table.1", {"Name", "Table"}, {"Name.2", "Table"}),
  #"Expanded Table 1" = Table.ExpandTableColumn(#"Expanded Table.1", "Table", {"Name", "Table"}, {"Name.3", "Table.1"}),
  #"Expanded Table.1 1" = Table.ExpandTableColumn(#"Expanded Table 1", "Table.1", {"arrests", "arrests_update", "cp_alcohol", "assess_dt", "assess_type", "cp_drugs", "disability_1", "disability_2", "disability_3", "disability_4", "disability_5", "drug_1", "drug_1_update", "drug_2", "drug_2_update", "drug_3", "drug_3_updat", "drug_1_age", "drug_2_age", "drug_3_ag", "drug_1_freq", "drug_1_freq_update", "drug_2_freq", "drug_2_freq_update", "drug_3_freq", "drug_3_freq_updat", "drug_1_rte", "drug_1_rte_update", "drug_2_rte", "drug_2_rte_update", "drug_3_rte_updat", "dx_primary", "dx_sec", "encounters", "encounters_update", "episode_uid", "school_absence", "school_absence_update", "school_enrollment", "school_enroll_update", "school_suspension", "school_susp_update", "sp_date", "sp_smi", "dx_3", "dx_4", "drug_3_rte", "rev_timein", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "region"}, {"arrests", "arrests_update", "cp_alcohol", "assess_dt", "assess_type", "cp_drugs", "disability_1", "disability_2", "disability_3", "disability_4", "disability_5", "drug_1", "drug_1_update", "drug_2", "drug_2_update", "drug_3", "drug_3_updat", "drug_1_age", "drug_2_age", "drug_3_ag", "drug_1_freq", "drug_1_freq_update", "drug_2_freq", "drug_2_freq_update", "drug_3_freq", "drug_3_freq_updat", "drug_1_rte", "drug_1_rte_update", "drug_2_rte", "drug_2_rte_update", "drug_3_rte_updat", "dx_primary", "dx_sec", "encounters", "encounters_update", "episode_uid", "school_absence", "school_absence_update", "school_enrollment", "school_enroll_update", "school_suspension", "school_susp_update", "sp_date", "sp_smi", "dx_3", "dx_4", "drug_3_rte", "rev_timein", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "region"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Table.1 1", {"Name", "Name.1", "Name.2", "Name.3"}),
  #"Expanded cp_alcohol" = Table.ExpandTableColumn(#"Removed columns", "cp_alcohol", {"Element:Text"}, {"cp_alcohol"}),
  #"Expanded cp_drugs" = Table.ExpandTableColumn(#"Expanded cp_alcohol", "cp_drugs", {"Element:Text"}, {"cp_drugs"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Expanded cp_drugs", {"urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"arrests_update", type date}, {"assess_dt", type date}, {"arrests", type text}, {"disability_2", type text}, {"disability_3", type text}, {"disability_4", type text}, {"disability_5", type text}, {"drug_1", type text}, {"drug_2", type text}, {"drug_1_update", type date}, {"drug_2_update", type date}, {"drug_3", type text}, {"drug_3_updat", type date}, {"drug_1_age", type text}, {"drug_2_age", type text}, {"drug_3_ag", type text}, {"drug_1_freq", type text}, {"drug_1_freq_update", type date}, {"drug_2_freq", type text}, {"drug_2_freq_update", type date}, {"drug_3_freq_updat", type date}, {"drug_1_rte_update", type date}, {"drug_2_rte_update", type date}, {"drug_3_rte_updat", type date}, {"dx_primary", type text}, {"dx_sec", type text}, {"encounters", type text}, {"encounters_update", type date}, {"episode_uid", type text}, {"school_absence", type text}, {"school_absence_update", type date}, {"school_enrollment", type text}, {"school_suspension", type text}, {"school_enroll_update", type date}, {"school_susp_update", type date}, {"sp_date", type date}, {"sp_smi", type text}, {"dx_3", type text}, {"dx_4", type text}, {"drug_3_rte", type text}}),
  #"Extracted text range" = Table.TransformColumns(#"Changed column type", {{"rev_timein", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Extracted text range", {{"rev_timein", type date}, {"region", type text}, {"disability_1", type text}, {"cp_drugs", type text}, {"assess_type", type text}, {"cp_alcohol", type text}}),
  // ONE STEP: make every column name ALL CAPS
  #"Uppercased column names" = Table.RenameColumns(
      #"Changed column type 1",
      List.Transform(
        Table.ColumnNames(#"Changed column type 1"),
        (c) => {c, Text.Upper(c)}
      ),
      MissingField.Ignore
    ),
  #"Transform columns" = Table.TransformColumnTypes(#"Uppercased column names", {{"DRUG_3_FREQ", type text}, {"DRUG_1_RTE", type text}, {"DRUG_2_RTE", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"DRUG_3_FREQ", null}, {"DRUG_1_RTE", null}, {"DRUG_2_RTE", null}}),
  #"Merged queries" = Table.NestedJoin(#"Replace errors", {"EPISODE_UID"}, #"episode-controller", {"Episode ID"}, "API - Episodes", JoinKind.Inner),
  // Use REV_TIMEIN as the date controller for filling blank dates
  #"Inserted conditional column 1" = Table.AddColumn(#"Merged queries", "arrests-update-fixed", each if [ARRESTS_UPDATE] = null then [REV_TIMEIN] else [ARRESTS_UPDATE]),
  #"Removed columns 2" = Table.RemoveColumns(#"Inserted conditional column 1", {"ARRESTS_UPDATE"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed columns 2", {"ARRESTS", "arrests-update-fixed", "CP_ALCOHOL", "ASSESS_DT", "ASSESS_TYPE", "CP_DRUGS", "DISABILITY_1", "DISABILITY_2", "DISABILITY_3", "DISABILITY_4", "DISABILITY_5", "DRUG_1", "DRUG_1_UPDATE", "DRUG_2", "DRUG_2_UPDATE", "DRUG_3", "DRUG_3_UPDAT", "DRUG_1_AGE", "DRUG_2_AGE", "DRUG_3_AG", "DRUG_1_FREQ", "DRUG_1_FREQ_UPDATE", "DRUG_2_FREQ", "DRUG_2_FREQ_UPDATE", "DRUG_3_FREQ", "DRUG_3_FREQ_UPDAT", "DRUG_1_RTE", "DRUG_1_RTE_UPDATE", "DRUG_2_RTE", "DRUG_2_RTE_UPDATE", "DRUG_3_RTE_UPDAT", "DX_PRIMARY", "DX_SEC", "ENCOUNTERS", "ENCOUNTERS_UPDATE", "EPISODE_UID", "SCHOOL_ABSENCE", "SCHOOL_ABSENCE_UPDATE", "SCHOOL_ENROLLMENT", "SCHOOL_ENROLL_UPDATE", "SCHOOL_SUSPENSION", "SCHOOL_SUSP_UPDATE", "SP_DATE", "SP_SMI", "DX_3", "DX_4", "DRUG_3_RTE", "REV_TIMEIN", "REGION", "API - Episodes"}),
  #"Renamed columns" = Table.RenameColumns(#"Reordered columns", {{"arrests-update-fixed", "ARRESTS_UPDATE"}}),
  // Fix ASSESS_DT
  #"Inserted conditional column 2" = Table.AddColumn(#"Renamed columns", "assess-dt-fix", each if [ASSESS_DT] = null then [REV_TIMEIN] else [ASSESS_DT]),
  #"Removed ASSESS_DT" = Table.RemoveColumns(#"Inserted conditional column 2", {"ASSESS_DT"}),
  #"Renamed ASSESS_DT" = Table.RenameColumns(#"Removed ASSESS_DT", {{"assess-dt-fix", "ASSESS_DT"}}),
  // Fix DRUG_1_UPDATE
  #"Fix DRUG_1_UPDATE" = Table.AddColumn(#"Renamed ASSESS_DT", "drug-1-update-fix", each if [DRUG_1_UPDATE] = null then [REV_TIMEIN] else [DRUG_1_UPDATE]),
  #"Remove DRUG_1_UPDATE" = Table.RemoveColumns(#"Fix DRUG_1_UPDATE", {"DRUG_1_UPDATE"}),
  #"Rename DRUG_1_UPDATE" = Table.RenameColumns(#"Remove DRUG_1_UPDATE", {{"drug-1-update-fix", "DRUG_1_UPDATE"}}),
  // Fix DRUG_2_UPDATE
  #"Fix DRUG_2_UPDATE" = Table.AddColumn(#"Rename DRUG_1_UPDATE", "drug-2-update-fix", each if [DRUG_2_UPDATE] = null then [REV_TIMEIN] else [DRUG_2_UPDATE]),
  #"Remove DRUG_2_UPDATE" = Table.RemoveColumns(#"Fix DRUG_2_UPDATE", {"DRUG_2_UPDATE"}),
  #"Rename DRUG_2_UPDATE" = Table.RenameColumns(#"Remove DRUG_2_UPDATE", {{"drug-2-update-fix", "DRUG_2_UPDATE"}}),
  // Fix DRUG_3_UPDAT
  #"Fix DRUG_3_UPDAT" = Table.AddColumn(#"Rename DRUG_2_UPDATE", "drug-3-updat-fix", each if [DRUG_3_UPDAT] = null then [REV_TIMEIN] else [DRUG_3_UPDAT]),
  #"Remove DRUG_3_UPDAT" = Table.RemoveColumns(#"Fix DRUG_3_UPDAT", {"DRUG_3_UPDAT"}),
  #"Rename DRUG_3_UPDAT" = Table.RenameColumns(#"Remove DRUG_3_UPDAT", {{"drug-3-updat-fix", "DRUG_3_UPDAT"}}),
  // Fix DRUG_1_FREQ_UPDATE
  #"Fix DRUG_1_FREQ_UPDATE" = Table.AddColumn(#"Rename DRUG_3_UPDAT", "drug-1-freq-update-fix", each if [DRUG_1_FREQ_UPDATE] = null then [REV_TIMEIN] else [DRUG_1_FREQ_UPDATE]),
  #"Remove DRUG_1_FREQ_UPDATE" = Table.RemoveColumns(#"Fix DRUG_1_FREQ_UPDATE", {"DRUG_1_FREQ_UPDATE"}),
  #"Rename DRUG_1_FREQ_UPDATE" = Table.RenameColumns(#"Remove DRUG_1_FREQ_UPDATE", {{"drug-1-freq-update-fix", "DRUG_1_FREQ_UPDATE"}}),
  // Fix DRUG_2_FREQ_UPDATE
  #"Fix DRUG_2_FREQ_UPDATE" = Table.AddColumn(#"Rename DRUG_1_FREQ_UPDATE", "drug-2-freq-update-fix", each if [DRUG_2_FREQ_UPDATE] = null then [REV_TIMEIN] else [DRUG_2_FREQ_UPDATE]),
  #"Remove DRUG_2_FREQ_UPDATE" = Table.RemoveColumns(#"Fix DRUG_2_FREQ_UPDATE", {"DRUG_2_FREQ_UPDATE"}),
  #"Rename DRUG_2_FREQ_UPDATE" = Table.RenameColumns(#"Remove DRUG_2_FREQ_UPDATE", {{"drug-2-freq-update-fix", "DRUG_2_FREQ_UPDATE"}}),
  // Fix DRUG_3_FREQ_UPDAT
  #"Fix DRUG_3_FREQ_UPDAT" = Table.AddColumn(#"Rename DRUG_2_FREQ_UPDATE", "drug-3-freq-updat-fix", each if [DRUG_3_FREQ_UPDAT] = null then [REV_TIMEIN] else [DRUG_3_FREQ_UPDAT]),
  #"Remove DRUG_3_FREQ_UPDAT" = Table.RemoveColumns(#"Fix DRUG_3_FREQ_UPDAT", {"DRUG_3_FREQ_UPDAT"}),
  #"Rename DRUG_3_FREQ_UPDAT" = Table.RenameColumns(#"Remove DRUG_3_FREQ_UPDAT", {{"drug-3-freq-updat-fix", "DRUG_3_FREQ_UPDAT"}}),
  // Fix DRUG_1_RTE_UPDATE
  #"Fix DRUG_1_RTE_UPDATE" = Table.AddColumn(#"Rename DRUG_3_FREQ_UPDAT", "drug-1-rte-update-fix", each if [DRUG_1_RTE_UPDATE] = null then [REV_TIMEIN] else [DRUG_1_RTE_UPDATE]),
  #"Remove DRUG_1_RTE_UPDATE" = Table.RemoveColumns(#"Fix DRUG_1_RTE_UPDATE", {"DRUG_1_RTE_UPDATE"}),
  #"Rename DRUG_1_RTE_UPDATE" = Table.RenameColumns(#"Remove DRUG_1_RTE_UPDATE", {{"drug-1-rte-update-fix", "DRUG_1_RTE_UPDATE"}}),
  // Fix DRUG_2_RTE_UPDATE
  #"Fix DRUG_2_RTE_UPDATE" = Table.AddColumn(#"Rename DRUG_1_RTE_UPDATE", "drug-2-rte-update-fix", each if [DRUG_2_RTE_UPDATE] = null then [REV_TIMEIN] else [DRUG_2_RTE_UPDATE]),
  #"Remove DRUG_2_RTE_UPDATE" = Table.RemoveColumns(#"Fix DRUG_2_RTE_UPDATE", {"DRUG_2_RTE_UPDATE"}),
  #"Rename DRUG_2_RTE_UPDATE" = Table.RenameColumns(#"Remove DRUG_2_RTE_UPDATE", {{"drug-2-rte-update-fix", "DRUG_2_RTE_UPDATE"}}),
  // Fix DRUG_3_RTE_UPDAT
  #"Fix DRUG_3_RTE_UPDAT" = Table.AddColumn(#"Rename DRUG_2_RTE_UPDATE", "drug-3-rte-updat-fix", each if [DRUG_3_RTE_UPDAT] = null then [REV_TIMEIN] else [DRUG_3_RTE_UPDAT]),
  #"Remove DRUG_3_RTE_UPDAT" = Table.RemoveColumns(#"Fix DRUG_3_RTE_UPDAT", {"DRUG_3_RTE_UPDAT"}),
  #"Rename DRUG_3_RTE_UPDAT" = Table.RenameColumns(#"Remove DRUG_3_RTE_UPDAT", {{"drug-3-rte-updat-fix", "DRUG_3_RTE_UPDAT"}}),
  // Fix ENCOUNTERS_UPDATE
  #"Fix ENCOUNTERS_UPDATE" = Table.AddColumn(#"Rename DRUG_3_RTE_UPDAT", "encounters-update-fix", each if [ENCOUNTERS_UPDATE] = null then [REV_TIMEIN] else [ENCOUNTERS_UPDATE]),
  #"Remove ENCOUNTERS_UPDATE" = Table.RemoveColumns(#"Fix ENCOUNTERS_UPDATE", {"ENCOUNTERS_UPDATE"}),
  #"Rename ENCOUNTERS_UPDATE" = Table.RenameColumns(#"Remove ENCOUNTERS_UPDATE", {{"encounters-update-fix", "ENCOUNTERS_UPDATE"}}),
  // Fix SCHOOL_ABSENCE_UPDATE
  #"Fix SCHOOL_ABSENCE_UPDATE" = Table.AddColumn(#"Rename ENCOUNTERS_UPDATE", "school-absence-update-fix", each if [SCHOOL_ABSENCE_UPDATE] = null then [REV_TIMEIN] else [SCHOOL_ABSENCE_UPDATE]),
  #"Remove SCHOOL_ABSENCE_UPDATE" = Table.RemoveColumns(#"Fix SCHOOL_ABSENCE_UPDATE", {"SCHOOL_ABSENCE_UPDATE"}),
  #"Rename SCHOOL_ABSENCE_UPDATE" = Table.RenameColumns(#"Remove SCHOOL_ABSENCE_UPDATE", {{"school-absence-update-fix", "SCHOOL_ABSENCE_UPDATE"}}),
  // Fix SCHOOL_ENROLL_UPDATE
  #"Fix SCHOOL_ENROLL_UPDATE" = Table.AddColumn(#"Rename SCHOOL_ABSENCE_UPDATE", "school-enroll-update-fix", each if [SCHOOL_ENROLL_UPDATE] = null then [REV_TIMEIN] else [SCHOOL_ENROLL_UPDATE]),
  #"Remove SCHOOL_ENROLL_UPDATE" = Table.RemoveColumns(#"Fix SCHOOL_ENROLL_UPDATE", {"SCHOOL_ENROLL_UPDATE"}),
  #"Rename SCHOOL_ENROLL_UPDATE" = Table.RenameColumns(#"Remove SCHOOL_ENROLL_UPDATE", {{"school-enroll-update-fix", "SCHOOL_ENROLL_UPDATE"}}),
  // Fix SCHOOL_SUSP_UPDATE
  #"Fix SCHOOL_SUSP_UPDATE" = Table.AddColumn(#"Rename SCHOOL_ENROLL_UPDATE", "school-susp-update-fix", each if [SCHOOL_SUSP_UPDATE] = null then [REV_TIMEIN] else [SCHOOL_SUSP_UPDATE]),
  #"Remove SCHOOL_SUSP_UPDATE" = Table.RemoveColumns(#"Fix SCHOOL_SUSP_UPDATE", {"SCHOOL_SUSP_UPDATE"}),
  #"Rename SCHOOL_SUSP_UPDATE" = Table.RenameColumns(#"Remove SCHOOL_SUSP_UPDATE", {{"school-susp-update-fix", "SCHOOL_SUSP_UPDATE"}}),
  // Fix SP_DATE
  #"Fix SP_DATE" = Table.AddColumn(#"Rename SCHOOL_SUSP_UPDATE", "sp-date-fix", each if [SP_DATE] = null then [REV_TIMEIN] else [SP_DATE]),
  #"Remove SP_DATE" = Table.RemoveColumns(#"Fix SP_DATE", {"SP_DATE"}),
  #"Rename SP_DATE" = Table.RenameColumns(#"Remove SP_DATE", {{"sp-date-fix", "SP_DATE"}}),
  // Rename truncated column names to match data dictionary
  #"Renamed truncated cols" = Table.RenameColumns(#"Rename SP_DATE", {
      {"DRUG_3_UPDAT", "DRUG_3_UPDATE"},
      {"DRUG_3_AG", "DRUG_3_AGE"},
      {"DRUG_3_FREQ_UPDAT", "DRUG_3_FREQ_UPDATE"},
      {"DRUG_3_RTE_UPDAT", "DRUG_3_RTE_UPDATE"}
  }, MissingField.Ignore),
  // Convert CP_ALCOHOL and CP_DRUGS from Yes/No to 1/0
  #"Fixed CP fields" = Table.TransformColumns(#"Renamed truncated cols", {
      {"CP_ALCOHOL", each if _ = "Yes" or _ = 1 or _ = "1" then 1 else if _ = "No" or _ = 0 or _ = "0" then 0 else _},
      {"CP_DRUGS", each if _ = "Yes" or _ = 1 or _ = "1" then 1 else if _ = "No" or _ = 0 or _ = "0" then 0 else _}
  }),
  // Clean "null" text to actual null across all columns
  #"Cleaned nulls" = Table.TransformColumns(#"Fixed CP fields", 
      List.Transform(Table.ColumnNames(#"Fixed CP fields"), (col) => 
          {col, each if _ = "null" or _ = "" then null else _, type any}
      )
  ),
  // Apply defaults for DISABILITY fields (08 = no disability)
  #"Fixed disabilities" = Table.TransformColumns(#"Cleaned nulls", {
      {"DISABILITY_1", each if _ = null then "08" else Text.From(_)},
      {"DISABILITY_2", each if _ = null then "08" else Text.From(_)},
      {"DISABILITY_3", each if _ = null then "08" else Text.From(_)},
      {"DISABILITY_4", each if _ = null then "08" else Text.From(_)},
      {"DISABILITY_5", each if _ = null then "08" else Text.From(_)}
  }),
  // Apply defaults for DRUG fields (14 = none, freq 8 = no use, rte 04 = oral/NA)
  #"Fixed drug defaults" = Table.TransformColumns(#"Fixed disabilities", {
      {"DRUG_1", each if _ = null then "14" else Text.From(_)},
      {"DRUG_2", each if _ = null then "14" else Text.From(_)},
      {"DRUG_3", each if _ = null then "14" else Text.From(_)},
      {"DRUG_1_FREQ", each if _ = null then "8" else Text.From(_)},
      {"DRUG_2_FREQ", each if _ = null then "8" else Text.From(_)},
      {"DRUG_3_FREQ", each if _ = null then "8" else Text.From(_)},
      {"DRUG_1_RTE", each if _ = null then "04" else Text.From(_)},
      {"DRUG_2_RTE", each if _ = null then "04" else Text.From(_)},
      {"DRUG_3_RTE", each if _ = null then "04" else Text.From(_)},
      {"DRUG_1_AGE", each if _ = null or _ = "null" then null else Text.From(_)},
      {"DRUG_2_AGE", each if _ = null or _ = "null" then null else Text.From(_)},
      {"DRUG_3_AGE", each if _ = null or _ = "null" then null else Text.From(_)},
      {"SCHOOL_ABSENCE", each if _ = null or _ = "null" then null else Text.From(_)},
      {"SCHOOL_SUSPENSION", each if _ = null or _ = "null" then null else Text.From(_)}
  }),
  // Add missing DX columns (DX_5, DX_6, DX_7, DX_8)
  #"Added DX_5" = Table.AddColumn(#"Fixed drug defaults", "DX_5", each null, type text),
  #"Added DX_6" = Table.AddColumn(#"Added DX_5", "DX_6", each null, type text),
  #"Added DX_7" = Table.AddColumn(#"Added DX_6", "DX_7", each null, type text),
  #"Added DX_8" = Table.AddColumn(#"Added DX_7", "DX_8", each null, type text),
  // Expand Episode controller to get Client ID (for filtering only, not in final output)
  #"Expanded Episode controller" = Table.ExpandTableColumn(#"Added DX_8", "API - Episodes", {"Client ID"}, {"CLIENT_ID"}),
  // Remove helper columns and REV_TIMEIN (not in target schema)
  #"Removed helper cols" = Table.RemoveColumns(#"Expanded Episode controller", {"REV_TIMEIN", "CLIENT_ID"}, MissingField.Ignore),
  // Select and reorder columns to match ASSE CSV order
  #"Final columns" = Table.SelectColumns(#"Removed helper cols", {
      "ARRESTS", "ARRESTS_UPDATE", "ASSESS_DT", "ASSESS_TYPE", "CP_ALCOHOL", "CP_DRUGS",
      "DISABILITY_1", "DISABILITY_2", "DISABILITY_3", "DISABILITY_4", "DISABILITY_5",
      "DRUG_1", "DRUG_1_UPDATE", "DRUG_2", "DRUG_2_UPDATE", "DRUG_3", "DRUG_3_UPDATE",
      "DRUG_1_AGE", "DRUG_2_AGE", "DRUG_3_AGE",
      "DRUG_1_FREQ", "DRUG_1_FREQ_UPDATE", "DRUG_2_FREQ", "DRUG_2_FREQ_UPDATE", "DRUG_3_FREQ", "DRUG_3_FREQ_UPDATE",
      "DRUG_1_RTE", "DRUG_1_RTE_UPDATE", "DRUG_2_RTE", "DRUG_2_RTE_UPDATE", "DRUG_3_RTE", "DRUG_3_RTE_UPDATE",
      "DX_PRIMARY", "DX_SEC", "ENCOUNTERS", "ENCOUNTERS_UPDATE", "EPISODE_UID",
      "SCHOOL_ABSENCE", "SCHOOL_ABSENCE_UPDATE", "SCHOOL_ENROLLMENT", "SCHOOL_ENROLL_UPDATE",
      "SCHOOL_SUSPENSION", "SCHOOL_SUSP_UPDATE", "SP_DATE", "SP_SMI", "REGION",
      "DX_3", "DX_4", "DX_5", "DX_6", "DX_7", "DX_8"
  }, MissingField.UseNull),
  #"Replaced value" = Table.ReplaceValue(#"Final columns", null, 22, Replacer.ReplaceValue, {"REGION"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", null, 1, Replacer.ReplaceValue, {"SP_SMI"}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Replaced value 1", {{"SP_SMI", type text}, {"DX_3", type text}, {"DX_4", type text}, {"DX_5", type text}, {"DX_6", type text}, {"DX_7", type text}, {"DX_8", type text}, {"REGION", type text}, {"ARRESTS", type text}, {"ASSESS_TYPE", type text}, {"CP_ALCOHOL", type text}, {"CP_DRUGS", type text}, {"DISABILITY_1", type text}, {"DISABILITY_2", type text}, {"DISABILITY_3", type text}, {"DISABILITY_4", type text}, {"DISABILITY_5", type text}, {"DRUG_1", type text}, {"DRUG_1_UPDATE", type date}, {"DRUG_2_UPDATE", type date}, {"DRUG_3_UPDATE", type date}, {"DRUG_1_FREQ_UPDATE", type date}, {"DRUG_2_FREQ_UPDATE", type date}, {"DRUG_3_FREQ_UPDATE", type date}, {"DRUG_1_RTE_UPDATE", type date}, {"DRUG_2_RTE_UPDATE", type date}, {"DRUG_3_RTE_UPDATE", type date}, {"ENCOUNTERS_UPDATE", type date}, {"EPISODE_UID", type text}, {"SCHOOL_ABSENCE", type text}, {"SCHOOL_ENROLLMENT", type text}, {"SCHOOL_SUSPENSION", type text}, {"SCHOOL_SUSP_UPDATE", type date}, {"SCHOOL_ENROLL_UPDATE", type date}, {"SCHOOL_ABSENCE_UPDATE", type date}, {"SP_DATE", type date}, {"ENCOUNTERS", type text}, {"DX_SEC", type text}, {"DX_PRIMARY", type text}, {"DRUG_3_RTE", type text}, {"DRUG_2_RTE", type text}, {"DRUG_1_RTE", type text}, {"DRUG_3_FREQ", type text}, {"DRUG_2_FREQ", type text}, {"DRUG_1_FREQ", type text}, {"DRUG_2_AGE", type text}, {"ASSESS_DT", type date}, {"ARRESTS_UPDATE", type date}}),
  #"Transform columns 1" = Table.TransformColumnTypes(#"Changed column type 2", {{"DRUG_2", type text}, {"DRUG_3", type text}, {"DRUG_1_AGE", type text}, {"DRUG_3_AGE", type text}}),
  #"Replace errors 1" = Table.ReplaceErrorValues(#"Transform columns 1", {{"DRUG_2", null}, {"DRUG_3", null}, {"DRUG_1_AGE", null}, {"DRUG_3_AGE", null}}),
  
  // Final cleanup: convert any remaining "null" text to actual null
  #"Final null cleanup" = Table.ReplaceValue(#"Replace errors 1", "null", null, Replacer.ReplaceValue, Table.ColumnNames(#"Replace errors 1"))
in
  #"Final null cleanup";
[BindToDefaultDestination = true]
shared EPISODE = let
  Source = PowerPlatform.Dataflows([]),
  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[workspaceId = "2e078b2f-481a-42df-8fee-6d81b3c2dac1"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[dataflowId = "dc6ce368-c8e7-4d12-b1d7-880bde95c99c"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[entity = "EPISODE", version = ""]}[Data],

  // Force ALL CAPS headers
  #"Uppercased column names" = Table.RenameColumns(
      #"Navigation 4",
      List.Transform(Table.ColumnNames(#"Navigation 4"), (c) => {c, Text.Upper(c)}),
      MissingField.Ignore
  ),

  // Rename column variants to match target schema
  #"Renamed variants" = Table.RenameColumns(#"Uppercased column names", {
      {"EMPL_ST_UPDATE", "EMPL_STATUS_UPDATE"},
      {"DATE_CREATED", "EPISODE_START_DATE"},
      {"PRIOR_TREATMENT_GROUP", "PRIMARY_TARGET_GROUP"}
  }, MissingField.Ignore),

  // Filter by episode-controller (inner join)
  #"Merged queries" = Table.NestedJoin(#"Renamed variants", {"EPISODE_UID"}, #"episode-controller", {"Episode ID"}, "episode-controller", JoinKind.Inner),
  #"Removed join column" = Table.RemoveColumns(#"Merged queries", {"episode-controller"}),

  // Join with CLIENTS to get gender for WOMAN_DEP logic
  // First deduplicate clients by CLUID to avoid row multiplication
  #"Navigation clients" = #"Navigation 2"{[dataflowId = "dc6ce368-c8e7-4d12-b1d7-880bde95c99c"]}[Data]{[entity = "CLIENTS", version = ""]}[Data],
  #"Distinct clients" = Table.Distinct(#"Navigation clients", {"CLUID"}),
  #"Merged with clients" = Table.NestedJoin(#"Removed join column", {"CLUID"}, #"Distinct clients", {"CLUID"}, "ClientData", JoinKind.LeftOuter),
  #"Expanded client gender" = Table.ExpandTableColumn(#"Merged with clients", "ClientData", {"SEX"}, {"CLIENT_SEX"}),

  // WOMAN_DEP: 1=Yes (woman w/dependents), 2=No (woman w/o dependents), 3=Not applicable (male)
  // Must be done here while CLIENT_SEX is still available
  // Also check for text "null" since source data may have that instead of actual null
  #"Fix WOMAN_DEP early" = Table.AddColumn(#"Expanded client gender", "WOMAN_DEP_FIXED", each 
      if [WOMAN_DEP] <> null and [WOMAN_DEP] <> "null" and [WOMAN_DEP] <> "" then [WOMAN_DEP] 
      else if [CLIENT_SEX] = "M" then "3" 
      else "2"),  // Default to "2" for females or when gender unknown
  #"Remove old WOMAN_DEP early" = Table.RemoveColumns(#"Fix WOMAN_DEP early", {"WOMAN_DEP"}, MissingField.Ignore),
  #"Rename WOMAN_DEP early" = Table.RenameColumns(#"Remove old WOMAN_DEP early", {{"WOMAN_DEP_FIXED", "WOMAN_DEP"}}),
  #"Remove CLIENT_SEX early" = Table.RemoveColumns(#"Rename WOMAN_DEP early", {"CLIENT_SEX"}, MissingField.Ignore),

  // Target schema + order (matching EPI CSV exactly - 39 columns)
  TargetCols = {
      "ADDICTIONTYPE", "EPISODE_AGENCY_UID", "ASSIGN_PV", "CLUID", "CONT_DT", "CONT_RES", "DC_DATE", "DISPOSITION",
      "ED_LEVEL", "ED_LEVEL_UPDATE", "EMPL_ST", "EMPL_STATUS_UPDATE", "FREQ_ATTEND", "EPISODE_UID", "FREQ_ATTEND_UPDATE",
      "HSE_COMP", "INTERVIEW_DT", "LEGAL_STATUS", "MARITAL_STATUS", "MARITAL_STATUS_UPDATE", "METHADONE", "SERV_PROGRAM",
      "PREGNANT", "PRIMARY_TARGET_GROUP", "PRIOR_MH", "PRIOR_TX_EPISODES", "PROGRAM_TYPE", "REF_SRCE", "RES_TYPE",
      "RES_TYPE_UPDATE", "TERMTYPE", "EPISODE_START_DATE", "WOMAN_DEP", "NUM_DEP_CHILD", "REGION", "EPISODE_AGENCY_NAME",
      "PROGRAM_TYPE_2", "PROGRAM_TYPE_3", "PROGRAM_TYPE_4"
  },

  // Add any missing target columns as nulls
  #"Added missing cols" = List.Accumulate(
      TargetCols,
      #"Remove CLIENT_SEX early",
      (state, col) => if Table.HasColumns(state, col) then state else Table.AddColumn(state, col, each null)
  ),

  // Select only target columns in exact order
  #"Final columns" = Table.SelectColumns(#"Added missing cols", TargetCols, MissingField.UseNull),

  // Clean "null" text to actual null across all columns
  #"Cleaned nulls" = Table.TransformColumns(#"Final columns", 
      List.Transform(Table.ColumnNames(#"Final columns"), (col) => 
          {col, each if _ = "null" or _ = "" then null else _, type any}
      )
  ),

  // Fill blank date fields with INTERVIEW_DT
  #"Fix CONT_DT" = Table.AddColumn(#"Cleaned nulls", "cont-dt-fix", each if [CONT_DT] = null then [INTERVIEW_DT] else [CONT_DT]),
  #"Remove CONT_DT" = Table.RemoveColumns(#"Fix CONT_DT", {"CONT_DT"}),
  #"Rename CONT_DT" = Table.RenameColumns(#"Remove CONT_DT", {{"cont-dt-fix", "CONT_DT"}}),

  #"Fix ED_LEVEL_UPDATE" = Table.AddColumn(#"Rename CONT_DT", "ed-level-update-fix", each if [ED_LEVEL_UPDATE] = null then [INTERVIEW_DT] else [ED_LEVEL_UPDATE]),
  #"Remove ED_LEVEL_UPDATE" = Table.RemoveColumns(#"Fix ED_LEVEL_UPDATE", {"ED_LEVEL_UPDATE"}),
  #"Rename ED_LEVEL_UPDATE" = Table.RenameColumns(#"Remove ED_LEVEL_UPDATE", {{"ed-level-update-fix", "ED_LEVEL_UPDATE"}}),

  #"Fix EMPL_STATUS_UPDATE" = Table.AddColumn(#"Rename ED_LEVEL_UPDATE", "empl-status-update-fix", each if [EMPL_STATUS_UPDATE] = null then [INTERVIEW_DT] else [EMPL_STATUS_UPDATE]),
  #"Remove EMPL_STATUS_UPDATE" = Table.RemoveColumns(#"Fix EMPL_STATUS_UPDATE", {"EMPL_STATUS_UPDATE"}),
  #"Rename EMPL_STATUS_UPDATE" = Table.RenameColumns(#"Remove EMPL_STATUS_UPDATE", {{"empl-status-update-fix", "EMPL_STATUS_UPDATE"}}),

  #"Fix FREQ_ATTEND_UPDATE" = Table.AddColumn(#"Rename EMPL_STATUS_UPDATE", "freq-attend-update-fix", each if [FREQ_ATTEND_UPDATE] = null then [INTERVIEW_DT] else [FREQ_ATTEND_UPDATE]),
  #"Remove FREQ_ATTEND_UPDATE" = Table.RemoveColumns(#"Fix FREQ_ATTEND_UPDATE", {"FREQ_ATTEND_UPDATE"}),
  #"Rename FREQ_ATTEND_UPDATE" = Table.RenameColumns(#"Remove FREQ_ATTEND_UPDATE", {{"freq-attend-update-fix", "FREQ_ATTEND_UPDATE"}}),

  #"Fix MARITAL_STATUS_UPDATE" = Table.AddColumn(#"Rename FREQ_ATTEND_UPDATE", "marital-status-update-fix", each if [MARITAL_STATUS_UPDATE] = null then [INTERVIEW_DT] else [MARITAL_STATUS_UPDATE]),
  #"Remove MARITAL_STATUS_UPDATE" = Table.RemoveColumns(#"Fix MARITAL_STATUS_UPDATE", {"MARITAL_STATUS_UPDATE"}),
  #"Rename MARITAL_STATUS_UPDATE" = Table.RenameColumns(#"Remove MARITAL_STATUS_UPDATE", {{"marital-status-update-fix", "MARITAL_STATUS_UPDATE"}}),

  #"Fix RES_TYPE_UPDATE" = Table.AddColumn(#"Rename MARITAL_STATUS_UPDATE", "res-type-update-fix", each if [RES_TYPE_UPDATE] = null then [INTERVIEW_DT] else [RES_TYPE_UPDATE]),
  #"Remove RES_TYPE_UPDATE" = Table.RemoveColumns(#"Fix RES_TYPE_UPDATE", {"RES_TYPE_UPDATE"}),
  #"Rename RES_TYPE_UPDATE" = Table.RenameColumns(#"Remove RES_TYPE_UPDATE", {{"res-type-update-fix", "RES_TYPE_UPDATE"}}),

  #"Fix EPISODE_START_DATE" = Table.AddColumn(#"Rename RES_TYPE_UPDATE", "episode-start-date-fix", each if [EPISODE_START_DATE] = null then [INTERVIEW_DT] else [EPISODE_START_DATE]),
  #"Remove EPISODE_START_DATE" = Table.RemoveColumns(#"Fix EPISODE_START_DATE", {"EPISODE_START_DATE"}),
  #"Rename EPISODE_START_DATE" = Table.RenameColumns(#"Remove EPISODE_START_DATE", {{"episode-start-date-fix", "EPISODE_START_DATE"}}),

  // Apply defaults
  #"Default EPISODE_AGENCY_UID" = Table.ReplaceValue(#"Rename EPISODE_START_DATE", null, 1417252230, Replacer.ReplaceValue, {"EPISODE_AGENCY_UID"}),
  #"Default ADDICTIONTYPE" = Table.ReplaceValue(#"Default EPISODE_AGENCY_UID", null, "5", Replacer.ReplaceValue, {"ADDICTIONTYPE"}),
  #"Default CONT_RES" = Table.ReplaceValue(#"Default ADDICTIONTYPE", null, "01", Replacer.ReplaceValue, {"CONT_RES"}),
  #"Default PRIMARY_TARGET_GROUP" = Table.ReplaceValue(#"Default CONT_RES", null, "1", Replacer.ReplaceValue, {"PRIMARY_TARGET_GROUP"}),
  #"Default REGION" = Table.ReplaceValue(#"Default PRIMARY_TARGET_GROUP", null, "22", Replacer.ReplaceValue, {"REGION"}),
  #"Default EPISODE_AGENCY_NAME" = Table.ReplaceValue(#"Default REGION", null, "50", Replacer.ReplaceValue, {"EPISODE_AGENCY_NAME"}),
  #"Default PROGRAM_TYPE_2" = Table.ReplaceValue(#"Default EPISODE_AGENCY_NAME", null, "19", Replacer.ReplaceValue, {"PROGRAM_TYPE_2"}),
  #"Default PROGRAM_TYPE_3" = Table.ReplaceValue(#"Default PROGRAM_TYPE_2", null, "28", Replacer.ReplaceValue, {"PROGRAM_TYPE_3"}),
  #"Default PROGRAM_TYPE_4" = Table.ReplaceValue(#"Default PROGRAM_TYPE_3", null, "32", Replacer.ReplaceValue, {"PROGRAM_TYPE_4"}),
  // Additional required field defaults
  #"Default FREQ_ATTEND" = Table.ReplaceValue(#"Default PROGRAM_TYPE_4", null, "7", Replacer.ReplaceValue, {"FREQ_ATTEND"}),
  #"Default MARITAL_STATUS" = Table.ReplaceValue(#"Default FREQ_ATTEND", null, "1", Replacer.ReplaceValue, {"MARITAL_STATUS"}),
  #"Default METHADONE" = Table.ReplaceValue(#"Default MARITAL_STATUS", null, "2", Replacer.ReplaceValue, {"METHADONE"}),
  #"Default PREGNANT" = Table.ReplaceValue(#"Default METHADONE", null, "2", Replacer.ReplaceValue, {"PREGNANT"}),
  #"Default PRIOR_TX_EPISODES" = Table.ReplaceValue(#"Default PREGNANT", null, "00", Replacer.ReplaceValue, {"PRIOR_TX_EPISODES"}),
  #"Default PROGRAM_TYPE" = Table.ReplaceValue(#"Default PRIOR_TX_EPISODES", null, "10", Replacer.ReplaceValue, {"PROGRAM_TYPE"}),
  #"Default SERV_PROGRAM" = Table.ReplaceValue(#"Default PROGRAM_TYPE", null, "12", Replacer.ReplaceValue, {"SERV_PROGRAM"}),
  #"Default NUM_DEP_CHILD" = Table.ReplaceValue(#"Default SERV_PROGRAM", null, "0", Replacer.ReplaceValue, {"NUM_DEP_CHILD"}),
  #"Default RES_TYPE" = Table.ReplaceValue(#"Default NUM_DEP_CHILD", null, "98", Replacer.ReplaceValue, {"RES_TYPE"}),
  #"Default REF_SRCE" = Table.ReplaceValue(#"Default RES_TYPE", null, "98", Replacer.ReplaceValue, {"REF_SRCE"}),

  // Reorder columns to match EPI CSV exactly
  #"Reordered columns" = Table.SelectColumns(#"Default REF_SRCE", {
      "ADDICTIONTYPE", "EPISODE_AGENCY_UID", "ASSIGN_PV", "CLUID", "CONT_DT", "CONT_RES", "DC_DATE", "DISPOSITION",
      "ED_LEVEL", "ED_LEVEL_UPDATE", "EMPL_ST", "EMPL_STATUS_UPDATE", "FREQ_ATTEND", "EPISODE_UID", "FREQ_ATTEND_UPDATE",
      "HSE_COMP", "INTERVIEW_DT", "LEGAL_STATUS", "MARITAL_STATUS", "MARITAL_STATUS_UPDATE", "METHADONE", "SERV_PROGRAM",
      "PREGNANT", "PRIMARY_TARGET_GROUP", "PRIOR_MH", "PRIOR_TX_EPISODES", "PROGRAM_TYPE", "REF_SRCE", "RES_TYPE",
      "RES_TYPE_UPDATE", "TERMTYPE", "EPISODE_START_DATE", "WOMAN_DEP", "NUM_DEP_CHILD", "REGION", "EPISODE_AGENCY_NAME",
      "PROGRAM_TYPE_2", "PROGRAM_TYPE_3", "PROGRAM_TYPE_4"
  }, MissingField.UseNull),

  // Set column types
  #"Changed types" = Table.TransformColumnTypes(#"Reordered columns", {
      {"ADDICTIONTYPE", type text}, {"EPISODE_AGENCY_UID", Int64.Type}, {"ASSIGN_PV", type text}, {"CLUID", type text},
      {"CONT_DT", type date}, {"CONT_RES", type text}, {"DC_DATE", type date}, {"DISPOSITION", type text},
      {"ED_LEVEL", type text}, {"ED_LEVEL_UPDATE", type date}, {"EMPL_ST", type text}, {"EMPL_STATUS_UPDATE", type date},
      {"FREQ_ATTEND", type text}, {"EPISODE_UID", type text}, {"FREQ_ATTEND_UPDATE", type date},
      {"HSE_COMP", type text}, {"INTERVIEW_DT", type date}, {"LEGAL_STATUS", type text},
      {"MARITAL_STATUS", type text}, {"MARITAL_STATUS_UPDATE", type date}, {"METHADONE", type text}, {"SERV_PROGRAM", type text},
      {"PREGNANT", type text}, {"PRIMARY_TARGET_GROUP", type text}, {"PRIOR_MH", type text}, {"PRIOR_TX_EPISODES", type text},
      {"PROGRAM_TYPE", type text}, {"REF_SRCE", type text}, {"RES_TYPE", type text}, {"RES_TYPE_UPDATE", type date},
      {"TERMTYPE", type text}, {"EPISODE_START_DATE", type date}, {"WOMAN_DEP", type text}, {"NUM_DEP_CHILD", type text},
      {"REGION", type text}, {"EPISODE_AGENCY_NAME", type text},
      {"PROGRAM_TYPE_2", type text}, {"PROGRAM_TYPE_3", type text}, {"PROGRAM_TYPE_4", type text}
  }),

  // Final null text cleanup
  #"Final null cleanup" = Table.ReplaceValue(#"Changed types", "null", null, Replacer.ReplaceValue, Table.ColumnNames(#"Changed types")),
  #"Replaced value" = Table.ReplaceValue(#"Final null cleanup", null, "4025", Replacer.ReplaceValue, {"ASSIGN_PV"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "1505", "05", Replacer.ReplaceValue, {"HSE_COMP"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "1503", "02", Replacer.ReplaceValue, {"HSE_COMP"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "1516", "01", Replacer.ReplaceValue, {"RES_TYPE"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "1514", "03", Replacer.ReplaceValue, {"RES_TYPE"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "1520", "03", Replacer.ReplaceValue, {"RES_TYPE"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "1513", "03", Replacer.ReplaceValue, {"RES_TYPE"})
in
  #"Replaced value 6";
shared #"episode-controller" = let
  Source = PowerPlatform.Dataflows([]),
  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[workspaceId = "2e078b2f-481a-42df-8fee-6d81b3c2dac1"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[dataflowId = "4564730f-6b6d-437e-8875-2e85af1f6351"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[entity = "API - Episodes", version = ""]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", each ([Episode Status] = "Enrolled") and ([Program ID] = "Epicenter") and ([Client ID] <> "1484"))
in
  #"Filtered rows";
[BindToDefaultDestination = true]
shared SERVICE = let
  Source = PowerPlatform.Dataflows([]),
  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[workspaceId = "f09e25de-cf5c-4a79-a1ba-c2673d34f3d3"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[dataflowId = "6017eed9-2ebe-48f4-b504-d5dcd251c6f0"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[entity = "API - Billing", version = ""]}[Data],

  // Select and rename columns
  #"Selected columns" = Table.SelectColumns(#"Navigation 4", {"Service ID", "Client ID", "Employee ID", "Service Type", "Status", "timein", "cotherapist", "Episode ID", "Timeout Time", "Service Date"}),
  
  // Rename to target schema names
  #"Renamed columns" = Table.RenameColumns(#"Selected columns", {
      {"Service ID", "TICKETNO"},
      {"Client ID", "CLUID"},
      {"Employee ID", "PV_SERV"},
      {"Service Type", "SERVICE"},
      {"timein", "BEGINTIME"},
      {"Timeout Time", "ENDTIME"},
      {"Episode ID", "EPISODE_UID"},
      {"cotherapist", "PV_CO_SERV"},
      {"Service Date", "SERV_DATE"}
  }),

  // Remove Status column
  #"Removed Status" = Table.RemoveColumns(#"Renamed columns", {"Status"}),

  // Filter by service types
  #"Filtered rows" = Table.SelectRows(#"Removed Status", each 
      Text.Contains([SERVICE], "BUN") or 
      [SERVICE] = "CPST" or 
      [SERVICE] = "Assessment CPST" or
      [SERVICE] = "PSR" or 
      [SERVICE] = "PSR Group" or
      Text.Contains([SERVICE], "Assessment") or 
      Text.Contains([SERVICE], "Psychotherapy") or
      Text.Contains([SERVICE], "FEP")),

  // Filter by date range
  #"Filtered by date" = Table.SelectRows(#"Filtered rows", each [SERV_DATE] >= #date(2025, 9, 1) and [SERV_DATE] <= #date(2026, 1, 23)),

  // Map SERVICE names to codes
  // BUN* = H2022, CPST = H0036, PSR = H2017, Assessment* = 90792, Psychotherapy* = 90834
  #"Map SERVICE codes" = Table.TransformColumns(#"Filtered by date", {
      {"SERVICE", each 
          if Text.Contains(_, "BUN") then "H2022"
          else if Text.Contains(_, "Assessment") then "90792"
          else if Text.Contains(_, "Psychotherapy") then "90834"
          else if _ = "CPST" then "H0036"
          else if Text.Contains(_, "PSR") then "H2017"
          else if Text.Contains(_, "FEP") then "H0036"  // FEP services map to H0036
          else _, type text}
  }),

  // Clean cotherapist column (replace "false" with empty)
  #"Clean PV_CO_SERV" = Table.ReplaceValue(#"Map SERVICE codes", "false", null, Replacer.ReplaceValue, {"PV_CO_SERV"}),

  // Add static columns
  #"Add APPT_STAT" = Table.AddColumn(#"Clean PV_CO_SERV", "APPT_STAT", each "1", type text),
  #"Add CLN_TYPE" = Table.AddColumn(#"Add APPT_STAT", "CLN_TYPE", each "2", type text),
  #"Add SERVICE_AGENCY_ID" = Table.AddColumn(#"Add CLN_TYPE", "SERVICE_AGENCY_ID", each "1417252230", type text),
  #"Add REGION" = Table.AddColumn(#"Add SERVICE_AGENCY_ID", "REGION", each "22", type text),

  // Format time columns to HH:MM (24-hour)
  // Source is datetimezone, so extract time portion first
  #"Format BEGINTIME" = Table.TransformColumns(#"Add REGION", {
      {"BEGINTIME", each if _ <> null then Time.ToText(DateTime.Time(DateTime.From(_)), "HH:mm") else null, type text}
  }),
  #"Format ENDTIME" = Table.TransformColumns(#"Format BEGINTIME", {
      {"ENDTIME", each if _ <> null then Time.ToText(DateTime.Time(DateTime.From(_)), "HH:mm") else null, type text}
  }),

  // Select columns in exact target order (13 columns)
  #"Final columns" = Table.SelectColumns(#"Format ENDTIME", {
      "APPT_STAT", "BEGINTIME", "CLN_TYPE", "CLUID", "ENDTIME", "EPISODE_UID", 
      "PV_CO_SERV", "PV_SERV", "SERV_DATE", "SERVICE", "TICKETNO", "SERVICE_AGENCY_ID", "REGION"
  }, MissingField.UseNull),

  // Set column types
  #"Changed types" = Table.TransformColumnTypes(#"Final columns", {
      {"APPT_STAT", type text}, {"BEGINTIME", type text}, {"CLN_TYPE", type text}, {"CLUID", type text},
      {"ENDTIME", type text}, {"EPISODE_UID", type text}, {"PV_CO_SERV", type text}, {"PV_SERV", type text},
      {"SERV_DATE", type date}, {"SERVICE", type text}, {"TICKETNO", type text}, 
      {"SERVICE_AGENCY_ID", type text}, {"REGION", type text}
  }),
  #"Merged queries" = Table.NestedJoin(#"Changed types", {"EPISODE_UID"}, #"episode-controller", {"Episode ID"}, "episode-controller", JoinKind.Inner),
  #"Removed columns" = Table.RemoveColumns(#"Merged queries", {"episode-controller"})
in
  #"Removed columns";
[BindToDefaultDestination = true]
shared HEADER = let
  // Get row counts from other tables
  ClientCount = Table.RowCount(#"CLIENTS"),
  EpisodeCount = Table.RowCount(#"EPISODE"),
  AssessmentCount = Table.RowCount(#"ASSESSMENT"),
  ServiceCount = Table.RowCount(#"SERVICE"),

  // Define reporting period dates (adjust as needed)
  TodayDate = DateTime.Date(DateTime.LocalNow()),
  BeginningPeriod = Date.AddMonths(TodayDate, -1),
  EndingPeriod = TodayDate,

  // Build the header record
  HeaderRecord = [
      ORGANIZATION_REPORTING_CODE = "VOA-NL",
      FILE_TYPE = "P",
      DATE = TodayDate,
      BEGINNING_REPORT_PERIOD = BeginningPeriod,
      ENDING_REPORT_PERIOD = EndingPeriod,
      CLIENT_RECORD_COUNT = ClientCount,
      EPISODE_RECORD_COUNT = EpisodeCount,
      ASSESSMENT_RECORD_COUNT = AssessmentCount,
      SERVICE_RECORD_COUNT = ServiceCount
  ],

  // Convert to table
  HeaderTable = Table.FromRecords({HeaderRecord}),

  // Set column types
  #"Changed types" = Table.TransformColumnTypes(HeaderTable, {
      {"ORGANIZATION_REPORTING_CODE", type text},
      {"FILE_TYPE", type text},
      {"DATE", type date},
      {"BEGINNING_REPORT_PERIOD", type date},
      {"ENDING_REPORT_PERIOD", type date},
      {"CLIENT_RECORD_COUNT", Int64.Type},
      {"EPISODE_RECORD_COUNT", Int64.Type},
      {"ASSESSMENT_RECORD_COUNT", Int64.Type},
      {"SERVICE_RECORD_COUNT", Int64.Type}
  })
in
  #"Changed types";
shared DefaultDestination = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data]
in
  #"Navigation 2";
