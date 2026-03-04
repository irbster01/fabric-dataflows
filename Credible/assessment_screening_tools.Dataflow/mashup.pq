[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared assessment_screening_tools = let
  Source = Xml.Tables(Web.Contents("https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2r!P8PDvfys6XiPCY4aEm7Ew__&start_date=&end_date=&custom_param1=&custom_param2=&custom_param3=")),
  Navigation = Source{1}[Table],
  #"Expanded Table" = Table.ExpandTableColumn(Navigation, "Table", {"Name", "Table"}, {"Name.1", "Table.1"}),
  #"Expanded Table.1" = Table.ExpandTableColumn(#"Expanded Table", "Table.1", {"Name", "Table"}, {"Name.2", "Table"}),
  #"Expanded Table 1" = Table.ExpandTableColumn(#"Expanded Table.1", "Table", {"Name", "Table"}, {"Name.3", "Table.1"}),
  #"Expanded Table.1 1" = Table.ExpandTableColumn(#"Expanded Table 1", "Table.1", {"clientvisit_id", "reason_for_scoring_461106", "risk_of_harm_score_461113", "functional_status_score_461120", "comorbidity_score_461128", "environmental_stress_score_461135", "resiliency_and_treatment_score_461149", "engagement_score_461156", "score_level_of_intensity_461158", "calocus_score_461157", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "total_score_454526", "reason_for_scoring_454968", "risk_of_harm_score_454976", "functional_status_score_454984", "comorbidity_score_454992", "environmental_stress_score_455000", "environmental_support_score_455007", "recovery_environmental_score_t_455008", "engagement_score_455023", "preliminary_level_of_intensity_455026", "preiliminary_locus_score_455025", "tx_recovery_history_score_455016", "score_454504", "score_interpretation_454527", "preliminary_level_of_intensity_455026_notes", "recommended_level_of_intensity_497684", "locus_score_455024"}, {"clientvisit_id", "reason_for_scoring_461106", "risk_of_harm_score_461113", "functional_status_score_461120", "comorbidity_score_461128", "environmental_stress_score_461135", "resiliency_and_treatment_score_461149", "engagement_score_461156", "score_level_of_intensity_461158", "calocus_score_461157", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "total_score_454526", "reason_for_scoring_454968", "risk_of_harm_score_454976", "functional_status_score_454984", "comorbidity_score_454992", "environmental_stress_score_455000", "environmental_support_score_455007", "recovery_environmental_score_t_455008", "engagement_score_455023", "preliminary_level_of_intensity_455026", "preiliminary_locus_score_455025", "tx_recovery_history_score_455016", "score_454504", "score_interpretation_454527", "preliminary_level_of_intensity_455026_notes", "recommended_level_of_intensity_497684", "locus_score_455024"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Table.1 1", {"Name", "Name.1", "Name.2", "Name.3"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"clientvisit_id", "Service ID"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns 1", {{"calocus_score_461157", "CALOCUS Score"}, {"preiliminary_locus_score_455025", "Pre LOCUS Score"}, {"locus_score_455024", "LOCUS Score"}, {"score_interpretation_454527", "Depression interpret"}}),
  #"Transform columns" = Table.TransformColumnTypes(#"Renamed columns 1", {{"Service ID", type text}, {"reason_for_scoring_461106", type text}, {"risk_of_harm_score_461113", type text}, {"functional_status_score_461120", type text}, {"comorbidity_score_461128", type text}, {"environmental_stress_score_461135", type text}, {"resiliency_and_treatment_score_461149", type text}, {"engagement_score_461156", type text}, {"score_level_of_intensity_461158", type text}, {"CALOCUS Score", type text}, {"total_score_454526", type text}, {"reason_for_scoring_454968", type text}, {"risk_of_harm_score_454976", type text}, {"functional_status_score_454984", type text}, {"comorbidity_score_454992", type text}, {"environmental_stress_score_455000", type text}, {"environmental_support_score_455007", type text}, {"recovery_environmental_score_t_455008", type text}, {"engagement_score_455023", type text}, {"preliminary_level_of_intensity_455026", type text}, {"Pre LOCUS Score", type text}, {"tx_recovery_history_score_455016", type text}, {"score_454504", type text}, {"Depression interpret", type text}, {"preliminary_level_of_intensity_455026_notes", type text}, {"recommended_level_of_intensity_497684", type text}, {"LOCUS Score", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"Service ID", null}, {"reason_for_scoring_461106", null}, {"risk_of_harm_score_461113", null}, {"functional_status_score_461120", null}, {"comorbidity_score_461128", null}, {"environmental_stress_score_461135", null}, {"resiliency_and_treatment_score_461149", null}, {"engagement_score_461156", null}, {"score_level_of_intensity_461158", null}, {"CALOCUS Score", null}, {"total_score_454526", null}, {"reason_for_scoring_454968", null}, {"risk_of_harm_score_454976", null}, {"functional_status_score_454984", null}, {"comorbidity_score_454992", null}, {"environmental_stress_score_455000", null}, {"environmental_support_score_455007", null}, {"recovery_environmental_score_t_455008", null}, {"engagement_score_455023", null}, {"preliminary_level_of_intensity_455026", null}, {"Pre LOCUS Score", null}, {"tx_recovery_history_score_455016", null}, {"score_454504", null}, {"Depression interpret", null}, {"preliminary_level_of_intensity_455026_notes", null}, {"recommended_level_of_intensity_497684", null}, {"LOCUS Score", null}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replace errors", {{"CALOCUS Score", Int64.Type}, {"Pre LOCUS Score", Int64.Type}, {"LOCUS Score", Int64.Type}}),
  #"Replaced value" = Table.ReplaceValue(#"Changed column type", "0-4 No or Minimal Depression", "0", Replacer.ReplaceText, {"Depression interpret"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "10-14 Moderate Depression", "2", Replacer.ReplaceText, {"Depression interpret"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "15-19 Moderately Severe Depression", "3", Replacer.ReplaceText, {"Depression interpret"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "5-9 Mild Depression", "1", Replacer.ReplaceText, {"Depression interpret"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "Severe Depression", "5", Replacer.ReplaceText, {"Depression interpret"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Replaced value 4", {{"Depression interpret", Int64.Type}}),
  #"Renamed columns 2" = Table.RenameColumns(#"Changed column type 1", {{"recommended_level_of_intensity_497684", "Recommended Level of Intensity"}, {"tx_recovery_history_score_455016", "Recovery History Score"}, {"preliminary_level_of_intensity_455026", "Prelim Level of Intesnity"}, {"engagement_score_455023", "Engagement Score"}, {"recovery_environmental_score_t_455008", "Recovery Environ Score"}, {"environmental_support_score_455007", "Environ Support Score"}, {"environmental_stress_score_455000", "Environ Stress Score"}, {"comorbidity_score_454992", "Co-Morbidity Score"}, {"functional_status_score_454984", "Functional Score"}, {"risk_of_harm_score_454976", "Risk of Harm Score"}, {"reason_for_scoring_454968", "Reason for Scoring"}, {"reason_for_scoring_461106", "Calocus Reason for Scoring"}, {"risk_of_harm_score_461113", "Calocus Risk of Harm Score"}, {"functional_status_score_461120", "Calocus functional status score"}, {"comorbidity_score_461128", "CALOCUS Co-Morbid Score"}, {"environmental_stress_score_461135", "Calocus Environ Stress Score"}, {"resiliency_and_treatment_score_461149", "CALOCUS resiliency Score"}, {"engagement_score_461156", "CALOCUS Engagement Score"}, {"score_level_of_intensity_461158", "CALOCUS Level of Intensity Score"}}),
  #"Sorted rows" = Table.Sort(#"Renamed columns 2", {{"LOCUS Score", Order.Descending}}),
  #"Renamed columns 3" = Table.RenameColumns(#"Sorted rows", {{"Recovery History Score", "LOCUS Recovery History Score"}, {"Prelim Level of Intesnity", "LOCUS Prelim Level of Intesnity"}, {"Engagement Score", "Locus Engagement Score"}, {"Recovery Environ Score", "Locus Recovery Environ Score"}, {"Environ Support Score", "LOCUS Environ Support Score"}, {"Environ Stress Score", "LOCUS Environ Stress Score"}, {"Co-Morbidity Score", "LOCUS Co-Morbidity Score"}, {"Functional Score", "LOCUS Functional Score"}, {"Risk of Harm Score", "LOCUS Risk of Harm Score"}, {"Reason for Scoring", "LOCUS Reason for Scoring"}}),
  #"Sorted rows 1" = Table.Sort(#"Renamed columns 3", {{"Depression interpret", Order.Descending}}),
  #"Merged queries" = Table.NestedJoin(#"Sorted rows 1", {"Service ID"}, raw_credible_services, {"service_id"}, "raw_credible_services", JoinKind.LeftOuter),
  #"Expanded raw_credible_services" = Table.ExpandTableColumn(#"Merged queries", "raw_credible_services", {"client_id", "service_type", "service_date"}, {"client_id", "service_type", "service_date"}),
  #"Renamed columns 4" = Table.RenameColumns(#"Expanded raw_credible_services", {{"Service ID", "service_id"}, {"Calocus Reason for Scoring", "calocus_reason"}, {"Calocus Risk of Harm Score", "calocus_risk_of_harm_score"}, {"Calocus functional status score", "calocus_functional_status_score"}, {"CALOCUS Co-Morbid Score", "calocus_comorbidity_score"}, {"Calocus Environ Stress Score", "calocus_env_stress_score"}, {"CALOCUS resiliency Score", "calocus_resiliency_score"}, {"CALOCUS Engagement Score", "calocus_engagement_score"}, {"CALOCUS Level of Intensity Score", "calocus_level_of_intensity"}, {"CALOCUS Score", "calocus_score"}, {"total_score_454526", "total_score"}, {"LOCUS Reason for Scoring", "locus_reason"}, {"LOCUS Risk of Harm Score", "locs_risk_of_harm"}, {"LOCUS Functional Score", "locus_functional_score"}, {"LOCUS Co-Morbidity Score", "locus_comorbidity_score"}, {"LOCUS Environ Stress Score", "locus_env_stress_score"}, {"LOCUS Environ Support Score", "locus_env_support_score"}, {"Locus Recovery Environ Score", "locus_recovery_env_score"}, {"Locus Engagement Score", "locus_engagement_score"}, {"LOCUS Prelim Level of Intesnity", "locus_prelim_level_of_intensity"}, {"Pre LOCUS Score", "pre_locus_score"}, {"LOCUS Recovery History Score", "locus_recovery_history_score"}, {"Depression interpret", "depression_interpretation"}, {"preliminary_level_of_intensity_455026_notes", "locus_pre_level_of_intensity_notes"}, {"Recommended Level of Intensity", "rec_level_of_intensity"}, {"LOCUS Score", "locus_score"}}),
  #"Merged queries 1" = Table.NestedJoin(#"Renamed columns 4", {"client_id"}, raw_credible_clients, {"Client ID"}, "raw_credible_clients", JoinKind.LeftOuter),
  #"Expanded raw_credible_clients" = Table.ExpandTableColumn(#"Merged queries 1", "raw_credible_clients", {"Client Name", "DOB"}, {"Client Name", "DOB"}),
  #"Renamed columns 5" = Table.RenameColumns(#"Expanded raw_credible_clients", {{"Client Name", "client_name"}, {"DOB", "dob"}})
in
  #"Renamed columns 5";
[BindToDefaultDestination = true]
shared raw_calocus = let
  Source = assessment_screening_tools,
  #"Removed other columns" = Table.SelectColumns(Source, {"service_id", "calocus_reason", "calocus_risk_of_harm_score", "calocus_functional_status_score", "calocus_comorbidity_score", "calocus_env_stress_score", "calocus_resiliency_score", "calocus_engagement_score", "calocus_level_of_intensity", "calocus_score", "total_score", "score_454504", "rec_level_of_intensity", "service_type", "service_date", "client_name", "dob"}),
  #"Reordered columns 1" = Table.ReorderColumns(#"Removed other columns", {"service_id", "service_date", "calocus_reason", "calocus_risk_of_harm_score", "calocus_functional_status_score", "calocus_comorbidity_score", "calocus_env_stress_score", "calocus_resiliency_score", "calocus_engagement_score", "calocus_level_of_intensity", "calocus_score", "total_score", "score_454504", "rec_level_of_intensity", "service_type"}),
  #"Filtered rows" = Table.SelectRows(#"Reordered columns 1", each ([service_type] = "CAL LOCUS" or [service_type] = "CALOCUS")),
  #"Reordered columns" = Table.ReorderColumns(#"Filtered rows", {"service_id", "client_name", "dob", "service_date", "service_type", "calocus_reason", "calocus_risk_of_harm_score", "calocus_functional_status_score", "calocus_comorbidity_score", "calocus_env_stress_score", "calocus_resiliency_score", "calocus_engagement_score", "calocus_level_of_intensity", "calocus_score", "total_score", "score_454504", "rec_level_of_intensity"})
in
  #"Reordered columns";
[BindToDefaultDestination = true]
shared raw_locus = let
  Source = assessment_screening_tools,
  #"Filtered rows" = Table.SelectRows(Source, each ([service_type] = "LOCUS")),
  #"Removed columns" = Table.RemoveColumns(#"Filtered rows", {"calocus_score", "calocus_level_of_intensity", "calocus_engagement_score", "calocus_resiliency_score", "calocus_env_stress_score", "calocus_comorbidity_score", "calocus_functional_status_score", "calocus_risk_of_harm_score", "calocus_reason", "total_score"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed columns", {"service_id", "client_id", "service_type", "service_date", "client_name", "dob", "locus_reason", "locs_risk_of_harm", "locus_functional_score", "locus_comorbidity_score", "locus_env_stress_score", "locus_env_support_score", "locus_recovery_env_score", "locus_engagement_score", "locus_prelim_level_of_intensity", "pre_locus_score", "locus_recovery_history_score", "score_454504", "depression_interpretation", "locus_pre_level_of_intensity_notes", "rec_level_of_intensity", "locus_score"})
in
  #"Reordered columns";
[BindToDefaultDestination = true]
shared desoto_active_phq9 = let
  Source =
    Xml.Tables(
      Web.Contents(
        "https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2rJczhl8Dgm6D4O25!lJ5CfQ__&start_date=&end_date=&custom_param1=&custom_param2=&custom_param3="
      )
    ),

  #"Navigation 1" = Source{1}[Table],
  #"Expanded Table" = Table.ExpandTableColumn(#"Navigation 1", "Table", {"Name", "Table"}, {"Name.1", "Table.1"}),
  #"Expanded Table.1" = Table.ExpandTableColumn(#"Expanded Table", "Table.1", {"Name", "Table"}, {"Name.2", "Table"}),
  #"Expanded Table 1" = Table.ExpandTableColumn(#"Expanded Table.1", "Table", {"Name", "Table"}, {"Name.3", "Table.1"}),

  #"Expanded Table.1 1" =
    Table.ExpandTableColumn(
      #"Expanded Table 1",
      "Table.1",
      {
        "clientvisit_id",
        "urn:schemas-microsoft-com:xml-diffgram-v1",
        "urn:schemas-microsoft-com:xml-msdata",
        "_x0031__feeling_down_depressed_irrit_454513",
        "_x0032__little_interest_or_pleasure__454514",
        "_x0033__trouble_falling_asleep_stayi_454515",
        "_x0034__poor_appetitie_weight_loss_o_454516",
        "_x0035__feeling_tired_or_having_litt_454517",
        "_x0036__feeling_bad_about_yourself__454518",
        "_x0037__trouble_concentrating_on_thi_454519",
        "_x0038__moving_or_speaking_so_slowly_454520",
        "_x0039__thoughts_that_you_would_be_b_454521",
        "date_of_evaluation_454967",
        "_x0032__feeling_down_depressed_or_ho_454495",
        "_x0031__little_interest_in_doing_thi_454494",
        "_x0033__trouble_falling_asleep_or_st_454496",
        "_x0035__poor_appetite_or_overeating_454498",
        "_x0034__feeling_tired_or_having_litt_454497",
        "_x0036__feeling_bad_about_yourself_o_454499",
        "_x0037__trouble_concentrating_on_thi_454500",
        "_x0038__moving_or_speaking_so_slowly_454501",
        "_x0039__thoughts_that_you_would_be_b_454502"
      },
      {
        "clientvisit_id",
        "urn:schemas-microsoft-com:xml-diffgram-v1",
        "urn:schemas-microsoft-com:xml-msdata",
        "_x0031__feeling_down_depressed_irrit_454513",
        "_x0032__little_interest_or_pleasure__454514",
        "_x0033__trouble_falling_asleep_stayi_454515",
        "_x0034__poor_appetitie_weight_loss_o_454516",
        "_x0035__feeling_tired_or_having_litt_454517",
        "_x0036__feeling_bad_about_yourself__454518",
        "_x0037__trouble_concentrating_on_thi_454519",
        "_x0038__moving_or_speaking_so_slowly_454520",
        "_x0039__thoughts_that_you_would_be_b_454521",
        "date_of_evaluation_454967",
        "_x0032__feeling_down_depressed_or_ho_454495",
        "_x0031__little_interest_in_doing_thi_454494",
        "_x0033__trouble_falling_asleep_or_st_454496",
        "_x0035__poor_appetite_or_overeating_454498",
        "_x0034__feeling_tired_or_having_litt_454497",
        "_x0036__feeling_bad_about_yourself_o_454499",
        "_x0037__trouble_concentrating_on_thi_454500",
        "_x0038__moving_or_speaking_so_slowly_454501",
        "_x0039__thoughts_that_you_would_be_b_454502"
      }
    ),

  #"Removed columns" =
    Table.RemoveColumns(
      #"Expanded Table.1 1",
      {"Name", "Name.1", "Name.2", "Name.3", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}
    ),

  #"Filtered rows" = Table.SelectRows(#"Removed columns", each ([_x0031__feeling_down_depressed_irrit_454513] <> null)),

  #"Changed column type" =
    Table.TransformColumnTypes(
      #"Filtered rows",
      {
        {"_x0031__feeling_down_depressed_irrit_454513", Int64.Type},
        {"_x0032__little_interest_or_pleasure__454514", Int64.Type},
        {"_x0033__trouble_falling_asleep_stayi_454515", Int64.Type},
        {"_x0034__poor_appetitie_weight_loss_o_454516", Int64.Type},
        {"_x0035__feeling_tired_or_having_litt_454517", Int64.Type},
        {"_x0036__feeling_bad_about_yourself__454518", Int64.Type},
        {"_x0037__trouble_concentrating_on_thi_454519", Int64.Type},
        {"_x0038__moving_or_speaking_so_slowly_454520", Int64.Type},
        {"_x0039__thoughts_that_you_would_be_b_454521", Int64.Type},
        {"_x0032__feeling_down_depressed_or_ho_454495", Int64.Type},
        {"_x0031__little_interest_in_doing_thi_454494", Int64.Type},
        {"_x0033__trouble_falling_asleep_or_st_454496", Int64.Type},
        {"_x0035__poor_appetite_or_overeating_454498", Int64.Type},
        {"_x0034__feeling_tired_or_having_litt_454497", Int64.Type},
        {"_x0036__feeling_bad_about_yourself_o_454499", Int64.Type},
        {"_x0037__trouble_concentrating_on_thi_454500", Int64.Type},
        {"_x0038__moving_or_speaking_so_slowly_454501", Int64.Type},
        {"_x0039__thoughts_that_you_would_be_b_454502", Int64.Type},
        {"date_of_evaluation_454967", type date},
        {"clientvisit_id", type text}
      }
    ),

  #"Renamed columns" =
    Table.RenameColumns(
      #"Changed column type",
      {
        {"_x0031__feeling_down_depressed_irrit_454513", "down_depressed_hopeless"},
        {"_x0032__little_interest_or_pleasure__454514", "little_interest_or_pleasure"},
        {"_x0033__trouble_falling_asleep_stayi_454515", "trouble_sleeping"},
        {"_x0034__poor_appetitie_weight_loss_o_454516", "poor_appetite_weight_loss"},
        {"_x0035__feeling_tired_or_having_litt_454517", "low_energy"},
        {"_x0036__feeling_bad_about_yourself__454518", "bad_about_self"},
        {"_x0037__trouble_concentrating_on_thi_454519", "trouble_concentrating"},
        {"_x0038__moving_or_speaking_so_slowly_454520", "moving_speaking_slowly"},
        {"_x0039__thoughts_that_you_would_be_b_454521", "suicidal_thoughts"},
        {"date_of_evaluation_454967", "date_of_evaluation"},
        {"_x0032__feeling_down_depressed_or_ho_454495", "feeling_depressed"},
        {"_x0031__little_interest_in_doing_thi_454494", "little_interest"}
      }
    ),

  #"Merged services" =
    Table.NestedJoin(#"Renamed columns", {"clientvisit_id"}, raw_credible_services, {"service_id"}, "svc", JoinKind.LeftOuter),
  #"Expanded services" =
    Table.ExpandTableColumn(#"Merged services", "svc", {"client_id", "service_type", "service_date"}, {"client_id", "service_type", "service_date"}),

  #"Filtered PHQ9" = Table.SelectRows(#"Expanded services", each ([service_type] = "PHQ 9")),

  #"Merged clients" =
    Table.NestedJoin(#"Filtered PHQ9", {"client_id"}, raw_credible_clients, {"Client ID"}, "cl", JoinKind.LeftOuter),
  #"Expanded clients" =
    Table.ExpandTableColumn(#"Merged clients", "cl", {"Client Name", "DOB"}, {"Client Name", "DOB"}),

  // Drop the “duplicate” item set you were removing before
  #"Removed duplicate item columns" =
    Table.RemoveColumns(
      #"Expanded clients",
      {
        "feeling_depressed",
        "little_interest",
        "_x0033__trouble_falling_asleep_or_st_454496",
        "_x0035__poor_appetite_or_overeating_454498",
        "_x0034__feeling_tired_or_having_litt_454497",
        "_x0036__feeling_bad_about_yourself_o_454499",
        "_x0037__trouble_concentrating_on_thi_454500",
        "_x0038__moving_or_speaking_so_slowly_454501",
        "_x0039__thoughts_that_you_would_be_b_454502"
      }
    ),

  #"Filtered rows 2" = Table.SelectRows(#"Removed duplicate item columns", each ([client_id] <> "1484")),

  #"Inserted total_score" =
    Table.AddColumn(
      #"Filtered rows 2",
      "total_score",
      each
        List.Sum(
          List.RemoveNulls(
            {
              [down_depressed_hopeless],
              [little_interest_or_pleasure],
              [trouble_sleeping],
              [poor_appetite_weight_loss],
              [low_energy],
              [bad_about_self],
              [trouble_concentrating],
              [moving_speaking_slowly],
              [suicidal_thoughts]
            }
          )
        ),
      Int64.Type
    ),

  #"Inserted severity" =
    Table.AddColumn(
      #"Inserted total_score",
      "severity",
      each
        if [total_score] = null then null
        else if [total_score] >= 20 then "Severe Depression"
        else if [total_score] >= 15 then "Moderately Severe"
        else if [total_score] >= 10 then "Moderate"
        else if [total_score] >= 5 then "Mild"
        else "Minimal",
      type text
    ),

  #"Added evaluation_date" =
    Table.AddColumn(
      #"Inserted severity",
      "evaluation_date",
      each if [date_of_evaluation] <> null then [date_of_evaluation] else [service_date],
      type date
    ),

  // Keep your “active desoto clients” restriction
  #"Merged active" =
    Table.NestedJoin(#"Added evaluation_date", {"client_id"}, active_desoto_clients, {"client_id"}, "active", JoinKind.Inner),
  #"Removed active" = Table.RemoveColumns(#"Merged active", {"active"}),

  // Per-client sequencing + prev + deltas
  fnAddTrend = (t as table) as table =>
    let
      s = Table.Sort(t, {{"evaluation_date", Order.Ascending}, {"service_date", Order.Ascending}, {"clientvisit_id", Order.Ascending}}),
      withSeq = Table.AddIndexColumn(s, "phq9_seq", 1, 1, Int64.Type),

      prevBase = Table.SelectColumns(withSeq, {"phq9_seq", "total_score", "severity", "evaluation_date"}),
      prevRenamed =
        Table.RenameColumns(
          prevBase,
          {
            {"phq9_seq", "phq9_seq_prev"},
            {"total_score", "prev_total_score"},
            {"severity", "prev_severity"},
            {"evaluation_date", "prev_evaluation_date"}
          }
        ),
      prevShifted = Table.TransformColumns(prevRenamed, {{"phq9_seq_prev", each _ + 1, Int64.Type}}),

      joined = Table.NestedJoin(withSeq, {"phq9_seq"}, prevShifted, {"phq9_seq_prev"}, "prev", JoinKind.LeftOuter),
      expandedPrev =
        Table.ExpandTableColumn(
          joined,
          "prev",
          {"prev_total_score", "prev_severity", "prev_evaluation_date"},
          {"prev_total_score", "prev_severity", "prev_evaluation_date"}
        ),

      addChange =
        Table.AddColumn(
          expandedPrev,
          "score_change",
          each if [prev_total_score] = null then null else [total_score] - [prev_total_score],
          Int64.Type
        ),

      addDays =
        Table.AddColumn(
          addChange,
          "days_since_prev",
          each if [prev_evaluation_date] = null then null else Duration.Days([evaluation_date] - [prev_evaluation_date]),
          Int64.Type
        )
    in
      addDays,

  Grouped = Table.Group(#"Removed active", {"client_id"}, {{"Data", each fnAddTrend(_), type table}}),
  Combined = Table.Combine(Grouped[Data]),
  #"Sorted rows" = Table.Sort(Combined, {{"score_change", Order.Descending}}),
  #"Filtered rows 1" = Table.SelectRows(#"Sorted rows", each ([client_id] = "5689"))
in
  #"Filtered rows 1";
shared raw_credible_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_services", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared raw_credible_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_clients", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data];
[BindToDefaultDestination = true]
shared raw_credible_episodes = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_episodes", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared active_desoto_clients = let
  Source = raw_credible_episodes,
  #"Filtered rows" = Table.SelectRows(Source, each ([program] = "6401") and ([entry_status] = "ACTIVE" or [entry_status] = "Enrolled" or [entry_status] = "Pro Bono"))
in
  #"Filtered rows";
[BindToDefaultDestination = true]
shared #"desoto_followup_active_phq9 (2)" = let
  Source = Xml.Tables(Web.Contents("https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2rJczhl8Dgm6D4O25!lJ5CfQ__ &start_date=&end_date=&custom_param1=&custom_param2=&custom_param3=")),
  #"Navigation 1" = Source{1}[Table],
  #"Expanded Table" = Table.ExpandTableColumn(#"Navigation 1", "Table", {"Name", "Table"}, {"Name.1", "Table.1"}),
  #"Expanded Table.1" = Table.ExpandTableColumn(#"Expanded Table", "Table.1", {"Name", "Table"}, {"Name.2", "Table"}),
  #"Expanded Table 1" = Table.ExpandTableColumn(#"Expanded Table.1", "Table", {"Name", "Table"}, {"Name.3", "Table.1"}),
  #"Expanded Table.1 1" = Table.ExpandTableColumn(#"Expanded Table 1", "Table.1", {"clientvisit_id", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "_x0031__feeling_down_depressed_irrit_454513", "_x0032__little_interest_or_pleasure__454514", "_x0033__trouble_falling_asleep_stayi_454515", "_x0034__poor_appetitie_weight_loss_o_454516", "_x0035__feeling_tired_or_having_litt_454517", "_x0036__feeling_bad_about_yourself__454518", "_x0037__trouble_concentrating_on_thi_454519", "_x0038__moving_or_speaking_so_slowly_454520", "_x0039__thoughts_that_you_would_be_b_454521", "date_of_evaluation_454967", "_x0032__feeling_down_depressed_or_ho_454495", "_x0031__little_interest_in_doing_thi_454494", "_x0033__trouble_falling_asleep_or_st_454496", "_x0035__poor_appetite_or_overeating_454498", "_x0034__feeling_tired_or_having_litt_454497", "_x0036__feeling_bad_about_yourself_o_454499", "_x0037__trouble_concentrating_on_thi_454500", "_x0038__moving_or_speaking_so_slowly_454501", "_x0039__thoughts_that_you_would_be_b_454502"}, {"clientvisit_id", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "_x0031__feeling_down_depressed_irrit_454513", "_x0032__little_interest_or_pleasure__454514", "_x0033__trouble_falling_asleep_stayi_454515", "_x0034__poor_appetitie_weight_loss_o_454516", "_x0035__feeling_tired_or_having_litt_454517", "_x0036__feeling_bad_about_yourself__454518", "_x0037__trouble_concentrating_on_thi_454519", "_x0038__moving_or_speaking_so_slowly_454520", "_x0039__thoughts_that_you_would_be_b_454521", "date_of_evaluation_454967", "_x0032__feeling_down_depressed_or_ho_454495", "_x0031__little_interest_in_doing_thi_454494", "_x0033__trouble_falling_asleep_or_st_454496", "_x0035__poor_appetite_or_overeating_454498", "_x0034__feeling_tired_or_having_litt_454497", "_x0036__feeling_bad_about_yourself_o_454499", "_x0037__trouble_concentrating_on_thi_454500", "_x0038__moving_or_speaking_so_slowly_454501", "_x0039__thoughts_that_you_would_be_b_454502"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Table.1 1", {"Name", "Name.1", "Name.2", "Name.3", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}),
  #"Filtered rows" = Table.SelectRows(#"Removed columns", each ([_x0031__feeling_down_depressed_irrit_454513] <> null)),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows", {{"_x0031__feeling_down_depressed_irrit_454513", Int64.Type}, {"_x0032__little_interest_or_pleasure__454514", Int64.Type}, {"_x0033__trouble_falling_asleep_stayi_454515", Int64.Type}, {"_x0034__poor_appetitie_weight_loss_o_454516", Int64.Type}, {"_x0035__feeling_tired_or_having_litt_454517", Int64.Type}, {"_x0036__feeling_bad_about_yourself__454518", Int64.Type}, {"_x0037__trouble_concentrating_on_thi_454519", Int64.Type}, {"_x0038__moving_or_speaking_so_slowly_454520", Int64.Type}, {"_x0039__thoughts_that_you_would_be_b_454521", Int64.Type}, {"_x0032__feeling_down_depressed_or_ho_454495", Int64.Type}, {"_x0031__little_interest_in_doing_thi_454494", Int64.Type}, {"_x0033__trouble_falling_asleep_or_st_454496", Int64.Type}, {"_x0035__poor_appetite_or_overeating_454498", Int64.Type}, {"_x0034__feeling_tired_or_having_litt_454497", Int64.Type}, {"_x0036__feeling_bad_about_yourself_o_454499", Int64.Type}, {"_x0037__trouble_concentrating_on_thi_454500", Int64.Type}, {"_x0038__moving_or_speaking_so_slowly_454501", Int64.Type}, {"_x0039__thoughts_that_you_would_be_b_454502", Int64.Type}, {"date_of_evaluation_454967", type date}, {"clientvisit_id", type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"_x0031__feeling_down_depressed_irrit_454513", "down_depressed_hopeless"}, {"_x0032__little_interest_or_pleasure__454514", "little_interest_or_pleasure"}, {"_x0033__trouble_falling_asleep_stayi_454515", "trouble_sleeping"}, {"_x0034__poor_appetitie_weight_loss_o_454516", "poor_appetite_weight_loss"}, {"_x0035__feeling_tired_or_having_litt_454517", "low_energy"}, {"_x0036__feeling_bad_about_yourself__454518", "bad_about_self"}, {"_x0037__trouble_concentrating_on_thi_454519", "trouble_concentrating"}, {"_x0038__moving_or_speaking_so_slowly_454520", "moving_speaking_slowly"}, {"_x0039__thoughts_that_you_would_be_b_454521", "suicidal_thoughts"}, {"date_of_evaluation_454967", "date_of_evaluation"}, {"_x0032__feeling_down_depressed_or_ho_454495", "feeling_depressed"}, {"_x0031__little_interest_in_doing_thi_454494", "little_interest"}}),
  #"Merged queries" = Table.NestedJoin(#"Renamed columns", {"clientvisit_id"}, raw_credible_services, {"service_id"}, "raw_credible_services", JoinKind.LeftOuter),
  #"Expanded raw_credible_services 1" = Table.ExpandTableColumn(#"Merged queries", "raw_credible_services", {"client_id", "service_type", "service_date"}, {"client_id", "service_type", "service_date"}),
  #"Filtered rows 1" = Table.SelectRows(#"Expanded raw_credible_services 1", each ([service_type] = "PHQ 9")),
  #"Merged queries 1" = Table.NestedJoin(#"Filtered rows 1", {"client_id"}, raw_credible_clients, {"Client ID"}, "raw_credible_clients", JoinKind.LeftOuter),
  #"Expanded raw_credible_clients" = Table.ExpandTableColumn(#"Merged queries 1", "raw_credible_clients", {"Client Name", "DOB"}, {"Client Name", "DOB"}),
  #"Reordered columns" = Table.ReorderColumns(#"Expanded raw_credible_clients", {"clientvisit_id", "client_id", "service_type", "service_date", "Client Name", "DOB", "down_depressed_hopeless", "little_interest_or_pleasure", "trouble_sleeping", "poor_appetite_weight_loss", "low_energy", "bad_about_self", "trouble_concentrating", "moving_speaking_slowly", "suicidal_thoughts", "date_of_evaluation", "feeling_depressed", "little_interest", "_x0033__trouble_falling_asleep_or_st_454496", "_x0035__poor_appetite_or_overeating_454498", "_x0034__feeling_tired_or_having_litt_454497", "_x0036__feeling_bad_about_yourself_o_454499", "_x0037__trouble_concentrating_on_thi_454500", "_x0038__moving_or_speaking_so_slowly_454501", "_x0039__thoughts_that_you_would_be_b_454502"}),
  #"Sorted rows" = Table.Sort(#"Reordered columns", {{"feeling_depressed", Order.Descending}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Sorted rows", {"feeling_depressed", "little_interest", "_x0033__trouble_falling_asleep_or_st_454496", "_x0035__poor_appetite_or_overeating_454498", "_x0034__feeling_tired_or_having_litt_454497", "_x0036__feeling_bad_about_yourself_o_454499", "_x0037__trouble_concentrating_on_thi_454500", "_x0038__moving_or_speaking_so_slowly_454501", "_x0039__thoughts_that_you_would_be_b_454502"}),
  #"Sorted rows 1" = Table.Sort(#"Removed columns 1", {{"service_date", Order.Descending}}),
  #"Filtered rows 2" = Table.SelectRows(#"Sorted rows 1", each ([client_id] <> "1484")),
  #"Inserted sum" = Table.AddColumn(#"Filtered rows 2", "Sum", each List.Sum({[down_depressed_hopeless], [little_interest_or_pleasure], [trouble_sleeping], [poor_appetite_weight_loss], [low_energy], [bad_about_self], [trouble_concentrating], [moving_speaking_slowly], [suicidal_thoughts]}), type nullable Int64.Type),
  #"Reordered columns 1" = Table.ReorderColumns(#"Inserted sum", {"clientvisit_id", "client_id", "service_type", "service_date", "Client Name", "DOB", "down_depressed_hopeless", "little_interest_or_pleasure", "trouble_sleeping", "poor_appetite_weight_loss", "low_energy", "bad_about_self", "trouble_concentrating", "moving_speaking_slowly", "suicidal_thoughts", "Sum", "date_of_evaluation"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Reordered columns 1", {{"Sum", "total_score"}}),
  #"Merged queries 2" = Table.NestedJoin(#"Renamed columns 1", {"client_id"}, active_desoto_clients, {"client_id"}, "active_desoto_clients", JoinKind.Inner),
  #"Removed columns 2" = Table.RemoveColumns(#"Merged queries 2", {"active_desoto_clients"}),
  #"Inserted conditional column" = Table.AddColumn(#"Removed columns 2", "Custom", each if [total_score] >= 20 then "Severe Depression" else if [total_score] >= 15 then "Moderately Severe" else if [total_score] >= 10 then "Moderate" else if [total_score] >= 5 then "Mild" else if [total_score] <= 4 then "Minimal" else ""),
  #"Reordered columns 2" = Table.ReorderColumns(#"Inserted conditional column", {"clientvisit_id", "client_id", "service_type", "service_date", "Client Name", "DOB", "down_depressed_hopeless", "little_interest_or_pleasure", "trouble_sleeping", "poor_appetite_weight_loss", "low_energy", "bad_about_self", "trouble_concentrating", "moving_speaking_slowly", "suicidal_thoughts", "total_score", "Custom", "date_of_evaluation"}),
  #"Sorted rows 2" = Table.Sort(#"Reordered columns 2", {{"total_score", Order.Descending}})
in
  #"Sorted rows 2";
[BindToDefaultDestination = true]
shared desoto_phq_aggregates = let
  /**********************
    SOURCE (XML)
  **********************/
  Source =
    Xml.Tables(
      Web.Contents(
        "https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2rJczhl8Dgm6D4O25!lJ5CfQ__&start_date=&end_date=&custom_param1=&custom_param2=&custom_param3="
      )
    ),

  #"Navigation 1" = Source{1}[Table],
  #"Expanded Table" = Table.ExpandTableColumn(#"Navigation 1", "Table", {"Name", "Table"}, {"Name.1", "Table.1"}),
  #"Expanded Table.1" = Table.ExpandTableColumn(#"Expanded Table", "Table.1", {"Name", "Table"}, {"Name.2", "Table"}),
  #"Expanded Table 1" = Table.ExpandTableColumn(#"Expanded Table.1", "Table", {"Name", "Table"}, {"Name.3", "Table.1"}),

  #"Expanded Table.1 1" =
    Table.ExpandTableColumn(
      #"Expanded Table 1",
      "Table.1",
      {
        "clientvisit_id",
        "urn:schemas-microsoft-com:xml-diffgram-v1",
        "urn:schemas-microsoft-com:xml-msdata",
        "_x0031__feeling_down_depressed_irrit_454513",
        "_x0032__little_interest_or_pleasure__454514",
        "_x0033__trouble_falling_asleep_stayi_454515",
        "_x0034__poor_appetitie_weight_loss_o_454516",
        "_x0035__feeling_tired_or_having_litt_454517",
        "_x0036__feeling_bad_about_yourself__454518",
        "_x0037__trouble_concentrating_on_thi_454519",
        "_x0038__moving_or_speaking_so_slowly_454520",
        "_x0039__thoughts_that_you_would_be_b_454521",
        "date_of_evaluation_454967",
        "_x0032__feeling_down_depressed_or_ho_454495",
        "_x0031__little_interest_in_doing_thi_454494",
        "_x0033__trouble_falling_asleep_or_st_454496",
        "_x0035__poor_appetite_or_overeating_454498",
        "_x0034__feeling_tired_or_having_litt_454497",
        "_x0036__feeling_bad_about_yourself_o_454499",
        "_x0037__trouble_concentrating_on_thi_454500",
        "_x0038__moving_or_speaking_so_slowly_454501",
        "_x0039__thoughts_that_you_would_be_b_454502"
      },
      {
        "clientvisit_id",
        "urn:schemas-microsoft-com:xml-diffgram-v1",
        "urn:schemas-microsoft-com:xml-msdata",
        "_x0031__feeling_down_depressed_irrit_454513",
        "_x0032__little_interest_or_pleasure__454514",
        "_x0033__trouble_falling_asleep_stayi_454515",
        "_x0034__poor_appetitie_weight_loss_o_454516",
        "_x0035__feeling_tired_or_having_litt_454517",
        "_x0036__feeling_bad_about_yourself__454518",
        "_x0037__trouble_concentrating_on_thi_454519",
        "_x0038__moving_or_speaking_so_slowly_454520",
        "_x0039__thoughts_that_you_would_be_b_454521",
        "date_of_evaluation_454967",
        "_x0032__feeling_down_depressed_or_ho_454495",
        "_x0031__little_interest_in_doing_thi_454494",
        "_x0033__trouble_falling_asleep_or_st_454496",
        "_x0035__poor_appetite_or_overeating_454498",
        "_x0034__feeling_tired_or_having_litt_454497",
        "_x0036__feeling_bad_about_yourself_o_454499",
        "_x0037__trouble_concentrating_on_thi_454500",
        "_x0038__moving_or_speaking_so_slowly_454501",
        "_x0039__thoughts_that_you_would_be_b_454502"
      }
    ),

  #"Removed columns" =
    Table.RemoveColumns(
      #"Expanded Table.1 1",
      {"Name", "Name.1", "Name.2", "Name.3", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}
    ),

  // keep only PHQ rows where at least one key item exists
  #"Filtered rows" = Table.SelectRows(#"Removed columns", each ([_x0031__feeling_down_depressed_irrit_454513] <> null)),

  #"Changed column type" =
    Table.TransformColumnTypes(
      #"Filtered rows",
      {
        {"_x0031__feeling_down_depressed_irrit_454513", Int64.Type},
        {"_x0032__little_interest_or_pleasure__454514", Int64.Type},
        {"_x0033__trouble_falling_asleep_stayi_454515", Int64.Type},
        {"_x0034__poor_appetitie_weight_loss_o_454516", Int64.Type},
        {"_x0035__feeling_tired_or_having_litt_454517", Int64.Type},
        {"_x0036__feeling_bad_about_yourself__454518", Int64.Type},
        {"_x0037__trouble_concentrating_on_thi_454519", Int64.Type},
        {"_x0038__moving_or_speaking_so_slowly_454520", Int64.Type},
        {"_x0039__thoughts_that_you_would_be_b_454521", Int64.Type},
        {"_x0032__feeling_down_depressed_or_ho_454495", Int64.Type},
        {"_x0031__little_interest_in_doing_thi_454494", Int64.Type},
        {"_x0033__trouble_falling_asleep_or_st_454496", Int64.Type},
        {"_x0035__poor_appetite_or_overeating_454498", Int64.Type},
        {"_x0034__feeling_tired_or_having_litt_454497", Int64.Type},
        {"_x0036__feeling_bad_about_yourself_o_454499", Int64.Type},
        {"_x0037__trouble_concentrating_on_thi_454500", Int64.Type},
        {"_x0038__moving_or_speaking_so_slowly_454501", Int64.Type},
        {"_x0039__thoughts_that_you_would_be_b_454502", Int64.Type},
        {"date_of_evaluation_454967", type date},
        {"clientvisit_id", type text}
      }
    ),

  #"Renamed columns" =
    Table.RenameColumns(
      #"Changed column type",
      {
        {"_x0031__feeling_down_depressed_irrit_454513", "down_depressed_hopeless"},
        {"_x0032__little_interest_or_pleasure__454514", "little_interest_or_pleasure"},
        {"_x0033__trouble_falling_asleep_stayi_454515", "trouble_sleeping"},
        {"_x0034__poor_appetitie_weight_loss_o_454516", "poor_appetite_weight_loss"},
        {"_x0035__feeling_tired_or_having_litt_454517", "low_energy"},
        {"_x0036__feeling_bad_about_yourself__454518", "bad_about_self"},
        {"_x0037__trouble_concentrating_on_thi_454519", "trouble_concentrating"},
        {"_x0038__moving_or_speaking_so_slowly_454520", "moving_speaking_slowly"},
        {"_x0039__thoughts_that_you_would_be_b_454521", "suicidal_thoughts"},
        {"date_of_evaluation_454967", "date_of_evaluation"},
        {"_x0032__feeling_down_depressed_or_ho_454495", "feeling_depressed"},
        {"_x0031__little_interest_in_doing_thi_454494", "little_interest"}
      }
    ),

  /**********************
    JOINS (SERVICES + CLIENTS)
  **********************/
  ServicesTyped =
    Table.TransformColumnTypes(
      raw_credible_services,
      {{"service_id", type text}, {"client_id", type text}, {"service_type", type text}, {"service_date", type date}}
    ),

  ClientsTyped =
    Table.TransformColumnTypes(
      raw_credible_clients,
      {{"Client ID", type text}, {"Client Name", type text}, {"DOB", type date}}
    ),

  ActiveTyped =
    Table.TransformColumnTypes(
      active_desoto_clients,
      {{"client_id", type text}}
    ),

  #"Merged services" =
    Table.NestedJoin(#"Renamed columns", {"clientvisit_id"}, ServicesTyped, {"service_id"}, "svc", JoinKind.LeftOuter),
  #"Expanded services" =
    Table.ExpandTableColumn(#"Merged services", "svc", {"client_id", "service_type", "service_date"}, {"client_id", "service_type", "service_date"}),

  #"Filtered PHQ9" = Table.SelectRows(#"Expanded services", each ([service_type] = "PHQ 9")),

  #"Merged clients" =
    Table.NestedJoin(#"Filtered PHQ9", {"client_id"}, ClientsTyped, {"Client ID"}, "cl", JoinKind.LeftOuter),
  #"Expanded clients" =
    Table.ExpandTableColumn(#"Merged clients", "cl", {"Client Name", "DOB"}, {"Client Name", "DOB"}),

  // remove your duplicate item-set columns (keep the renamed set)
  #"Removed duplicate item columns" =
    Table.RemoveColumns(
      #"Expanded clients",
      {
        "feeling_depressed",
        "little_interest",
        "_x0033__trouble_falling_asleep_or_st_454496",
        "_x0035__poor_appetite_or_overeating_454498",
        "_x0034__feeling_tired_or_having_litt_454497",
        "_x0036__feeling_bad_about_yourself_o_454499",
        "_x0037__trouble_concentrating_on_thi_454500",
        "_x0038__moving_or_speaking_so_slowly_454501",
        "_x0039__thoughts_that_you_would_be_b_454502"
      }
    ),

  // your exclusion
  #"Filtered client 1484" = Table.SelectRows(#"Removed duplicate item columns", each ([client_id] <> "1484")),

  /**********************
    SCORE + SEVERITY + EVAL DATE
  **********************/
  #"Inserted total_score" =
    Table.AddColumn(
      #"Filtered client 1484",
      "total_score",
      each
        List.Sum(
          List.RemoveNulls(
            {
              [down_depressed_hopeless],
              [little_interest_or_pleasure],
              [trouble_sleeping],
              [poor_appetite_weight_loss],
              [low_energy],
              [bad_about_self],
              [trouble_concentrating],
              [moving_speaking_slowly],
              [suicidal_thoughts]
            }
          )
        ),
      Int64.Type
    ),

  #"Inserted severity" =
    Table.AddColumn(
      #"Inserted total_score",
      "severity",
      each
        if [total_score] = null then null
        else if [total_score] >= 20 then "Severe Depression"
        else if [total_score] >= 15 then "Moderately Severe"
        else if [total_score] >= 10 then "Moderate"
        else if [total_score] >= 5 then "Mild"
        else "Minimal",
      type text
    ),

  // prefer the form's eval date; fall back to service_date
  #"Added evaluation_date" =
    Table.AddColumn(
      #"Inserted severity",
      "evaluation_date",
      each if [date_of_evaluation] <> null then [date_of_evaluation] else [service_date],
      type date
    ),

  // your active-desoto filter
  #"Merged active" =
    Table.NestedJoin(#"Added evaluation_date", {"client_id"}, ActiveTyped, {"client_id"}, "active", JoinKind.Inner),
  #"Removed active" = Table.RemoveColumns(#"Merged active", {"active"}),

  /**********************
    PER-CLIENT SEQUENCE (1..N)
  **********************/
  fnAddSeq = (t as table) as table =>
    let
      s = Table.Sort(t, {{"evaluation_date", Order.Ascending}, {"service_date", Order.Ascending}, {"clientvisit_id", Order.Ascending}}),
      withSeq = Table.AddIndexColumn(s, "phq9_seq", 1, 1, Int64.Type)
    in
      withSeq,

  Grouped = Table.Group(#"Removed active", {"client_id"}, {{"Data", each fnAddSeq(_), type table}}),
  Combined = Table.Combine(Grouped[Data]),

  /**********************
    OPTION A: WIDE COLUMNS total_score_1..N (+ eval_date_1..N, severity_1..N)
  **********************/
  WideBase =
    Table.SelectColumns(
      Combined,
      {"client_id","Client Name","DOB","phq9_seq","evaluation_date","total_score","severity"}
    ),

  WithScoreCol = Table.AddColumn(WideBase, "score_col", each "total_score_" & Text.From([phq9_seq]), type text),
  WithDateCol  = Table.AddColumn(WithScoreCol, "date_col",  each "eval_date_"   & Text.From([phq9_seq]), type text),
  WithSevCol   = Table.AddColumn(WithDateCol,  "sev_col",   each "severity_"    & Text.From([phq9_seq]), type text),

  // Pivot TOTAL SCORE into total_score_1..N
  ScorePivotCols = List.Sort(List.Distinct(WithSevCol[score_col])),
  ScorePivot =
    Table.Pivot(
      Table.SelectColumns(WithSevCol, {"client_id","Client Name","DOB","score_col","total_score"}),
      ScorePivotCols,
      "score_col",
      "total_score",
      List.First
    ),

  // Pivot EVALUATION DATE into eval_date_1..N
  DatePivotCols = List.Sort(List.Distinct(WithSevCol[date_col])),
  DatePivot =
    Table.Pivot(
      Table.SelectColumns(WithSevCol, {"client_id","Client Name","DOB","date_col","evaluation_date"}),
      DatePivotCols,
      "date_col",
      "evaluation_date",
      List.First
    ),

  // Pivot SEVERITY into severity_1..N
  SevPivotCols = List.Sort(List.Distinct(WithSevCol[sev_col])),
  SevPivot =
    Table.Pivot(
      Table.SelectColumns(WithSevCol, {"client_id","Client Name","DOB","sev_col","severity"}),
      SevPivotCols,
      "sev_col",
      "severity",
      List.First
    ),

  // Join score + date + severity wide tables
  JoinDate = Table.NestedJoin(ScorePivot, {"client_id","Client Name","DOB"}, DatePivot, {"client_id","Client Name","DOB"}, "Dates", JoinKind.LeftOuter),
  ExpandDate = Table.ExpandTableColumn(JoinDate, "Dates", List.RemoveItems(Table.ColumnNames(DatePivot), {"client_id","Client Name","DOB"})),

  JoinSev = Table.NestedJoin(ExpandDate, {"client_id","Client Name","DOB"}, SevPivot, {"client_id","Client Name","DOB"}, "Sev", JoinKind.LeftOuter),
  Wide = Table.ExpandTableColumn(JoinSev, "Sev", List.RemoveItems(Table.ColumnNames(SevPivot), {"client_id","Client Name","DOB"})),
  #"Sorted rows" = Table.Sort(Wide, {{"total_score_6", Order.Descending}})
in
  #"Sorted rows";
