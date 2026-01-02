[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_cred_assess_assignment = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cred_assess_assignment", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"service_id"}, raw_credible_services, {"Service ID"}, "raw_credible_services", JoinKind.Inner),
  #"Expanded raw_credible_services" = Table.ExpandTableColumn(
    #"Merged queries",
    "raw_credible_services",
    {"Client ID", "timein"},
    {"Client ID", "timein"}
),
  #"Extracted text range" = Table.TransformColumns(#"Expanded raw_credible_services", {{"timein", each Text.Middle(_, 0, 10), type text}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Extracted text range", {{"timein", type date}}),
  #"Reordered columns" = Table.ReorderColumns(#"Changed column type", {"Client ID", "service_id", "primary_insurance", "assess_type", "lmhp_assigned", "auth_due", "auth_exp", "assess_reason", "completion_date", "issues_impacting", "font_colorpurpleadditional_not_594565", "notes", "impact_notes", "ref_outcome", "font_colornavyreferral_datefon_594566"}),
  #"Renamed columns" = Table.RenameColumns(#"Reordered columns", {{"font_colornavyreferral_datefon_594566", "referral_date"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"referral_date", type date}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type 1", {"timein"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"font_colorpurpleadditional_not_594565", "addtl_notes"}, {"issues_impacting", "reassess_status"}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Renamed columns 1", {{"completion_date", type date}})
in
  #"Changed column type 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "transform_enrolled_credible_clients_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared transform_enrolled_credible_clients = let
  // --- Source ---
  Source      = Lakehouse.Contents(null),
  Navigation  = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  LH          = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  Base        = LH{[Id = "transform_enrolled_credible_clients", ItemKind = "Table"]}[Data],

  // Only pull what you actually use later
  BaseColumns = Table.SelectColumns(
      Base,
      {"Client ID", "Client Name", "DOB", "Age", "Profile Auth End Date"},
      MissingField.Ignore
  ),

  //---------------------------------------------
  // 1️⃣ FIRST MERGE: EPISODES (API/EPISODE TABLE)
  //---------------------------------------------
  // Left join to raw_credible_episodes on Client ID
  J0_Episodes = Table.NestedJoin(
      BaseColumns,
      {"Client ID"},
      active_clients,
      {"Client ID"},
      "raw_credible_episodes",
      JoinKind.LeftOuter
  ),
  #"Expanded raw_credible_episodes" = Table.ExpandTableColumn(J0_Episodes, "raw_credible_episodes", {"Episode Status"}, {"Episode Status"}),

  //------------------------------------------------
  // 2️⃣ MERGE: raw_cred_assess_assignment (NO CHANGE)
  //------------------------------------------------
  J1 = Table.NestedJoin(
      #"Expanded raw_credible_episodes",
      {"Client ID"},
      raw_cred_assess_assignment,
      {"Client ID"},
      "assess_assignment",
      JoinKind.LeftOuter
  ),
  #"Expanded assess_assignment" = Table.ExpandTableColumn(J1, "assess_assignment", {"Client ID", "service_id", "primary_insurance", "assess_type", "lmhp_assigned", "auth_due", "auth_exp", "assess_reason", "completion_date", "issues_impacting", "font_colorpurpleadditional_not_594565", "notes", "impact_notes", "ref_outcome", "font_colornavyreferral_datefon_594566", "service_date"}, {"Client ID.1", "service_id", "primary_insurance", "assess_type", "lmhp_assigned", "auth_due", "auth_exp", "assess_reason", "completion_date", "issues_impacting", "font_colorpurpleadditional_not_594565", "notes", "impact_notes", "ref_outcome", "font_colornavyreferral_datefon_594566", "service_date"}),

  // Assignment label, null-safe
  WithAssignment = Table.AddColumn(
      #"Expanded assess_assignment",
      "Assignment",
      each if [service_id] = null then "NO ASSIGNMENT" else [lmhp_assigned],
      type text
  ),

  //------------------------------------------------
  // 3️⃣ MERGE: #"raw_credible_services (2)" (NO CHANGE)
  //------------------------------------------------
  J2 = Table.NestedJoin(
      WithAssignment,
      {"Client ID"},
      #"raw_credible_services (2)",
      {"Client ID"},
      "rcs2",
      JoinKind.LeftOuter
  ),

  ExpandJ2 = Table.ExpandTableColumn(J2, "rcs2", {"last_assess"}, {"last_assess"}),

  //---------------------------------------------
  // 4️⃣ TYPE NORMALIZATION + METRICS (NO CHANGE)
  //---------------------------------------------
  TypedDates = Table.TransformColumnTypes(
      ExpandJ2,
      {
        {"last_assess", type datetime},
        {"Profile Auth End Date", type datetime}
      }
  ),

  // Compute days_since_assess (null-safe)
  WithDaysSinceAssess = Table.AddColumn(
      TypedDates,
      "days_since_assess",
      each if [last_assess] = null
           then null
           else Duration.Days(DateTime.LocalNow() - [last_assess]),
      Int64.Type
  ),

  // Compute days_to_auth = now - end date (null-safe)
  WithDaysToAuth = Table.AddColumn(
      WithDaysSinceAssess,
      "days_to_auth",
      each if [Profile Auth End Date] = null
           then null
           else Duration.Days(DateTime.LocalNow() - [Profile Auth End Date]),
      Int64.Type
  ),

  // Assess status from days_since_assess (handle nulls explicitly)
  WithAssessStatus = Table.AddColumn(
      WithDaysToAuth,
      "assess_status",
      each
        if [days_since_assess] = null then "NO ASSESSMENT DATE"
        else if [days_since_assess] > 365 then "EXPIRED ASSESSMENT"
        else if [days_since_assess] > 334 then "<1 MONTH LEFT ASSESS"
        else "GOOD ASSESSMENT",
      type text
  ),

  // Auth status from days_to_auth (handle nulls explicitly)
  WithAuthStatus = Table.AddColumn(
      WithAssessStatus,
      "auth_status",
      each
        if [days_to_auth] = null then "NO AUTH END DATE"
        else if [days_to_auth] >= 0 then "EXPIRED AUTH"
        else if [days_to_auth] >= -30 then "<1 MONTH AUTH REMAINING"
        else "GOOD AUTH",
      type text
  ),

  //---------------------------------------------
  // 5️⃣ CLEANUP STATUS TEXT (NO CHANGE)
  //---------------------------------------------
  #"Replaced value"  = Table.ReplaceValue(WithAuthStatus, "GOOD ASSESSMENT", "", Replacer.ReplaceValue, {"assess_status"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "GOOD AUTH", "", Replacer.ReplaceValue, {"auth_status"}),

  #"Merged columns" = Table.CombineColumns(
      #"Replaced value 1",
      {"assess_status", "auth_status"},
      Combiner.CombineTextByDelimiter(" / ", QuoteStyle.None),
      "status"
  ),

  #"Replaced value 2" = Table.ReplaceValue(#"Merged columns", " / ", "", Replacer.ReplaceValue, {"status"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", " / EXPIRED AUTH", "EXPIRED AUTH", Replacer.ReplaceValue, {"status"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", " / <1 MONTH AUTH REMAINING", "<1 MONTH AUTH REMAINING", Replacer.ReplaceValue, {"status"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "<1 MONTH LEFT ASSESS / <1 MONTH AUTH REMAINING", "<1 MONTH FOR ASSESS AND AUTH", Replacer.ReplaceValue, {"status"}),

  // Drop rows with blank combined status
  #"Filtered rows" = Table.SelectRows(#"Replaced value 5", each ([status] <> "")),

  #"Replaced value 6" = Table.ReplaceValue(
      #"Filtered rows",
      "<1 MONTH LEFT ASSESS / ",
      "<1 MONTH LEFT ASSESS",
      Replacer.ReplaceValue,
      {"status"}
  )
in
  #"Replaced value 6";
shared raw_credible_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_services", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared #"raw_credible_services (2)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_services", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Name", "Name.1", "Name.2", "Name.3"}),
  #"Filtered rows" = Table.SelectRows(#"Removed columns", each ([Service Type] = "Assessment" or [Service Type] = "Assessment CPST" or [Service Type] = "FEP Assessment" or [Service Type] = "Non-Billable Assmt")),
  #"Extracted text range" = Table.TransformColumns(#"Filtered rows", {{"timein", each Text.Middle(_, 0, 10), type text}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Extracted text range", {{"timein", type date}}),
  #"Grouped rows" = Table.Group(#"Changed column type", {"Client ID"}, {{"last_assess", each List.Max([timein]), type nullable date}})
in
  #"Grouped rows";
shared transform_enrolled_credible_clients_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c46fe4a6-e106-4e77-993b-efc9ce22e551"]}[Data],
  TableNavigation = Navigation_2{[Id = "cred_client_assess_assignment", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared active_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_episodes", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([Episode Status] = "Enrolled") and ([Program ID] <> "IBHH" and [Program ID] <> "Supervision")),
  #"Removed other columns" = Table.SelectColumns(#"Filtered rows", {"Episode ID", "Client ID", "Episode Status", "Program ID"}),
  #"Removed duplicates" = Table.Distinct(#"Removed other columns", {"Episode ID"}),
  #"Filtered rows 1" = Table.SelectRows(#"Removed duplicates", each ([Program ID] <> "IBHH" and [Program ID] <> "Supervision"))
in
  #"Filtered rows 1";
shared last_assess_staging = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_services", ItemKind = "Table"]}[Data],
  #"FILTER ONLY ASSESSMENT SERVICES" = Table.SelectRows(#"Navigation 2", each ([service_type] = "Assessment" or [service_type] = "Assessment - Medical" or [service_type] = "Assessment CPST" or [service_type] = "FEP Assessment" or [service_type] = "Non-Billable Assmt")),
  #"GROUP BY CLIENT & SERVTYPE" = Table.Group(#"FILTER ONLY ASSESSMENT SERVICES", {"client_id", "service_type"}, {{"last_assess", each List.Max([service_date]), type nullable date}}),
  #"Grouped rows" = Table.Group(#"GROUP BY CLIENT & SERVTYPE", {"client_id"}, {{"last_assess", each List.Max([last_assess]), type nullable date}}),
  #"Sorted rows" = Table.Sort(#"Grouped rows", {{"last_assess", Order.Descending}})
in
  #"Sorted rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "gold_assigment_table_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared gold_assigment_table = let
  Source = Table.NestedJoin(active_clients, {"Client ID"}, last_assess_staging, {"client_id"}, "last_assess_staging", JoinKind.LeftOuter),
  #"Expanded last_assess_staging" = Table.ExpandTableColumn(Source, "last_assess_staging", {"last_assess"}, {"last_assess"}),
  #"Merged queries 1" = Table.NestedJoin(#"Expanded last_assess_staging", {"Client ID"}, max_assignment_date, {"Client ID"}, "max_assignment_date", JoinKind.LeftOuter),
  #"Expanded max_assignment_date" = Table.ExpandTableColumn(#"Merged queries 1", "max_assignment_date", {"service_id", "last_assignment"}, {"service_id", "last_assignment"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded max_assignment_date", {{"service_id", "last_assignment_id"}, {"last_assignment", "last_assignment_service_date"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns", null, "Never", Replacer.ReplaceValue, {"last_assignment_id"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Replaced value", {{"last_assess", "last_assessment_date"}}),
  #"Merged queries" = Table.NestedJoin(#"Renamed columns 1", {"Client ID"}, raw_cred_assess_assignment, {"Client ID"}, "raw_cred_assess_assignment", JoinKind.LeftOuter),
  #"Expanded raw_cred_assess_assignment" = Table.ExpandTableColumn(#"Merged queries", "raw_cred_assess_assignment", {"service_id", "primary_insurance", "assess_type", "lmhp_assigned", "auth_due", "auth_exp", "assess_reason", "completion_date", "reassess_status", "addtl_notes", "notes", "impact_notes", "ref_outcome", "service_date", "referral_date"}, {"service_id", "primary_insurance", "assess_type", "lmhp_assigned", "auth_due", "auth_exp", "assess_reason", "completion_date", "reassess_status", "addtl_notes", "notes", "impact_notes", "ref_outcome", "service_date", "referral_date"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded raw_cred_assess_assignment", {{"last_assessment_date", type datetime}}),
  #"Added custom" = Table.AddColumn(#"Changed column type", "days_since_assess", each Duration.Days(DateTime.LocalNow() - [last_assessment_date])),
  Custom = Table.AddColumn(
    #"Added custom",
    "AssessmentAgeBucket",
    each if Duration.Days(DateTime.LocalNow() - [last_assessment_date]) <= 30 then "0–30 days"
         else if Duration.Days(DateTime.LocalNow() - [last_assessment_date]) <= 90 then "31–90 days"
         else if Duration.Days(DateTime.LocalNow() - [last_assessment_date]) <= 180 then "91–180 days"
         else if Duration.Days(DateTime.LocalNow() - [last_assessment_date]) <= 365 then "181–365 days"
         else "Over 1 year"
),
  #"Changed column type 1" = Table.TransformColumnTypes(Custom, {{"auth_exp", type date}, {"auth_due", type datetime}}),
  #"Replaced value 1" = Table.ReplaceValue(#"Changed column type 1", null, "Never", Replacer.ReplaceValue, {"service_id"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", null, "NO ASSIGMENT SERVICE ON FILE", Replacer.ReplaceValue, {"primary_insurance"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", null, "NO ASSIGMENT SERVICE ON FILE", Replacer.ReplaceValue, {"assess_type"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", null, "NO ASSIGMENT SERVICE ON FILE", Replacer.ReplaceValue, {"lmhp_assigned"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", null, "NO ASSIGMENT SERVICE ON FILE", Replacer.ReplaceValue, {"assess_reason"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", null, "NO ASSIGMENT SERVICE ON FILE", Replacer.ReplaceValue, {"reassess_status"}),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(#"Replaced value 6", "days_til auth_due", each Duration.Days(DateTime.LocalNow() - [auth_due])), {{"days_til auth_due", Int64.Type}}),
  #"Custom 2" = let
    Source = #"Added custom 1",
    AddStatus = Table.AddColumn(
        Source,
        "AssessmentStatus",
        each 
            let
                daysAgo = Duration.Days(DateTime.LocalNow() - [last_assessment_date])
            in
                if daysAgo >= 330 and daysAgo < 365 then "Due within 30 days"
                else if daysAgo >= 365 then "Assessment due"
                else null
    ),
    AddDaysUntilDue = Table.AddColumn(
        AddStatus,
        "DaysUntilDue",
        each 365 - Duration.Days(DateTime.LocalNow() - [last_assessment_date])
    )
in
    AddDaysUntilDue,
  #"Custom 3" = Table.AddColumn(
    #"Custom 2",
    "AssessmentDueBucket",
    each 
        let
            daysAgo = Duration.Days(DateTime.LocalNow() - [last_assessment_date])
        in
            if daysAgo < 180 then "Not due (0–179 days)"
            else if daysAgo >= 180 and daysAgo < 270 then "Approaching due (180–269 days)"
            else if daysAgo >= 270 and daysAgo < 330 then "Due within 90 days (270–329 days)"
            else if daysAgo >= 330 and daysAgo < 365 then "Due within 30 days (330–364 days)"
            else if daysAgo >= 365 and daysAgo < 395 then "Assessment due (0–30 days overdue)"
            else if daysAgo >= 395 and daysAgo < 425 then "Assessment due (31–60 days overdue)"
            else if daysAgo >= 425 and daysAgo < 455 then "Assessment due (61–90 days overdue)"
            else if daysAgo >= 455 and daysAgo < 545 then "Assessment due (91–180 days overdue)"
            else if daysAgo >= 545 then "Assessment due (Over 180 days overdue)"
            else null
),
  #"Custom 4" = Table.AddColumn(
    #"Custom 2",
    "AssessmentYearPct",
    each 
        let
            daysAgo = Duration.Days(DateTime.LocalNow() - [last_assessment_date]),
            pct = (daysAgo / 365) * 100
        in
            if daysAgo < 0 then null else Number.Round(pct, 1)
),
  #"Replaced value 7" = Table.ReplaceValue(#"Custom 4", null, #datetime(1999, 1, 1, 0, 0, 0), Replacer.ReplaceValue, {"auth_due"}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Replaced value 7", {{"AssessmentYearPct", type number}}),
  #"Custom 1" = Table.AddColumn(
   #"Changed column type 3",
    "AuthDueBucket",
    each 
        let
            // Normalize auth_due to a Date (handles DateTime or text)
            authRaw = [auth_due],
            authDate = 
                if authRaw = null then null
                else try Date.From(authRaw) otherwise Date.FromText(Text.From(authRaw)),

            // Flag the placeholder (1/1/1999) or missing
            isPlaceholder = (authDate = #date(1999, 1, 1)) or (authDate = null),

            // Only compute days when not placeholder
            days = 
                if isPlaceholder then null 
                else Duration.Days(DateTime.LocalNow() - DateTime.From(authRaw))
        in
            if isPlaceholder then "No assignment service on file"
            else if days <= 30 then "0–30 days"
            else if days <= 90 then "31–90 days"
            else if days <= 180 then "91–180 days"
            else if days <= 365 then "181–365 days"
            else "Over 1 year"
),
  #"Divided column" = Table.TransformColumns(#"Custom 1", {{"AssessmentYearPct", each _ / 100, type number}}),
  #"Changed column type 4" = Table.TransformColumnTypes(#"Divided column", {{"AssessmentYearPct", Percentage.Type}, {"AssessmentAgeBucket", type text}, {"days_since_assess", Int64.Type}, {"AuthDueBucket", type text}, {"DaysUntilDue", Int64.Type}, {"AssessmentStatus", type text}}),
  #"Grouped rows" = Table.Group(#"Changed column type 4", {"Client ID", "Program ID", "AssessmentAgeBucket", "AuthDueBucket", "AssessmentYearPct", "AssessmentStatus"}, {{"Count", each List.Max([auth_due]), type nullable datetime}, {"last assess", each List.Max([last_assessment_date]), type nullable datetime}, {"last assign", each List.Max([last_assignment_service_date]), type nullable date}, {"completion date", each List.Max([completion_date]), type nullable date}, {"lmhp assigned", each List.Max([lmhp_assigned]), type nullable text}}),
  #"Replaced value 9" = Table.ReplaceValue(#"Grouped rows", "NO ASSIGMENT SERVICE ON FILE", "No assignment service on file", Replacer.ReplaceValue, {"lmhp assigned"}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Replaced value 9", {{"AuthDueBucket", type text}}),
  #"Added prefix" = Table.TransformColumns(#"Changed column type 2", {{"AuthDueBucket", each "Due in " & _, type text}}),
  #"Replaced value 8" = Table.ReplaceValue(#"Added prefix", "Due in No assignment service on file", "No assignment service on file", Replacer.ReplaceValue, {"AuthDueBucket"}),
  #"Merged queries 2" = Table.NestedJoin(#"Replaced value 8", {"Client ID"}, raw_credible_clients, {"Client ID"}, "raw_credible_clients", JoinKind.LeftOuter),
  #"Expanded raw_credible_clients" = Table.ExpandTableColumn(#"Merged queries 2", "raw_credible_clients", {"Client Name"}, {"Client Name"}),
  #"Reordered columns" = Table.ReorderColumns(#"Expanded raw_credible_clients", {"Client ID", "Client Name", "Program ID", "AssessmentAgeBucket", "AuthDueBucket", "AssessmentYearPct", "AssessmentStatus", "Count", "last assess", "last assign", "completion date", "lmhp assigned"})


in
  #"Reordered columns";
shared max_assignment_date = let
  Source = raw_cred_assess_assignment,
  #"Grouped rows" = Table.Group(Source, {"Client ID", "service_id"}, {{"last_assignment", each List.Max([service_date]), type nullable date}})
in
  #"Grouped rows";
shared #"active_clients (2)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_episodes", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([Episode Status] = "Enrolled") and ([Program ID] <> "IBHH" and [Program ID] <> "Supervision")),
  #"Removed other columns" = Table.SelectColumns(#"Filtered rows", {"Episode ID", "Client ID", "Episode Status", "Program ID"}),
  #"Removed duplicates" = Table.Distinct(#"Removed other columns", {"Episode ID"}),
  #"Filtered rows 1" = Table.SelectRows(#"Removed duplicates", each ([Program ID] <> "IBHH" and [Program ID] <> "Supervision"))
in
  #"Filtered rows 1";
shared gold_assigment_table_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "staging_assigment_table", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared raw_credible_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_clients", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
