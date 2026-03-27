[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_caddo_referrals_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared raw_caddo_referrals = let
    Source = Lakehouse.Contents(null),
    #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
    #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
    #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
    #"Navigation 4" = #"Navigation 3"{[Name = "caddo_jcampus"]}[Content],
    #"Navigation 5" = #"Navigation 4"{[Name = "referrals"]}[Content],

    SafeRename = (tbl as table, renames as list) as table =>
        let
            cols = Table.ColumnNames(tbl),
            validRenames = List.Select(renames, each List.Contains(cols, _{0}))
        in
            Table.RenameColumns(tbl, validRenames, MissingField.Ignore),

    SafeRemove = (tbl as table, colsToRemove as list) as table =>
        let
            cols = Table.ColumnNames(tbl),
            validRemovals = List.Intersect({cols, colsToRemove})
        in
            Table.RemoveColumns(tbl, validRemovals),

    SafeTypes = (tbl as table, typePairs as list) as table =>
        let
            cols = Table.ColumnNames(tbl),
            validTypes = List.Select(typePairs, each List.Contains(cols, _{0}))
        in
            Table.TransformColumnTypes(tbl, validTypes),

    SafeReplaceErrors = (tbl as table, replacements as list) as table =>
        let
            cols = Table.ColumnNames(tbl),
            validReplacements = List.Select(replacements, each List.Contains(cols, _{0}))
        in
            if List.IsEmpty(validReplacements) then tbl
            else Table.ReplaceErrorValues(tbl, validReplacements),

    #"Filtered hidden files" = Table.SelectRows(#"Navigation 5", each [Attributes]?[Hidden]? <> true),
    #"Filtered files only" = Table.SelectRows(#"Filtered hidden files", each Value.Is([Content], type binary)),
    #"Invoke custom function" = Table.AddColumn(#"Filtered files only", "Transform file", each #"Transform file"([Content])),
    #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
    #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
    #"Promoted headers" = Table.PromoteHeaders(#"Expanded table column", [PromoteAllScalars = true]),

    // Step 1: Remove columns we don't need
    #"Removed columns" = SafeRemove(#"Promoted headers", {
        "RSch", "ESch", "Eth", "Ref Time", "Loc", "ICD", "ICD2", "ICD3", "ICD4",
        "Refer ID", "T Eth", "T Gen", "TCD", "TCD2", "TCD3", "TCD4",
        "ACD", "ACD2", "ACD3", "ACD4", "ACD5", "ACD6", "ACD7",
        "Adm Date", "Adm2 Date", "Adm3 Date", "Adm4 Date", "Adm5 Date", "Adm6 Date", "Adm7 Date",
        "Admin Code2 Description", "Admin Code3 Description", "Admin Code4 Description",
        "Admin Code5 Description", "Admin Code6 Description", "Admin Code7 Description",
        "Teacher Code2 Description", "Teacher Code3 Description", "Teacher Code4 Description",
        "Incident CD2 Description", "Incident CD3 Description", "Incident CD4 Description",
        "Refer Name", "Bus", "CNF", "CNF Date", "CNF Time",
        "ISS Start", "ISS Return",
        "Mon Det Start", "Mon Det End", "Tue Det Start", "Tue Det End",
        "Wed Det Start", "Wed Det End", "Thu Det Start", "Thu Det End",
        "Fri Det Start", "Fri Det End", "Sat Det Start", "Sat Det End",
        "Oth Det Start", "Oth Det End",
        "Detention Room", "Detention Room 2", "Det Start", "Det Return",
        "Bus Start", "Bus End", "Exp Start", "Exp Return",
        "TR", "RT", "Lunch", "504", "Ethnic Description"
    }),

    // Step 2: Rename to clean names
    #"Renamed columns" = SafeRename(#"Removed columns", {
        {"Student Name", "student_name"},
        {"ESch Name", "school_name"},
        {"SIDNO", "sidno"},
        {"State ID", "sasid"},
        {"GD", "grade"},
        {"Sex", "gender"},
        {"IEP", "iep"},
        {"Sped", "sped"},
        {"Ref#", "ref_num"},
        {"Ref Date", "ref_date"},
        {"Event ID", "event_id"},
        {"Location Description", "location"},
        {"Incident CD Description", "incident_cd_desc"},
        {"Teacher Code Description", "teacher_code_desc"},
        {"Admin Code Description", "admin_code_desc"},
        {"Administrative Comments", "admin_comments"},
        {"On Date", "on_date"},
        {"S Days", "susp_days"},
        {"OSS Start", "oss_start"},
        {"OSS Return", "oss_return"},
        {"Exp", "expulsion"},
        {"Det Days", "det_days"},
        {"Man Date", "man_date"},
        {"Hearing Date", "hearing_date"},
        {"Hearing Results", "hearing_results"},
        {"Special Codes", "special_codes"},
        {"SIS Incident", "sis_incident"},
        {"SIS Description", "sis_desc"},
        {"Disciplinarian Signed Form", "disc_signed_form"},
        {"Other Agencies", "other_agencies"},
        {"Internal Remarks", "int_remarks"},
        {"Remarks", "remarks"},
        {"Gangs", "gangs"},
        {"Alcohol Use", "alcohol"},
        {"Drugs", "drugs"},
        {"Hate/Bias", "hate_bias"}
    }),

    // Step 3: Set types
    #"Changed types" = SafeTypes(#"Renamed columns", {
        {"sidno", type text},
        {"sasid", type text},
        {"grade", type text},
        {"gender", type text},
        {"school_name", type text},
        {"student_name", type text},
        {"ref_num", Int64.Type},
        {"ref_date", type date},
        {"event_id", type text},
        {"location", type text},
        {"incident_cd_desc", type text},
        {"teacher_code_desc", type text},
        {"admin_code_desc", type text},
        {"admin_comments", type text},
        {"on_date", type date},
        {"susp_days", Int64.Type},
        {"oss_start", type date},
        {"oss_return", type date},
        {"expulsion", type text},
        {"det_days", type text},
        {"man_date", type date},
        {"hearing_date", type date},
        {"hearing_results", type text},
        {"special_codes", type text},
        {"sis_incident", type text},
        {"sis_desc", type text},
        {"disc_signed_form", type text},
        {"other_agencies", type text},
        {"int_remarks", type text},
        {"remarks", type text},
        {"gangs", type text},
        {"alcohol", type text},
        {"drugs", type text},
        {"hate_bias", type text},
        {"iep", type text},
        {"sped", type text}
    }),

    // Step 4: Replace date parse errors with null
    #"Replaced errors" = SafeReplaceErrors(#"Changed types", {
        {"ref_date", null}, {"on_date", null}, {"oss_start", null}, {"oss_return", null},
        {"man_date", null}, {"hearing_date", null}
    }),

    // Step 5: Sort and deduplicate
    #"Sorted rows" =
        if List.Contains(Table.ColumnNames(#"Replaced errors"), "on_date")
        then Table.Sort(#"Replaced errors", {{"on_date", Order.Descending}})
        else #"Replaced errors",

    #"Removed duplicates" =
        if List.Contains(Table.ColumnNames(#"Sorted rows"), "event_id")
        then Table.Distinct(#"Sorted rows", {"event_id"})
        else #"Sorted rows"
in
    #"Removed duplicates";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "caddo_jcampus"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "referrals"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Csv.Document(Parameter, [Delimiter = ",", QuoteStyle = QuoteStyle.Csv])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", QuoteStyle = QuoteStyle.Csv])
in
  Source;
shared raw_caddo_referrals_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_caddo_referrals", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
