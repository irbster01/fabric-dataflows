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

    #"Filtered hidden files" = Table.SelectRows(#"Navigation 5", each [Attributes]?[Hidden]? <> true),
    #"Filtered files only" = Table.SelectRows(#"Filtered hidden files", each Value.Is([Content], type binary)),
    #"Invoke custom function" = Table.AddColumn(#"Filtered files only", "Transform file", each #"Transform file"([Content])),
    #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
    #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
    #"Promoted headers" = Table.PromoteHeaders(#"Expanded table column", [PromoteAllScalars = true]),

    #"Changed column type 1" = SafeTypes(
        #"Promoted headers",
        {
            {"RSch", type text},
            {"ESch", Int64.Type},
            {"SIDNO", type text},
            {"GD", type text},
            {"Eth", type text},
            {"504", Int64.Type},
            {"Ref#", Int64.Type},
            {"Ref Date", type date},
            {"Ref Time", Int64.Type},
            {"Event ID", type text},
            {"Loc", Int64.Type},
            {"ICD", Int64.Type},
            {"ICD2", Int64.Type},
            {"ICD3", Int64.Type},
            {"ICD4", Int64.Type},
            {"Refer ID", Int64.Type},
            {"T Eth", Int64.Type},
            {"TCD", Int64.Type},
            {"TCD2", Int64.Type},
            {"TCD3", Int64.Type},
            {"ACD", Int64.Type},
            {"Adm Date", type date},
            {"ACD2", Int64.Type},
            {"Adm2 Date", type date},
            {"ACD3", Int64.Type},
            {"Adm3 Date", type date},
            {"ACD4", Int64.Type},
            {"Adm4 Date", type date},
            {"ACD5", Int64.Type},
            {"Adm5 Date", type date},
            {"On Date", type date},
            {"CNF Date", type date},
            {"CNF Time", Int64.Type},
            {"S Days", Int64.Type},
            {"ISS Start", type date},
            {"ISS Return", type date},
            {"OSS Start", type date},
            {"OSS Return", type date},
            {"Exp Start", type date},
            {"Exp Return", type date},
            {"Bus Start", type date},
            {"Bus End", type date},
            {"Mon Det Start", type time},
            {"Mon Det End", type time},
            {"Thu Det Start", type time},
            {"Thu Det End", type time},
            {"Fri Det Start", type time},
            {"Fri Det End", type time},
            {"Det Start", type date},
            {"Det Return", type date},
            {"Man Date", type date},
            {"Hearing Results", Int64.Type},
            {"SIS Incident", type text},
            {"State ID", type text}
        }
    ),

    #"Renamed columns 1" = SafeRename(#"Changed column type 1", {{"SIS Incident", "sis_incident"}}),

    #"Renamed columns" = SafeRename(
        #"Renamed columns 1",
        {
            {"State ID", "sasid"},
            {"Disciplinarian Signed Form", "disc_signed_form"},
            {"Other Agencies", "other_agencies"},
            {"SIS Description", "sis_desc"},
            {"Internal Remarks", "int_remarks"},
            {"Remarks", "remarks"},
            {"Gangs", "gangs"},
            {"Alcohol Use", "alcohol"},
            {"Drugs", "drugs"},
            {"Hate/Bias", "hate_bias"}
        }
    ),

    #"Changed column type" = SafeTypes(
        #"Renamed columns",
        {
            {"hate_bias", type text},
            {"drugs", type text},
            {"alcohol", type text},
            {"gangs", type text},
            {"remarks", type text},
            {"int_remarks", type text},
            {"sis_incident", type text},
            {"sis_desc", type text},
            {"disc_signed_form", type text},
            {"other_agencies", type text}
        }
    ),

    #"Renamed columns 2" = SafeRename(
        #"Changed column type",
        {
            {"Special Codes", "special_codes"},
            {"Hearing Results", "hearing_results"},
            {"Hearing Date", "hearing_date"}
        }
    ),

    #"Changed column type 2" = SafeTypes(#"Renamed columns 2", {{"hearing_date", type date}}),

    #"Removed errors" =
        if List.Contains(Table.ColumnNames(#"Changed column type 2"), "hearing_date")
        then Table.RemoveRowsWithErrors(#"Changed column type 2", {"hearing_date"})
        else #"Changed column type 2",

    #"Renamed columns 3" = SafeRename(#"Removed errors", {{"Man Date", "man_date"}}),
    #"Removed columns" = SafeRemove(#"Renamed columns 3", {"Detention Room 2", "Detention Room", "Det Return", "Det Start"}),
    #"Renamed columns 4" = SafeRename(#"Removed columns", {{"Det Days", "det_days"}}),

    #"Removed columns 1" = SafeRemove(
        #"Renamed columns 4",
        {
            "Oth Det End", "Oth Det Start", "Sat Det End", "Sat Det Start",
            "Fri Det End", "Fri Det Start", "Thu Det End", "Thu Det Start",
            "Wed Det End", "Wed Det Start", "Tue Det End", "Tue Det Start",
            "Mon Det End", "Mon Det Start", "Bus End", "Bus Start",
            "Exp Return", "TR", "RT"
        }
    ),

    #"Renamed columns 5" = SafeRename(
        #"Removed columns 1",
        {
            {"Exp", "expulsion"},
            {"OSS Return", "oss_return"},
            {"OSS Start", "oss_start"}
        }
    ),

    #"Removed columns 2" = SafeRemove(#"Renamed columns 5", {"ISS Return", "ISS Start", "CNF Time", "CNF Date", "CNF"}),
    #"Renamed columns 6" = SafeRename(#"Removed columns 2", {{"S Days", "susp_days"}, {"On Date", "on_date"}}),

    #"Sorted rows" =
        if List.Contains(Table.ColumnNames(#"Renamed columns 6"), "on_date")
        then Table.Sort(#"Renamed columns 6", {{"on_date", Order.Descending}})
        else #"Renamed columns 6",

    #"Renamed columns 7" = SafeRename(#"Sorted rows", {{"Administrative Comments", "admin_comments"}}),

    #"Removed columns 3" = SafeRemove(
        #"Renamed columns 7",
        {
            "Adm7 Date", "Admin Code7 Description", "ACD7",
            "Adm6 Date", "Admin Code6 Description", "ACD6",
            "Adm5 Date", "Admin Code5 Description", "ACD5",
            "Adm4 Date", "Admin Code4 Description", "ACD4",
            "Adm3 Date", "Admin Code3 Description", "ACD3",
            "Adm2 Date", "Admin Code2 Description", "ACD2",
            "Adm Date"
        }
    ),

    #"Renamed columns 8" = SafeRename(#"Removed columns 3", {{"Admin Code Description", "admin_code_desc"}}),
    #"Removed columns 4" = SafeRemove(#"Renamed columns 8", {"ACD"}),
    #"Changed column type 3" = SafeTypes(#"Removed columns 4", {{"admin_code_desc", type text}}),

    #"Removed columns 5" = SafeRemove(
        #"Changed column type 3",
        {"Teacher Code4 Description", "TCD4", "Teacher Code3 Description", "TCD3", "Teacher Code2 Description", "TCD2"}
    ),

    #"Renamed columns 9" = SafeRename(#"Removed columns 5", {{"Teacher Code Description", "teacher_code_desc"}}),

    #"Removed columns 6" = SafeRemove(
        #"Renamed columns 9",
        {
            "TCD", "T Gen", "T Eth", "Refer Name", "Refer ID", "Bus",
            "Incident CD4 Description", "ICD4", "Incident CD3 Description", "ICD3",
            "Incident CD2 Description", "ICD2"
        }
    ),

    #"Renamed columns 10" = SafeRename(#"Removed columns 6", {{"Incident CD Description", "incident_cd_desc"}}),
    #"Removed columns 7" = SafeRemove(#"Renamed columns 10", {"ICD"}),
    #"Renamed columns 11" = SafeRename(#"Removed columns 7", {{"Location Description", "location"}}),
    #"Removed columns 8" = SafeRemove(#"Renamed columns 11", {"Loc"}),
    #"Renamed columns 12" = SafeRename(#"Removed columns 8", {{"Event ID", "event_id"}}),
    #"Removed columns 9" = SafeRemove(#"Renamed columns 12", {"RSch"}),
    #"Renamed columns 13" = SafeRename(#"Removed columns 9", {{"Student Name", "student_name"}}),
    #"Removed columns 10" = SafeRemove(#"Renamed columns 13", {"ESch"}),

    #"Renamed columns 14" = SafeRename(
        #"Removed columns 10",
        {
            {"ESch Name", "school_name"},
            {"SIDNO", "sidno"},
            {"GD", "grade"},
            {"Sex", "gender"}
        }
    ),

    #"Removed columns 11" = SafeRemove(#"Renamed columns 14", {"Eth"}),
    #"Renamed columns 15" = SafeRename(#"Removed columns 11", {{"Ethnic Description", "race"}, {"IEP", "iep"}, {"Sped", "sped"}}),
    #"Removed columns 12" = SafeRemove(#"Renamed columns 15", {"Lunch", "504"}),
    #"Renamed columns 16" = SafeRename(#"Removed columns 12", {{"Ref#", "ref_num"}, {"Ref Date", "ref_date"}}),
    #"Removed columns 13" = SafeRemove(#"Renamed columns 16", {"Ref Time"}),

    #"Changed column type 4" = SafeTypes(
        #"Removed columns 13",
        {
            {"sped", type text},
            {"iep", type text},
            {"race", type text},
            {"gender", type text},
            {"school_name", type text},
            {"student_name", type text},
            {"admin_comments", type text},
            {"admin_code_desc", type text},
            {"teacher_code_desc", type text},
            {"incident_cd_desc", type text},
            {"location", type text},
            {"special_codes", type text}
        }
    ),

    #"Removed duplicates" =
        if List.Contains(Table.ColumnNames(#"Changed column type 4"), "event_id")
        then Table.Distinct(#"Changed column type 4", {"event_id"})
        else #"Changed column type 4",
  #"Changed column type 5" = Table.TransformColumnTypes(#"Removed duplicates", {{"det_days", type text}, {"expulsion", type text}})
in
    #"Changed column type 5";
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
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 112, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 112, QuoteStyle = QuoteStyle.None])
in
  Source;
shared raw_caddo_referrals_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_caddo_referrals", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
