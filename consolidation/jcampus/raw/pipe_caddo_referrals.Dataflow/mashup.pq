[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "caddo_referrals_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "student_name", DestinationColumnName = "student_name"], [SourceColumnName = "school_name", DestinationColumnName = "school_name"], [SourceColumnName = "gender", DestinationColumnName = "gender"], [SourceColumnName = "iep", DestinationColumnName = "iep"], [SourceColumnName = "sped", DestinationColumnName = "sped"], [SourceColumnName = "ref_num", DestinationColumnName = "ref_num"], [SourceColumnName = "ref_date", DestinationColumnName = "ref_date"], [SourceColumnName = "teacher_code_desc", DestinationColumnName = "teacher_description"], [SourceColumnName = "admin_code_desc", DestinationColumnName = "admin_description"], [SourceColumnName = "oss_start", DestinationColumnName = "oss_start"], [SourceColumnName = "oss_return", DestinationColumnName = "oss_return"], [SourceColumnName = "hearing_results", DestinationColumnName = "hearing_results"], [SourceColumnName = "special_codes", DestinationColumnName = "special_codes"], [SourceColumnName = "hate_bias", DestinationColumnName = "hate_bias"], [SourceColumnName = "drugs", DestinationColumnName = "drugs"], [SourceColumnName = "alcohol", DestinationColumnName = "alcohol"], [SourceColumnName = "gangs", DestinationColumnName = "gangs"], [SourceColumnName = "sasid", DestinationColumnName = "sasid"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared caddo_referrals = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "caddo_referrals"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Promoted headers" = Table.PromoteHeaders(#"Expanded table column", [PromoteAllScalars = true]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Promoted headers", {{"RSch", type text}, {"ESch", Int64.Type}, {"SIDNO", type text}, {"GD", type text}, {"Eth", type text}, {"504", Int64.Type}, {"Ref#", Int64.Type}, {"Ref Date", type date}, {"Ref Time", Int64.Type}, {"Event ID", type text}, {"Loc", Int64.Type}, {"ICD", Int64.Type}, {"ICD2", Int64.Type}, {"ICD3", Int64.Type}, {"ICD4", Int64.Type}, {"Refer ID", Int64.Type}, {"T Eth", Int64.Type}, {"TCD", Int64.Type}, {"TCD2", Int64.Type}, {"TCD3", Int64.Type}, {"ACD", Int64.Type}, {"Adm Date", type date}, {"ACD2", Int64.Type}, {"Adm2 Date", type date}, {"ACD3", Int64.Type}, {"Adm3 Date", type date}, {"ACD4", Int64.Type}, {"Adm4 Date", type date}, {"ACD5", Int64.Type}, {"Adm5 Date", type date}, {"On Date", type date}, {"CNF Date", type date}, {"CNF Time", Int64.Type}, {"S Days", Int64.Type}, {"ISS Start", type date}, {"ISS Return", type date}, {"OSS Start", type date}, {"OSS Return", type date}, {"Exp Start", type date}, {"Exp Return", type date}, {"Bus Start", type date}, {"Bus End", type date}, {"Mon Det Start", type time}, {"Mon Det End", type time}, {"Thu Det Start", type time}, {"Thu Det End", type time}, {"Fri Det Start", type time}, {"Fri Det End", type time}, {"Det Start", type date}, {"Det Return", type date}, {"Man Date", type date}, {"Hearing Results", Int64.Type}, {"SIS Incident", type text}, {"State ID", type text}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type 1", {{"SIS Incident", "sis_incident"}}),
  #"Renamed columns" = Table.RenameColumns(#"Renamed columns 1", {{"State ID", "sasid"}, {"Disciplinarian Signed Form", "disc_signed_form"}, {"Other Agencies", "other_agencies"}, {"SIS Description", "sis_desc"}, {"Internal Remarks", "int_remarks"}, {"Remarks", "remarks"}, {"Gangs", "gangs"}, {"Alcohol Use", "alcohol"}, {"Drugs", "drugs"}, {"Hate/Bias", "hate_bias"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"hate_bias", type text}, {"drugs", type text}, {"alcohol", type text}, {"gangs", type text}, {"remarks", type text}, {"int_remarks", type text}, {"sis_incident", type text}, {"sis_desc", type text}, {"disc_signed_form", type text}, {"other_agencies", type text}}),
  #"Renamed columns 2" = Table.RenameColumns(#"Changed column type", {{"Special Codes", "special_codes"}, {"Hearing Results", "hearing_results"}, {"Hearing Date", "hearing_date"}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Renamed columns 2", {{"hearing_date", type date}}),
  #"Removed errors" = Table.RemoveRowsWithErrors(#"Changed column type 2", {"hearing_date"}),
  #"Renamed columns 3" = Table.RenameColumns(#"Removed errors", {{"Man Date", "man_date"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns 3", {"Detention Room 2", "Detention Room", "Det Return", "Det Start"}),
  #"Renamed columns 4" = Table.RenameColumns(#"Removed columns", {{"Det Days", "det_days"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns 4", {"Oth Det End", "Oth Det Start", "Sat Det End", "Sat Det Start", "Fri Det End", "Fri Det Start", "Thu Det End", "Thu Det Start", "Wed Det End", "Wed Det Start", "Tue Det End", "Tue Det Start", "Mon Det End", "Mon Det Start", "Bus End", "Bus Start", "Exp Return", "TR", "RT"}),
  #"Renamed columns 5" = Table.RenameColumns(#"Removed columns 1", {{"Exp", "expulsion"}, {"OSS Return", "oss_return"}, {"OSS Start", "oss_start"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns 5", {"ISS Return", "ISS Start", "CNF Time", "CNF Date", "CNF"}),
  #"Renamed columns 6" = Table.RenameColumns(#"Removed columns 2", {{"S Days", "susp_days"}, {"On Date", "on_date"}}),
  #"Sorted rows" = Table.Sort(#"Renamed columns 6", {{"on_date", Order.Descending}}),
  #"Renamed columns 7" = Table.RenameColumns(#"Sorted rows", {{"Administrative Comments", "admin_comments"}}),
  #"Removed columns 3" = Table.RemoveColumns(#"Renamed columns 7", {"Adm7 Date", "Admin Code7 Description", "ACD7", "Adm6 Date", "Admin Code6 Description", "ACD6", "Adm5 Date", "Admin Code5 Description", "ACD5", "Adm4 Date", "Admin Code4 Description", "ACD4", "Adm3 Date", "Admin Code3 Description", "ACD3", "Adm2 Date", "Admin Code2 Description", "ACD2", "Adm Date"}),
  #"Renamed columns 8" = Table.RenameColumns(#"Removed columns 3", {{"Admin Code Description", "admin_code_desc"}}),
  #"Removed columns 4" = Table.RemoveColumns(#"Renamed columns 8", {"ACD"}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Removed columns 4", {{"admin_code_desc", type text}}),
  #"Removed columns 5" = Table.RemoveColumns(#"Changed column type 3", {"Teacher Code4 Description", "TCD4", "Teacher Code3 Description", "TCD3", "Teacher Code2 Description", "TCD2"}),
  #"Renamed columns 9" = Table.RenameColumns(#"Removed columns 5", {{"Teacher Code Description", "teacher_code_desc"}}),
  #"Removed columns 6" = Table.RemoveColumns(#"Renamed columns 9", {"TCD", "T Gen", "T Eth", "Refer Name", "Refer ID", "Bus", "Incident CD4 Description", "ICD4", "Incident CD3 Description", "ICD3", "Incident CD2 Description", "ICD2"}),
  #"Renamed columns 10" = Table.RenameColumns(#"Removed columns 6", {{"Incident CD Description", "incident_cd_desc"}}),
  #"Removed columns 7" = Table.RemoveColumns(#"Renamed columns 10", {"ICD"}),
  #"Renamed columns 11" = Table.RenameColumns(#"Removed columns 7", {{"Location Description", "location"}}),
  #"Removed columns 8" = Table.RemoveColumns(#"Renamed columns 11", {"Loc"}),
  #"Renamed columns 12" = Table.RenameColumns(#"Removed columns 8", {{"Event ID", "event_id"}}),
  #"Removed columns 9" = Table.RemoveColumns(#"Renamed columns 12", {"RSch"}),
  #"Renamed columns 13" = Table.RenameColumns(#"Removed columns 9", {{"Student Name", "student_name"}}),
  #"Removed columns 10" = Table.RemoveColumns(#"Renamed columns 13", {"ESch"}),
  #"Renamed columns 14" = Table.RenameColumns(#"Removed columns 10", {{"ESch Name", "school_name"}, {"SIDNO", "sidno"}, {"GD", "grade"}, {"Sex", "gender"}}),
  #"Removed columns 11" = Table.RemoveColumns(#"Renamed columns 14", {"Eth"}),
  #"Renamed columns 15" = Table.RenameColumns(#"Removed columns 11", {{"Ethnic Description", "race"}, {"IEP", "iep"}, {"Sped", "sped"}}),
  #"Removed columns 12" = Table.RemoveColumns(#"Renamed columns 15", {"Lunch", "504"}),
  #"Renamed columns 16" = Table.RenameColumns(#"Removed columns 12", {{"Ref#", "ref_num"}, {"Ref Date", "ref_date"}}),
  #"Removed columns 13" = Table.RemoveColumns(#"Renamed columns 16", {"Ref Time"}),
  #"Changed column type 4" = Table.TransformColumnTypes(#"Removed columns 13", {{"sped", type text}, {"iep", type text}, {"race", type text}, {"gender", type text}, {"school_name", type text}, {"student_name", type text}, {"admin_comments", type text}, {"admin_code_desc", type text}, {"teacher_code_desc", type text}, {"incident_cd_desc", type text}, {"location", type text}, {"special_codes", type text}}),
  #"Removed duplicates" = Table.Distinct(#"Changed column type 4", {"event_id"})
in
  #"Removed duplicates";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "caddo_referrals"]}[Content],
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
shared caddo_referrals_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_caddo_referrals", ItemKind = "Table"]}[Data]
in
  TableNavigation;
