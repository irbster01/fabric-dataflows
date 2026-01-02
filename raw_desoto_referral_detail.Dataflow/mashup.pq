[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "desoto_referrals_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared desoto_referrals = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "desoto_referrals"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Date modified", "Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Column1", type text}, {"Column2", type text}, {"Column3", type text}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", type text}, {"Column8", type text}, {"Column9", type text}, {"Column10", type text}, {"Column11", type text}, {"Column12", Int64.Type}, {"Column13", type text}, {"Column14", type text}, {"Column15", type text}, {"Column16", type text}, {"Column17", type text}, {"Column18", type text}, {"Column19", type text}, {"Column20", type text}, {"Column21", type text}, {"Column22", type text}, {"Column23", type text}, {"Column24", type text}, {"Column25", type text}, {"Column26", type text}, {"Column27", type text}, {"Column28", type text}, {"Column29", type text}, {"Column30", type text}, {"Column31", type text}, {"Column32", type text}, {"Column33", type text}, {"Column34", type text}, {"Column35", type text}, {"Column36", type text}, {"Column37", type text}, {"Column38", type text}, {"Column39", type text}, {"Column40", type text}, {"Column41", type text}, {"Column42", type text}, {"Column43", type text}, {"Column44", type text}, {"Column45", type text}, {"Column46", type text}, {"Column47", type text}, {"Column48", type text}, {"Column49", type text}, {"Column50", type text}, {"Column51", type text}, {"Column52", type text}, {"Column53", type text}, {"Column54", type text}, {"Column55", type text}, {"Column56", type text}, {"Column57", type text}, {"Column58", type text}, {"Column59", type text}, {"Column60", type text}, {"Column61", type text}, {"Column62", type text}, {"Column63", type text}, {"Column64", type text}, {"Column65", type text}, {"Column66", type text}, {"Column67", type text}, {"Column68", type text}, {"Column69", type text}, {"Column70", type text}, {"Column71", type text}, {"Column72", type text}, {"Column73", type text}, {"Column74", type text}, {"Column75", type text}, {"Column76", type text}, {"Column77", type text}, {"Column78", type text}, {"Column79", type text}, {"Column80", type text}, {"Column81", type text}, {"Column82", type text}, {"Column83", type text}, {"Column84", type text}, {"Column85", type text}, {"Column86", type text}, {"Column87", type text}, {"Column88", type text}, {"Column89", type text}, {"Column90", type text}, {"Column91", type text}, {"Column92", type text}, {"Column93", type text}, {"Column94", type text}, {"Column95", type text}, {"Column96", type text}, {"Column97", type text}, {"Column98", type text}, {"Column99", type text}, {"Column100", type text}, {"Column101", type text}, {"Column102", type text}, {"Column103", type text}, {"Column104", type text}, {"Column105", type text}, {"Column106", type text}, {"Column107", type text}, {"Column108", type text}, {"Column109", type text}, {"Column110", type text}, {"Column111", type text}}),
  #"Promoted headers" = Table.PromoteHeaders(#"Changed column type", [PromoteAllScalars = true]),
  #"Renamed columns" = Table.RenameColumns(#"Promoted headers", {{"10/3/2025 3:56:04 PM", "date_modified"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"RSch"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"Student Name", "student_name"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns 1", {"ESch"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"ESch Name", "school_name"}, {"SIDNO", "sidno"}, {"GD", "grade"}, {"Sex", "gender"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns 2", {"Eth", "IEP", "Sped", "504"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed columns 2", {{"Ref Date", type date}}),
  #"Removed errors" = Table.RemoveRowsWithErrors(#"Changed column type 1", {"Ref Date"}),
  #"Removed columns 3" = Table.RemoveColumns(#"Removed errors", {"Ref Time"}),
  #"Renamed columns 3" = Table.RenameColumns(#"Removed columns 3", {{"Event ID", "event_id"}}),
  #"Removed columns 4" = Table.RemoveColumns(#"Renamed columns 3", {"Loc"}),
  #"Renamed columns 4" = Table.RenameColumns(#"Removed columns 4", {{"Location Description", "location"}}),
  #"Removed columns 5" = Table.RemoveColumns(#"Renamed columns 4", {"ICD"}),
  #"Renamed columns 5" = Table.RenameColumns(#"Removed columns 5", {{"Incident CD Description", "incident_description"}}),
  #"Removed columns 6" = Table.RemoveColumns(#"Renamed columns 5", {"ICD3", "ICD2", "Incident CD2 Description", "Incident CD3 Description", "ICD4", "Incident CD4 Description", "Bus", "Refer ID", "T Eth", "T Gen", "TCD"}),
  #"Renamed columns 6" = Table.RenameColumns(#"Removed columns 6", {{"Refer Name", "referer_name"}, {"Teacher Code Description", "teacher_description"}}),
  #"Removed columns 7" = Table.RemoveColumns(#"Renamed columns 6", {"TCD2", "Teacher Code2 Description", "TCD3", "Teacher Code3 Description", "TCD4", "ACD"}),
  #"Renamed columns 7" = Table.RenameColumns(#"Removed columns 7", {{"Admin Code Description", "admin_description"}}),
  #"Removed columns 8" = Table.RemoveColumns(#"Renamed columns 7", {"Teacher Code4 Description"}),
  #"Renamed columns 8" = Table.RenameColumns(#"Removed columns 8", {{"admin_description", "admin_description1"}, {"Admin Code2 Description", "admin_description2"}, {"Admin Code3 Description", "admin_description3"}, {"Admin Code4 Description", "admin_description5"}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Renamed columns 8", {{"Adm4 Date", type date}}),
  #"Renamed columns 9" = Table.RenameColumns(#"Changed column type 2", {{"Admin Code5 Description", "admin_description6"}}),
  #"Removed columns 9" = Table.RemoveColumns(#"Renamed columns 9", {"ACD5", "ACD4", "ACD3", "ACD2"}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Removed columns 9", {{"Adm Date", type date}}),
  #"Renamed columns 10" = Table.RenameColumns(#"Changed column type 3", {{"Adm Date", "admin_date1"}, {"Adm2 Date", "admin_date2"}, {"Adm3 Date", "admin_date4"}, {"Adm4 Date", "admin_date5"}, {"Adm5 Date", "admin_date6"}}),
  #"Changed column type 4" = Table.TransformColumnTypes(#"Renamed columns 10", {{"admin_date6", type date}, {"admin_date4", type date}, {"Adm6 Date", type date}}),
  #"Removed columns 10" = Table.RemoveColumns(#"Changed column type 4", {"ACD7", "Admin Code7 Description", "Adm7 Date"}),
  #"Renamed columns 11" = Table.RenameColumns(#"Removed columns 10", {{"Administrative Comments", "admin_comments"}}),
  #"Removed columns 11" = Table.RemoveColumns(#"Renamed columns 11", {"Adm6 Date", "Admin Code6 Description", "ACD6"}),
  #"Renamed columns 12" = Table.RenameColumns(#"Removed columns 11", {{"On Date", "on_date"}}),
  #"Changed column type 5" = Table.TransformColumnTypes(#"Renamed columns 12", {{"on_date", type date}}),
  #"Removed columns 12" = Table.RemoveColumns(#"Changed column type 5", {"CNF", "CNF Date", "CNF Time"}),
  #"Renamed columns 13" = Table.RenameColumns(#"Removed columns 12", {{"S Days", "suspension_days"}}),
  #"Changed column type 6" = Table.TransformColumnTypes(#"Renamed columns 13", {{"suspension_days", Int64.Type}}),
  #"Renamed columns 14" = Table.RenameColumns(#"Changed column type 6", {{"ISS Start", "iss_start"}, {"ISS Return", "iss_return"}}),
  #"Removed columns 13" = Table.RemoveColumns(#"Renamed columns 14", {"iss_start", "iss_return", "OSS Start", "OSS Return", "RT", "TR"}),
  #"Renamed columns 15" = Table.RenameColumns(#"Removed columns 13", {{"Exp", "expulsion"}, {"Exp Start", "expulsion_start"}, {"Exp Return", "expulsion_return"}}),
  #"Removed columns 14" = Table.RemoveColumns(#"Renamed columns 15", {"Bus Start", "Bus End", "Mon Det Start", "Mon Det End", "Tue Det Start", "Tue Det End", "Wed Det Start", "Wed Det End", "Thu Det Start", "Thu Det End", "Fri Det Start", "Fri Det End"}),
  #"Renamed columns 16" = Table.RenameColumns(#"Removed columns 14", {{"Remarks", "remarks"}, {"Gangs", "gangs"}, {"Alcohol Use", "alcohol_use"}, {"Drugs", "drugs"}}),
  #"Removed columns 15" = Table.RemoveColumns(#"Renamed columns 16", {"gangs", "alcohol_use", "drugs", "Hate/Bias", "Special Codes", "Detention Room 2", "Detention Room", "Det Return", "Det Start", "Det Days", "Oth Det End", "Oth Det Start", "Man Date", "Sat Det End", "Sat Det Start"}),
  #"Renamed columns 17" = Table.RenameColumns(#"Removed columns 15", {{"Hearing Date", "hearing_date"}, {"Hearing Results", "hearing_results"}, {"State ID", "state_id"}}),
  #"Removed columns 16" = Table.RemoveColumns(#"Renamed columns 17", {"Internal Remarks", "SIS Incident", "SIS Description", "Disciplinarian Signed Form", "Other Agencies"}),
  #"Changed column type 7" = Table.TransformColumnTypes(#"Removed columns 16", {{"Ref#", Int64.Type}}),
  #"Renamed columns 18" = Table.RenameColumns(#"Changed column type 7", {{"Ref#", "ref_num"}, {"Ethnic Description", "race"}, {"Ref Date", "ref_date"}}),
  #"Removed duplicates" = Table.Distinct(#"Renamed columns 18", {"event_id"}),
  #"Extracted text after delimiter" = Table.TransformColumns(#"Removed duplicates", {{"location", each Text.AfterDelimiter(_, " ", 0), type text}}),
  #"Capitalized each word" = Table.TransformColumns(#"Extracted text after delimiter", {{"school_name", each Text.Proper(_), type nullable text}}),
  #"Capitalized each word 1" = Table.TransformColumns(#"Capitalized each word", {{"student_name", each Text.Proper(_), type nullable text}}),
  #"Changed column type 8" = Table.TransformColumnTypes(#"Capitalized each word 1", {{"expulsion_start", type date}, {"expulsion_return", type date}, {"hearing_date", type date}}),
  #"Inserted conditional column" = Table.AddColumn(#"Changed column type 8", "major_flag", each if Text.Contains([teacher_description], "(Major)") then "MAJOR" else "NOT MAJOR"),
  #"Inserted conditional column 1" = Table.AddColumn(#"Inserted conditional column", "susp_flag", each if Text.Contains([admin_description1], "002") then "SUSPENSON" else if Text.Contains([admin_description1], "006") then "SUSPENSON" else if Text.Contains([admin_description2], "002") then "SUSPENSON" else if Text.Contains([admin_description2], "006") then "SUSPENSON" else if Text.Contains([admin_description3], "002") then "SUSPENSON" else if Text.Contains([admin_description3], "006") then "SUSPENSON" else if Text.Contains([admin_description5], "002") then "SUSPENSON" else if Text.Contains([admin_description5], "006") then "SUSPENSON" else "NOT SUSPENSON"),
  #"Inserted conditional column 2" = Table.AddColumn(#"Inserted conditional column 1", "incident_category", each if Text.Contains([incident_description], "fight") then "FIGHTING" else if Text.Contains([incident_description], "Bullying") then "BULLYING" else if Text.Contains([incident_description], "disobedience") then "DISOBEDIENCE" else if Text.Contains([incident_description], "immoral") then "IMMORAL BEHAVIOR" else if Text.Contains([incident_description], "threat") then "THREATS" else if Text.Contains([incident_description], "Disturbs") then "DISTURBING OTHERS" else if Text.Contains([incident_description], "disrespect") then "DISRESPECT" else if Text.Contains([incident_description], "injurious") then "INJURIOUS BEHAVIOR" else if Text.Contains([incident_description], "profane") then "PROFANE LANGUAGE" else if Text.Contains([incident_description], "weapons") then "WEAPONS" else if Text.Contains([incident_description], "Disrespect") then "DISRESPECT" else if Text.Contains([incident_description], "Takes") then "THEFT" else if Text.Contains([incident_description], "ID") then "ID VIOLATION" else if Text.Contains([incident_description], "Technology") then "UNAUTH USE OF TECHNOLOGY" else if Text.Contains([incident_description], "absent") then "HABITUALLY ABSENT" else if Text.Contains([incident_description], "serious") then "ANY OTHER SERIOUS OFFENSE" else if Text.Contains([incident_description], "Fight") then "FIGHTING" else if Text.Contains([incident_description], "Cellphone") then "CELLPHONE" else if Text.Contains([incident_description], "Dress Code") then "DRESS CODE" else if Text.Contains([incident_description], "Disturbance") then "DISTURBING OTHERS" else if Text.Contains([incident_description], "Dishonesty") then "DISHONESTY" else if Text.Contains([incident_description], "Sleeping") then "SLEEPING IN CLASS" else if Text.Contains([incident_description], "tobacco") then "TOBACCO" else if Text.Contains([incident_description], "Threatening") then "THREATS" else if Text.Contains([incident_description], "Leaves") then "LEAVING W/O PERMISSION" else if Text.Contains([incident_description], "Horse") then "HORSEPLAYING" else if Text.Contains([incident_description], "Skipping") then "SKIPPING" else if Text.Contains([incident_description], "participating") then "NOT PARTICIPATING IN CLASS" else if Text.Contains([incident_description], "Violations") then "HABITUALLY VIOLATING RULES" else if Text.Contains([incident_description], "materials") then "NO PROPER MATERIALS" else if Text.Contains([incident_description], "smoking device") then "VAPING" else if Text.Contains([incident_description], "Defacing") then "VANDALISM" else if Text.Contains([incident_description], "cultivation") then "DRUGS" else if Text.Contains([incident_description], "trespassing") then "TRESPASSING" else if Text.Contains([incident_description], "Littering") then "EATING/DRINKING/LITTERING" else if Text.Contains([incident_description], "Talking") then "TALKING IN CLASS" else if Text.Contains([incident_description], "vandalism") then "VANDALISM" else if Text.Contains([incident_description], "vaping") then "VAPING" else if Text.Contains([incident_description], "Criminal Damage") then "VANDALISM" else if Text.Contains([incident_description], "Trespassing") then "TRESPASSING" else if Text.Contains([incident_description], "Gambling") then "GAMBLING" else if Text.Contains([incident_description], "traffic") then "VIOLATION OF TRAFFIC REGS" else if Text.Contains([incident_description], "Off Limits") then "VIOLATION OF OF LIMITS AREA" else if Text.Contains([incident_description], "Contact") then "INNAPPROPRIATE CONTACT" else if Text.Contains([incident_description], "Internet") then "UNAUTH USE OF TECHNOLOGY" else if Text.Contains([incident_description], "Comply") then "DISOBEDIENCE" else if Text.Contains([incident_description], "missiles") then "THROWING OBJECTS" else if Text.Contains([incident_description], "Homework") then "NO HOMEWORK" else if Text.Contains([incident_description], "Sexual Harassment") then "SEXUAL HARASSMENT" else if Text.Contains([incident_description], "intimidate") then "THREATS" else if Text.Contains([incident_description], "Pornographic") then "PORNOGRAPHY" else if Text.Contains([incident_description], "alcohol") then "ALCOHOL" else if Text.Contains([incident_description], "Innappropriate Objects") then "POSESSION OF INNAPPROPRIATE OBJECTS" else if Text.Contains([incident_description], "substances") then "DRUGS" else if Text.Contains([incident_description], "Indecency") then "PUBLIC INDECENCY" else if Text.Contains([incident_description], "alcoholic") then "ALCOHOL" else if Text.Contains([incident_description], "unfounded") then "UNFOUNDED CHARGE AGAINST AUTHORIT" else if Text.Contains([incident_description], "Threat") then "THREATS" else if Text.Contains([incident_description], "Consequence") then "FAILURE TO SERVE CONSEQUENCE" else if Text.Contains([incident_description], "knife") then "POSSESSION OF KNIFE" else if Text.Contains([incident_description], "Electronic") then "UNAUTH USE OF TECHNOLOGY" else if Text.Contains([incident_description], "Objects") then "POSESSION OF INNAPPROPRIATE OBJECTS" else null),
  #"Changed column type 9" = Table.TransformColumnTypes(#"Inserted conditional column 2", {{"major_flag", type text}, {"susp_flag", type text}, {"incident_category", type text}})
in
  #"Changed column type 9";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "desoto_referrals"]}[Content],
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
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 111, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 111, QuoteStyle = QuoteStyle.None])
in
  Source;
shared desoto_referrals_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_jcampus_desoto_referrals", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
