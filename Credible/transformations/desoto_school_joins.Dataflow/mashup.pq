[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared desoto_attendance_summary = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "desoto_attendance_summary", ItemKind = "Table"]}[Data],
  #"Grouped rows" = Table.Group(#"Navigation 2", {"SIDNO", "School Name", "Student Name", "DOB"}, {{"f", each List.Max([F]), type nullable Int64.Type}, {"h", each List.Max([H]), type nullable Int64.Type}, {"o", each List.Max([O]), type nullable Int64.Type}, {"u", each List.Max([U]), type nullable Int64.Type}, {"s", each List.Max([S]), type nullable Int64.Type}, {"e", each List.Max([E]), type nullable Int64.Type}, {"nd", each List.Max([ND]), type nullable Int64.Type}, {"op", each List.Max([OP]), type nullable text}, {"d_pres", each List.Max([D Pres]), type nullable number}, {"p_pres", each List.Max([P Pres]), type nullable number}}),
  #"Filtered rows" = Table.SelectRows(#"Grouped rows", each ([School Name] <> "")),
  #"Renamed columns" = Table.RenameColumns(#"Filtered rows", {{"f", "full"}, {"h", "half"}, {"o", "check_out"}, {"u", "unexcused"}, {"s", "suspensions"}, {"e", "excused"}, {"nd", "doc_excused"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"op"}),
  #"Divided column" = Table.TransformColumns(#"Removed columns", {{"p_pres", each _ / 100, type number}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Divided column", {{"p_pres", Percentage.Type}, {"SIDNO", type text}}),
  #"Removed duplicates" = Table.Distinct(#"Changed column type")
in
  #"Removed duplicates";
shared raw_jcampus_desoto_census = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_jcampus_desoto_census", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared raw_jcampus_desoto_referrals = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_jcampus_desoto_referrals", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each [ref_date] > #date(2025, 8, 1)),
  #"Sorted rows" = Table.Sort(#"Filtered rows", {{"ref_date", Order.Ascending}})
in
  #"Sorted rows";
shared #"API - Billing" = let
  Source = PowerPlatform.Dataflows([]),
  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[workspaceId = "f09e25de-cf5c-4a79-a1ba-c2673d34f3d3"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[dataflowId = "6017eed9-2ebe-48f4-b504-d5dcd251c6f0"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[entity = "API - Billing", version = ""]}[Data]
in
  #"Navigation 4";
shared active_desoto_clients = let
    Source = PowerPlatform.Dataflows([]),
    #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
    #"Navigation 2" = #"Navigation 1"{[workspaceId = "2e078b2f-481a-42df-8fee-6d81b3c2dac1"]}[Data],
    #"Navigation 3" = #"Navigation 2"{[dataflowId = "4564730f-6b6d-437e-8875-2e85af1f6351"]}[Data],
    #"Navigation 4" = #"Navigation 3"{[entity = "API - Episodes", version = ""]}[Data],
    #"Filtered rows" = Table.SelectRows(
        #"Navigation 4",
        each
            ([Episode Status] = "ACTIVE" or [Episode Status] = "Enrolled" or [Episode Status] = "Pro Bono")
            and ([Program ID] = "6401")
    ),
    #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows", {{"Length of Stay", Int64.Type}}),

    // FIX: guard null before compare
    #"Inserted conditional column" =
        Table.AddColumn(
            #"Changed column type",
            "90_flag",
            each if (([Length of Stay] ?? 0) >= 90) then true else false
        ),

    #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted conditional column", {{"90_flag", type logical}}),
    #"Merged queries" = Table.NestedJoin(#"Changed column type 1", {"Client ID"}, #"API - Clients (Updated)", {"Client ID"}, "API - Clients (Updated)", JoinKind.LeftOuter),
    #"Expanded API - Clients (Updated) 1" = Table.ExpandTableColumn(#"Merged queries", "API - Clients (Updated)", {"Client Name", "Int ID"}, {"Client Name", "Int ID"}),

    // FIX: Text.Contains(null, ...) returns null; coalesce Client Name first
    #"Inserted conditional column 1" =
        Table.AddColumn(
            #"Expanded API - Clients (Updated) 1",
            "sidno_correction",
            each
                let cn = ([Client Name] ?? "")
                in
                    if cn = "Simone, John" then 7049886
                    else if Text.Contains(cn, "Marshall Jr.,") then 7199925
                    else if Text.Contains(cn, "Landers Jr,") then 9199835
                    else if Text.Contains(cn, "Walker, Ava") then 9049959
                    else if Text.Contains(cn, "Howard, Mya") then 6149644
                    else if Text.Contains(cn, "Whitaker Jr") then 9149681
                    else ""
        ),

    #"Changed column type 2" = Table.TransformColumnTypes(#"Inserted conditional column 1", {{"sidno_correction", type text}}),
    #"Merged queries 1" = Table.NestedJoin(#"Changed column type 2", {"Int ID"}, raw_jcampus_desoto_census, {"int_id"}, "raw_jcampus_desoto_census", JoinKind.LeftOuter),
    #"Expanded raw_jcampus_desoto_census" = Table.ExpandTableColumn(#"Merged queries 1", "raw_jcampus_desoto_census", {"sidno"}, {"sidno"}),
    #"Merged columns" = Table.CombineColumns(#"Expanded raw_jcampus_desoto_census", {"sidno", "sidno_correction"}, Combiner.CombineTextByDelimiter("", QuoteStyle.None), "sidno (2)"),
    #"Renamed columns" = Table.RenameColumns(#"Merged columns", {{"sidno (2)", "sidno"}}),
    #"Sorted rows" = Table.Sort(#"Renamed columns", {{"sidno", Order.Ascending}}),
    #"Filtered rows 1" = Table.SelectRows(#"Sorted rows", each ([Client Name] <> "Hearn, Plpc, Jessica" and [Client Name] <> "Outreach, Desoto")),
    #"Replaced value" = Table.ReplaceValue(#"Filtered rows 1", "0199787", "199787", Replacer.ReplaceValue, {"sidno"})
in
    #"Replaced value";
shared #"API - Clients (Updated)" = let
  Source = PowerPlatform.Dataflows([]),
  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[workspaceId = "2e078b2f-481a-42df-8fee-6d81b3c2dac1"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[dataflowId = "4564730f-6b6d-437e-8875-2e85af1f6351"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[entity = "API - Clients (Updated)", version = ""]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 4", {"employeeid", "programs", "Status", "Client Status Date", "Date Created", "Date Updated"})
in
  #"Removed columns";
shared census_wVOA_flag = let
  Source = raw_jcampus_desoto_census,
  #"Merged queries" = Table.NestedJoin(Source, {"sidno"}, active_desoto_clients, {"sidno"}, "active_desoto_clients", JoinKind.LeftOuter),
  #"Expanded active_desoto_clients" = Table.ExpandTableColumn(#"Merged queries", "active_desoto_clients", {"Program ID"}, {"Program ID"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded active_desoto_clients", {{"Program ID", "voa_flag"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns", "6401", "VOA", Replacer.ReplaceValue, {"voa_flag"})
in
  #"Replaced value";
shared whole_desoto_totalReferrals_table = let
  Source = raw_jcampus_desoto_referrals,
  #"Grouped rows" = Table.Group(Source, {"sidno", "student_name", "school_name"}, {{"Count", each List.Count(List.Distinct([event_id])), Int64.Type}}),
  #"Renamed columns" = Table.RenameColumns(#"Grouped rows", {{"Count", "total_referrals"}}),
  #"Merged queries" = Table.NestedJoin(#"Renamed columns", {"sidno", "student_name", "school_name"}, whole_desoto_major_table, {"sidno", "student_name", "school_name"}, "desoto_major_table", JoinKind.LeftOuter),
  #"Expanded desoto_major_table" = Table.ExpandTableColumn(#"Merged queries", "desoto_major_table", {"major_referrals"}, {"major_referrals"}),
  #"Replaced value" = Table.ReplaceValue(#"Expanded desoto_major_table", null, 0, Replacer.ReplaceValue, {"major_referrals"}),
  #"Merged queries 1" = Table.NestedJoin(#"Replaced value", {"sidno", "student_name", "school_name"}, whole_desoto_suspension_table, {"sidno", "student_name", "school_name"}, "desoto_suspension_table", JoinKind.LeftOuter),
  #"Expanded desoto_suspension_table" = Table.ExpandTableColumn(#"Merged queries 1", "desoto_suspension_table", {"suspensions"}, {"suspensions"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Expanded desoto_suspension_table", null, 0, Replacer.ReplaceValue, {"suspensions"})
in
  #"Replaced value 1";
shared whole_desoto_major_table = let
  Source = raw_jcampus_desoto_referrals,
  #"Filtered rows" = Table.SelectRows(Source, each ([major_flag] = "MAJOR")),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"sidno", "student_name", "school_name"}, {{"Count", each List.Count(List.Distinct([event_id])), Int64.Type}}),
  #"Renamed columns" = Table.RenameColumns(#"Grouped rows", {{"Count", "major_referrals"}})
in
  #"Renamed columns";
shared whole_desoto_suspension_table = let
  Source = raw_jcampus_desoto_referrals,
  #"Filtered rows" = Table.SelectRows(Source, each ([susp_flag] = "SUSPENSON")),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"sidno", "student_name", "school_name"}, {{"suspensions", each List.Count(List.Distinct([event_id])), Int64.Type}})
in
  #"Grouped rows";
shared voa_desoto_referral_table = let
  Source = active_desoto_clients,
  #"Merged queries" = Table.NestedJoin(Source, {"sidno"}, whole_desoto_totalReferrals_table, {"sidno"}, "whole_desoto_totalReferrals_table", JoinKind.LeftOuter),
  #"Expanded whole_desoto_totalReferrals_table 1" = Table.ExpandTableColumn(#"Merged queries", "whole_desoto_totalReferrals_table", {"school_name", "total_referrals", "major_referrals", "suspensions"}, {"school_name", "total_referrals", "major_referrals", "suspensions"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded whole_desoto_totalReferrals_table 1", {"Int ID", "fep_first_contact", "Referred Source", "Date Auto Closed", "Discharge Location", "Discharge Outcome", "discharge_date1", "Date Episode Created", "Date Episode Updated", "assessment_date", "program_id1", "Admission Date", "Assessment Date", "Discharge Date", "Referred By"}),
  #"Removed duplicates" = Table.Distinct(#"Removed columns", {"Episode ID", "Client ID"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed duplicates", null, 0, Replacer.ReplaceValue, {"total_referrals"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", null, 0, Replacer.ReplaceValue, {"major_referrals"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", null, 0, Replacer.ReplaceValue, {"suspensions"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Replaced value 2", {"Program ID", "Length of Stay", "Episode ID"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed columns 1", {"Client ID", "sidno", "Client Name", "Episode Status", "Referral Date", "90_flag", "total_referrals", "major_referrals", "suspensions"})
in
  #"Reordered columns";
shared voa_desoto_attendanceSum_table = let
  Source = active_desoto_clients,
  #"Removed columns" = Table.RemoveColumns(Source, {"Assessment Date", "Admission Date", "program_id1", "Program ID", "assessment_date", "Date Episode Updated", "Date Episode Created", "Length of Stay", "Discharge Date", "discharge_date1", "Discharge Outcome", "Discharge Location", "Date Auto Closed", "Referral Date", "Referred By", "Referred Source", "fep_first_contact"}),
  #"Merged queries" = Table.NestedJoin(#"Removed columns", {"sidno"}, desoto_attendance_summary, {"SIDNO"}, "desoto_attendance_summary", JoinKind.Inner),
  #"Expanded desoto_attendance_summary" = Table.ExpandTableColumn(#"Merged queries", "desoto_attendance_summary", {"School Name", "Student Name", "full", "half", "check_out", "unexcused", "suspensions", "excused", "doc_excused", "d_pres", "p_pres"}, {"School Name", "Student Name", "full", "half", "check_out", "unexcused", "suspensions", "excused", "doc_excused", "d_pres", "p_pres"}),
  #"Grouped rows" = Table.Group(#"Expanded desoto_attendance_summary", {"Client ID", "sidno", "Client Name", "School Name", "90_flag"}, {{"full", each List.Max([full]), type nullable Int64.Type}, {"half", each List.Max([half]), type nullable Int64.Type}, {"check_out", each List.Max([check_out]), type nullable Int64.Type}, {"unexcused", each List.Max([unexcused]), type nullable Int64.Type}, {"suspensions", each List.Max([suspensions]), type nullable Int64.Type}, {"excused", each List.Max([excused]), type nullable Int64.Type}, {"doc_excused", each List.Max([doc_excused]), type nullable Int64.Type}, {"d_pres", each List.Max([d_pres]), type nullable number}, {"p_pres", each List.Max([p_pres]), type nullable Percentage.Type}}),
  #"Replaced value" = Table.ReplaceValue(#"Grouped rows", null, 0, Replacer.ReplaceValue, {"full"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", null, 0, Replacer.ReplaceValue, {"half"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", null, 0, Replacer.ReplaceValue, {"check_out"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", null, 0, Replacer.ReplaceValue, {"unexcused"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", null, 0, Replacer.ReplaceValue, {"suspensions"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", null, 0, Replacer.ReplaceValue, {"excused"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", null, 0, Replacer.ReplaceValue, {"doc_excused"})
in
  #"Replaced value 6";
shared whole_desoto_attendanceSum_table = let
    Source = desoto_attendance_summary,
    #"Replaced value" = Table.ReplaceValue(Source, null, 0, Replacer.ReplaceValue, {"half"}),
    #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", null, 0, Replacer.ReplaceValue, {"check_out"}),
    #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", null, 0, Replacer.ReplaceValue, {"unexcused"}),
    #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", null, 0, Replacer.ReplaceValue, {"suspensions"}),
    #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", null, 0, Replacer.ReplaceValue, {"excused"}),
    #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", null, 0, Replacer.ReplaceValue, {"doc_excused"}),
    #"Merged queries" = Table.NestedJoin(#"Replaced value 5", {"SIDNO"}, voa_desoto_attendanceSum_table, {"sidno"}, "voa_desoto_attendanceSum_table", JoinKind.LeftOuter),
    #"Expanded voa_desoto_attendanceSum_table" = Table.ExpandTableColumn(#"Merged queries", "voa_desoto_attendanceSum_table", {"90_flag"}, {"90_flag"}),

    // FIX: proper null check
    #"Inserted conditional column" =
        Table.AddColumn(
            #"Expanded voa_desoto_attendanceSum_table",
            "voa_flag",
            each if [90_flag] is null then "NOT VOA" else "VOA"
        ),

    #"Changed column type" = Table.TransformColumnTypes(#"Inserted conditional column", {{"voa_flag", type text}}),
    #"Reordered columns" = Table.ReorderColumns(#"Changed column type", {"SIDNO", "School Name", "Student Name", "voa_flag", "DOB", "full", "half", "check_out", "unexcused", "suspensions", "excused", "doc_excused", "d_pres", "p_pres", "90_flag"}),
    #"Removed columns" = Table.RemoveColumns(#"Reordered columns", {"90_flag"})
in
    #"Removed columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "complete_attendance_and_referral_table_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared complete_attendance_and_referral_table = let
  Source = whole_desoto_attendanceSum_table,
  #"Merged queries" = Table.NestedJoin(Source, {"SIDNO"}, whole_desoto_totalReferrals_table, {"sidno"}, "whole_desoto_totalReferrals_table", JoinKind.LeftOuter),
  #"Expanded whole_desoto_totalReferrals_table" = Table.ExpandTableColumn(#"Merged queries", "whole_desoto_totalReferrals_table", {"total_referrals", "major_referrals", "suspensions"}, {"total_referrals", "major_referrals", "suspensions.1"}),
  #"Replaced value" = Table.ReplaceValue(#"Expanded whole_desoto_totalReferrals_table", null, 0, Replacer.ReplaceValue, {"total_referrals"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", null, 0, Replacer.ReplaceValue, {"major_referrals"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", null, 0, Replacer.ReplaceValue, {"suspensions.1"}),
  #"Inserted multiplication" = Table.AddColumn(#"Replaced value 2", "Multiplication", each [total_referrals] * 2, type number),
  #"Renamed columns" = Table.RenameColumns(#"Inserted multiplication", {{"Multiplication", "weighted_referrals"}}),
  #"Inserted multiplication 1" = Table.AddColumn(#"Renamed columns", "weighted_suspensions", each [suspensions.1] * 3, type number),
  #"Inserted sum" = Table.AddColumn(#"Inserted multiplication 1", "Sum", each List.Sum({[weighted_referrals], [weighted_suspensions], [unexcused]}), type nullable number),
  #"Inserted division" = Table.AddColumn(#"Inserted sum", "needs_scale", each [Sum] / 10, type number),
  #"Grouped rows" = Table.Group(#"Inserted division", {"SIDNO", "Student Name", "School Name", "voa_flag"}, {{"full", each List.Max([full]), type nullable Int64.Type}, {"half", each List.Max([half]), type nullable number}, {"check_out", each List.Max([check_out]), type nullable number}, {"unexcused", each List.Max([unexcused]), type nullable number}, {"suspensions", each List.Max([suspensions]), type nullable number}, {"excused", each List.Max([excused]), type nullable number}, {"doc_excused", each List.Max([doc_excused]), type nullable number}, {"d_pres", each List.Max([d_pres]), type nullable number}, {"p_pres", each List.Max([p_pres]), type nullable Percentage.Type}, {"total_referrals", each List.Max([total_referrals]), type nullable number}, {"major_referrals", each List.Max([major_referrals]), type nullable number}, {"suspensions1", each List.Max([suspensions.1]), type nullable number}, {"weighted_referrals", each List.Max([weighted_referrals]), type nullable number}, {"weighted_suspensions", each List.Max([weighted_suspensions]), type nullable number}, {"needs_scale", each List.Max([needs_scale]), type nullable number}}),
  #"Sorted rows" = Table.Sort(#"Grouped rows", {{"needs_scale", Order.Descending}}),
  #"Capitalized each word" = Table.TransformColumns(#"Sorted rows", {{"Student Name", each Text.Proper(_), type nullable text}}),
  #"Capitalized each word 1" = Table.TransformColumns(#"Capitalized each word", {{"School Name", each Text.Proper(_), type nullable text}}),
  #"Replaced value 3" = Table.ReplaceValue(#"Capitalized each word 1", null, 0, Replacer.ReplaceValue, {"full"}),
  #"Merged queries 1" = Table.NestedJoin(#"Replaced value 3", {"SIDNO"}, nonactive_and_active_staging_table, {"sidno"}, "nonactive_and_active_table", JoinKind.LeftOuter),
  #"Expanded nonactive_and_active_table" = Table.ExpandTableColumn(#"Merged queries 1", "nonactive_and_active_table", {"Episode Status", "Program ID", "assessment_date", "Date Episode Updated", "Date Episode Created", "Discharge Date", "Discharge Outcome", "Discharge Location", "Referral Date", "Referred By", "Referred Source", "90_flag"}, {"Episode Status", "Program ID", "assessment_date", "Date Episode Updated", "Date Episode Created", "Discharge Date", "Discharge Outcome", "Discharge Location", "Referral Date", "Referred By", "Referred Source", "90_flag"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded nonactive_and_active_table", {"Referred Source", "Referred By", "Referral Date", "Discharge Location", "Discharge Outcome", "Discharge Date", "Date Episode Created", "Date Episode Updated", "assessment_date", "Program ID", "Episode Status"}),
  #"Merged queries 2" = Table.NestedJoin(#"Removed columns", {"SIDNO"}, voa_status_fact_table, {"sidno"}, "voa_status_fact_table", JoinKind.LeftOuter),
  #"Expanded voa_status_fact_table 2" = Table.ExpandTableColumn(#"Merged queries 2", "voa_status_fact_table", {"Client ID", "Assessment Date", "Admission Date", "Program ID", "Length of Stay", "closed_date", "Discharge Outcome", "Discharge Location", "Referral Date", "Referred By", "Referred Source", "status_desc", "last_crisis"}, {"Client ID", "Assessment Date", "Admission Date", "Program ID", "Length of Stay", "closed_date", "Discharge Outcome", "Discharge Location", "Referral Date", "Referred By", "Referred Source", "status_desc", "last_crisis"})
in
  #"Expanded voa_status_fact_table 2";
shared complete_attendance_and_referral_table_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "complete_attendance_and_referral_table", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared nonactive_and_active_staging_table = let
    Source = PowerPlatform.Dataflows([]),
    #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
    #"Navigation 2" = #"Navigation 1"{[workspaceId = "2e078b2f-481a-42df-8fee-6d81b3c2dac1"]}[Data],
    #"Navigation 3" = #"Navigation 2"{[dataflowId = "4564730f-6b6d-437e-8875-2e85af1f6351"]}[Data],
    #"Navigation 4" = #"Navigation 3"{[entity = "API - Episodes", version = ""]}[Data],
    #"Filtered rows" = Table.SelectRows(#"Navigation 4", each [Program ID] = "6401"),
    #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows", {{"Length of Stay", Int64.Type}}),

    // FIX: guard null before compare
    #"Inserted conditional column" =
        Table.AddColumn(
            #"Changed column type",
            "90_flag",
            each if (([Length of Stay] ?? 0) >= 90) then true else false
        ),

    #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted conditional column", {{"90_flag", type logical}}),
    #"Merged queries" = Table.NestedJoin(#"Changed column type 1", {"Client ID"}, #"API - Clients (Updated)", {"Client ID"}, "API - Clients (Updated)", JoinKind.LeftOuter),
    #"Expanded API - Clients (Updated) 1" = Table.ExpandTableColumn(#"Merged queries", "API - Clients (Updated)", {"Client Name", "Int ID"}, {"Client Name", "Int ID"}),

    // FIX: Text.Contains(null, ...) returns null; coalesce Client Name first
    #"Inserted conditional column 1" =
        Table.AddColumn(
            #"Expanded API - Clients (Updated) 1",
            "sidno_correction",
            each
                let cn = ([Client Name] ?? "")
                in
                    if cn = "Simone, John" then 7049886
                    else if Text.Contains(cn, "Marshall Jr.,") then 7199925
                    else if Text.Contains(cn, "Landers Jr,") then 9199835
                    else if Text.Contains(cn, "Walker, Ava") then 9049959
                    else if Text.Contains(cn, "Howard, Mya") then 6149644
                    else if Text.Contains(cn, "Whitaker Jr") then 9149681
                    else ""
        ),

    #"Changed column type 2" = Table.TransformColumnTypes(#"Inserted conditional column 1", {{"sidno_correction", type text}}),
    #"Merged queries 1" = Table.NestedJoin(#"Changed column type 2", {"Int ID"}, raw_jcampus_desoto_census, {"int_id"}, "raw_jcampus_desoto_census", JoinKind.LeftOuter),
    #"Expanded raw_jcampus_desoto_census" = Table.ExpandTableColumn(#"Merged queries 1", "raw_jcampus_desoto_census", {"sidno"}, {"sidno"}),
    #"Merged columns" = Table.CombineColumns(#"Expanded raw_jcampus_desoto_census", {"sidno", "sidno_correction"}, Combiner.CombineTextByDelimiter("", QuoteStyle.None), "sidno (2)"),
    #"Renamed columns" = Table.RenameColumns(#"Merged columns", {{"sidno (2)", "sidno"}}),
    #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"fep_first_contact"})
in
    #"Removed columns";
shared service_counts_by_month = let
  Source = #"API - Billing",
  #"Inserted start of month" = Table.AddColumn(Source, "Start of month", each Date.StartOfMonth([Service Date]), type nullable date),
  #"Renamed columns" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}}),
  #"Grouped rows" = Table.Group(#"Renamed columns", {"Client ID", "Client Name", "service_month", "Service Type"}, {{"service_count", each List.Count(List.Distinct([Service ID])), Int64.Type}})
in
  #"Grouped rows";
[Description = "This will feed into the high needs table to give us VOA statuses.", DataDestinations = {[Definition = [Kind = "Reference", QueryName = "voa_status_fact_table_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared voa_status_fact_table = let
    Source = PowerPlatform.Dataflows([]),
    #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
    #"Navigation 2" = #"Navigation 1"{[workspaceId = "2e078b2f-481a-42df-8fee-6d81b3c2dac1"]}[Data],
    #"Navigation 3" = #"Navigation 2"{[dataflowId = "4564730f-6b6d-437e-8875-2e85af1f6351"]}[Data],
    #"Navigation 4" = #"Navigation 3"{[entity = "API - Episodes", version = ""]}[Data],

    #"Filtered rows" =
        Table.SelectRows(#"Navigation 4", each [Program ID] = "6401"),

    // Ensure Date Episode Created is a datetime (or datetimezone) so sorting/max works reliably
    #"Typed dates" =
        Table.TransformColumnTypes(
            #"Filtered rows",
            {{"Date Episode Created", type datetime}}
        ),

    // Sort so "newest first" within each Client ID, then take first row per Client ID
    #"Sorted" =
        Table.Sort(
            #"Typed dates",
            {
                {"Client ID", Order.Ascending},
                {"Date Episode Created", Order.Descending}
            }
        ),

    // Group by Client ID and keep the first (newest) row from each group
    #"Grouped" =
        Table.Group(
            #"Sorted",
            {"Client ID"},
            {{"TopRow", each Table.FirstN(_, 1), type table}}
        ),
  #"Expanded TopRow" = Table.ExpandTableColumn(Grouped, "TopRow", {"Episode ID", "Client ID", "Episode Status", "Assessment Date", "Admission Date", "program_id1", "Program ID", "assessment_date", "Date Episode Updated", "Date Episode Created", "Length of Stay", "Discharge Date", "discharge_date1", "Discharge Outcome", "Discharge Location", "Date Auto Closed", "Referral Date", "Referred By", "Referred Source", "fep_first_contact"}, {"Episode ID", "Client ID.1", "Episode Status", "Assessment Date", "Admission Date", "program_id1", "Program ID", "assessment_date", "Date Episode Updated", "Date Episode Created", "Length of Stay", "Discharge Date", "discharge_date1", "Discharge Outcome", "Discharge Location", "Date Auto Closed", "Referral Date", "Referred By", "Referred Source", "fep_first_contact"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded TopRow", {"Date Auto Closed", "fep_first_contact"}),
  #"Inserted conditional column" = Table.AddColumn(#"Removed columns", "Custom", each if [Episode Status] = "CLOSED" then [Discharge Outcome] else [Episode Status]),
  #"Replaced value" = Table.ReplaceValue(#"Inserted conditional column", "", "Data not Collected", Replacer.ReplaceValue, {"Custom"}),
  #"Renamed columns" = Table.RenameColumns(#"Replaced value", {{"Custom", "status_desc"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"discharge_date1", "assessment_date", "Client ID.1"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"Episode ID", type text}, {"Episode Status", type text}, {"program_id1", type text}, {"Program ID", type text}, {"Assessment Date", type date}, {"Admission Date", type date}, {"Date Episode Updated", type date}, {"Date Episode Created", type datetime}, {"Discharge Date", type date}, {"Length of Stay", Int64.Type}, {"Discharge Outcome", type text}, {"Discharge Location", type text}, {"Referred By", type text}, {"Referral Date", type date}, {"Referred Source", type text}, {"status_desc", type text}}),
  #"Replaced value 1" = Table.ReplaceValue(#"Changed column type", "", "Data not Collected", Replacer.ReplaceValue, {"Referred By"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "1000", "Desoto School", Replacer.ReplaceValue, {"Referred Source"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "1006", "Relative", Replacer.ReplaceValue, {"Referred Source"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "1004", "Internal VOA", Replacer.ReplaceValue, {"Referred Source"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "992", "Easter Seals", Replacer.ReplaceValue, {"Referred Source"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "", "Data not Collected", Replacer.ReplaceValue, {"Referred Source"}),
  #"Sorted rows" = Table.Sort(#"Replaced value 6", {{"Referred Source", Order.Ascending}}),
  #"Replaced value 7" = Table.ReplaceValue(#"Sorted rows", "1003", "DCFS", Replacer.ReplaceValue, {"Referred Source"}),
  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 7", "1005", "Previous Client", Replacer.ReplaceValue, {"Referred Source"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 8", "997", "PCP", Replacer.ReplaceValue, {"Referred Source"}),
  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 9", "965", "Unknown", Replacer.ReplaceValue, {"Discharge Location"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", "983", "In care of Family", Replacer.ReplaceValue, {"Discharge Location"}),
  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", "973", "No interview completed", Replacer.ReplaceValue, {"Discharge Location"}),
  #"Replaced value 13" = Table.ReplaceValue(#"Replaced value 12", "", "Data not Collected", Replacer.ReplaceValue, {"Discharge Location"}),
  #"Removed columns 2" = Table.RemoveColumns(#"Replaced value 13", {"program_id1"}),
  #"Merged queries" = Table.NestedJoin(#"Removed columns 2", {"Client ID"}, last_crisis_staging, {"Client ID"}, "API - Billing (2)", JoinKind.LeftOuter),
  #"Expanded API - Billing (2)" = Table.ExpandTableColumn(#"Merged queries", "API - Billing (2)", {"last_crisis"}, {"last_crisis"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Expanded API - Billing (2)", {{"Discharge Date", "closed_date"}}),
  #"Merged queries 1" = Table.NestedJoin(#"Renamed columns 1", {"Client ID"}, #"API - Clients (Updated)", {"Client ID"}, "API - Clients (Updated)", JoinKind.LeftOuter),
  #"Expanded API - Clients (Updated)" = Table.ExpandTableColumn(#"Merged queries 1", "API - Clients (Updated)", {"Int ID"}, {"Int ID"}),
  #"Merged queries 2" = Table.NestedJoin(#"Expanded API - Clients (Updated)", {"Int ID"}, raw_jcampus_desoto_census, {"int_id"}, "raw_jcampus_desoto_census", JoinKind.LeftOuter),
  #"Expanded raw_jcampus_desoto_census" = Table.ExpandTableColumn(#"Merged queries 2", "raw_jcampus_desoto_census", {"sidno", "school_name"}, {"sidno", "school_name"}),
  #"Removed columns 3" = Table.RemoveColumns(#"Expanded raw_jcampus_desoto_census", {"Int ID"})
in
    #"Removed columns 3";
shared last_crisis_staging = let
  Source = #"API - Billing",
  #"Filtered rows" = Table.SelectRows(Source, each ([Service Type] = "Crisis Follow-Up" or [Service Type] = "Crisis Intervention")),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"Client ID", "Client Name", "Program ID"}, {{"last_crisis", each List.Max([Service Date]), type nullable date}})
in
  #"Grouped rows";
shared voa_status_fact_table_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "voa_status_fact_table", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
