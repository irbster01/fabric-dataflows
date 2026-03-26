[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_cis_caselist = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "e4d2de35-5f5d-457f-8e5b-43f758299057"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "dbo", Item = "raw_cis_caselist"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([Enrollment Status] = "Assessments Pending" or [Enrollment Status] = "Enrolled")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows", {"Student ID"}),
  #"Trimmed text" = Table.TransformColumns(#"Removed duplicates", {{"Student ID", each Text.Trim(_), type nullable text}}),
  #"Cleaned text" = Table.TransformColumns(#"Trimmed text", {{"Student ID", each Text.Clean(_), type nullable text}}),
  #"Merged queries" = Table.NestedJoin(#"Cleaned text", {"Student ID"}, #"silver caddo_census", {"SASID"}, "silver caddo_census", JoinKind.LeftOuter),
  #"Expanded silver caddo_census" = Table.ExpandTableColumn(#"Merged queries", "silver caddo_census", {"SIDNO"}, {"SIDNO"}),
  #"Sorted rows" = Table.Sort(#"Expanded silver caddo_census", {{"Student", Order.Ascending}}),
  #"Inserted text before delimiter" = Table.AddColumn(#"Sorted rows", "Text before delimiter", each Text.BeforeDelimiter([Student], ",", 0), type text),
  #"Replaced value" = Table.ReplaceValue(#"Inserted text before delimiter", "-", "", Replacer.ReplaceText, {"Text before delimiter"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "'", "", Replacer.ReplaceText, {"Text before delimiter"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", ".", "", Replacer.ReplaceText, {"Text before delimiter"}),
  #"Cleaned text 1" = Table.TransformColumns(#"Replaced value 2", {{"Text before delimiter", each Text.Clean(_), type nullable text}}),
  #"Trimmed text 1" = Table.TransformColumns(#"Cleaned text 1", {{"Text before delimiter", each Text.Trim(_), type nullable text}}),
  #"Uppercased text" = Table.TransformColumns(#"Trimmed text 1", {{"Text before delimiter", each Text.Upper(_), type nullable text}}),
  #"Inserted text after delimiter" = Table.AddColumn(#"Uppercased text", "Text after delimiter", each Text.AfterDelimiter([Student], ", ", 0), type text),
  #"Replaced value 3" = Table.ReplaceValue(#"Inserted text after delimiter", "'", "", Replacer.ReplaceText, {"Text after delimiter"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "'", "", Replacer.ReplaceText, {"Text after delimiter"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "-", "", Replacer.ReplaceText, {"Text after delimiter"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "-", "", Replacer.ReplaceText, {"Text after delimiter"}),
  #"Trimmed text 2" = Table.TransformColumns(#"Replaced value 6", {{"Text after delimiter", each Text.Trim(_), type nullable text}}),
  #"Cleaned text 2" = Table.TransformColumns(#"Trimmed text 2", {{"Text after delimiter", each Text.Clean(_), type nullable text}}),
  #"Uppercased text 1" = Table.TransformColumns(#"Cleaned text 2", {{"Text after delimiter", each Text.Upper(_), type nullable text}}),
  #"Duplicated column" = Table.DuplicateColumn(#"Uppercased text 1", "Birth Date", "Birth Date - Copy"),
  #"Changed column type" = Table.TransformColumnTypes(#"Duplicated column", {{"Birth Date - Copy", type date}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Changed column type", {{"Birth Date - Copy", Int64.Type}}),
  #"Extracted text range" = Table.TransformColumns(#"Changed column type 1", {{"Text after delimiter", each Text.Middle(_, 0, 4), type text}}),
  #"Merged columns" = Table.CombineColumns(Table.TransformColumnTypes(#"Extracted text range", {{"Birth Date - Copy", type text}}), {"Text before delimiter", "Text after delimiter", "Birth Date - Copy"}, Combiner.CombineTextByDelimiter("", QuoteStyle.None), "Int ID")
in
  #"Merged columns";
shared #"silver lighthouse_studentData" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "e4d2de35-5f5d-457f-8e5b-43f758299057"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "dbo", Item = "silver.lighthouse_studentData"]}[Data],
  #"Sorted rows" = Table.Sort(#"Navigation 2", {{"Last Name", Order.Ascending}})
in
  #"Sorted rows";
shared #"LH Undupe" = let
  Source = Table.NestedJoin(raw_cis_caselist, {"Int ID"}, #"silver lighthouse_studentData", {"Int ID"}, "silver lighthouse_studentData", JoinKind.RightAnti),
  #"Expanded silver lighthouse_studentData 1" = Table.ExpandTableColumn(Source, "silver lighthouse_studentData", {"Client ID", "Entry Date", "Entry Exit ID", "Exit Date", "Provider", "First Name", "Last Name", "Date of Birth", "Int ID", "SIDNO", "School", "Grade", "Year", "SASID"}, {"silver lighthouse_studentData.Client ID", "silver lighthouse_studentData.Entry Date", "silver lighthouse_studentData.Entry Exit ID", "silver lighthouse_studentData.Exit Date", "silver lighthouse_studentData.Provider", "silver lighthouse_studentData.First Name", "silver lighthouse_studentData.Last Name", "silver lighthouse_studentData.Date of Birth", "silver lighthouse_studentData.Int ID", "silver lighthouse_studentData.SIDNO", "silver lighthouse_studentData.School", "silver lighthouse_studentData.Grade", "silver lighthouse_studentData.Year", "silver lighthouse_studentData.SASID"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded silver lighthouse_studentData 1", {"NCES ID", "Site ID", "Organization ID", "Initiatives", "Family Risk Factors", "Individual Student Risk Factors", "Student Attributes", "All Referral Reason_s_", "Most Recent _Referral Source", "Most Recent _Referral Reason_s_", "Most Recent_ Referral Date", "First CIS Enrollment Date", "Enrollment_Exit Date", "Enrollment _Begin Date", "Enrollment Status", "Grade Level", "Case Manager", "Case Management _Intensity Level", "Case Managed Status", "Birth Date", "Employment Status", "Public Assisstance Program_s_", "Languages", "Tribe", "Ethnicity", "Race/Ethnicity", "Sex/Gender", "Student", "Student ID", "Student System ID", "School Year", "Site Model Status", "Home School"}),
  #"Removed duplicates" = Table.Distinct(#"Removed columns", {"silver lighthouse_studentData.Client ID"}),
  #"Grouped rows" = Table.Group(#"Removed duplicates", {"silver lighthouse_studentData.Grade"}, {{"Count", each Table.RowCount(Table.Distinct(_)), Int64.Type}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Grouped rows", {{"silver lighthouse_studentData.Grade", "Grade"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns 1", null, 12, Replacer.ReplaceValue, {"Grade"}),
  #"Inserted conditional column" = Table.AddColumn(#"Replaced value", "Custom", each if [Grade] = 12 then 0 else [Count]),
  #"Removed columns 2" = Table.RemoveColumns(#"Inserted conditional column", {"Count"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns 2", {{"Custom", "Count"}}),
  #"Sorted rows" = Table.Sort(#"Renamed columns", {{"Grade", Order.Ascending}}),
  #"Filtered rows" = Table.SelectRows(#"Sorted rows", each ([Grade] <> null))
in
  #"Filtered rows";
shared #"CIS Undupe" = let
  Source = Table.NestedJoin(raw_cis_caselist, {"Int ID"}, #"silver lighthouse_studentData", {"Int ID"}, "silver lighthouse_studentData", JoinKind.LeftAnti),
  #"Removed duplicates" = Table.Distinct(Source, {"Student ID"}),
  #"Grouped rows" = Table.Group(#"Removed duplicates", {"Grade Level"}, {{"Count", each Table.RowCount(Table.Distinct(_)), Int64.Type}}),
  #"Replaced value" = Table.ReplaceValue(#"Grouped rows", "8th Grade", "8", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "11th Grade", "11", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "Kindergarten", "0", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "6th Grade", "6", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "7th Grade", "7", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "4th Grade", "4", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "9th Grade", "9", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", "1st Grade", "1", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 7", "5th Grade", "5", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 8", "2nd Grade", "2", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 9", "3rd Grade", "3", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", "10th Grade", "10", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", "12th Grade", "12", Replacer.ReplaceValue, {"Grade Level"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 12", {{"Grade Level", Int64.Type}}),
  #"Merged queries" = Table.NestedJoin(#"Changed column type", {"Grade Level"}, #"Matching CIS LH CLients", {"Grade Level"}, "CIS Undupe (2)", JoinKind.LeftOuter),
  #"Expanded CIS Undupe (2)" = Table.ExpandTableColumn(#"Merged queries", "CIS Undupe (2)", {"Count"}, {"Count.1"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded CIS Undupe (2)", {{"Count.1", "sub"}, {"Count", "mm"}}),
  #"Replaced value 13" = Table.ReplaceValue(#"Renamed columns", null, 0, Replacer.ReplaceValue, {"sub"}),
  #"Added custom" = Table.AddColumn(#"Replaced value 13", "Custom", each [mm] + [sub]),
  #"Removed columns" = Table.RemoveColumns(#"Added custom", {"mm", "sub"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"Custom", "Count"}}),
  #"Sorted rows" = Table.Sort(#"Renamed columns 1", {{"Grade Level", Order.Ascending}})
in
  #"Sorted rows";
shared #"Matching CIS LH CLients" = let
  Source = Table.NestedJoin(raw_cis_caselist, {"Student ID"}, #"silver lighthouse_studentData", {"SASID"}, "silver lighthouse_studentData", JoinKind.Inner),
  #"Replaced value" = Table.ReplaceValue(Source, "8th Grade", "8", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "11th Grade", "11", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "Kindergarten", "0", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "6th Grade", "6", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "7th Grade", "7", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "4th Grade", "4", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "9th Grade", "9", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", "1st Grade", "1", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 7", "5th Grade", "5", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 8", "2nd Grade", "2", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 9", "3rd Grade", "3", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", "10th Grade", "10", Replacer.ReplaceValue, {"Grade Level"}),
  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", "12th Grade", "12", Replacer.ReplaceValue, {"Grade Level"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 12", {{"Grade Level", Int64.Type}}),
  #"Sorted rows" = Table.Sort(#"Changed column type", {{"Grade Level", Order.Ascending}})
in
  #"Sorted rows";
shared #"silver caddo_census" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "e4d2de35-5f5d-457f-8e5b-43f758299057"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "dbo", Item = "silver.caddo_census"]}[Data]
in
  #"Navigation 2";
shared CIS_LH_byGrade_staging = let
  Source = Table.NestedJoin(#"CIS Undupe", {"Grade Level"}, #"LH Undupe", {"Grade"}, "LH", JoinKind.LeftOuter),
  Expand = Table.ExpandTableColumn(Source, "LH", {"Count"}, {"LH.Count"}),
  Buf1 = Table.Buffer(Expand),

  Keep = Table.SelectColumns(Buf1, {"Grade Level", "Count", "LH.Count"}),
  Typed = Table.TransformColumns(Keep, {
      {"Count", each try Number.From(_) otherwise 0, type number},
      {"LH.Count", each try Number.From(_) otherwise 0, type number}
  }),
  Added = Table.AddColumn(Typed, "Count.Sum", each [Count] + [LH.Count], type number),
  Final = Table.SelectColumns(Added, {"Grade Level", "Count.Sum"}),
  Renamed = Table.RenameColumns(Final, {{"Count.Sum","Count"}})
in
  Renamed;
shared #"CIS_LH_byGrade_staging (2)" = let
  Source = CIS_LH_byGrade_staging
in
  Source;
[StagingDefinition = [Kind = "FastCopy", Required = true], DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Matching_CIS_LH_Final_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "Grade Level", DestinationColumnName = "Grade Level"], [SourceColumnName = "Count", DestinationColumnName = "Count"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Matching_CIS_LH_Final = let
  // --- Helpers to survive "shitty datamart" text ---
  SanitizeText = (x as any) as text =>
    let
      t0 = Text.From(x),
      // remove non-breaking space (U+00A0) + trim + clean control chars
      t1 = Text.Replace(t0, Character.FromNumber(160), " "),
      t2 = Text.Clean(Text.Trim(t1))
    in
      t2,

  ToNumberSafe = (x as any) as number =>
    let
      s0  = SanitizeText(x),
      // handle (123) as -123
      s1  = if Text.StartsWith(s0, "(") and Text.EndsWith(s0, ")")
            then "-" & Text.Middle(s0, 1, Text.Length(s0)-2) else s0,
      // strip currency symbols and spaces
      s2  = Text.Select(s1, {"0".."9","-",".",","}),
      // if both comma+dot present, assume comma is thousands -> drop commas
      s3  = if Text.Contains(s2, ",") and Text.Contains(s2, ".")
            then Text.Replace(s2, ",", "")
            else s2,
      // if only comma present, assume it's decimal and swap to dot
      s4  = if Text.Contains(s3, ",") and not Text.Contains(s3, ".")
            then Text.Replace(s3, ",", ".")
            else s3,
      n   = try Number.From(s4) otherwise 0
    in
      n,

  Normalize = (tbl as table, gradeCol as text, countCol as text) as table =>
    let
      // Select (tolerate missing) and rename to common schema
      S = Table.SelectColumns(tbl, {gradeCol, countCol}, MissingField.UseNull),
      R = Table.RenameColumns(S, {{gradeCol, "Grade Level"}, {countCol, "Count"}}, MissingField.Ignore),
      // Scrub text, coerce number robustly
      T = Table.TransformColumns(
            R,
            {
              {"Grade Level", each SanitizeText(_), type text},
              {"Count",       each ToNumberSafe(_), type number}
            }
          )
    in
      T,

  // --- Normalize each source (note LH uses "Grade") ---
  A = Normalize(#"CIS Undupe",              "Grade Level", "Count"),
  B = Normalize(#"LH Undupe",               "Grade",       "Count"),
  M = Normalize(#"Matching CIS LH CLients", "Grade Level", "Count"),

  // --- Stack and total by Grade Level (no joins = no dup columns) ---
  Combined = Table.Combine({A, B, M}),
  Grouped  = Table.Group(
               Combined,
               {"Grade Level"},
               {{"Count", each List.Sum(List.Transform([Count], (n) => if n is number then n else 0)), type number}}
             ),

  // --- Harden for lakehouse write (kills 10079) ---
  Typed  = Table.TransformColumnTypes(Grouped, {{"Grade Level", type text}, {"Count", Int64.Type}}, "en-US"),
  Sorted = Table.Sort(Typed, {{"Grade Level", Order.Ascending}})
in
  Sorted;
shared Matching_CIS_LH_Final_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c46fe4a6-e106-4e77-993b-efc9ce22e551"]}[Data],
  TableNavigation = Navigation_2{[Id = "cis_lighthouse_counts_byGrade", ItemKind = "Table"]}[Data]
in
  TableNavigation;
