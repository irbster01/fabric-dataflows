[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "caddo_tests_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared caddo_tests = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "caddo_tests"]}[Content],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", let latest = List.Max(#"Navigation 4"[Date modified]) in each [Date modified] = latest),
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Filtered rows", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Promoted headers" = Table.PromoteHeaders(#"Expanded table column", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"School", type text}, {"Student Name", type text}, {"Grade", type text}, {"SASID", type text}, {"SIDNO", type text}, {"ACT 833 (April Dunn Act)", type text}, {"Cumulative GPA", type number}, {"Passed <BR> Requirements", type text}, {"Graduation Pathway", type text}, {"Diploma Endorsements", type text}, {"ELA Init 3-8", type text}, {"Math Init 3-8", type text}, {"Sci Init 3-8", type text}, {"Soc Init 3-8", type text}, {"Civ Init 3-8", type text}, {"ELA High 3-8", type text}, {"Math High 3-8", type text}, {"Sci High 3-8", type text}, {"Soc High 3-8", type text}, {"Civ High 3-8", type text}, {"LEAP 2025 Connect Test Date", type text}, {"Honor <BR> Courses", Int64.Type}, {"Dual <BR> Enrollment", Int64.Type}, {"IBC", type text}, {"Career Options", type text}, {"LEAP 2025 Test Date", type date}, {"ALG I Init LEAP", type text}, {"GEO Init LEAP", type text}, {"ENG I Init LEAP", type text}, {"ENG II Init LEAP", type text}, {"BIO Init LEAP", type text}, {"AM HIS Init LEAP", type text}, {"CIV Init LEAP", type text}, {"ALG I High LEAP", type text}, {"GEO High LEAP", type text}, {"ENG I High LEAP", type text}, {"ENG II High LEAP", type text}, {"BIO High LEAP", type text}, {"AM HIS High LEAP", type text}, {"CIV High LEAP", type text}, {"Math Pair Flag Passed Requirement", type text}, {"English Pair Flag Passed Requirement", type text}, {"History-Science Pair Flag Passed Requirement", type text}, {"Civics-Science Pair Flag Passed Requirement ", type text}, {"LEAP 2025 Grade 3-8 Test Date", type date}, {"ELA Init Connect", type text}, {"Math Init Connect", type text}, {"Sci Init Connect", type text}, {"ELA High Connect", type text}, {"Math High Connect", type text}, {"Sci High Connect", type text}, {"Plan Score", type text}, {"Explore Score", type text}, {"ACT - E", type text}, {"ACT - M", type text}, {"ACT - R", type text}, {"ACT - S", type text}, {"ACT Composite", type text}, {"WK – W-AM", type text}, {"WK – W-GL", type text}, {"WK – W-WD", type text}, {"WK - RFI", type text}, {"WK - LI", type text}, {"WK - AM", type text}, {"WK - Overall", type text}, {"WK 2.0 - Overall", type text}, {"BOY", type text}, {"MOY", type text}, {"EOY", type text}, {"ELPT", type text}, {"Clep Score", type text}, {"AP", type text}, {"AP Test Score", type text}, {"AP Exam Name", type text}, {"Community Service Hrs", type text}, {"Total Hours", Int64.Type}, {"Pre Engl", type text}, {"Pre Math", type text}, {"Pre Read", type text}, {"Pre Scie", type text}, {"Pre Comp", type text}, {"ASVAB", type text}, {"Acceptance", type text}})
in
  #"Changed column type";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "JCAMPUS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "caddo_tests"]}[Content],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", let latest = List.Max(#"Navigation 4"[Date modified]) in each [Date modified] = latest),
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Filtered rows", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 83, Encoding = 65001, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 83, Encoding = 65001, QuoteStyle = QuoteStyle.None])
in
  Source;
shared caddo_tests_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "caddo_tests", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
