[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"exporting tests" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"})
in
  #"Expanded Attributes";
[BindToDefaultDestination = true]
shared Client = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each ([Name] = "Client.csv")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"PersonalID", Int64.Type}, {"FirstName", type text}, {"MiddleName", type text}, {"LastName", type text}, {"NameSuffix", type text}, {"NameDataQuality", Int64.Type}, {"SSN", Int64.Type}, {"SSNDataQuality", Int64.Type}, {"DOB", type date}, {"DOBDataQuality", Int64.Type}, {"AmIndAKNative", Int64.Type}, {"Asian", Int64.Type}, {"BlackAfAmerican", Int64.Type}, {"HispanicLatinaeo", Int64.Type}, {"MidEastNAfrican", Int64.Type}, {"NativeHIPacific", Int64.Type}, {"White", Int64.Type}, {"RaceNone", type text}, {"AdditionalRaceEthnicity", type text}, {"Woman", Int64.Type}, {"Man", Int64.Type}, {"NonBinary", Int64.Type}, {"CulturallySpecific", Int64.Type}, {"Transgender", Int64.Type}, {"Questioning", Int64.Type}, {"DifferentIdentity", Int64.Type}, {"GenderNone", type text}, {"DifferentIdentityText", type text}, {"VeteranStatus", Int64.Type}, {"YearEnteredService", Int64.Type}, {"YearSeparated", Int64.Type}, {"WorldWarII", Int64.Type}, {"KoreanWar", Int64.Type}, {"VietnamWar", Int64.Type}, {"DesertStorm", Int64.Type}, {"AfghanistanOEF", Int64.Type}, {"IraqOIF", Int64.Type}, {"IraqOND", Int64.Type}, {"OtherTheater", Int64.Type}, {"MilitaryBranch", Int64.Type}, {"DischargeStatus", Int64.Type}, {"DateCreated", type datetime}, {"DateUpdated", type datetime}, {"UserID", Int64.Type}, {"DateDeleted", type text}, {"ExportID", Int64.Type}}),
  
  // ============ VA FY2026 TRANSFORMATIONS START ============
  
  // Fix HispanicLatinaeo column name (typo in source)
  #"Renamed HispanicLatinao" = Table.RenameColumns(#"Changed column type", {{"HispanicLatinaeo", "HispanicLatinao"}}),
  
  // 1. CREATE SEX FIELD (Convert Woman/Man to single Sex field)
  #"Added Sex" = Table.AddColumn(#"Renamed HispanicLatinao", "Sex", each 
    if [Man] = 1 then "1" 
    else if [Woman] = 1 then "0" 
    else "99", type text),
  
  // 2. FIX WHITE FIELD (Cannot be blank, must be 0 or 1)
  #"Fixed White" = Table.ReplaceValue(#"Added Sex", null, 0, Replacer.ReplaceValue, {"White"}),
  #"White to Text" = Table.TransformColumnTypes(#"Fixed White", {{"White", type text}}),
  #"White Replace Blank" = Table.ReplaceValue(#"White to Text", "", "0", Replacer.ReplaceValue, {"White"}),
  
  // 3. FIX HISPANICLATINAO (Cannot be blank)
  #"Fixed HispanicLatinao" = Table.ReplaceValue(#"White Replace Blank", null, 0, Replacer.ReplaceValue, {"HispanicLatinao"}),
  #"HispanicLatinao to Text" = Table.TransformColumnTypes(#"Fixed HispanicLatinao", {{"HispanicLatinao", type text}}),
  
  // 4. FIX MIDEASTNАFRICAN (Cannot be blank)
  #"Fixed MidEastNAfrican" = Table.ReplaceValue(#"HispanicLatinao to Text", null, 0, Replacer.ReplaceValue, {"MidEastNAfrican"}),
  #"MidEastNAfrican to Text" = Table.TransformColumnTypes(#"Fixed MidEastNAfrican", {{"MidEastNAfrican", type text}}),
  
  // 5. FIX RACENONE (Cannot be "0", must be "8", "9", "99", or blank)
  #"Added RaceNone_AnySelected" = Table.AddColumn(#"MidEastNAfrican to Text", "AnyRaceSelected", each 
    [AmIndAKNative] = 1 or [Asian] = 1 or [BlackAfAmerican] = 1 or 
    [NativeHIPacific] = 1 or [White] = "1" or [HispanicLatinao] = "1" or [MidEastNAfrican] = "1"),
  
  #"Fixed RaceNone" = Table.AddColumn(#"Added RaceNone_AnySelected", "RaceNone_Fixed", each 
    if [AnyRaceSelected] then "99"
    else if [RaceNone] = "0" or [RaceNone] = null or [RaceNone] = "" then "99"
    else [RaceNone], type text),
  
  // 6. FIX VETERANSTATUS (Required, cannot be blank)
  #"Fixed VeteranStatus" = Table.AddColumn(#"Fixed RaceNone", "VeteranStatus_Fixed", each 
    if [VeteranStatus] = null or Text.From([VeteranStatus]) = "1973" or Text.From([VeteranStatus]) = "2008" then "99"
    else Text.From([VeteranStatus]), type text),
  
  // 7. VALIDATE VETERAN YEARS (Must be 1920-2021 or blank)
  #"Fixed YearEnteredService" = Table.AddColumn(#"Fixed VeteranStatus", "YearEnteredService_Fixed", each 
    if [VeteranStatus_Fixed] <> "1" then null
    else if [YearEnteredService] = null then null
    else if [YearEnteredService] >= 1920 and [YearEnteredService] <= 2021 then [YearEnteredService]
    else null, Int64.Type),
  
  #"Fixed YearSeparated" = Table.AddColumn(#"Fixed YearEnteredService", "YearSeparated_Fixed", each 
    if [VeteranStatus_Fixed] <> "1" then null
    else if [YearSeparated] = null then null
    else if [YearSeparated] >= 1920 and [YearSeparated] <= 2021 then [YearSeparated]
    else null, Int64.Type),
  
  // 8. FIX THEATER FIELDS (Must be 0, 1, or blank when VeteranStatus = 1)
  #"Fixed WorldWarII" = Table.AddColumn(#"Fixed YearSeparated", "WorldWarII_Fixed", each 
    if [VeteranStatus_Fixed] <> "1" then null
    else if [WorldWarII] = 0 or [WorldWarII] = 1 then [WorldWarII]
    else null, Int64.Type),
  
  #"Fixed KoreanWar" = Table.AddColumn(#"Fixed WorldWarII", "KoreanWar_Fixed", each 
    if [VeteranStatus_Fixed] <> "1" then null
    else if [KoreanWar] = 0 or [KoreanWar] = 1 then [KoreanWar]
    else null, Int64.Type),
  
  #"Fixed VietnamWar" = Table.AddColumn(#"Fixed KoreanWar", "VietnamWar_Fixed", each 
    if [VeteranStatus_Fixed] <> "1" then null
    else if [VietnamWar] = 0 or [VietnamWar] = 1 then [VietnamWar]
    else null, Int64.Type),
  
  #"Fixed DesertStorm" = Table.AddColumn(#"Fixed VietnamWar", "DesertStorm_Fixed", each 
    if [VeteranStatus_Fixed] <> "1" then null
    else if [DesertStorm] = 0 or [DesertStorm] = 1 then [DesertStorm]
    else null, Int64.Type),
  
  #"Fixed AfghanistanOEF" = Table.AddColumn(#"Fixed DesertStorm", "AfghanistanOEF_Fixed", each 
    if [VeteranStatus_Fixed] <> "1" then null
    else if [AfghanistanOEF] = 0 or [AfghanistanOEF] = 1 then [AfghanistanOEF]
    else null, Int64.Type),
  
  #"Fixed IraqOIF" = Table.AddColumn(#"Fixed AfghanistanOEF", "IraqOIF_Fixed", each 
    if [VeteranStatus_Fixed] <> "1" then null
    else if [IraqOIF] = 0 or [IraqOIF] = 1 then [IraqOIF]
    else null, Int64.Type),
  
  #"Fixed IraqOND" = Table.AddColumn(#"Fixed IraqOIF", "IraqOND_Fixed", each 
    if [VeteranStatus_Fixed] <> "1" then null
    else if [IraqOND] = 0 or [IraqOND] = 1 then [IraqOND]
    else null, Int64.Type),
  
  #"Fixed OtherTheater" = Table.AddColumn(#"Fixed IraqOND", "OtherTheater_Fixed", each 
    if [VeteranStatus_Fixed] <> "1" then null
    else if [OtherTheater] = 0 or [OtherTheater] = 1 then [OtherTheater]
    else null, Int64.Type),
  
  // 9. FIX MILITARYBRANCH (Must be 1-6 or blank)
  #"Fixed MilitaryBranch" = Table.AddColumn(#"Fixed OtherTheater", "MilitaryBranch_Fixed", each 
    if [VeteranStatus_Fixed] <> "1" then null
    else if [MilitaryBranch] >= 1 and [MilitaryBranch] <= 6 then [MilitaryBranch]
    else null, Int64.Type),
  
  // 10. FIX DISCHARGESTATUS (Must be 1-6, 8-9, or blank)
  #"Fixed DischargeStatus" = Table.AddColumn(#"Fixed MilitaryBranch", "DischargeStatus_Fixed", each 
    if [VeteranStatus_Fixed] <> "1" then null
    else if [DischargeStatus] = null then null
    else if List.Contains({1,2,3,4,5,6,8,9}, [DischargeStatus]) then [DischargeStatus]
    else null, Int64.Type),
  
  // 11. FIX DATEDELETED (Optional, can be blank)
  #"Fixed DateDeleted" = Table.ReplaceValue(#"Fixed DischargeStatus", null, "", Replacer.ReplaceValue, {"DateDeleted"}),
  
  // 12. REMOVE OLD COLUMNS (Woman, Man, NonBinary, etc.)
  #"Removed Gender Columns" = Table.RemoveColumns(#"Fixed DateDeleted", {
    "Woman", "Man", "NonBinary", "CulturallySpecific", "Transgender", 
    "Questioning", "DifferentIdentity", "GenderNone", "DifferentIdentityText",
    "RaceNone", "VeteranStatus", "YearEnteredService", "YearSeparated",
    "WorldWarII", "KoreanWar", "VietnamWar", "DesertStorm", 
    "AfghanistanOEF", "IraqOIF", "IraqOND", "OtherTheater",
    "MilitaryBranch", "DischargeStatus", "AnyRaceSelected"
  }),
  
  // 13. RENAME FIXED COLUMNS (Remove _Fixed suffix)
  #"Renamed Fixed Columns" = Table.RenameColumns(#"Removed Gender Columns", {
    {"RaceNone_Fixed", "RaceNone"},
    {"VeteranStatus_Fixed", "VeteranStatus"},
    {"YearEnteredService_Fixed", "YearEnteredService"},
    {"YearSeparated_Fixed", "YearSeparated"},
    {"WorldWarII_Fixed", "WorldWarII"},
    {"KoreanWar_Fixed", "KoreanWar"},
    {"VietnamWar_Fixed", "VietnamWar"},
    {"DesertStorm_Fixed", "DesertStorm"},
    {"AfghanistanOEF_Fixed", "AfghanistanOEF"},
    {"IraqOIF_Fixed", "IraqOIF"},
    {"IraqOND_Fixed", "IraqOND"},
    {"OtherTheater_Fixed", "OtherTheater"},
    {"MilitaryBranch_Fixed", "MilitaryBranch"},
    {"DischargeStatus_Fixed", "DischargeStatus"}
  }),
  
  // 14. REORDER COLUMNS TO VA FY2026 SPEC (38 columns)
  #"Reordered Columns" = Table.ReorderColumns(#"Renamed Fixed Columns", {
    "PersonalID", "FirstName", "MiddleName", "LastName", "NameSuffix", "NameDataQuality",
    "SSN", "SSNDataQuality", "DOB", "DOBDataQuality", "Sex",
    "AmIndAKNative", "Asian", "BlackAfAmerican", "HispanicLatinao", "MidEastNAfrican",
    "NativeHIPacific", "White", "RaceNone", "AdditionalRaceEthnicity",
    "VeteranStatus", "YearEnteredService", "YearSeparated", "WorldWarII", "KoreanWar",
    "VietnamWar", "DesertStorm", "AfghanistanOEF", "IraqOIF", "IraqOND", "OtherTheater",
    "MilitaryBranch", "DischargeStatus", "DateCreated", "DateUpdated", "UserID",
    "DateDeleted", "ExportID"
  })
  
  // ============ VA FY2026 TRANSFORMATIONS END ============
in
  #"Reordered Columns";
[BindToDefaultDestination = true]
shared Disabilities = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each ([Name] = "Disabilities.csv")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (2)", each #"Transform file (2)"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (2)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (2)", Table.ColumnNames(#"Transform file (2)"(#"Sample file (2)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"DisabilitiesID", type text}, {"EnrollmentID", Int64.Type}, {"PersonalID", Int64.Type}, {"InformationDate", type date}, {"DisabilityType", Int64.Type}, {"DisabilityResponse", Int64.Type}, {"IndefiniteAndImpairs", Int64.Type}, {"TCellCountAvailable", type text}, {"TCellCount", type text}, {"TCellSource", type text}, {"ViralLoadAvailable", type text}, {"ViralLoad", type text}, {"ViralLoadSource", type text}, {"AntiRetroviral", type text}, {"DataCollectionStage", Int64.Type}, {"DateCreated", type datetime}, {"DateUpdated", type datetime}, {"UserID", Int64.Type}, {"DateDeleted", type text}, {"ExportID", Int64.Type}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared EmploymentEducation = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each ([Name] = "EmploymentEducation.csv")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (3)", each #"Transform file (3)"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (3)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (3)", Table.ColumnNames(#"Transform file (3)"(#"Sample file (3)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"EmploymentEducationID", type text}, {"EnrollmentID", Int64.Type}, {"PersonalID", Int64.Type}, {"InformationDate", type date}, {"LastGradeCompleted", Int64.Type}, {"SchoolStatus", type text}, {"Employed", Int64.Type}, {"EmploymentType", type text}, {"NotEmployedReason", Int64.Type}, {"DataCollectionStage", Int64.Type}, {"DateCreated", type datetime}, {"DateUpdated", type datetime}, {"UserID", Int64.Type}, {"DateDeleted", type text}, {"ExportID", Int64.Type}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared Enrollment = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each ([Name] = "Enrollment.csv")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (4)", each #"Transform file (4)"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (4)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (4)", Table.ColumnNames(#"Transform file (4)"(#"Sample file (4)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"EnrollmentID", Int64.Type}, {"PersonalID", Int64.Type}, {"ProjectID", Int64.Type}, {"EntryDate", type date}, {"HouseholdID", type text}, {"RelationshipToHoH", Int64.Type}, {"EnrollmentCoC", type text}, {"LivingSituation", Int64.Type}, {"RentalSubsidyType", type text}, {"LengthOfStay", Int64.Type}, {"LOSUnderThreshold", Int64.Type}, {"PreviousStreetESSH", type text}, {"DateToStreetESSH", type date}, {"TimesHomelessPastThreeYears", Int64.Type}, {"MonthsHomelessPastThreeYears", Int64.Type}, {"DisablingCondition", Int64.Type}, {"DateOfEngagement", type text}, {"MoveInDate", type date}, {"DateOfPATHStatus", type text}, {"ClientEnrolledInPATH", type text}, {"ReasonNotEnrolled", type text}, {"PercentAMI", Int64.Type}, {"ReferralSource", type text}, {"CountOutreachReferralApproaches", type text}, {"DateOfBCPStatus", type text}, {"EligibleForRHY", type text}, {"ReasonNoServices", type text}, {"RunawayYouth", type text}, {"SexualOrientation", type text}, {"SexualOrientationOther", type text}, {"FormerWardChildWelfare", type text}, {"ChildWelfareYears", type text}, {"ChildWelfareMonths", type text}, {"FormerWardJuvenileJustice", type text}, {"JuvenileJusticeYears", type text}, {"JuvenileJusticeMonths", type text}, {"UnemploymentFam", type text}, {"MentalHealthDisorderFam", type text}, {"PhysicalDisabilityFam", type text}, {"AlcoholDrugUseDisorderFam", type text}, {"InsufficientIncome", type text}, {"IncarceratedParent", type text}, {"VAMCStation", Int64.Type}, {"TargetScreenReqd", Int64.Type}, {"TimeToHousingLoss", type text}, {"AnnualPercentAMI", type text}, {"LiteralHomelessHistory", type text}, {"ClientLeaseholder", type text}, {"HOHLeaseholder", type text}, {"SubsidyAtRisk", type text}, {"EvictionHistory", type text}, {"CriminalRecord", type text}, {"IncarceratedAdult", type text}, {"PrisonDischarge", type text}, {"SexOffender", type text}, {"DisabledHoH", type text}, {"CurrentPregnant", type text}, {"SingleParent", type text}, {"DependentUnder6", type text}, {"HH5Plus", type text}, {"CoCPrioritized", type text}, {"HPScreeningScore", type text}, {"ThresholdScore", type text}, {"TranslationNeeded", type text}, {"PreferredLanguage", type text}, {"PreferredLanguageDifferent", type text}, {"DateCreated", type datetime}, {"DateUpdated", type datetime}, {"UserID", Int64.Type}, {"DateDeleted", type text}, {"ExportID", Int64.Type}}),
  
  // ============ VA FY2026 ENROLLMENT FIXES ============
  
  // Remove FY2026 fields that aren't in VA spec (these 3 don't belong):
  // - SexualOrientationOther
  // - AlcoholDrugUseDisorderFam  
  // - PreferredLanguageDifferent
  #"Removed FY2026 Fields" = Table.RemoveColumns(#"Changed column type", {
    "SexualOrientationOther", 
    "AlcoholDrugUseDisorderFam", 
    "PreferredLanguageDifferent"
  })
  
  // ============ END ENROLLMENT FIXES ============
in
  #"Removed FY2026 Fields";
[BindToDefaultDestination = true]
shared Export = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each ([Name] = "Export.csv")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (6)", each #"Transform file (6)"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (6)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (6)", Table.ColumnNames(#"Transform file (6)"(#"Sample file (6)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"ExportID", Int64.Type}, {"SourceType", Int64.Type}, {"SourceID", type text}, {"SourceName", type text}, {"SourceContactFirst", type text}, {"SourceContactLast", type text}, {"SourceContactPhone", type text}, {"SourceContactExtension", type text}, {"SourceContactEmail", type text}, {"ExportDate", type datetime}, {"ExportStartDate", type date}, {"ExportEndDate", type date}, {"SoftwareName", type text}, {"SoftwareVersion", type text}, {"CSVVersion", type text}, {"ExportPeriodType", Int64.Type}, {"ExportDirective", Int64.Type}, {"HashStatus", Int64.Type}, {"ImplementationID", type text}}),
  #"Replaced value" = Table.ReplaceValue(#"Changed column type", "2024 v1.6", "2024 v1.10", Replacer.ReplaceValue, {"CSVVersion"})
in
  #"Replaced value";
[BindToDefaultDestination = true]
shared Exit = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each ([Name] = "Exit.csv")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (7)", each #"Transform file (7)"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (7)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (7)", Table.ColumnNames(#"Transform file (7)"(#"Sample file (7)"))),
  #"Promoted headers" = Table.PromoteHeaders(#"Expanded table column", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers",{
    {"ExitID", type text},
    {"EnrollmentID", type text},
    {"PersonalID", type text},
    {"ExitDate", type date},
    {"Destination", Int64.Type},
    {"DestinationSubsidyType", Int64.Type},
    {"OtherDestination", type text},
    {"HousingAssessment", Int64.Type},
    {"SubsidyInformation", Int64.Type},
    {"ProjectCompletionStatus", Int64.Type},
    {"EarlyExitReason", Int64.Type},
    {"ExchangeForSex", Int64.Type},
    {"ExchangeForSexPastThreeMonths", Int64.Type},
    {"CountOfExchangeForSex", Int64.Type},
    {"AskedOrForcedToExchangeForSex", Int64.Type},
    {"AskedOrForcedToExchangeForSexPastThreeMonths", Int64.Type},
    {"WorkplaceViolenceThreats", Int64.Type},
    {"WorkplacePromiseDifference", Int64.Type},
    {"CoercedToContinueWork", Int64.Type},
    {"LaborExploitPastThreeMonths", Int64.Type},
    {"CounselingReceived", Int64.Type},
    {"IndividualCounseling", Int64.Type},
    {"FamilyCounseling", Int64.Type},
    {"GroupCounseling", Int64.Type},
    {"SessionCountAtExit", Int64.Type},
    {"PostExitCounselingPlan", Int64.Type},
    {"SessionsInPlan", Int64.Type},
    {"DestinationSafeClient", Int64.Type},
    {"DestinationSafeWorker", Int64.Type},
    {"PosAdultConnections", Int64.Type},
    {"PosPeerConnections", Int64.Type},
    {"PosCommunityConnections", Int64.Type},
    {"AftercareDate", type date},
    {"AftercareProvided", Int64.Type},
    {"EmailSocialMedia", Int64.Type},
    {"Telephone", Int64.Type},
    {"InPersonIndividual", Int64.Type},
    {"InPersonGroup", Int64.Type},
    {"CMExitReason", Int64.Type},
    {"DateCreated", type datetime},
    {"DateUpdated", type datetime},
    {"UserID", type text},
    {"DateDeleted", type datetime},
    {"ExportID", type text}
  })
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared Funder = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each ([Name] = "Funder.csv")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (8)", each #"Transform file (8)"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (8)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (8)", Table.ColumnNames(#"Transform file (8)"(#"Sample file (8)"))),
  #"Promoted headers" = Table.PromoteHeaders(#"Expanded table column", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers",{
    {"FunderID", type text},
    {"ProjectID", type text},
    {"Funder", type text},
    {"GrantID", type text},
    {"StartDate", type date},
    {"EndDate", type date},
    {"DateCreated", type datetime},
    {"DateUpdated", type datetime},
    {"UserID", type text},
    {"DateDeleted", type datetime},
    {"ExportID", type text}
  })
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared HealthAndDV = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each ([Name] = "HealthAndDV.csv")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (9)", each #"Transform file (9)"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (9)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (9)", Table.ColumnNames(#"Transform file (9)"(#"Sample file (9)"))),
  #"Promoted headers" = Table.PromoteHeaders(#"Expanded table column", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers",{
    {"HealthAndDVID", type text},
    {"EnrollmentID", type text},
    {"PersonalID", type text},
    {"InformationDate", type date},
    {"DomesticViolenceSurvivor", Int64.Type},
    {"WhenOccurred", Int64.Type},
    {"CurrentlyFleeing", Int64.Type},
    {"GeneralHealthStatus", Int64.Type},
    {"DentalHealthStatus", Int64.Type},
    {"MentalHealthStatus", Int64.Type},
    {"PregnancyStatus", Int64.Type},
    {"DueDate", type date},
    {"DataCollectionStage", Int64.Type},
    {"DateCreated", type datetime},
    {"DateUpdated", type datetime},
    {"UserID", type text},
    {"DateDeleted", type datetime},
    {"ExportID", type text}
  })
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared IncomeBenefits = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each ([Name] = "IncomeBenefits.csv")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (10)", each #"Transform file (10)"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (10)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (10)", Table.ColumnNames(#"Transform file (10)"(#"Sample file (10)"))),
  #"Promoted headers" = Table.PromoteHeaders(#"Expanded table column", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers",{
    {"IncomeBenefitsID", type text},
    {"EnrollmentID", type text},
    {"PersonalID", type text},
    {"InformationDate", type date},
    {"IncomeFromAnySource", Int64.Type},
    {"TotalMonthlyIncome", type number},
    {"Earned", Int64.Type},
    {"EarnedAmount", type number},
    {"Unemployment", Int64.Type},
    {"UnemploymentAmount", type number},
    {"SSI", Int64.Type},
    {"SSIAmount", type number},
    {"SSDI", Int64.Type},
    {"SSDIAmount", type number},
    {"VADisabilityService", Int64.Type},
    {"VADisabilityServiceAmount", type number},
    {"VADisabilityNonService", Int64.Type},
    {"VADisabilityNonServiceAmount", type number},
    {"PrivateDisability", Int64.Type},
    {"PrivateDisabilityAmount", type number},
    {"WorkersComp", Int64.Type},
    {"WorkersCompAmount", type number},
    {"TANF", Int64.Type},
    {"TANFAmount", type number},
    {"GA", Int64.Type},
    {"GAAmount", type number},
    {"SocSecRetirement", Int64.Type},
    {"SocSecRetirementAmount", type number},
    {"Pension", Int64.Type},
    {"PensionAmount", type number},
    {"ChildSupport", Int64.Type},
    {"ChildSupportAmount", type number},
    {"Alimony", Int64.Type},
    {"AlimonyAmount", type number},
    {"OtherIncomeSource", Int64.Type},
    {"OtherIncomeAmount", type number},
    {"OtherIncomeSourceIdentify", type text},
    {"BenefitsFromAnySource", Int64.Type},
    {"SNAP", Int64.Type},
    {"WIC", Int64.Type},
    {"TANFChildCare", Int64.Type},
    {"TANFTransportation", Int64.Type},
    {"OtherTANF", Int64.Type},
    {"OtherBenefitsSource", Int64.Type},
    {"OtherBenefitsSourceIdentify", type text},
    {"InsuranceFromAnySource", Int64.Type},
    {"Medicaid", Int64.Type},
    {"NoMedicaidReason", Int64.Type},
    {"Medicare", Int64.Type},
    {"NoMedicareReason", Int64.Type},
    {"SCHIP", Int64.Type},
    {"NoSCHIPReason", Int64.Type},
    {"VHAServices", Int64.Type},
    {"NoVHAReason", Int64.Type},
    {"EmployerProvided", Int64.Type},
    {"NoEmployerProvidedReason", Int64.Type},
    {"COBRA", Int64.Type},
    {"NoCOBRAReason", Int64.Type},
    {"PrivatePay", Int64.Type},
    {"NoPrivatePayReason", Int64.Type},
    {"StateHealthIns", Int64.Type},
    {"NoStateHealthInsReason", Int64.Type},
    {"IndianHealthServices", Int64.Type},
    {"NoIndianHealthServicesReason", Int64.Type},
    {"OtherInsurance", Int64.Type},
    {"OtherInsuranceIdentify", type text},
    {"ADAP", Int64.Type},
    {"NoADAPReason", Int64.Type},
    {"RyanWhiteMedDent", Int64.Type},
    {"NoRyanWhiteReason", Int64.Type},
    {"ConnectionWithSOAR", Int64.Type},
    {"DataCollectionStage", Int64.Type},
    {"DateCreated", type datetime},
    {"DateUpdated", type datetime},
    {"UserID", type text},
    {"DateDeleted", type datetime},
    {"ExportID", type text}
  })
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared Project = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each ([Name] = "Project.csv")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (11)", each #"Transform file (11)"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (11)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (11)", Table.ColumnNames(#"Transform file (11)"(#"Sample file (11)"))),
  #"Promoted headers" = Table.PromoteHeaders(#"Expanded table column", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers",{
    {"ProjectID", type text},
    {"OrganizationID", type text},
    {"ProjectName", type text},
    {"ProjectCommonName", type text},
    {"OperatingStartDate", type date},
    {"OperatingEndDate", type date},
    {"ContinuumProject", Int64.Type},
    {"ProjectType", Int64.Type},
    {"HousingType", Int64.Type},
    {"RRHSubType", Int64.Type},
    {"ResidentialAffiliation", Int64.Type},
    {"TargetPopulation", Int64.Type},
    {"HOPWAMedAssistedLivingFac", Int64.Type},
    {"DateCreated", type datetime},
    {"DateUpdated", type datetime},
    {"UserID", type text},
    {"DateDeleted", type datetime},
    {"ExportID", type text}
  })
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared ProjectCoC = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each ([Name] = "ProjectCoC.csv")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (12)", each #"Transform file (12)"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (12)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (12)", Table.ColumnNames(#"Transform file (12)"(#"Sample file (12)"))),
  #"Promoted headers" = Table.PromoteHeaders(#"Expanded table column", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers",{
    {"ProjectCoCID", type text},
    {"ProjectID", type text},
    {"CoCCode", type text},
    {"Geocode", type text},
    {"Address1", type text},
    {"Address2", type text},
    {"City", type text},
    {"State", type text},
    {"ZIP", type text},
    {"GeographyType", Int64.Type},
    {"DateCreated", type datetime},
    {"DateUpdated", type datetime},
    {"UserID", type text},
    {"DateDeleted", type datetime},
    {"ExportID", type text}
  })
in
  #"Changed column type";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each [Name] = "Client.csv"),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 46, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 46, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"Sample file (2)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each [Name] = "Disabilities.csv"),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (2)" = let
  #"Parameter (2)" = #"Sample file (2)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (2)"]
in
  #"Parameter (2)";
shared #"Transform Sample file (2)" = let
  Source = Csv.Document(#"Parameter (2)", [Delimiter = ",", Columns = 20, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (2)""}"]
shared #"Transform file (2)" = (#"Parameter (2)" as binary) => let
  Source = Csv.Document(#"Parameter (2)", [Delimiter = ",", Columns = 20, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"Sample file (3)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each [Name] = "EmploymentEducation.csv"),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (3)" = let
  #"Parameter (3)" = #"Sample file (3)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (3)"]
in
  #"Parameter (3)";
shared #"Transform Sample file (3)" = let
  Source = Csv.Document(#"Parameter (3)", [Delimiter = ",", Columns = 15, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (3)""}"]
shared #"Transform file (3)" = (#"Parameter (3)" as binary) => let
  Source = Csv.Document(#"Parameter (3)", [Delimiter = ",", Columns = 15, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"Sample file (4)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each [Name] = "Enrollment.csv"),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (4)" = let
  #"Parameter (4)" = #"Sample file (4)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (4)"]
in
  #"Parameter (4)";
shared #"Transform Sample file (4)" = let
  Source = Csv.Document(#"Parameter (4)", [Delimiter = ",", Columns = 71, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (4)""}"]
shared #"Transform file (4)" = (#"Parameter (4)" as binary) => let
  Source = Csv.Document(#"Parameter (4)", [Delimiter = ",", Columns = 71, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"Sample file (5)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each [Name] = "Exit.csv"),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (5)" = let
  #"Parameter (5)" = #"Sample file (5)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (5)"]
in
  #"Parameter (5)";
shared #"Transform Sample file (5)" = let
  Source = Csv.Document(#"Parameter (5)", [Delimiter = ",", Columns = 44, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (5)""}"]
shared #"Transform file (5)" = (#"Parameter (5)" as binary) => let
  Source = Csv.Document(#"Parameter (5)", [Delimiter = ",", Columns = 44, QuoteStyle = QuoteStyle.None])
in
  Source;
shared #"Sample file (6)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each [Name] = "Export.csv"),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (6)" = let
  #"Parameter (6)" = #"Sample file (6)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (6)"]
in
  #"Parameter (6)";
shared #"Transform Sample file (6)" = let
  Source = Csv.Document(#"Parameter (6)", [Delimiter = ",", Columns = 19, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (6)""}"]
shared #"Transform file (6)" = (#"Parameter (6)" as binary) => let
  Source = Csv.Document(#"Parameter (6)", [Delimiter = ",", Columns = 19, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"Sample file (7)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each [Name] = "Exit.csv"),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (7)" = let
  #"Parameter (7)" = #"Sample file (7)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (7)"]
in
  #"Parameter (7)";
shared #"Transform Sample file (7)" = let
  Source = Csv.Document(#"Parameter (7)", [Delimiter = ",", Columns = 44, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (7)""}"]
shared #"Transform file (7)" = (#"Parameter (7)" as binary) => let
  Source = Csv.Document(#"Parameter (7)", [Delimiter = ",", Columns = 44, QuoteStyle = QuoteStyle.None])
in
  Source;
shared #"Sample file (8)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each [Name] = "Funder.csv"),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (8)" = let
  #"Parameter (8)" = #"Sample file (8)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (8)"]
in
  #"Parameter (8)";
shared #"Transform Sample file (8)" = let
  Source = Csv.Document(#"Parameter (8)", [Delimiter = ",", Columns = 12, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (8)""}"]
shared #"Transform file (8)" = (#"Parameter (8)" as binary) => let
  Source = Csv.Document(#"Parameter (8)", [Delimiter = ",", Columns = 12, QuoteStyle = QuoteStyle.None])
in
  Source;
shared #"Sample file (9)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each [Name] = "HealthAndDV.csv"),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (9)" = let
  #"Parameter (9)" = #"Sample file (9)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (9)"]
in
  #"Parameter (9)";
shared #"Transform Sample file (9)" = let
  Source = Csv.Document(#"Parameter (9)", [Delimiter = ",", Columns = 18, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (9)""}"]
shared #"Transform file (9)" = (#"Parameter (9)" as binary) => let
  Source = Csv.Document(#"Parameter (9)", [Delimiter = ",", Columns = 18, QuoteStyle = QuoteStyle.None])
in
  Source;
shared #"Sample file (10)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each [Name] = "IncomeBenefits.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (10)" = let
  #"Parameter (10)" = #"Sample file (10)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (10)"]
in
  #"Parameter (10)";
shared #"Transform Sample file (10)" = let
  Source = Csv.Document(#"Parameter (10)", [Delimiter = ",", Columns = 77, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (10)""}"]
shared #"Transform file (10)" = (#"Parameter (10)" as binary) => let
  Source = Csv.Document(#"Parameter (10)", [Delimiter = ",", Columns = 77, QuoteStyle = QuoteStyle.None])
in
  Source;
shared #"Sample file (11)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each [Name] = "Project.csv"),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (11)" = let
  #"Parameter (11)" = #"Sample file (11)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (11)"]
in
  #"Parameter (11)";
shared #"Transform Sample file (11)" = let
  Source = Csv.Document(#"Parameter (11)", [Delimiter = ",", Columns = 19, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (11)""}"]
shared #"Transform file (11)" = (#"Parameter (11)" as binary) => let
  Source = Csv.Document(#"Parameter (11)", [Delimiter = ",", Columns = 19, QuoteStyle = QuoteStyle.None])
in
  Source;
[BindToDefaultDestination = true]
shared Services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each ([Name] = "Services.csv")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (13)", each #"Transform file (13)"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (13)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (13)", Table.ColumnNames(#"Transform file (13)"(#"Sample file (13)"))),
  #"Promoted headers" = Table.PromoteHeaders(#"Expanded table column", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers",{
    {"ServicesID", type text},
    {"EnrollmentID", type text},
    {"PersonalID", type text},
    {"DateProvided", type date},
    {"RecordType", Int64.Type},
    {"TypeProvided", Int64.Type},
    {"OtherTypeProvided", type text},
    {"MovingOnOtherType", type text},
    {"SubTypeProvided", Int64.Type},
    {"FAAmount", type number},
    {"FAStartDate", type date},
    {"FAEndDate", type date},
    {"ReferralOutcome", Int64.Type},
    {"DateCreated", type datetime},
    {"DateUpdated", type datetime},
    {"UserID", type text},
    {"DateDeleted", type datetime},
    {"ExportID", type text}
  })
in
  #"Changed column type";
shared #"Sample file (12)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each [Name] = "ProjectCoC.csv"),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (12)" = let
  #"Parameter (12)" = #"Sample file (12)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (12)"]
in
  #"Parameter (12)";
shared #"Transform Sample file (12)" = let
  Source = Csv.Document(#"Parameter (12)", [Delimiter = ",", Columns = 15, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (12)""}"]
shared #"Transform file (12)" = (#"Parameter (12)" as binary) => let
  Source = Csv.Document(#"Parameter (12)", [Delimiter = ",", Columns = 15, QuoteStyle = QuoteStyle.None])
in
  Source;
shared #"Sample file (13)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each [Name] = "Services.csv"),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (13)" = let
  #"Parameter (13)" = #"Sample file (13)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (13)"]
in
  #"Parameter (13)";
shared #"Transform Sample file (13)" = let
  Source = Csv.Document(#"Parameter (13)", [Delimiter = ",", Columns = 18, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (13)""}"]
shared #"Transform file (13)" = (#"Parameter (13)" as binary) => let
  Source = Csv.Document(#"Parameter (13)", [Delimiter = ",", Columns = 18, QuoteStyle = QuoteStyle.None])
in
  Source;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data];
