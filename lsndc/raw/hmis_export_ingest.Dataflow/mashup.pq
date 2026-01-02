[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared Client = let
    Source = Lakehouse.Contents(null),
    Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
    #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
    #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
    #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
    #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],

    #"Expanded Attributes" =
        Table.ExpandRecordColumn(
            #"Navigation 4",
            "Attributes",
            {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"},
            {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}
        ),

    #"Expanded Content" =
        Table.ExpandTableColumn(
            #"Expanded Attributes",
            "Content",
            {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"},
            {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}
        ),

    #"Filtered rows" =
        Table.SelectRows(
            #"Expanded Content",
            each [Name.1] = "Client.csv"
        ),

    #"Filtered hidden files" =
        Table.SelectRows(
            #"Filtered rows",
            each [Attributes]?[Hidden]? <> true
        ),

    #"Invoke custom function" =
        Table.AddColumn(
            #"Filtered hidden files",
            "Transform file (2)",
            each #"Transform file (2)"([Content.1])
        ),

    #"Removed other columns" =
        Table.SelectColumns(
            #"Invoke custom function",
            {"Transform file (2)"},
            MissingField.Ignore
        ),

    #"Expanded table column" =
        Table.ExpandTableColumn(
            #"Removed other columns",
            "Transform file (2)",
            Table.ColumnNames(#"Transform file (2)"(#"Sample file (2)"))
        ),

    // Everything to text first so PromoteHeaders is safe
    #"Changed column type" =
        Table.TransformColumnTypes(
            #"Expanded table column",
            List.Transform({1..46}, each {"Column" & Text.From(_), type text})
        ),

    #"Promoted headers" =
        Table.PromoteHeaders(
            #"Changed column type",
            [PromoteAllScalars = true]
        ),

    // ***** THIS IS THE IMPORTANT PART: final 46 columns in the exact order you want *****
    ClientColumnsToKeep = {
        "PersonalID",
        "FirstName",
        "MiddleName",
        "LastName",
        "NameSuffix",
        "NameDataQuality",
        "SSN",
        "SSNDataQuality",
        "DOB",
        "DOBDataQuality",
        "AmIndAKNative",
        "Asian",
        "BlackAfAmerican",
        "HispanicLatinaeo",
        "MidEastNAfrican",
        "NativeHIPacific",
        "White",
        "RaceNone",
        "AdditionalRaceEthnicity",
        "Woman",
        "Man",
        "NonBinary",
        "CulturallySpecific",
        "Transgender",
        "Questioning",
        "DifferentIdentity",
        "GenderNone",
        "DifferentIdentityText",
        "VeteranStatus",
        "YearEnteredService",
        "YearSeparated",
        "WorldWarII",
        "KoreanWar",
        "VietnamWar",
        "DesertStorm",
        "AfghanistanOEF",
        "IraqOIF",
        "IraqOND",
        "OtherTheater",
        "MilitaryBranch",
        "DischargeStatus",
        "DateCreated",
        "DateUpdated",
        "UserID",
        "DateDeleted",
        "ExportID"
    },

    #"ClientColumnsTrimmed" =
        Table.SelectColumns(
            #"Promoted headers",
            ClientColumnsToKeep,
            MissingField.Ignore
        ),

    // ---- Clean numeric vet fields ----
    #"Cleaned Veteran Fields" =
        Table.TransformColumns(
            #"ClientColumnsTrimmed",
            {
                {
                    "YearEnteredService",
                    each
                        let
                            vText = if _ = null then null else Text.Trim(Text.From(_)),
                            vNum  = if vText = null or vText = "" then null else try Number.From(vText) otherwise null
                        in
                            if vNum = null or vNum = 0 then null else vNum,
                    type nullable number
                },
                {
                    "YearSeparated",
                    each
                        let
                            vText = if _ = null then null else Text.Trim(Text.From(_)),
                            vNum  = if vText = null or vText = "" then null else try Number.From(vText) otherwise null
                        in
                            if vNum = null or vNum = 0 then null else vNum,
                    type nullable number
                },
                {
                    "WorldWarII",
                    each
                        let
                            vText = if _ = null then null else Text.Trim(Text.From(_)),
                            vNum  = if vText = null or vText = "" then null else try Number.From(vText) otherwise null
                        in
                            if vNum = null or vNum = 0 then null else vNum,
                    type nullable number
                },
                {
                    "KoreanWar",
                    each
                        let
                            vText = if _ = null then null else Text.Trim(Text.From(_)),
                            vNum  = if vText = null or vText = "" then null else try Number.From(vText) otherwise null
                        in
                            if vNum = null or vNum = 0 then null else vNum,
                    type nullable number
                },
                {
                    "VietnamWar",
                    each
                        let
                            vText = if _ = null then null else Text.Trim(Text.From(_)),
                            vNum  = if vText = null or vText = "" then null else try Number.From(vText) otherwise null
                        in
                            if vNum = null or vNum = 0 then null else vNum,
                    type nullable number
                },
                {
                    "DesertStorm",
                    each
                        let
                            vText = if _ = null then null else Text.Trim(Text.From(_)),
                            vNum  = if vText = null or vText = "" then null else try Number.From(vText) otherwise null
                        in
                            if vNum = null or vNum = 0 then null else vNum,
                    type nullable number
                },
                {
                    "AfghanistanOEF",
                    each
                        let
                            vText = if _ = null then null else Text.Trim(Text.From(_)),
                            vNum  = if vText = null or vText = "" then null else try Number.From(vText) otherwise null
                        in
                            if vNum = null or vNum = 0 then null else vNum,
                    type nullable number
                },
                {
                    "IraqOIF",
                    each
                        let
                            vText = if _ = null then null else Text.Trim(Text.From(_)),
                            vNum  = if vText = null or vText = "" then null else try Number.From(vText) otherwise null
                        in
                            if vNum = null or vNum = 0 then null else vNum,
                    type nullable number
                },
                {
                    "IraqOND",
                    each
                        let
                            vText = if _ = null then null else Text.Trim(Text.From(_)),
                            vNum  = if vText = null or vText = "" then null else try Number.From(vText) otherwise null
                        in
                            if vNum = null then null else if vNum > 99 then null else vNum,
                    type nullable number
                },
                {
                    "OtherTheater",
                    each
                        let
                            vText = if _ = null then null else Text.Trim(Text.From(_)),
                            vNum  = if vText = null or vText = "" then null else try Number.From(vText) otherwise null
                        in
                            if vNum = null then null else if vNum > 99 then null else vNum,
                    type nullable number
                },
                {
                    "MilitaryBranch",
                    each
                        let
                            vText = if _ = null then null else Text.Trim(Text.From(_)),
                            vNum  = if vText = null or vText = "" then null else try Number.From(vText) otherwise null
                        in
                            if vNum = null or vNum = 0 then null else vNum,
                    type nullable number
                },
                {
                    "DischargeStatus",
                    each
                        let
                            vText = if _ = null then null else Text.Trim(Text.From(_)),
                            vNum  = if vText = null or vText = "" then null else try Number.From(vText) otherwise null
                        in
                            if vNum = null or vNum = 0 then null else vNum,
                    type nullable number
                }
            }
        ),

    // Dates
    #"Formatted Dates" =
        Table.TransformColumns(
            #"Cleaned Veteran Fields",
            {
                {
                    "DOB",
                    each
                        let
                            vText = if _ = null then null else Text.Trim(Text.From(_)),
                            vDate = if vText = null or vText = "" then null else try Date.From(vText) otherwise null
                        in
                            if vDate = null then null else Date.ToText(vDate, "yyyy-MM-dd"),
                    type text
                },
                {
                    "DateCreated",
                    each
                        let
                            vText = if _ = null then null else Text.Trim(Text.From(_)),
                            vDT   = if vText = null or vText = "" then null else try DateTime.From(vText) otherwise null
                        in
                            if vDT = null then null else DateTime.ToText(vDT, "yyyy-MM-dd HH:mm:ss"),
                    type text
                },
                {
                    "DateUpdated",
                    each
                        let
                            vText = if _ = null then null else Text.Trim(Text.From(_)),
                            vDT   = if vText = null or vText = "" then null else try DateTime.From(vText) otherwise null
                        in
                            if vDT = null then null else DateTime.ToText(vDT, "yyyy-MM-dd HH:mm:ss"),
                    type text
                },
                {
                    "DateDeleted",
                    each
                        let
                            vText = if _ = null then null else Text.Trim(Text.From(_)),
                            vDT   = if vText = null or vText = "" then null else try DateTime.From(vText) otherwise null
                        in
                            if vDT = null then null else DateTime.ToText(vDT, "yyyy-MM-dd HH:mm:ss"),
                    type text
                }
            }
        ),

    // White cannot be null
    #"Filled White" =
        Table.ReplaceValue(
            #"Formatted Dates",
            null,
            0,
            Replacer.ReplaceValue,
            {"White"}
        ),

    // IDs / ExportID as text
    #"Fixed IDs" =
        Table.TransformColumns(
            #"Filled White",
            {
                {"PersonalID", each if _ = null then null else Text.From(_), type text},
                {"SSN",        each if _ = null then null else Text.From(_), type text},
                {
                    "UserID",
                    each
                        let
                            vText = if _ = null then "" else Text.Trim(Text.From(_))
                        in
                            if vText = "" then "1" else vText,
                    type text
                },
                {
                    "ExportID",
                    each "377",
                    type text
                }
            }
        ),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Fixed IDs", {{"YearEnteredService", type text}, {"YearSeparated", type text}}),
  #"Replaced value" = Table.ReplaceValue(#"Changed column type 1", 0, null, Replacer.ReplaceValue, {"IraqOND"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", 0, null, Replacer.ReplaceValue, {"OtherTheater"})

in
    #"Replaced value 1";
[BindToDefaultDestination = true]
shared Disabilities = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each ([Name.1] = "Disabilities.csv"))
in
  #"Filtered rows";
[BindToDefaultDestination = true]
shared EmploymentEducation = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each ([Name.1] = "EmploymentEducation.csv"))
in
  #"Filtered rows";
[BindToDefaultDestination = true]
shared Enrollment = let
    Source = Lakehouse.Contents(null),
    Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
    #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
    #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
    #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
    #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],

    #"Expanded Attributes" =
        Table.ExpandRecordColumn(
            #"Navigation 4",
            "Attributes",
            {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"},
            {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}
        ),

    #"Expanded Content" =
        Table.ExpandTableColumn(
            #"Expanded Attributes",
            "Content",
            {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"},
            {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}
        ),

    #"Filtered rows" =
        Table.SelectRows(
            #"Expanded Content",
            each [Name.1] = "Enrollment.csv"
        ),

    #"Filtered hidden files" =
        Table.SelectRows(
            #"Filtered rows",
            each [Attributes]?[Hidden]? <> true
        ),

    #"Invoke custom function" =
        Table.AddColumn(
            #"Filtered hidden files",
            "Transform file (3)",
            each #"Transform file (3)"([Content.1])
        ),

    #"Removed other columns" =
        Table.SelectColumns(
            #"Invoke custom function",
            {"Transform file (3)"}
        ),

    #"Expanded table column" =
        Table.ExpandTableColumn(
            #"Removed other columns",
            "Transform file (3)",
            Table.ColumnNames(#"Transform file (3)"(#"Sample file (3)"))
        ),

    // Type the columns – IDs as text, codes mostly as Int64, dates as date/datetime
    #"Typed columns" =
        Table.TransformColumnTypes(
            #"Expanded table column",
            {
                {"EnrollmentID",           type text},
                {"PersonalID",             type text},
                {"ProjectID",              type text},
                {"EntryDate",              type date},
                {"HouseholdID",            type text},
                {"RelationshipToHoH",      Int64.Type},
                {"EnrollmentCoC",          type text},
                {"LivingSituation",        Int64.Type},
                {"RentalSubsidyType",      type text},
                {"LengthOfStay",           Int64.Type},
                {"LOSUnderThreshold",      Int64.Type},
                {"PreviousStreetESSH",     type text},
                {"DateToStreetESSH",       type date},
                {"TimesHomelessPastThreeYears", Int64.Type},
                {"MonthsHomelessPastThreeYears", Int64.Type},
                {"DisablingCondition",     Int64.Type},
                {"DateOfEngagement",       type text},
                {"MoveInDate",             type date},
                {"DateOfPATHStatus",       type text},
                {"ClientEnrolledInPATH",   type text},
                {"ReasonNotEnrolled",      type text},
                {"PercentAMI",             Int64.Type},
                {"ReferralSource",         type text},
                {"CountOutreachReferralApproaches", type text},
                {"DateOfBCPStatus",        type text},
                {"EligibleForRHY",         type text},
                {"ReasonNoServices",       type text},
                {"RunawayYouth",           type text},
                {"SexualOrientation",      type text},
                {"SexualOrientationOther", type text},
                {"FormerWardChildWelfare", type text},
                {"ChildWelfareYears",      type text},
                {"ChildWelfareMonths",     type text},
                {"FormerWardJuvenileJustice", type text},
                {"JuvenileJusticeYears",   type text},
                {"JuvenileJusticeMonths",  type text},
                {"UnemploymentFam",        type text},
                {"MentalHealthDisorderFam", type text},
                {"PhysicalDisabilityFam",  type text},
                {"AlcoholDrugUseDisorderFam", type text},
                {"InsufficientIncome",     type text},
                {"IncarceratedParent",     type text},
                {"VAMCStation",            Int64.Type},
                {"TargetScreenReqd",       Int64.Type},
                {"TimeToHousingLoss",      type text},
                {"AnnualPercentAMI",       type text},
                {"LiteralHomelessHistory", type text},
                {"ClientLeaseholder",      type text},
                {"HOHLeaseholder",         type text},
                {"SubsidyAtRisk",          type text},
                {"EvictionHistory",        type text},
                {"CriminalRecord",         type text},
                {"IncarceratedAdult",      type text},
                {"PrisonDischarge",        type text},
                {"SexOffender",            type text},
                {"DisabledHoH",            type text},
                {"CurrentPregnant",        type text},
                {"SingleParent",           type text},
                {"DependentUnder6",        type text},
                {"HH5Plus",                type text},
                {"CoCPrioritized",         type text},
                {"HPScreeningScore",       type text},
                {"ThresholdScore",         type text},
                {"TranslationNeeded",      type text},
                {"PreferredLanguage",      type text},
                {"PreferredLanguageDifferent", type text},
                {"DateCreated",            type datetime},
                {"DateUpdated",            type datetime},
                {"UserID",                 type text},
                {"DateDeleted",            type text},
                {"ExportID",               type text}
            },
            "en-US"
        ),
  #"Replaced value" = Table.ReplaceValue(#"Typed columns", "", "AR-503", Replacer.ReplaceValue, {"EnrollmentCoC"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", null, 0, Replacer.ReplaceValue, {"LOSUnderThreshold"}),
  #"Renamed columns" = Table.RenameColumns(#"Replaced value 1", {{"EligibleForRHY", "EligibleforRHY"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"ExportID", type text}})
in
    #"Changed column type";
[BindToDefaultDestination = true]
shared Exit = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each ([Name.1] = "Exit.csv")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (20)", each #"Transform file (20)"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (20)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (20)", Table.ColumnNames(#"Transform file (20)"(#"Sample file (20)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Column1", type text}, {"Column2", type text}, {"Column3", type text}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", type text}, {"Column8", type text}, {"Column9", type text}, {"Column10", type text}, {"Column11", type text}, {"Column12", type text}, {"Column13", type text}, {"Column14", type text}, {"Column15", type text}, {"Column16", type text}, {"Column17", type text}, {"Column18", type text}, {"Column19", type text}, {"Column20", type text}, {"Column21", type text}, {"Column22", type text}, {"Column23", type text}, {"Column24", type text}, {"Column25", type text}, {"Column26", type text}, {"Column27", type text}, {"Column28", type text}, {"Column29", type text}, {"Column30", type text}, {"Column31", type text}, {"Column32", type text}, {"Column33", type text}, {"Column34", type text}, {"Column35", type text}, {"Column36", type text}, {"Column37", type text}, {"Column38", type text}, {"Column39", type text}, {"Column40", type text}, {"Column41", type text}, {"Column42", type text}, {"Column43", type text}, {"Column44", type text}}),
  #"Promoted headers" = Table.PromoteHeaders(#"Changed column type", [PromoteAllScalars = true])
in
  #"Promoted headers";
[BindToDefaultDestination = true]
shared Export = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" =
      Table.ExpandRecordColumn(
          #"Navigation 4",
          "Attributes",
          {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"},
          {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}
      ),
  #"Expanded Content" =
      Table.ExpandTableColumn(
          #"Expanded Attributes",
          "Content",
          {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"},
          {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}
      ),
  #"Filtered rows" =
      Table.SelectRows(
          #"Expanded Content",
          each [Name.1] = "Export.csv"
      ),
  #"Filtered hidden files" =
      Table.SelectRows(
          #"Filtered rows",
          each [Attributes]?[Hidden]? <> true
      ),
  #"Invoke custom function" =
      Table.AddColumn(
          #"Filtered hidden files",
          "Transform file (19)",
          each #"Transform file (19)"([Content.1])
      ),
  #"Removed other columns" =
      Table.SelectColumns(
          #"Invoke custom function",
          {"Transform file (19)"}
      ),
  #"Expanded table column" =
      Table.ExpandTableColumn(
          #"Removed other columns",
          "Transform file (19)",
          Table.ColumnNames(#"Transform file (19)"(#"Sample file (19)"))
      ),

  // Basic typing – treat ExportID as text, dates as date/datetime
  #"Changed column type" =
      Table.TransformColumnTypes(
          #"Expanded table column",
          {
              {"ExportID", type text},
              {"SourceType", Int64.Type},
              {"SourceID", type text},
              {"SourceName", type text},
              {"SourceContactFirst", type text},
              {"SourceContactLast", type text},
              {"SourceContactPhone", type text},
              {"SourceContactExtension", type text},
              {"SourceContactEmail", type text},
              {"ExportDate", type datetime},
              {"ExportStartDate", type date},
              {"ExportEndDate", type date},
              {"SoftwareName", type text},
              {"SoftwareVersion", type text},
              {"CSVVersion", type text},
              {"ExportPeriodType", Int64.Type},
              {"ExportDirective", Int64.Type},
              {"HashStatus", Int64.Type},
              {"ImplementationID", type text}
          },
          "en-US"
      ),

  // 🔹 Make ExportEndDate use the same calendar day as ExportDate
  #"EndDate from ExportDate" =
      let
          Added =
              Table.AddColumn(
                  #"Changed column type",
                  "ExportEndDate_fixed",
                  each Date.From([ExportDate]),
                  type date
              ),
          RemovedOld =
              Table.RemoveColumns(Added, {"ExportEndDate"}),
          Renamed =
              Table.RenameColumns(RemovedOld, {{"ExportEndDate_fixed", "ExportEndDate"}})
      in
          Renamed,

  // 🔹 Format ExportDate as datetime text, Start/End as date text (yyyy-MM-dd)
  #"Formatted Export Dates" =
      Table.TransformColumns(
          #"EndDate from ExportDate",
          {
              {"ExportDate",      each DateTime.ToText(_, "yyyy-MM-dd HH:mm:ss"), type text},
              {"ExportStartDate", each Date.ToText(_, "yyyy-MM-dd"), type text},
              {"ExportEndDate",   each Date.ToText(_, "yyyy-MM-dd"), type text}
          }
      )

in
  #"Formatted Export Dates";
[BindToDefaultDestination = true]
shared Funder = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each ([Name.1] = "Funder.csv")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (18)", each #"Transform file (18)"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (18)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (18)", Table.ColumnNames(#"Transform file (18)"(#"Sample file (18)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"FunderID", Int64.Type}, {"ProjectID", Int64.Type}, {"Funder", Int64.Type}, {"OtherFunder", type text}, {"GrantID", type text}, {"StartDate", type date}, {"EndDate", type date}, {"DateCreated", type datetime}, {"DateUpdated", type datetime}, {"UserID", Int64.Type}, {"DateDeleted", type text}, {"ExportID", Int64.Type}})
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
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each ([Name.1] = "HealthAndDV.csv")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (17)", each #"Transform file (17)"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (17)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (17)", Table.ColumnNames(#"Transform file (17)"(#"Sample file (17)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"HealthAndDVID", type text}, {"EnrollmentID", Int64.Type}, {"PersonalID", Int64.Type}, {"InformationDate", type date}, {"DomesticViolenceSurvivor", Int64.Type}, {"WhenOccurred", type text}, {"CurrentlyFleeing", type text}, {"GeneralHealthStatus", type text}, {"DentalHealthStatus", type text}, {"MentalHealthStatus", type text}, {"PregnancyStatus", type text}, {"DueDate", type text}, {"DataCollectionStage", Int64.Type}, {"DateCreated", type datetime}, {"DateUpdated", type datetime}, {"UserID", Int64.Type}, {"DateDeleted", type text}, {"ExportID", Int64.Type}})
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
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each ([Name.1] = "IncomeBenefits.csv")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (15)", each #"Transform file (15)"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (15)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (15)", Table.ColumnNames(#"Transform file (15)"(#"Sample file (15)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"IncomeBenefitsID", type text}, {"EnrollmentID", Int64.Type}, {"PersonalID", Int64.Type}, {"InformationDate", type date}, {"IncomeFromAnySource", Int64.Type}, {"TotalMonthlyIncome", Int64.Type}, {"Earned", Int64.Type}, {"EarnedAmount", type text}, {"Unemployment", Int64.Type}, {"UnemploymentAmount", type text}, {"SSI", Int64.Type}, {"SSIAmount", Int64.Type}, {"SSDI", Int64.Type}, {"SSDIAmount", type text}, {"VADisabilityService", Int64.Type}, {"VADisabilityServiceAmount", Int64.Type}, {"VADisabilityNonService", Int64.Type}, {"VADisabilityNonServiceAmount", type text}, {"PrivateDisability", Int64.Type}, {"PrivateDisabilityAmount", type text}, {"WorkersComp", Int64.Type}, {"WorkersCompAmount", type text}, {"TANF", Int64.Type}, {"TANFAmount", type text}, {"GA", Int64.Type}, {"GAAmount", type text}, {"SocSecRetirement", Int64.Type}, {"SocSecRetirementAmount", type text}, {"Pension", Int64.Type}, {"PensionAmount", type text}, {"ChildSupport", Int64.Type}, {"ChildSupportAmount", type text}, {"Alimony", Int64.Type}, {"AlimonyAmount", type text}, {"OtherIncomeSource", Int64.Type}, {"OtherIncomeAmount", type text}, {"OtherIncomeSourceIdentify", type text}, {"BenefitsFromAnySource", Int64.Type}, {"SNAP", Int64.Type}, {"WIC", Int64.Type}, {"TANFChildCare", Int64.Type}, {"TANFTransportation", Int64.Type}, {"OtherTANF", Int64.Type}, {"OtherBenefitsSource", Int64.Type}, {"OtherBenefitsSourceIdentify", type text}, {"InsuranceFromAnySource", Int64.Type}, {"Medicaid", Int64.Type}, {"NoMedicaidReason", type text}, {"Medicare", Int64.Type}, {"NoMedicareReason", type text}, {"SCHIP", Int64.Type}, {"NoSCHIPReason", type text}, {"VHAServices", Int64.Type}, {"NoVHAReason", type text}, {"EmployerProvided", Int64.Type}, {"NoEmployerProvidedReason", type text}, {"COBRA", Int64.Type}, {"NoCOBRAReason", type text}, {"PrivatePay", Int64.Type}, {"NoPrivatePayReason", type text}, {"StateHealthIns", Int64.Type}, {"NoStateHealthInsReason", type text}, {"IndianHealthServices", Int64.Type}, {"NoIndianHealthServicesReason", type text}, {"OtherInsurance", Int64.Type}, {"OtherInsuranceIdentify", type text}, {"ADAP", type text}, {"NoADAPReason", type text}, {"RyanWhiteMedDent", type text}, {"NoRyanWhiteReason", type text}, {"ConnectionWithSOAR", Int64.Type}, {"DataCollectionStage", Int64.Type}, {"DateCreated", type datetime}, {"DateUpdated", type datetime}, {"UserID", Int64.Type}, {"DateDeleted", type text}, {"ExportID", Int64.Type}})
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
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each ([Name.1] = "Project.csv")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (12)", each #"Transform file (12)"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (12)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (12)", Table.ColumnNames(#"Transform file (12)"(#"Sample file (12)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"ProjectID", Int64.Type}, {"OrganizationID", Int64.Type}, {"ProjectName", type text}, {"ProjectCommonName", type text}, {"OperatingStartDate", type date}, {"OperatingEndDate", type text}, {"ContinuumProject", Int64.Type}, {"ProjectType", Int64.Type}, {"HousingType", type text}, {"RRHSubType", type text}, {"ResidentialAffiliation", type text}, {"TargetPopulation", type text}, {"HOPWAMedAssistedLivingFac", Int64.Type}, {"PITCount", type text}, {"DateCreated", type datetime}, {"DateUpdated", type datetime}, {"UserID", Int64.Type}, {"DateDeleted", type text}, {"ExportID", Int64.Type}})
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
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each ([Name.1] = "ProjectCoC.csv")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (11)", each #"Transform file (11)"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (11)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (11)", Table.ColumnNames(#"Transform file (11)"(#"Sample file (11)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"ProjectCoCID", Int64.Type}, {"ProjectID", Int64.Type}, {"CoCCode", type text}, {"Geocode", type text}, {"Address1", type text}, {"Address2", type text}, {"City", type text}, {"State", type text}, {"ZIP", Int64.Type}, {"GeographyType", type text}, {"DateCreated", type datetime}, {"DateUpdated", type datetime}, {"UserID", Int64.Type}, {"DateDeleted", type text}, {"ExportID", Int64.Type}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared Services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each ([Name.1] = "Services.csv")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (8)", each #"Transform file (8)"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (8)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (8)", Table.ColumnNames(#"Transform file (8)"(#"Sample file (8)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"ServicesID", Int64.Type}, {"EnrollmentID", Int64.Type}, {"PersonalID", Int64.Type}, {"DateProvided", type date}, {"RecordType", Int64.Type}, {"TypeProvided", Int64.Type}, {"OtherTypeProvided", type text}, {"MovingOnOtherType", type text}, {"SubTypeProvided", type text}, {"FAAmount", type number}, {"FAStartDate", type date}, {"FAEndDate", type date}, {"ReferralOutcome", type text}, {"DateCreated", type datetime}, {"DateUpdated", type datetime}, {"UserID", Int64.Type}, {"DateDeleted", type text}, {"ExportID", Int64.Type}})
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
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "Affiliation.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 8, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 8, QuoteStyle = QuoteStyle.None])
in
  Source;
shared #"Sample file (2)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "Client.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (2)" = let
  #"Parameter (2)" = #"Sample file (2)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (2)"]
in
  #"Parameter (2)";
shared #"Transform Sample file (2)" = let
  Source = Csv.Document(#"Parameter (2)", [Delimiter = ",", Columns = 46, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (2)""}"]
shared #"Transform file (2)" = (#"Parameter (2)" as binary) => let
  Source = Csv.Document(#"Parameter (2)", [Delimiter = ",", Columns = 46, QuoteStyle = QuoteStyle.None])
in
  Source;
shared #"Sample file (3)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "Enrollment.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (3)" = let
  #"Parameter (3)" = #"Sample file (3)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (3)"]
in
  #"Parameter (3)";
shared #"Transform Sample file (3)" = let
  Source = Csv.Document(#"Parameter (3)", [Delimiter = ",", Columns = 71, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (3)""}"]
shared #"Transform file (3)" = (#"Parameter (3)" as binary) => let
  Source = Csv.Document(#"Parameter (3)", [Delimiter = ",", Columns = 71, QuoteStyle = QuoteStyle.None]),
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
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "Assessment.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (4)" = let
  #"Parameter (4)" = #"Sample file (4)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (4)"]
in
  #"Parameter (4)";
shared #"Transform Sample file (4)" = let
  Source = Csv.Document(#"Parameter (4)", [Delimiter = ",", Columns = 13, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (4)""}"]
shared #"Transform file (4)" = (#"Parameter (4)" as binary) => let
  Source = Csv.Document(#"Parameter (4)", [Delimiter = ",", Columns = 13, QuoteStyle = QuoteStyle.None])
in
  Source;
shared #"Sample file (5)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "AssessmentQuestions.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (5)" = let
  #"Parameter (5)" = #"Sample file (5)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (5)"]
in
  #"Parameter (5)";
shared #"Transform Sample file (5)" = let
  Source = Csv.Document(#"Parameter (5)", [Delimiter = ",", Columns = 13, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (5)""}"]
shared #"Transform file (5)" = (#"Parameter (5)" as binary) => let
  Source = Csv.Document(#"Parameter (5)", [Delimiter = ",", Columns = 13, QuoteStyle = QuoteStyle.None])
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
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "AssessmentResults.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (6)" = let
  #"Parameter (6)" = #"Sample file (6)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (6)"]
in
  #"Parameter (6)";
shared #"Transform Sample file (6)" = let
  Source = Csv.Document(#"Parameter (6)", [Delimiter = ",", Columns = 11, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (6)""}"]
shared #"Transform file (6)" = (#"Parameter (6)" as binary) => let
  Source = Csv.Document(#"Parameter (6)", [Delimiter = ",", Columns = 11, QuoteStyle = QuoteStyle.None])
in
  Source;
shared #"Sample file (7)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "CEParticipation.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (7)" = let
  #"Parameter (7)" = #"Sample file (7)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (7)"]
in
  #"Parameter (7)";
shared #"Transform Sample file (7)" = let
  Source = Csv.Document(#"Parameter (7)", [Delimiter = ",", Columns = 15, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (7)""}"]
shared #"Transform file (7)" = (#"Parameter (7)" as binary) => let
  Source = Csv.Document(#"Parameter (7)", [Delimiter = ",", Columns = 15, QuoteStyle = QuoteStyle.None])
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
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "Services.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (8)" = let
  #"Parameter (8)" = #"Sample file (8)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (8)"]
in
  #"Parameter (8)";
shared #"Transform Sample file (8)" = let
  Source = Csv.Document(#"Parameter (8)", [Delimiter = ",", Columns = 18, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (8)""}"]
shared #"Transform file (8)" = (#"Parameter (8)" as binary) => let
  Source = Csv.Document(#"Parameter (8)", [Delimiter = ",", Columns = 18, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"Sample file (9)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "User.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (9)" = let
  #"Parameter (9)" = #"Sample file (9)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (9)"]
in
  #"Parameter (9)";
shared #"Transform Sample file (9)" = let
  Source = Csv.Document(#"Parameter (9)", [Delimiter = ",", Columns = 10, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (9)""}"]
shared #"Transform file (9)" = (#"Parameter (9)" as binary) => let
  Source = Csv.Document(#"Parameter (9)", [Delimiter = ",", Columns = 10, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"Sample file (10)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "YouthEducationStatus.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (10)" = let
  #"Parameter (10)" = #"Sample file (10)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (10)"]
in
  #"Parameter (10)";
shared #"Transform Sample file (10)" = let
  Source = Csv.Document(#"Parameter (10)", [Delimiter = ",", Columns = 13, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (10)""}"]
shared #"Transform file (10)" = (#"Parameter (10)" as binary) => let
  Source = Csv.Document(#"Parameter (10)", [Delimiter = ",", Columns = 13, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"Sample file (11)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "ProjectCoC.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (11)" = let
  #"Parameter (11)" = #"Sample file (11)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (11)"]
in
  #"Parameter (11)";
shared #"Transform Sample file (11)" = let
  Source = Csv.Document(#"Parameter (11)", [Delimiter = ",", Columns = 15, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (11)""}"]
shared #"Transform file (11)" = (#"Parameter (11)" as binary) => let
  Source = Csv.Document(#"Parameter (11)", [Delimiter = ",", Columns = 15, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"Sample file (12)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "Project.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (12)" = let
  #"Parameter (12)" = #"Sample file (12)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (12)"]
in
  #"Parameter (12)";
shared #"Transform Sample file (12)" = let
  Source = Csv.Document(#"Parameter (12)", [Delimiter = ",", Columns = 19, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (12)""}"]
shared #"Transform file (12)" = (#"Parameter (12)" as binary) => let
  Source = Csv.Document(#"Parameter (12)", [Delimiter = ",", Columns = 19, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"Sample file (13)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "Organization.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (13)" = let
  #"Parameter (13)" = #"Sample file (13)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (13)"]
in
  #"Parameter (13)";
shared #"Transform Sample file (13)" = let
  Source = Csv.Document(#"Parameter (13)", [Delimiter = ",", Columns = 9, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (13)""}"]
shared #"Transform file (13)" = (#"Parameter (13)" as binary) => let
  Source = Csv.Document(#"Parameter (13)", [Delimiter = ",", Columns = 9, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"Sample file (14)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "Inventory.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (14)" = let
  #"Parameter (14)" = #"Sample file (14)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (14)"]
in
  #"Parameter (14)";
shared #"Transform Sample file (14)" = let
  Source = Csv.Document(#"Parameter (14)", [Delimiter = ",", Columns = 22, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (14)""}"]
shared #"Transform file (14)" = (#"Parameter (14)" as binary) => let
  Source = Csv.Document(#"Parameter (14)", [Delimiter = ",", Columns = 22, QuoteStyle = QuoteStyle.None])
in
  Source;
shared #"Sample file (15)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "IncomeBenefits.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (15)" = let
  #"Parameter (15)" = #"Sample file (15)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (15)"]
in
  #"Parameter (15)";
shared #"Transform Sample file (15)" = let
  Source = Csv.Document(#"Parameter (15)", [Delimiter = ",", Columns = 77, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (15)""}"]
shared #"Transform file (15)" = (#"Parameter (15)" as binary) => let
  Source = Csv.Document(#"Parameter (15)", [Delimiter = ",", Columns = 77, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"Sample file (16)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "HMISParticipation.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (16)" = let
  #"Parameter (16)" = #"Sample file (16)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (16)"]
in
  #"Parameter (16)";
shared #"Transform Sample file (16)" = let
  Source = Csv.Document(#"Parameter (16)", [Delimiter = ",", Columns = 10, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (16)""}"]
shared #"Transform file (16)" = (#"Parameter (16)" as binary) => let
  Source = Csv.Document(#"Parameter (16)", [Delimiter = ",", Columns = 10, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"Sample file (17)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "HealthAndDV.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (17)" = let
  #"Parameter (17)" = #"Sample file (17)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (17)"]
in
  #"Parameter (17)";
shared #"Transform Sample file (17)" = let
  Source = Csv.Document(#"Parameter (17)", [Delimiter = ",", Columns = 18, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (17)""}"]
shared #"Transform file (17)" = (#"Parameter (17)" as binary) => let
  Source = Csv.Document(#"Parameter (17)", [Delimiter = ",", Columns = 18, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"Sample file (18)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "Funder.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (18)" = let
  #"Parameter (18)" = #"Sample file (18)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (18)"]
in
  #"Parameter (18)";
shared #"Transform Sample file (18)" = let
  Source = Csv.Document(#"Parameter (18)", [Delimiter = ",", Columns = 12, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (18)""}"]
shared #"Transform file (18)" = (#"Parameter (18)" as binary) => let
  Source = Csv.Document(#"Parameter (18)", [Delimiter = ",", Columns = 12, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"Sample file (19)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "Export.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (19)" = let
  #"Parameter (19)" = #"Sample file (19)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (19)"]
in
  #"Parameter (19)";
shared #"Transform Sample file (19)" = let
  Source = Csv.Document(#"Parameter (19)", [Delimiter = ",", Columns = 19, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (19)""}"]
shared #"Transform file (19)" = (#"Parameter (19)" as binary) => let
  Source = Csv.Document(#"Parameter (19)", [Delimiter = ",", Columns = 19, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"Sample file (20)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "exporting tests"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each [Name.1] = "Exit.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (20)" = let
  #"Parameter (20)" = #"Sample file (20)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (20)"]
in
  #"Parameter (20)";
shared #"Transform Sample file (20)" = let
  Source = Csv.Document(#"Parameter (20)", [Delimiter = ",", Columns = 44, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (20)""}"]
shared #"Transform file (20)" = (#"Parameter (20)" as binary) => let
  Source = Csv.Document(#"Parameter (20)", [Delimiter = ",", Columns = 44, QuoteStyle = QuoteStyle.None])
in
  Source;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data];
