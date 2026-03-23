[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared hmis_checks = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "LSNDC"]}[Content],
  #"Navigation 5" = #"Navigation 4"{[Name = "hmis_checks2"]}[Content],
  // optional: keep only actual files
  #"Filtered to files" = Table.SelectRows(#"Navigation 4", each [Extension] <> null),
  #"Selected columns" = Table.SelectColumns(#"Filtered to files", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Folder Path"}),
  Navigation = #"Selected columns"{[Name = "hmis_checks2"]}[Content],
  #"Expanded Content" = Table.ExpandTableColumn(Navigation, "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes.1", "Folder Path.1"})
in
  #"Expanded Content";
shared hmis_client = let
  Source = hmis_checks,
  #"Filtered rows" = Table.SelectRows(Source, each ([Name.1] = "Client.csv")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"PersonalID", type text}, {"FirstName", type text}, {"MiddleName", type text}, {"LastName", type text}, {"NameSuffix", type text}, {"NameDataQuality", Int64.Type}, {"SSN", Int64.Type}, {"SSNDataQuality", Int64.Type}, {"DOB", type date}, {"DOBDataQuality", Int64.Type}, {"Sex", Int64.Type}, {"AmIndAKNative", Int64.Type}, {"Asian", Int64.Type}, {"BlackAfAmerican", Int64.Type}, {"HispanicLatinao", Int64.Type}, {"MidEastNAfrican", Int64.Type}, {"NativeHIPacific", Int64.Type}, {"White", Int64.Type}, {"RaceNone", Int64.Type}, {"AdditionalRaceEthnicity", type text}, {"VeteranStatus", Int64.Type}, {"YearEnteredService", Int64.Type}, {"YearSeparated", Int64.Type}, {"WorldWarII", Int64.Type}, {"KoreanWar", Int64.Type}, {"VietnamWar", Int64.Type}, {"DesertStorm", Int64.Type}, {"AfghanistanOEF", Int64.Type}, {"IraqOIF", Int64.Type}, {"IraqOND", Int64.Type}, {"OtherTheater", Int64.Type}, {"MilitaryBranch", Int64.Type}, {"DischargeStatus", Int64.Type}, {"DateCreated", type datetime}, {"DateUpdated", type datetime}, {"UserID", Int64.Type}, {"DateDeleted", type text}, {"ExportID", Int64.Type}}),

  // Use all columns currently in the table.
  // If you only want certain columns checked, replace this with a hard-coded list.
  ColumnsToCheck = Table.ColumnNames(#"Changed column type"),

  // Null = blank for all types
  // Empty string / whitespace = blank for text columns
  IsBlank = (v as any) as logical =>
    v = null or (Value.Is(v, type text) and Text.Trim(v) = ""),

  // Add one boolean column per field: <ColumnName>_isBlank
  #"Added blank flags" =
    List.Accumulate(
      ColumnsToCheck,
      #"Changed column type",
      (state, col) =>
        Table.AddColumn(
          state,
          col & "_isBlank",
          each IsBlank(Record.Field(_, col)),
          type logical
        )
    ),

  // Human-readable list of missing fields for each row
  #"Added missing fields text" =
    Table.AddColumn(
      #"Added blank flags",
      "MissingFields",
      each
        let
          Missing = List.Select(ColumnsToCheck, (col) => IsBlank(Record.Field(_, col)))
        in
          if List.Count(Missing) = 0 then null else Text.Combine(Missing, ", "),
      type text
    )

in
  #"Added missing fields text";
shared #"Sample file" = let
  Source = hmis_checks,
  #"Filtered rows" = Table.SelectRows(Source, each [Name.1] = "Client.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  Navigation = #"Filtered hidden files"{0}[Content.1]
in
  Navigation;
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 38, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 38, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared hmis_enrollment = let
  Source = hmis_checks,
  #"Filtered rows" = Table.SelectRows(Source, each ([Name.1] = "Enrollment.csv")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (2)", each #"Transform file (2)"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (2)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (2)", Table.ColumnNames(#"Transform file (2)"(#"Sample file (2)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"EnrollmentID", Int64.Type}, {"PersonalID", type text}, {"ProjectID", Int64.Type}, {"EntryDate", type date}, {"HouseholdID", type text}, {"RelationshipToHoH", Int64.Type}, {"EnrollmentCoC", type text}, {"LivingSituation", Int64.Type}, {"RentalSubsidyType", Int64.Type}, {"LengthOfStay", Int64.Type}, {"LOSUnderThreshold", Int64.Type}, {"PreviousStreetESSH", Int64.Type}, {"DateToStreetESSH", type date}, {"TimesHomelessPastThreeYears", Int64.Type}, {"MonthsHomelessPastThreeYears", Int64.Type}, {"DisablingCondition", Int64.Type}, {"DateOfEngagement", type date}, {"MoveInDate", type date}, {"DateOfPATHStatus", type text}, {"ClientEnrolledInPATH", type text}, {"ReasonNotEnrolled", type text}, {"PercentAMI", Int64.Type}, {"ReferralSource", type text}, {"CountOutreachReferralApproaches", type text}, {"DateOfBCPStatus", type text}, {"EligibleForRHY", type text}, {"ReasonNoServices", type text}, {"RunawayYouth", type text}, {"FormerWardChildWelfare", type text}, {"ChildWelfareYears", type text}, {"ChildWelfareMonths", type text}, {"FormerWardJuvenileJustice", type text}, {"JuvenileJusticeYears", type text}, {"JuvenileJusticeMonths", type text}, {"UnemploymentFam", type text}, {"MentalHealthDisorderFam", type text}, {"PhysicalDisabilityFam", type text}, {"AlcoholDrugUseDisorderFam", type text}, {"InsufficientIncome", type text}, {"IncarceratedParent", type text}, {"VAMCStation", Int64.Type}, {"TargetScreenReqd", Int64.Type}, {"TimeToHousingLoss", Int64.Type}, {"AnnualPercentAMI", Int64.Type}, {"LiteralHomelessHistory", Int64.Type}, {"ClientLeaseholder", Int64.Type}, {"HOHLeaseholder", Int64.Type}, {"SubsidyAtRisk", Int64.Type}, {"EvictionHistory", Int64.Type}, {"CriminalRecord", Int64.Type}, {"IncarceratedAdult", Int64.Type}, {"PrisonDischarge", Int64.Type}, {"SexOffender", Int64.Type}, {"DisabledHoH", Int64.Type}, {"CurrentPregnant", Int64.Type}, {"SingleParent", Int64.Type}, {"DependentUnder6", Int64.Type}, {"HH5Plus", Int64.Type}, {"CoCPrioritized", Int64.Type}, {"HPScreeningScore", Int64.Type}, {"ThresholdScore", Int64.Type}, {"MentalHealthConsultation", Int64.Type}, {"DateCreated", type datetime}, {"DateUpdated", type datetime}, {"UserID", Int64.Type}, {"DateDeleted", type text}, {"ExportID", Int64.Type}}),
  #"Merged queries" = Table.NestedJoin(#"Changed column type", {"PersonalID"}, client_control_table, {"PersonalID"}, "client_control_table", JoinKind.LeftOuter),
  #"Expanded client_control_table" = Table.ExpandTableColumn(#"Merged queries", "client_control_table", {"client_id", "FirstName", "MiddleName", "LastName", "DOB"}, {"client_id", "FirstName", "MiddleName", "LastName", "DOB"}),
  #"Reordered columns" = Table.ReorderColumns(#"Expanded client_control_table", {"client_id", "FirstName", "MiddleName", "LastName", "DOB", "EnrollmentID", "PersonalID", "ProjectID", "EntryDate", "HouseholdID", "RelationshipToHoH", "EnrollmentCoC", "LivingSituation", "RentalSubsidyType", "LengthOfStay", "LOSUnderThreshold", "PreviousStreetESSH", "DateToStreetESSH", "TimesHomelessPastThreeYears", "MonthsHomelessPastThreeYears", "DisablingCondition", "DateOfEngagement", "MoveInDate", "DateOfPATHStatus", "ClientEnrolledInPATH", "ReasonNotEnrolled", "PercentAMI", "ReferralSource", "CountOutreachReferralApproaches", "DateOfBCPStatus", "EligibleForRHY", "ReasonNoServices", "RunawayYouth", "FormerWardChildWelfare", "ChildWelfareYears", "ChildWelfareMonths", "FormerWardJuvenileJustice", "JuvenileJusticeYears", "JuvenileJusticeMonths", "UnemploymentFam", "MentalHealthDisorderFam", "PhysicalDisabilityFam", "AlcoholDrugUseDisorderFam", "InsufficientIncome", "IncarceratedParent", "VAMCStation", "TargetScreenReqd", "TimeToHousingLoss", "AnnualPercentAMI", "LiteralHomelessHistory", "ClientLeaseholder", "HOHLeaseholder", "SubsidyAtRisk", "EvictionHistory", "CriminalRecord", "IncarceratedAdult", "PrisonDischarge", "SexOffender", "DisabledHoH", "CurrentPregnant", "SingleParent", "DependentUnder6", "HH5Plus", "CoCPrioritized", "HPScreeningScore", "ThresholdScore", "MentalHealthConsultation", "DateCreated", "DateUpdated", "UserID", "DateDeleted", "ExportID"})
in
  #"Reordered columns";
shared #"Sample file (2)" = let
  Source = hmis_checks,
  #"Filtered rows" = Table.SelectRows(Source, each [Name.1] = "Enrollment.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  Navigation = #"Filtered hidden files"{0}[Content.1]
in
  Navigation;
shared #"Parameter (2)" = let
  #"Parameter (2)" = #"Sample file (2)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (2)"]
in
  #"Parameter (2)";
shared #"Transform Sample file (2)" = let
  Source = Csv.Document(#"Parameter (2)", [Delimiter = ",", Columns = 67, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (2)""}"]
shared #"Transform file (2)" = (#"Parameter (2)" as binary) => let
  Source = Csv.Document(#"Parameter (2)", [Delimiter = ",", Columns = 67, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared hmis_exit = let
  Source = hmis_checks,
  #"Filtered rows" = Table.SelectRows(Source, each ([Name.1] = "Exit.csv")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (3)", each #"Transform file (3)"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (3)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (3)", Table.ColumnNames(#"Transform file (3)"(#"Sample file (3)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"ExitID", Int64.Type}, {"EnrollmentID", Int64.Type}, {"PersonalID", type text}, {"ExitDate", type date}, {"Destination", Int64.Type}, {"DestinationSubsidyType", Int64.Type}, {"OtherDestination", type text}, {"HousingAssessment", Int64.Type}, {"SubsidyInformation", Int64.Type}, {"ProjectCompletionStatus", type text}, {"EarlyExitReason", type text}, {"ExchangeForSex", type text}, {"ExchangeForSexPastThreeMonths", type text}, {"CountOfExchangeForSex", type text}, {"AskedOrForcedToExchangeForSex", type text}, {"AskedOrForcedToExchangeForSexPastThreeMonths", type text}, {"WorkplaceViolenceThreats", type text}, {"WorkplacePromiseDifference", type text}, {"CoercedToContinueWork", type text}, {"LaborExploitPastThreeMonths", type text}, {"CounselingReceived", type text}, {"IndividualCounseling", type text}, {"FamilyCounseling", type text}, {"GroupCounseling", type text}, {"SessionCountAtExit", type text}, {"PostExitCounselingPlan", type text}, {"SessionsInPlan", type text}, {"DestinationSafeClient", type text}, {"DestinationSafeWorker", type text}, {"PosAdultConnections", type text}, {"PosPeerConnections", type text}, {"PosCommunityConnections", type text}, {"AftercareDate", type text}, {"AftercareProvided", type text}, {"EmailSocialMedia", type text}, {"Telephone", type text}, {"InPersonIndividual", type text}, {"InPersonGroup", type text}, {"CMExitReason", type text}, {"DateCreated", type datetime}, {"DateUpdated", type datetime}, {"UserID", Int64.Type}, {"DateDeleted", type text}, {"ExportID", Int64.Type}}),
  #"Merged queries" = Table.NestedJoin(#"Changed column type", {"PersonalID"}, client_control_table, {"PersonalID"}, "client_control_table", JoinKind.LeftOuter),
  #"Expanded client_control_table" = Table.ExpandTableColumn(#"Merged queries", "client_control_table", {"client_id", "FirstName", "MiddleName", "LastName", "DOB"}, {"client_id", "FirstName", "MiddleName", "LastName", "DOB"}),
  #"Reordered columns" = Table.ReorderColumns(#"Expanded client_control_table", {"client_id", "FirstName", "MiddleName", "LastName", "DOB", "ExitID", "EnrollmentID", "PersonalID", "ExitDate", "Destination", "DestinationSubsidyType", "OtherDestination", "HousingAssessment", "SubsidyInformation", "ProjectCompletionStatus", "EarlyExitReason", "ExchangeForSex", "ExchangeForSexPastThreeMonths", "CountOfExchangeForSex", "AskedOrForcedToExchangeForSex", "AskedOrForcedToExchangeForSexPastThreeMonths", "WorkplaceViolenceThreats", "WorkplacePromiseDifference", "CoercedToContinueWork", "LaborExploitPastThreeMonths", "CounselingReceived", "IndividualCounseling", "FamilyCounseling", "GroupCounseling", "SessionCountAtExit", "PostExitCounselingPlan", "SessionsInPlan", "DestinationSafeClient", "DestinationSafeWorker", "PosAdultConnections", "PosPeerConnections", "PosCommunityConnections", "AftercareDate", "AftercareProvided", "EmailSocialMedia", "Telephone", "InPersonIndividual", "InPersonGroup", "CMExitReason", "DateCreated", "DateUpdated", "UserID", "DateDeleted", "ExportID"})
in
  #"Reordered columns";
shared #"Sample file (3)" = let
  Source = hmis_checks,
  #"Filtered rows" = Table.SelectRows(Source, each [Name.1] = "Exit.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  Navigation = #"Filtered hidden files"{0}[Content.1]
in
  Navigation;
shared #"Parameter (3)" = let
  #"Parameter (3)" = #"Sample file (3)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (3)"]
in
  #"Parameter (3)";
shared #"Transform Sample file (3)" = let
  Source = Csv.Document(#"Parameter (3)", [Delimiter = ",", Columns = 44, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (3)""}"]
shared #"Transform file (3)" = (#"Parameter (3)" as binary) => let
  Source = Csv.Document(#"Parameter (3)", [Delimiter = ",", Columns = 44, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared hmis_healthdv = let
  Source = hmis_checks,
  #"Filtered rows" = Table.SelectRows(Source, each ([Name.1] = "HealthAndDV.csv")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (4)", each #"Transform file (4)"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (4)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (4)", Table.ColumnNames(#"Transform file (4)"(#"Sample file (4)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"HealthAndDVID", type text}, {"EnrollmentID", Int64.Type}, {"PersonalID", type text}, {"InformationDate", type date}, {"DomesticViolenceSurvivor", Int64.Type}, {"WhenOccurred", Int64.Type}, {"CurrentlyFleeing", Int64.Type}, {"GeneralHealthStatus", type text}, {"DentalHealthStatus", type text}, {"MentalHealthStatus", type text}, {"PregnancyStatus", Int64.Type}, {"DueDate", type text}, {"DataCollectionStage", Int64.Type}, {"DateCreated", type datetime}, {"DateUpdated", type datetime}, {"UserID", Int64.Type}, {"DateDeleted", type text}, {"ExportID", Int64.Type}}),
  #"Merged queries" = Table.NestedJoin(#"Changed column type", {"PersonalID"}, client_control_table, {"PersonalID"}, "client_control_table", JoinKind.LeftOuter),
  #"Expanded client_control_table" = Table.ExpandTableColumn(#"Merged queries", "client_control_table", {"client_id", "FirstName", "MiddleName", "LastName", "DOB"}, {"client_id", "FirstName", "MiddleName", "LastName", "DOB"}),
  #"Reordered columns" = Table.ReorderColumns(#"Expanded client_control_table", {"client_id", "FirstName", "MiddleName", "LastName", "DOB", "HealthAndDVID", "EnrollmentID", "PersonalID", "InformationDate", "DomesticViolenceSurvivor", "WhenOccurred", "CurrentlyFleeing", "GeneralHealthStatus", "DentalHealthStatus", "MentalHealthStatus", "PregnancyStatus", "DueDate", "DataCollectionStage", "DateCreated", "DateUpdated", "UserID", "DateDeleted", "ExportID"})
in
  #"Reordered columns";
shared #"Sample file (4)" = let
  Source = hmis_checks,
  #"Filtered rows" = Table.SelectRows(Source, each [Name.1] = "HealthAndDV.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  Navigation = #"Filtered hidden files"{0}[Content.1]
in
  Navigation;
shared #"Parameter (4)" = let
  #"Parameter (4)" = #"Sample file (4)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (4)"]
in
  #"Parameter (4)";
shared #"Transform Sample file (4)" = let
  Source = Csv.Document(#"Parameter (4)", [Delimiter = ",", Columns = 18, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (4)""}"]
shared #"Transform file (4)" = (#"Parameter (4)" as binary) => let
  Source = Csv.Document(#"Parameter (4)", [Delimiter = ",", Columns = 18, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared hmis_income = let
  Source = hmis_checks,
  #"Filtered rows" = Table.SelectRows(Source, each ([Name.1] = "IncomeBenefits.csv")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (5)", each #"Transform file (5)"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (5)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (5)", Table.ColumnNames(#"Transform file (5)"(#"Sample file (5)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"IncomeBenefitsID", type text}, {"EnrollmentID", Int64.Type}, {"PersonalID", type text}, {"InformationDate", type date}, {"IncomeFromAnySource", Int64.Type}, {"TotalMonthlyIncome", type number}, {"Earned", Int64.Type}, {"EarnedAmount", type number}, {"Unemployment", Int64.Type}, {"UnemploymentAmount", Int64.Type}, {"SSI", Int64.Type}, {"SSIAmount", type number}, {"SSDI", Int64.Type}, {"SSDIAmount", Int64.Type}, {"VADisabilityService", Int64.Type}, {"VADisabilityServiceAmount", type number}, {"VADisabilityNonService", Int64.Type}, {"VADisabilityNonServiceAmount", Int64.Type}, {"PrivateDisability", Int64.Type}, {"PrivateDisabilityAmount", Int64.Type}, {"WorkersComp", Int64.Type}, {"WorkersCompAmount", type text}, {"TANF", Int64.Type}, {"TANFAmount", type text}, {"GA", Int64.Type}, {"GAAmount", type text}, {"SocSecRetirement", Int64.Type}, {"SocSecRetirementAmount", Int64.Type}, {"Pension", Int64.Type}, {"PensionAmount", Int64.Type}, {"ChildSupport", Int64.Type}, {"ChildSupportAmount", type text}, {"Alimony", Int64.Type}, {"AlimonyAmount", type text}, {"OtherIncomeSource", Int64.Type}, {"OtherIncomeAmount", type text}, {"OtherIncomeSourceIdentify", type text}, {"BenefitsFromAnySource", Int64.Type}, {"SNAP", Int64.Type}, {"WIC", Int64.Type}, {"TANFChildCare", Int64.Type}, {"TANFTransportation", Int64.Type}, {"OtherTANF", Int64.Type}, {"OtherBenefitsSource", Int64.Type}, {"OtherBenefitsSourceIdentify", type text}, {"InsuranceFromAnySource", Int64.Type}, {"Medicaid", Int64.Type}, {"NoMedicaidReason", type text}, {"Medicare", Int64.Type}, {"NoMedicareReason", type text}, {"SCHIP", Int64.Type}, {"NoSCHIPReason", type text}, {"VHAServices", Int64.Type}, {"NoVHAReason", type text}, {"EmployerProvided", Int64.Type}, {"NoEmployerProvidedReason", type text}, {"COBRA", Int64.Type}, {"NoCOBRAReason", type text}, {"PrivatePay", Int64.Type}, {"NoPrivatePayReason", type text}, {"StateHealthIns", Int64.Type}, {"NoStateHealthInsReason", type text}, {"IndianHealthServices", Int64.Type}, {"NoIndianHealthServicesReason", type text}, {"OtherInsurance", Int64.Type}, {"OtherInsuranceIdentify", type text}, {"ADAP", type text}, {"NoADAPReason", type text}, {"RyanWhiteMedDent", type text}, {"NoRyanWhiteReason", type text}, {"ConnectionWithSOAR", Int64.Type}, {"DataCollectionStage", Int64.Type}, {"DateCreated", type datetime}, {"DateUpdated", type datetime}, {"UserID", Int64.Type}, {"DateDeleted", type text}, {"ExportID", Int64.Type}}),
  #"Merged queries" = Table.NestedJoin(#"Changed column type", {"PersonalID"}, client_control_table, {"PersonalID"}, "client_control_table", JoinKind.LeftOuter),
  #"Expanded client_control_table" = Table.ExpandTableColumn(#"Merged queries", "client_control_table", {"client_id", "FirstName", "MiddleName", "LastName", "DOB"}, {"client_id", "FirstName", "MiddleName", "LastName", "DOB"}),
  #"Reordered columns" = Table.ReorderColumns(#"Expanded client_control_table", {"client_id", "FirstName", "MiddleName", "LastName", "DOB", "IncomeBenefitsID", "EnrollmentID", "PersonalID", "InformationDate", "IncomeFromAnySource", "TotalMonthlyIncome", "Earned", "EarnedAmount", "Unemployment", "UnemploymentAmount", "SSI", "SSIAmount", "SSDI", "SSDIAmount", "VADisabilityService", "VADisabilityServiceAmount", "VADisabilityNonService", "VADisabilityNonServiceAmount", "PrivateDisability", "PrivateDisabilityAmount", "WorkersComp", "WorkersCompAmount", "TANF", "TANFAmount", "GA", "GAAmount", "SocSecRetirement", "SocSecRetirementAmount", "Pension", "PensionAmount", "ChildSupport", "ChildSupportAmount", "Alimony", "AlimonyAmount", "OtherIncomeSource", "OtherIncomeAmount", "OtherIncomeSourceIdentify", "BenefitsFromAnySource", "SNAP", "WIC", "TANFChildCare", "TANFTransportation", "OtherTANF", "OtherBenefitsSource", "OtherBenefitsSourceIdentify", "InsuranceFromAnySource", "Medicaid", "NoMedicaidReason", "Medicare", "NoMedicareReason", "SCHIP", "NoSCHIPReason", "VHAServices", "NoVHAReason", "EmployerProvided", "NoEmployerProvidedReason", "COBRA", "NoCOBRAReason", "PrivatePay", "NoPrivatePayReason", "StateHealthIns", "NoStateHealthInsReason", "IndianHealthServices", "NoIndianHealthServicesReason", "OtherInsurance", "OtherInsuranceIdentify", "ADAP", "NoADAPReason", "RyanWhiteMedDent", "NoRyanWhiteReason", "ConnectionWithSOAR", "DataCollectionStage", "DateCreated", "DateUpdated", "UserID", "DateDeleted", "ExportID"})
in
  #"Reordered columns";
shared #"Sample file (5)" = let
  Source = hmis_checks,
  #"Filtered rows" = Table.SelectRows(Source, each [Name.1] = "IncomeBenefits.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  Navigation = #"Filtered hidden files"{0}[Content.1]
in
  Navigation;
shared #"Parameter (5)" = let
  #"Parameter (5)" = #"Sample file (5)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (5)"]
in
  #"Parameter (5)";
shared #"Transform Sample file (5)" = let
  Source = Csv.Document(#"Parameter (5)", [Delimiter = ",", Columns = 77, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (5)""}"]
shared #"Transform file (5)" = (#"Parameter (5)" as binary) => let
  Source = Csv.Document(#"Parameter (5)", [Delimiter = ",", Columns = 77, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared hmis_employment = let
  Source = hmis_checks,
  #"Filtered rows" = Table.SelectRows(Source, each ([Name.1] = "EmploymentEducation.csv")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (6)", each #"Transform file (6)"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (6)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (6)", Table.ColumnNames(#"Transform file (6)"(#"Sample file (6)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"EmploymentEducationID", type text}, {"EnrollmentID", Int64.Type}, {"PersonalID", type text}, {"InformationDate", type date}, {"LastGradeCompleted", Int64.Type}, {"SchoolStatus", type text}, {"Employed", Int64.Type}, {"EmploymentType", Int64.Type}, {"NotEmployedReason", Int64.Type}, {"DataCollectionStage", Int64.Type}, {"DateCreated", type datetime}, {"DateUpdated", type datetime}, {"UserID", Int64.Type}, {"DateDeleted", type text}, {"ExportID", Int64.Type}}),
  #"Merged queries" = Table.NestedJoin(#"Changed column type", {"PersonalID"}, client_control_table, {"PersonalID"}, "client_control_table", JoinKind.LeftOuter),
  #"Expanded client_control_table" = Table.ExpandTableColumn(#"Merged queries", "client_control_table", {"client_id", "FirstName", "MiddleName", "LastName"}, {"client_id", "FirstName", "MiddleName", "LastName"}),
  #"Reordered columns" = Table.ReorderColumns(#"Expanded client_control_table", {"client_id", "FirstName", "MiddleName", "LastName", "EmploymentEducationID", "EnrollmentID", "PersonalID", "InformationDate", "LastGradeCompleted", "SchoolStatus", "Employed", "EmploymentType", "NotEmployedReason", "DataCollectionStage", "DateCreated", "DateUpdated", "UserID", "DateDeleted", "ExportID"})
in
  #"Reordered columns";
shared #"Sample file (6)" = let
  Source = hmis_checks,
  #"Filtered rows" = Table.SelectRows(Source, each [Name.1] = "EmploymentEducation.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  Navigation = #"Filtered hidden files"{0}[Content.1]
in
  Navigation;
shared #"Parameter (6)" = let
  #"Parameter (6)" = #"Sample file (6)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (6)"]
in
  #"Parameter (6)";
shared #"Transform Sample file (6)" = let
  Source = Csv.Document(#"Parameter (6)", [Delimiter = ",", Columns = 15, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (6)""}"]
shared #"Transform file (6)" = (#"Parameter (6)" as binary) => let
  Source = Csv.Document(#"Parameter (6)", [Delimiter = ",", Columns = 15, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared hmis_disabilities = let
  Source = hmis_checks,
  #"Filtered rows" = Table.SelectRows(Source, each ([Name.1] = "Disabilities.csv")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (7)", each #"Transform file (7)"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (7)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (7)", Table.ColumnNames(#"Transform file (7)"(#"Sample file (7)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"DisabilitiesID", type text}, {"EnrollmentID", Int64.Type}, {"PersonalID", type text}, {"InformationDate", type date}, {"DisabilityType", Int64.Type}, {"DisabilityResponse", Int64.Type}, {"IndefiniteAndImpairs", Int64.Type}, {"TCellCountAvailable", type text}, {"TCellCount", type text}, {"TCellSource", type text}, {"ViralLoadAvailable", type text}, {"ViralLoad", type text}, {"ViralLoadSource", type text}, {"AntiRetroviral", type text}, {"DataCollectionStage", Int64.Type}, {"DateCreated", type datetime}, {"DateUpdated", type datetime}, {"UserID", Int64.Type}, {"DateDeleted", type text}, {"ExportID", Int64.Type}}),
  #"Merged queries" = Table.NestedJoin(#"Changed column type", {"PersonalID"}, client_control_table, {"PersonalID"}, "client_control_table", JoinKind.LeftOuter),
  #"Expanded client_control_table" = Table.ExpandTableColumn(#"Merged queries", "client_control_table", {"client_id", "FirstName", "MiddleName", "LastName", "SSN", "DOB"}, {"client_id", "FirstName", "MiddleName", "LastName", "SSN", "DOB"}),
  #"Reordered columns" = Table.ReorderColumns(#"Expanded client_control_table", {"client_id", "FirstName", "MiddleName", "LastName", "SSN", "DOB", "DisabilitiesID", "EnrollmentID", "PersonalID", "InformationDate", "DisabilityType", "DisabilityResponse", "IndefiniteAndImpairs", "TCellCountAvailable", "TCellCount", "TCellSource", "ViralLoadAvailable", "ViralLoad", "ViralLoadSource", "AntiRetroviral", "DataCollectionStage", "DateCreated", "DateUpdated", "UserID", "DateDeleted", "ExportID"})
in
  #"Reordered columns";
shared #"Sample file (7)" = let
  Source = hmis_checks,
  #"Filtered rows" = Table.SelectRows(Source, each [Name.1] = "Disabilities.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  Navigation = #"Filtered hidden files"{0}[Content.1]
in
  Navigation;
shared #"Parameter (7)" = let
  #"Parameter (7)" = #"Sample file (7)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (7)"]
in
  #"Parameter (7)";
shared #"Transform Sample file (7)" = let
  Source = Csv.Document(#"Parameter (7)", [Delimiter = ",", Columns = 20, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (7)""}"]
shared #"Transform file (7)" = (#"Parameter (7)" as binary) => let
  Source = Csv.Document(#"Parameter (7)", [Delimiter = ",", Columns = 20, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared raw_lsndc_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_clients", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared client_control_table = let
  Source = hmis_client,
  #"Merged queries" = Table.NestedJoin(Source, {"FirstName", "LastName"}, raw_lsndc_clients, {"client_first_name", "client_last_name"}, "raw_lsndc_clients", JoinKind.LeftOuter),
  #"Expanded raw_lsndc_clients" = Table.ExpandTableColumn(#"Merged queries", "raw_lsndc_clients", {"client_id"}, {"client_id"}),
  #"Extracted text before delimiter" = Table.TransformColumns(#"Expanded raw_lsndc_clients", {{"client_id", each Text.BeforeDelimiter(_, "-", 0), type text}}),
  #"Reordered columns" = Table.ReorderColumns(#"Extracted text before delimiter", {"client_id", "PersonalID", "FirstName", "MiddleName", "LastName", "NameSuffix", "NameDataQuality", "SSN", "SSNDataQuality", "DOB", "DOBDataQuality", "Sex", "AmIndAKNative", "Asian", "BlackAfAmerican", "HispanicLatinao", "MidEastNAfrican", "NativeHIPacific", "White", "RaceNone", "AdditionalRaceEthnicity", "VeteranStatus", "YearEnteredService", "YearSeparated", "WorldWarII", "KoreanWar", "VietnamWar", "DesertStorm", "AfghanistanOEF", "IraqOIF", "IraqOND", "OtherTheater", "MilitaryBranch", "DischargeStatus", "DateCreated", "DateUpdated", "UserID", "DateDeleted", "ExportID"}),
  #"Removed other columns" = Table.SelectColumns(#"Reordered columns", {"client_id", "PersonalID", "FirstName", "MiddleName", "LastName", "SSN", "DOB"})
in
  #"Removed other columns";
shared sec_client_control_table = let
  Source = hmis_disabilities,
  #"Removed other columns" = Table.SelectColumns(Source, {"client_id", "FirstName", "MiddleName", "LastName", "SSN", "DOB", "PersonalID"}),
  #"Removed duplicates" = Table.Distinct(#"Removed other columns", {"PersonalID"})
in
  #"Removed duplicates";
shared hmis_client_flag = let
  Source = hmis_client
in
  Source;
[BindToDefaultDestination = true]
shared blank_sex = let
  Source = hmis_client,
  #"Filtered rows" = Table.SelectRows(Source, each ([Sex] = null)),
  #"Removed other columns" = Table.SelectColumns(#"Filtered rows", {"Sex", "DOBDataQuality", "DOB", "SSNDataQuality", "SSN", "NameDataQuality", "NameSuffix", "LastName", "MiddleName", "FirstName", "PersonalID"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed other columns", {"PersonalID", "FirstName", "MiddleName", "LastName", "Sex", "DOBDataQuality", "DOB", "SSNDataQuality", "SSN", "NameDataQuality", "NameSuffix"}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Reordered columns", "deficiency", each "No Sex"), {{"deficiency", type text}}),
  #"Removed other columns 1" = Table.SelectColumns(#"Added custom", {"PersonalID", "FirstName", "MiddleName", "LastName", "deficiency"})
in
  #"Removed other columns 1";
shared ssn_check = let
  Source = hmis_client,
  #"Removed other columns" = Table.SelectColumns(Source, {"PersonalID", "FirstName", "MiddleName", "LastName", "NameSuffix", "NameDataQuality", "SSN", "SSNDataQuality"}),

  #"Added SSN blank flag" = Table.AddColumn(
    #"Removed other columns",
    "SSN_isBlank",
    each [SSN] = null or Text.Trim(Text.From([SSN])) = "",
    type logical
  ),

  #"Added SSN incomplete flag" = Table.AddColumn(
    #"Added SSN blank flag",
    "SSN_isLessThan9Digits",
    each
      let
        DigitsOnly =
          if [SSN] = null then
            ""
          else
            Text.Select(Text.From([SSN]), {"0".."9"})
      in
        DigitsOnly <> "" and Text.Length(DigitsOnly) < 9,
    type logical
  )

in
  #"Added SSN incomplete flag";
[BindToDefaultDestination = true]
shared ssn_blank = let
  Source = ssn_check,
  #"Filtered rows" = Table.SelectRows(Source, each ([SSN_isBlank] = true)),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Filtered rows", "deficiency", each "Blank SSN"), {{"deficiency", type text}}),
  #"Removed other columns" = Table.SelectColumns(#"Added custom", {"PersonalID", "FirstName", "MiddleName", "LastName", "deficiency"})
in
  #"Removed other columns";
[BindToDefaultDestination = true]
shared ssn_incomplete = let
  Source = ssn_check,
  #"Filtered rows" = Table.SelectRows(Source, each ([SSN_isLessThan9Digits] = true)),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Filtered rows", "deficiency", each "Incomplete SSN"), {{"deficiency", type text}}),
  #"Removed other columns" = Table.SelectColumns(#"Added custom", {"LastName", "MiddleName", "FirstName", "PersonalID", "deficiency"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed other columns", {"PersonalID", "FirstName", "MiddleName", "LastName", "deficiency"})
in
  #"Reordered columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "employment_blank_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared employment_blank = let
  Source = hmis_employment,
  #"Reordered columns" = Table.ReorderColumns(Source, {"PersonalID", "client_id", "FirstName", "MiddleName", "LastName", "EmploymentEducationID", "EnrollmentID", "InformationDate", "LastGradeCompleted", "SchoolStatus", "Employed", "EmploymentType", "NotEmployedReason", "DataCollectionStage", "DateCreated", "DateUpdated", "UserID", "DateDeleted", "ExportID"}),
  #"Filtered rows" = Table.SelectRows(#"Reordered columns", each ([Employed] = null)),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Filtered rows", "deficiency", each "No Employment"), {{"deficiency", type text}}),
  #"Removed other columns" = Table.SelectColumns(#"Added custom", {"PersonalID", "client_id", "FirstName", "MiddleName", "LastName", "deficiency"}),
  #"Removed duplicates" = Table.Distinct(#"Removed other columns", {"PersonalID"}),
  #"Removed columns" = Table.RemoveColumns(#"Removed duplicates", {"PersonalID"})
in
  #"Removed columns";
[BindToDefaultDestination = true]
shared last_grade_blank = let
  Source = hmis_employment,
  #"Filtered rows" = Table.SelectRows(Source, each ([LastGradeCompleted] = null)),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Filtered rows", "deficiency", each "No Last Grade Completed"), {{"deficiency", type text}}),
  #"Removed other columns" = Table.SelectColumns(#"Added custom", {"client_id", "FirstName", "MiddleName", "LastName", "deficiency"}),
  #"Removed duplicates" = Table.Distinct(#"Removed other columns", {"client_id"})
in
  #"Removed duplicates";
shared employment_blank_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  TableNavigation = Navigation_2{[Id = "employment_blank", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "blank_dv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared blank_dv = let
  Source = hmis_healthdv,
  #"Filtered rows" = Table.SelectRows(Source, each ([DomesticViolenceSurvivor] = null)),
  #"Removed other columns" = Table.SelectColumns(#"Filtered rows", {"client_id", "FirstName", "MiddleName", "LastName", "DomesticViolenceSurvivor"}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Removed other columns", "Custom", each "No DV"), {{"Custom", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Added custom", {"DomesticViolenceSurvivor"})
in
  #"Removed columns";
shared blank_dv_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  TableNavigation = Navigation_2{[Id = "blank_dv", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[BindToDefaultDestination = true]
shared blank_vamc = let
  Source = hmis_enrollment,
  #"Removed other columns" = Table.SelectColumns(Source, {"VAMCStation", "client_id", "FirstName", "MiddleName", "LastName"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed other columns", {"client_id", "FirstName", "MiddleName", "LastName", "VAMCStation"}),
  #"Filtered rows" = Table.SelectRows(#"Reordered columns", each ([VAMCStation] = null)),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Filtered rows", "deficiency", each "Blank VAMC"), {{"deficiency", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Added custom", {"VAMCStation"})
in
  #"Removed columns";
[BindToDefaultDestination = true]
shared blank_coc = let
  Source = hmis_enrollment,
  #"Removed other columns" = Table.SelectColumns(Source, {"EnrollmentCoC", "client_id", "FirstName", "MiddleName", "LastName"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed other columns", {"client_id", "FirstName", "MiddleName", "LastName", "EnrollmentCoC"}),
  #"Filtered rows" = Table.SelectRows(#"Reordered columns", each ([EnrollmentCoC] = "")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows"),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Removed duplicates", "deficiency", each "Blank CoC"), {{"deficiency", type text}}),
  #"Removed duplicates 1" = Table.Distinct(#"Added custom", {"FirstName", "MiddleName", "LastName"})
in
  #"Removed duplicates 1";
[BindToDefaultDestination = true]
shared blank_vet_status = let
  Source = hmis_client,
  #"Filtered rows" = Table.SelectRows(Source, each ([VeteranStatus] = 99))
in
  #"Filtered rows";
shared #"hmis_enrollment (2)" = let
  Source = hmis_enrollment
in
  Source;
shared hmis_event = let
  Source = hmis_checks,
  #"Filtered rows" = Table.SelectRows(Source, each ([Name.1] = "Event.csv")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (8)", each #"Transform file (8)"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (8)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (8)", Table.ColumnNames(#"Transform file (8)"(#"Sample file (8)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"EventID", Int64.Type}, {"EnrollmentID", Int64.Type}, {"PersonalID", type text}, {"EventDate", type date}, {"Event", Int64.Type}, {"ProbSolDivRRResult", type text}, {"ReferralCaseManageAfter", type text}, {"LocationCrisisOrPHHousing", type text}, {"ReferralResult", Int64.Type}, {"ResultDate", type date}, {"DateCreated", type datetime}, {"DateUpdated", type datetime}, {"UserID", Int64.Type}, {"DateDeleted", type text}, {"ExportID", Int64.Type}})
in
  #"Changed column type";
shared #"Sample file (8)" = let
  Source = hmis_checks,
  #"Filtered rows" = Table.SelectRows(Source, each [Name.1] = "Event.csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  Navigation = #"Filtered hidden files"{0}[Content.1]
in
  Navigation;
shared #"Parameter (8)" = let
  #"Parameter (8)" = #"Sample file (8)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (8)"]
in
  #"Parameter (8)";
shared #"Transform Sample file (8)" = let
  Source = Csv.Document(#"Parameter (8)", [Delimiter = ",", Columns = 15, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (8)""}"]
shared #"Transform file (8)" = (#"Parameter (8)" as binary) => let
  Source = Csv.Document(#"Parameter (8)", [Delimiter = ",", Columns = 15, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[BindToDefaultDestination = true]
shared blank_income = let
  Source = hmis_income,
  #"Removed other columns" = Table.SelectColumns(Source, {"client_id", "FirstName", "MiddleName", "LastName", "TotalMonthlyIncome", "IncomeFromAnySource"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed other columns", {"client_id", "FirstName", "MiddleName", "LastName", "IncomeFromAnySource", "TotalMonthlyIncome"}),
  #"Filtered rows" = Table.SelectRows(#"Reordered columns", each ([TotalMonthlyIncome] = null)),
  #"Removed columns" = Table.RemoveColumns(#"Filtered rows", {"IncomeFromAnySource"}),
  #"Removed duplicates" = Table.Distinct(#"Removed columns", {"client_id"}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Removed duplicates", "deficiency", each "No Total Monthly Income"), {{"deficiency", type text}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Added custom", {"TotalMonthlyIncome"})
in
  #"Removed columns 1";
