[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "LH Registration_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"LH Registration" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "ServicePoint"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "LH Registration"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", let latest = List.Max(#"Expanded Attributes"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Date modified", "Transform file"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed other columns", {{"Date modified", "date_file_modified"}}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Renamed columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Client ID", type text}, {"Address", type text}, {"Bus number:", type text}, {"VOANLA - Zip Code", type text}, {"Cell Phone", type text}, {"City", type text}, {"Does child ride the bus?", type text}, {"Does your child receive behavioral health services?", type text}, {"Education Release Effective for School Year:", type text}, {"Education Release on file?", type text}, {"Email Address", type text}, {"Food Stamps", type text}, {"Household Type", type text}, {"If yes, please list the provider", type text}, {"Is student enrolled in CIS?", type text}, {"JCAMPUS signed permission on file?", type text}, {"JCAMPUS/School Loop Password:", type text}, {"JCAMPUS/School Loop Username:", type text}, {"Media Release Effective for School Year:", type text}, {"Media Release on file?", type text}, {"Name Of Teacher/Homeroom Teacher", type text}, {"Program Contract Effective for School Year:", type text}, {"Program Contract Signed and On File?", type text}, {"School Year", type text}, {"ServicePoint Release of Information on File?", type text}, {"State", type text}, {"Student ID#", type text}, {"Summer Session", type text}, {"Total number of Persons Living at the Address", Int64.Type}, {"Allergy/Medical Condition?", type text}, {"Application Date", type date}, {"Contact #1 Name", type text}, {"Contact #1 Phone", type text}, {"Contact #2 Name", type text}, {"Contact #2 Phone", type text}, {"Clinic or Doctor's Name", type text}, {"Doctor's Phone", type text}, {"Does your child have an Individual Education Plan (IEP)?", type text}, {"Drop-off location:", type text}, {"If yes, pickup location:", type text}, {"Current Grade Level", type text}, {"Has your child been placed in special education by the school system?", type text}, {"Home Phone", type text}, {"If yes, please explain:", type text}, {"If yes, please list:", type text}, {"Lighthouse Location Enrolling", type text}, {"Medication at site during program hours?", type text}, {"Parent/Guardian Name", type text}, {"Place of Employment", type text}, {"School District", type text}, {"School Name", type text}, {"School Phone Number", type text}, {"Social Security", type text}, {"SSI", type text}, {"Total Annual Income", type text}, {"Van Information", type text}, {"Welfare (TANF)", type text}, {"Does the child need to be picked up in a VOA van before Camp Glow?", type text}, {"Relationship to Child", type text}, {"WIOA?", type text}, {"Will this provider want to visit during the program?", type text}, {"Work Phone", type text}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type", {{"School District", "district"}, {"School Name", "school_name"}, {"School Phone Number", "school_phone"}, {"Social Security", "social_sec"}, {"SSI", "ssi"}, {"Total Annual Income", "total_annual_income"}, {"Van Information", "van_info"}, {"Welfare (TANF)", "welfare_tanf"}, {"Does the child need to be picked up in a VOA van before Camp Glow?", "pick_up_glow"}, {"Relationship to Child", "relationship_to_child"}, {"WIOA?", "wioa"}, {"Will this provider want to visit during the program?", "provider_visit"}, {"Work Phone", "work_phone"}, {"Client ID", "client_id"}, {"Address", "street_address"}, {"Bus number:", "bus_num"}, {"VOANLA - Zip Code", "zip"}, {"Cell Phone", "phone"}, {"City", "city"}, {"Does child ride the bus?", "ride_bus"}, {"Does your child receive behavioral health services?", "behavioral_health"}, {"Education Release Effective for School Year:", "edu_release"}, {"Education Release on file?", "edu_release_onFile"}, {"Email Address", "email"}, {"Food Stamps", "food_stamps"}, {"Household Type", "household_type"}, {"If yes, please list the provider", "yes_provider"}, {"Is student enrolled in CIS?", "enrolled_cis"}, {"JCAMPUS signed permission on file?", "jcampus_permission_onFile"}, {"JCAMPUS/School Loop Password:", "jcampus_login"}, {"JCAMPUS/School Loop Username:", "jcampus_username"}, {"Media Release Effective for School Year:", "media_release_effective"}, {"Media Release on file?", "rekease_onFile"}, {"Name Of Teacher/Homeroom Teacher", "homeroom_teacher"}, {"Program Contract Effective for School Year:", "prgram_contract_year"}, {"Program Contract Signed and On File?", "program_contract_on_file"}, {"School Year", "school_year"}, {"ServicePoint Release of Information on File?", "sp_release_on_file"}, {"State", "state"}, {"Student ID#", "student_id"}, {"Summer Session", "summer"}, {"Total number of Persons Living at the Address", "num_at_address"}, {"Allergy/Medical Condition?", "allergy_med_condition"}, {"Application Date", "app_date"}, {"Contact #1 Name", "contact1_name"}, {"Contact #1 Phone", "contact1_phone"}, {"Contact #2 Name", "contact2_name"}, {"Contact #2 Phone", "contact2_phone"}, {"Clinic or Doctor's Name", "dr_name"}, {"Doctor's Phone", "dr_phone"}, {"Does your child have an Individual Education Plan (IEP)?", "iep"}, {"Drop-off location:", "dropoff_location"}, {"If yes, pickup location:", "pickup_location"}, {"Current Grade Level", "current_grade"}, {"Has your child been placed in special education by the school system?", "sped"}, {"Home Phone", "home_phone"}, {"If yes, please explain:", "allergy_med_condition_details"}, {"If yes, please list:", "medicine_detail"}, {"Lighthouse Location Enrolling", "lighthouse_location_enrolling"}, {"Medication at site during program hours?", "med_on_site"}, {"Parent/Guardian Name", "parent_name"}, {"Place of Employment", "place_of_employment"}}),
  #"Sorted rows" = Table.Sort(#"Renamed columns 1", {{"app_date", Order.Descending}})
in
  #"Sorted rows";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "ServicePoint"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "LH Registration"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", let latest = List.Max(#"Expanded Attributes"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 62, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 62, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"LH Registration_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_lighthouse_registration", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
