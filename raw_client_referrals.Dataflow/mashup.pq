[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Client Referral_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Client Referral" = let
  Source = SharePoint.Tables("https://voanorthlaorg.sharepoint.com/sites/TheVOAHub", [Implementation = "2.0", ViewMode = "All"]),
  #"Navigation 1" = Source{[Id = "dc4f6942-1365-4a6e-ac3f-7b44640f33f6"]}[Items],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 1", {{"Referral Date", "referral_date"}, {"Referral Program", "referral_program"}, {"Referral Status", "referral_status"}, {"First Name", "client_first_name"}, {"Last Name", "client_last_name"}, {"Phone Number", "client_phone"}, {"Behavioral Health", "behavioral_health"}, {"Housing", "housing"}, {"Lighthouse", "lighthouse"}, {"Referral Outcome", "referral_outcome"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"Staff Given Referral"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"Referring Organization", "referring_org"}, {"Referral Source", "referral_source"}, {"Referral Phone", "referral_phone"}, {"Client E-mail", "client_email"}, {"Date Referral Closed", "date_referral_closed"}, {"Assigned Assessor", "assigned_assessor"}, {"School District", "school_district"}, {"Created", "date_created"}, {"Title", "referral_id"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns 1", {"Color Tag", "Compliance Asset Id", "1st Referral", "2nd Referral?", "Control"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"Last 4", "client_last4"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns 2", {"Outcome ID"}),
  #"Renamed columns 3" = Table.RenameColumns(#"Removed columns 2", {{"Age", "client_age"}, {"School", "client_school"}, {"Current Grade", "client_grade"}, {"Previous Hospitalization?", "hospitalization"}, {"Where did you stay last night?", "stay_last_night"}, {"Client Gender", "client_gender"}, {"Address", "client_address"}, {"City", "client_city"}, {"Zip", "client_zip"}, {"Additional Comments", "addtl_comments"}, {"VI-SPDAT Score", "vispdat"}, {"ID", "id"}}),
  #"Removed columns 3" = Table.RemoveColumns(#"Renamed columns 3", {"Content Type", "Modified", "Created By", "Modified By", "Version", "Attachments", "Edit", "Type", "Item Child Count", "Folder Child Count", "Label setting", "Retention label", "Retention label Applied", "Label applied by", "Item is a Record", "App Created By", "App Modified By"})
in
  #"Removed columns 3";
shared #"Client Referral_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "fccbdcf0-4c42-431d-8e07-ae4b00bda780"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_client_referrals", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "BH New Referrals_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"BH New Referrals" = let
  Source = #"Client Referral",
  #"Filtered rows" = Table.SelectRows(Source, each ([referral_program] <> "Housing" and [referral_program] <> "Lighthouse")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each Date.IsInPreviousNDays([referral_date], 1))
in
  #"Filtered rows 1";
shared #"BH New Referrals_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "new_bh_referrals", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
