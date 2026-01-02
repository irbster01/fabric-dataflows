[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_credible_clients-warehouse_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"raw_credible_clients-warehouse" = let
  Source = PowerPlatform.Dataflows([]),
  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[dataflowId = "19d1d202-7944-4b34-9997-fe7bc7779285"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[entity = "raw_credible_clients-warehouse", version = ""]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 4", {"employeeid", "programs", "external_id", "Status", "Client Status Date", "Date Created", "Date Updated", "Age", "SSN", "county", "preferred_language", "Admission Date", "Date Diagnosis Updated", "Diagnosis 1", "race_omb", "race_omb2", "ethnicity_omb", "Client Type", "dd1", "Phone", "last_svc_date", "Profile Auth End Date", "teams", "Diagnosis 2", "mobile_phone", "client_email", "Diagnosis 3", "Clinician Team", "Days Between Auth", "Long ID", "Client Name - Copy", "Int ID", "Text after delimiter", "6021 Flag", "6401 Flag", "OfficeSvs Flag", "EpiCenter Flag", "Text before delimiter", "DOB - Copy", "IBHH Flag", "6020 Flag", "Employee Flag", "axis_1f", "currentschoolupdate", "currentschool", "maritalstatupdate", "maritalstatus", "axis_1e", "Redzone", "dd7", "Case Manager Emp ID", "show_pbm_medhistory", "dd6", "Discharge Date", "dd4"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Client Name", "client_name"}, {"Client ID", "client_id"}, {"First Name", "client_first_name"}, {"Last Name", "client_last_name"}, {"DOB", "dob"}, {"Gender", "gender"}, {"Zip", "zip"}, {"City", "city"}, {"address1", "address"}, {"Full Address", "full_address"}, {"Today", "today"}}),
  #"Trimmed text" = Table.TransformColumns(#"Renamed columns", {{"full_address", each Text.Trim(_), type nullable text}}),
  #"Trimmed text 1" = Table.TransformColumns(#"Trimmed text", {{"address", each Text.Trim(_), type nullable text}})
in
  #"Trimmed text 1";
shared #"raw_credible_clients-warehouse_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_credible_client_addresses", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
