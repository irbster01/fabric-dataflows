[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_credible_clients = let
  Source = PowerPlatform.Dataflows([]),
  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[dataflowId = "19d1d202-7944-4b34-9997-fe7bc7779285"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[entity = "raw_credible_clients-warehouse", version = ""]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 4", {"Client ID"}, raw_credible_episodes, {"Client ID"}, "raw_credible_episodes", JoinKind.LeftOuter),
  #"Expanded raw_credible_episodes" = Table.ExpandTableColumn(#"Merged queries", "raw_credible_episodes", {"Episode ID", "Client ID", "Episode Status", "Assessment Date", "Admission Date", "program_id1", "Program ID", "assessment_date", "Date Episode Updated", "Date Episode Created", "Length of Stay", "Discharge Date", "discharge_date1", "Discharge Outcome", "Discharge Location", "Date Auto Closed", "Referral Date", "Referred By", "Referred Source", "fep_first_contact"}, {"Episode ID", "Client ID.1", "Episode Status", "Assessment Date", "Admission Date.1", "program_id1", "Program ID", "assessment_date", "Date Episode Updated", "Date Episode Created", "Length of Stay", "Discharge Date.1", "discharge_date1", "Discharge Outcome", "Discharge Location", "Date Auto Closed", "Referral Date", "Referred By", "Referred Source", "fep_first_contact"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded raw_credible_episodes", {"employeeid"})
in
  #"Removed columns";
shared raw_credible_episodes = let
  Source = PowerPlatform.Dataflows([]),
  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[dataflowId = "1bb092d7-7030-409d-bcb2-d319cb85108c"]}[Data],
  #"Expanded Data" = Table.ExpandTableColumn(#"Navigation 3", "Data", {"Episode ID", "Client ID", "Episode Status", "Assessment Date", "Admission Date", "program_id1", "Program ID", "assessment_date", "Date Episode Updated", "Date Episode Created", "Length of Stay", "Discharge Date", "discharge_date1", "Discharge Outcome", "Discharge Location", "Date Auto Closed", "Referral Date", "Referred By", "Referred Source", "fep_first_contact"}, {"Episode ID", "Client ID", "Episode Status", "Assessment Date", "Admission Date", "program_id1", "Program ID", "assessment_date", "Date Episode Updated", "Date Episode Created", "Length of Stay", "Discharge Date", "discharge_date1", "Discharge Outcome", "Discharge Location", "Date Auto Closed", "Referral Date", "Referred By", "Referred Source", "fep_first_contact"})
in
  #"Expanded Data";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "transform-credible-clients-active_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"transform-credible-clients-active" = let
  Source = PowerPlatform.Dataflows([]),
  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[dataflowId = "19d1d202-7944-4b34-9997-fe7bc7779285"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[entity = "raw_credible_clients-warehouse", version = ""]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 4", {"Client ID"}, raw_credible_episodes, {"Client ID"}, "raw_credible_episodes", JoinKind.LeftOuter),
  #"Expanded raw_credible_episodes" = Table.ExpandTableColumn(#"Merged queries", "raw_credible_episodes", {"Episode ID", "Client ID", "Episode Status", "Assessment Date", "Admission Date", "program_id1", "Program ID", "assessment_date", "Date Episode Updated", "Date Episode Created", "Length of Stay", "Discharge Date", "discharge_date1", "Discharge Outcome", "Discharge Location", "Date Auto Closed", "Referral Date", "Referred By", "Referred Source", "fep_first_contact"}, {"Episode ID", "Client ID.1", "Episode Status", "Assessment Date", "Admission Date.1", "program_id1", "Program ID", "assessment_date", "Date Episode Updated", "Date Episode Created", "Length of Stay", "Discharge Date.1", "discharge_date1", "Discharge Outcome", "Discharge Location", "Date Auto Closed", "Referral Date", "Referred By", "Referred Source", "fep_first_contact"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded raw_credible_episodes", {"employeeid"}),
  #"Filtered rows" = Table.SelectRows(#"Removed columns", each [Episode Status] = "Enrolled"),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows", {"programs", "external_id", "Status", "Client Status Date", "Date Created", "First Name", "Last Name", "preferred_language", "Date Diagnosis Updated", "Diagnosis 1", "race_omb", "race_omb2", "ethnicity_omb", "Client Type"})
in
  #"Removed columns 1";
[Description = "An active count of clients by Program", DataDestinations = {[Definition = [Kind = "Reference", QueryName = "analytics-credible-active-client-count_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"analytics-credible-active-client-count" = let
  Source = PowerPlatform.Dataflows([]),
  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[dataflowId = "19d1d202-7944-4b34-9997-fe7bc7779285"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[entity = "raw_credible_clients-warehouse", version = ""]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 4", {"Client ID"}, raw_credible_episodes, {"Client ID"}, "raw_credible_episodes", JoinKind.LeftOuter),
  #"Expanded raw_credible_episodes" = Table.ExpandTableColumn(#"Merged queries", "raw_credible_episodes", {"Episode ID", "Client ID", "Episode Status", "Assessment Date", "Admission Date", "program_id1", "Program ID", "assessment_date", "Date Episode Updated", "Date Episode Created", "Length of Stay", "Discharge Date", "discharge_date1", "Discharge Outcome", "Discharge Location", "Date Auto Closed", "Referral Date", "Referred By", "Referred Source", "fep_first_contact"}, {"Episode ID", "Client ID.1", "Episode Status", "Assessment Date", "Admission Date.1", "program_id1", "Program ID", "assessment_date", "Date Episode Updated", "Date Episode Created", "Length of Stay", "Discharge Date.1", "discharge_date1", "Discharge Outcome", "Discharge Location", "Date Auto Closed", "Referral Date", "Referred By", "Referred Source", "fep_first_contact"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded raw_credible_episodes", {"employeeid"}),
  #"Filtered rows" = Table.SelectRows(#"Removed columns", each [Episode Status] = "Enrolled"),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows", {"programs", "external_id", "Status", "Client Status Date", "Date Created", "First Name", "Last Name", "preferred_language", "Date Diagnosis Updated", "Diagnosis 1", "race_omb", "race_omb2", "ethnicity_omb", "Client Type"}),
  #"Grouped rows" = Table.Group(#"Removed columns 1", {"Program ID"}, {{"Count", each Table.RowCount(Table.Distinct(_)), Int64.Type}})
in
  #"Grouped rows";
shared #"transform-credible-clients-active_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "transform-credible-clients-active", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"analytics-credible-active-client-count_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c46fe4a6-e106-4e77-993b-efc9ce22e551"]}[Data],
  TableNavigation = Navigation_2{[Id = "analytics-credible-active-client-count", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
