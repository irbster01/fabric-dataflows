[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_sp_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_services", ItemKind = "Table"]}[Data],
  #"Added prefix" = Table.TransformColumns(#"Navigation 2", {{"client_id", each "SP-" & _, type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Added prefix", {{"units", "hours"}}),
  #"Inserted conditional column" = Table.AddColumn(#"Renamed columns", "ed_flag", each if Text.Contains([program_creating], "Light") then 1 else if Text.Contains([program_creating], "Teen") then 1 else 0),
  #"Merged columns" = Table.CombineColumns(#"Inserted conditional column", {"client_last_name", "client_first_name"}, Combiner.CombineTextByDelimiter(", ", QuoteStyle.None), "client_full_name"),
  #"Added prefix 1" = Table.TransformColumns(#"Merged columns", {{"service_id", each "SP-" & _, type text}}),
  #"Reordered columns" = Table.ReorderColumns(#"Added prefix 1", {"service_id", "client_id", "service_date", "client_full_name", "service_type", "hours", "user", "program_creating", "service_month", "ed_flag"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Reordered columns", {{"program_creating", "program"}}),
  #"Reordered columns 1" = Table.ReorderColumns(#"Renamed columns 1", {"service_id", "client_id", "service_date", "client_full_name", "service_type", "hours", "program", "user", "service_month", "ed_flag"}),
  #"Removed columns" = Table.RemoveColumns(#"Reordered columns 1", {"user", "service_month", "client_full_name"})
in
  #"Removed columns";
shared raw_credible_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_services", ItemKind = "Table"]}[Data],
  #"Added prefix" = Table.TransformColumns(#"Navigation 2", {{"client_id", each "BH-" & _, type text}}),
  #"Divided column" = Table.TransformColumns(#"Added prefix", {{"duration", each _ / 60, type number}}),
  #"Renamed columns" = Table.RenameColumns(#"Divided column", {{"duration", "hours"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"service_status", "rate", "units", "signature_datetime", "appr_date", "non_billable", "contract_rate", "ins_paid_amount", "ins_due", "is_late", "manual_update_to_modifier", "auth_id", "manual_redx", "manual_redx_note", "delayed_reason_code", "episode_id", "comb_rate", "comb_units", "comb_secondary", "comb_primary", "cotherapist", "appr"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed columns", "6", "Adult BH", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "12", "6021", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "13", "Desoto BH", Replacer.ReplaceValue, {"program_id"}),
  #"Filtered rows" = Table.SelectRows(#"Replaced value 2", each ([program_id] <> "10" and [program_id] <> "11")),
  #"Replaced value 3" = Table.ReplaceValue(#"Filtered rows", "14", "Epicenter", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "15", "IBHH", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "16", "CHRP", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "5", "BH - Office Services", Replacer.ReplaceValue, {"program_id"}),
  #"Sorted rows" = Table.Sort(#"Replaced value 6", {{"program_id", Order.Ascending}}),
  #"Filtered rows 1" = Table.SelectRows(#"Sorted rows", each ([program_id] <> "17" and [program_id] <> "7" and [program_id] <> "8" and [program_id] <> "9")),
  #"Inserted conditional column" = Table.AddColumn(#"Filtered rows 1", "ed_flag", each if Text.Contains([program_id], "Desoto") then 1 else if Text.Contains([program_id], "6021") then 1 else if Text.Contains([emp_name], "Kelley") then 1 else if Text.Contains([emp_name], "Dukes") then 1 else 0),
  #"Removed columns 1" = Table.RemoveColumns(#"Inserted conditional column", {"employee_id", "emp_name"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"ed_flag", Int64.Type}}),
  #"Merged columns" = Table.CombineColumns(#"Changed column type", {"client_last_name", "client_first_name"}, Combiner.CombineTextByDelimiter(", ", QuoteStyle.None), "client_full_name"),
  #"Added prefix 1" = Table.TransformColumns(#"Merged columns", {{"service_id", each "BH-" & _, type text}}),
  #"Reordered columns" = Table.ReorderColumns(#"Added prefix 1", {"service_id", "client_id", "client_full_name", "service_type", "hours", "program_id", "service_date", "ed_flag"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Reordered columns", {{"program_id", "program"}}),
  #"Reordered columns 1" = Table.ReorderColumns(#"Renamed columns 1", {"service_id", "client_id", "service_date", "client_full_name", "service_type", "hours", "program", "ed_flag"}),
  #"Removed columns 2" = Table.RemoveColumns(#"Reordered columns 1", {"client_full_name"})
in
  #"Removed columns 2";
shared raw_lsndc_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_services", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"group_id", "code", "household_id", "need_id", "path_referral_outcome", "path_referral_type", "path_service_type", "needs_referral_date", "shelter_bed", "shelter_bedlist", "shelter_behavior_notes", "shelter_item", "shelter_on_prem", "supplies_given", "supplies_returned", "direct_public_benefits", "public_benefits", "ssvf_other", "va_ben"}),
  #"Added prefix" = Table.TransformColumns(#"Removed columns", {{"service_id", each "LS-" & _, type text}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Added prefix", "ed_flag", each 0), {{"ed_flag", Int64.Type}}),
  #"Reordered columns" = Table.ReorderColumns(#"Added custom", {"client_id", "service_id", "service_date", "service_type", "program_creating", "program", "assistance_amount", "assistance_type", "ed_flag"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Reordered columns", {"program"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns 1", {{"assistance_type", "notes"}}),
  #"Reordered columns 1" = Table.ReorderColumns(#"Renamed columns", {"service_id", "client_id", "service_date", "service_type", "program_creating", "assistance_amount", "notes", "ed_flag"}),
  #"Removed columns 2" = Table.RemoveColumns(#"Reordered columns 1", {"service_type"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns 2", {{"notes", "service_type"}, {"program_creating", "program"}})
in
  #"Renamed columns 1";
shared raw_cisdm_case_management_entries = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_case_management_entries", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 2", {{"sasid", "client_id"}}),
  #"Added prefix" = Table.TransformColumns(#"Renamed columns", {{"client_id", each "CIS-" & _, type text}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Added prefix", "ed_flag", each 1), {{"ed_flag", Int64.Type}}),
  #"Reordered columns" = Table.ReorderColumns(#"Added custom", {"service_id", "client_id", "org", "school_name", "client_full_name", "grade", "service_type", "notes", "activity", "ind_or_group", "hours", "provider", "coordinator_name", "entry_date", "exit_date", "school_year", "ed_flag"})
in
  #"Reordered columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "cred_sp_lsndc_services_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared cred_sp_lsndc_services = let
  Source = Table.Combine({raw_credible_services, raw_lsndc_services, raw_sp_services}),
  #"Changed column type" = Table.TransformColumnTypes(Source, {{"ed_flag", Int64.Type}}),
  #"Merged queries" = Table.NestedJoin(#"Changed column type", {"client_id"}, raw_credible_client_addresses, {"client_id"}, "raw_credible_client_addresses", JoinKind.LeftOuter),
  #"Expanded raw_credible_client_addresses" = Table.ExpandTableColumn(#"Merged queries", "raw_credible_client_addresses", {"client_name", "dob", "gender", "zip", "city", "address", "state", "full_address"}, {"client_name", "dob", "gender", "zip", "city", "address", "state", "full_address"}),
  #"Merged queries 1" = Table.NestedJoin(#"Expanded raw_credible_client_addresses", {"client_id"}, raw_sp_client_addresses, {"client_id"}, "raw_sp_client_addresses", JoinKind.LeftOuter),
  #"Expanded raw_sp_client_addresses" = Table.ExpandTableColumn(#"Merged queries 1", "raw_sp_client_addresses", {"z", "client_last_name", "client_id", "full_address", "zip"}, {"z", "client_last_name", "client_id.1", "full_address.1", "zip.1"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded raw_sp_client_addresses", {{"z", "client_first_name"}}),
  #"Merged columns" = Table.CombineColumns(#"Renamed columns", {"client_last_name", "client_first_name"}, Combiner.CombineTextByDelimiter(",", QuoteStyle.None), "client_name (2)"),
  #"Replaced value" = Table.ReplaceValue(#"Merged columns", ",", "", Replacer.ReplaceValue, {"client_name (2)"}),
  #"Merged columns 1" = Table.CombineColumns(#"Replaced value", {"client_name", "client_name (2)"}, Combiner.CombineTextByDelimiter("", QuoteStyle.None), "client_name"),
  #"Merged columns 2" = Table.CombineColumns(#"Merged columns 1", {"address", "full_address.1"}, Combiner.CombineTextByDelimiter("", QuoteStyle.None), "client_full_address"),
  #"Renamed columns 1" = Table.RenameColumns(#"Merged columns 2", {{"client_id", "client_id2"}}),
  #"Merged columns 3" = Table.CombineColumns(#"Renamed columns 1", {"client_id2", "client_id.1"}, Combiner.CombineTextByDelimiter("", QuoteStyle.None), "client_id")
in
  #"Merged columns 3";
shared cred_sp_lsndc_services_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c46fe4a6-e106-4e77-993b-efc9ce22e551"]}[Data],
  TableNavigation = Navigation_2{[Id = "cred_sp_lsndc_services_merged", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared raw_cred_eligibility = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cred_eligibility", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared raw_credible_client_addresses = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_client_addresses", ItemKind = "Table"]}[Data],
  #"Added prefix" = Table.TransformColumns(#"Navigation 2", {{"client_id", each "BH-" & _, type text}})
in
  #"Added prefix";
shared raw_sp_client_addresses = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_client_addresses", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 2", {{"client_id", "z"}, {"client_first_name", "client_id"}}),
  #"Added prefix" = Table.TransformColumns(#"Renamed columns", {{"client_id", each "SP-" & _, type text}})
in
  #"Added prefix";
