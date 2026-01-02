[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "lsndc_services_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared lsndc_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "lsndc_services"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Client ID", type text}, {"Service Start Date", type date}, {"Provider Specific Service", type text}, {"Provider Creating", type text}, {"Service Staff", type text}, {"Completed Follow Up Date", type text}, {"Call Record ID", type text}, {"Client ID_1", Int64.Type}, {"Code", type text}, {"Destination Other", type text}, {"Destination", type text}, {"Follow Up Made", type text}, {"Follow Up User", type text}, {"Group ID", Int64.Type}, {"HOPWA Financial Assistance Amount", type text}, {"HOPWA Financial Assistance Type", type text}, {"HOPWA Service Type", type text}, {"Household ID", Int64.Type}, {"HPRP Financial Assistance Type (Retired)", type text}, {"HPRP Financial Assistance End Date (Retired)", type text}, {"HPRP Financial Assistance Start Date (Retired)", type text}, {"HPRP Housing Relocation And Stabilization Service (Retired)", type text}, {"Moving On Other", type text}, {"Moving On Assistance", type text}, {"MAP Call Recordset ID", type text}, {"Need ID", Int64.Type}, {"PATH Referral Outcome", type text}, {"PATH Referral Type", type text}, {"PATH Service Type", type text}, {"Service Provider", type text}, {"Needs Referral Date", type text}, {"Shelter Bed", type text}, {"Shelter Bedlist", type text}, {"Shelter Behavior Notes", type text}, {"Shelter Item", type text}, {"Shelter On Premises", type text}, {"Shelter Supplies Given", type text}, {"Shelter Supplies Returned", type text}, {"SSVF Direct Public Benefits", type text}, {"SSVF Financial Assistance Amount", type number}, {"SSVF Financial Assistance Type", type text}, {"SSVF Other Public Benefits", type text}, {"SSVF Other Supportive Service", type text}, {"SSVF Service Type", type text}, {"SSVF VA Benefits", type text}, {"Service ID", type text}}),
  #"Removed duplicates" = Table.Distinct(#"Changed column type", {"Client ID", "Service Start Date", "Provider Specific Service", "Provider Creating", "Service Staff", "Completed Follow Up Date", "Call Record ID", "Client ID_1", "Code", "Destination Other", "Destination", "Follow Up Made", "Follow Up User", "Group ID", "HOPWA Financial Assistance Amount", "HOPWA Financial Assistance Type", "HOPWA Service Type", "Household ID", "HPRP Financial Assistance Type (Retired)", "HPRP Financial Assistance End Date (Retired)", "HPRP Financial Assistance Start Date (Retired)", "HPRP Housing Relocation And Stabilization Service (Retired)", "Moving On Other", "Moving On Assistance", "MAP Call Recordset ID", "Need ID", "PATH Referral Outcome", "PATH Referral Type", "PATH Service Type", "Service Provider", "Needs Referral Date", "Shelter Bed", "Shelter Bedlist", "Shelter Behavior Notes", "Shelter Item", "Shelter On Premises", "Shelter Supplies Given", "Shelter Supplies Returned", "SSVF Direct Public Benefits", "SSVF Financial Assistance Amount", "SSVF Financial Assistance Type", "SSVF Other Public Benefits", "SSVF Other Supportive Service", "SSVF Service Type", "SSVF VA Benefits", "Service ID"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed duplicates", {{"Service ID", "service_id"}, {"SSVF VA Benefits", "va_ben"}, {"SSVF Service Type", "service_type"}, {"SSVF Other Supportive Service", "ssvf_other"}, {"SSVF Other Public Benefits", "public_benefits"}}),
  #"Marked key columns" = Table.AddKey(#"Renamed columns", {"service_id"}, false),
  #"Renamed columns 1" = Table.RenameColumns(#"Marked key columns", {{"SSVF Financial Assistance Type", "assistance_type"}, {"SSVF Financial Assistance Amount", "assistance_amount"}, {"SSVF Direct Public Benefits", "direct_public_benefits"}, {"Shelter Supplies Returned", "supplies_returned"}, {"Shelter Supplies Given", "supplies_given"}, {"Shelter On Premises", "shelter_on_prem"}, {"Shelter Item", "shelter_item"}, {"Shelter Behavior Notes", "shelter_behavior_notes"}, {"Shelter Bedlist", "shelter_bedlist"}, {"Shelter Bed", "shelter_bed"}, {"Needs Referral Date", "needs_referral_date"}, {"Service Provider", "program"}, {"PATH Service Type", "path_service_type"}, {"PATH Referral Type", "path_referral_type"}, {"PATH Referral Outcome", "path_referral_outcome"}, {"Need ID", "need_id"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns 1", {"MAP Call Recordset ID", "Moving On Assistance", "Moving On Other", "HPRP Housing Relocation And Stabilization Service (Retired)", "HPRP Financial Assistance Start Date (Retired)", "HPRP Financial Assistance End Date (Retired)", "HPRP Financial Assistance Type (Retired)"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns", {{"Household ID", "household_id"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns 2", {"HOPWA Service Type", "HOPWA Financial Assistance Type", "HOPWA Financial Assistance Amount"}),
  #"Renamed columns 3" = Table.RenameColumns(#"Removed columns 1", {{"Group ID", "group_id"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns 3", {"Follow Up User", "Follow Up Made", "Destination", "Destination Other"}),
  #"Renamed columns 4" = Table.RenameColumns(#"Removed columns 2", {{"Code", "code"}}),
  #"Removed columns 3" = Table.RemoveColumns(#"Renamed columns 4", {"Client ID_1", "Call Record ID", "Completed Follow Up Date", "Service Staff"}),
  #"Renamed columns 5" = Table.RenameColumns(#"Removed columns 3", {{"Provider Creating", "program_creating"}}),
  #"Removed columns 4" = Table.RemoveColumns(#"Renamed columns 5", {"service_type"}),
  #"Renamed columns 6" = Table.RenameColumns(#"Removed columns 4", {{"Provider Specific Service", "service_type"}, {"Service Start Date", "service_date"}, {"Client ID", "client_id"}}),
  #"Added suffix" = Table.TransformColumns(#"Renamed columns 6", {{"client_id", each _ & "-LS", type text}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added suffix", {{"need_id", type text}})
in
  #"Changed column type 1";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "lsndc_services"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 46, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 46, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared lsndc_services_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_lsndc_services", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
