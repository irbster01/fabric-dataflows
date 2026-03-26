[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw-credible-services-filtered_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"raw-credible-services-filtered" = let
  Source = Xml.Tables(Web.Contents("https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2r5nsP4Xxsjat16wHQZAEU1Q__&start_date=&end_date=&custom_param1=&custom_param2=&custom_param3=")),
  Navigation = Source{1}[Table],
  #"Expanded Table" = Table.ExpandTableColumn(Navigation, "Table", {"Name", "Table"}, {"Name.1", "Table.1"}),
  #"Expanded Table.1 1" = Table.ExpandTableColumn(#"Expanded Table", "Table.1", {"Name", "Table"}, {"Name.2", "Table"}),
  #"Expanded Table 2" = Table.ExpandTableColumn(#"Expanded Table.1 1", "Table", {"Name", "Table"}, {"Name.3", "Table.1"}),
  #"Expanded Table.1 2" = Table.ExpandTableColumn(#"Expanded Table 2", "Table.1", {"clientvisit_id", "client_id", "emp_id", "visittype", "status", "duration", "clientfirstname", "clientlastname", "emp_name", "rate", "units_of_svc", "signature_datetime", "appr_date", "program_id", "non_billable", "contract_rate", "ins_paid_amount", "ins_due", "timeout", "is_late", "manual_update_to_modifier", "auth_id", "timein", "manual_redx", "manual_redx_note", "appr", "cotherapist", "comb_primary", "comb_secondary", "comb_units", "comb_rate", "episode_id", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "delayed_reason_code"}, {"clientvisit_id", "client_id", "emp_id", "visittype", "status", "duration", "clientfirstname", "clientlastname", "emp_name", "rate", "units_of_svc", "signature_datetime", "appr_date", "program_id", "non_billable", "contract_rate", "ins_paid_amount", "ins_due", "timeout", "is_late", "manual_update_to_modifier", "auth_id", "timein", "manual_redx", "manual_redx_note", "appr", "cotherapist", "comb_primary", "comb_secondary", "comb_units", "comb_rate", "episode_id", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "delayed_reason_code"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded Table.1 2", {{"clientvisit_id", "Service ID"}, {"client_id", "Client ID"}, {"emp_id", "Employee ID"}, {"visittype", "Service Type"}, {"status", "Status"}, {"duration", "Duration"}}),
  #"Changed column type 14" = Table.TransformColumnTypes(#"Renamed columns", {{"comb_units", type number}, {"comb_rate", type number}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type 14", {{"units_of_svc", "Units"}}),
  
  // FIX: Clean up data types and handle nulls
  #"Remove Problem Columns" = Table.RemoveColumns(#"Renamed columns 1", {"urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}),
  #"Replace Nulls" = Table.ReplaceValue(#"Remove Problem Columns", null, "", Replacer.ReplaceValue, Table.ColumnNames(#"Remove Problem Columns")),
  #"Convert All Text" = Table.TransformColumnTypes(#"Replace Nulls", List.Transform(Table.ColumnNames(#"Replace Nulls"), each {_, type text})),
  #"Final Type Cleanup" = Table.TransformColumnTypes(#"Convert All Text", {
    {"Service ID", type text},
    {"Client ID", type text}, 
    {"Employee ID", type text},
    {"Service Type", type text},
    {"Duration", type number},
    {"Units", type number},
    {"rate", type number}
  }),
  #"Sorted rows" = Table.Sort(#"Final Type Cleanup", {{"rate", Order.Descending}})
in
  #"Sorted rows";
shared #"raw-credible-services-filtered_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw-credible-services-filtered", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw-credible-services-filtered-warehouse_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Name", DestinationColumnName = "Name"], [SourceColumnName = "Name.1", DestinationColumnName = "Name.1"], [SourceColumnName = "Name.2", DestinationColumnName = "Name.2"], [SourceColumnName = "Name.3", DestinationColumnName = "Name.3"], [SourceColumnName = "Service ID", DestinationColumnName = "Service ID"], [SourceColumnName = "Client ID", DestinationColumnName = "Client ID"], [SourceColumnName = "Employee ID", DestinationColumnName = "Employee ID"], [SourceColumnName = "Service Type", DestinationColumnName = "Service Type"], [SourceColumnName = "Status", DestinationColumnName = "Status"], [SourceColumnName = "Duration", DestinationColumnName = "Duration"], [SourceColumnName = "clientfirstname", DestinationColumnName = "clientfirstname"], [SourceColumnName = "clientlastname", DestinationColumnName = "clientlastname"], [SourceColumnName = "emp_name", DestinationColumnName = "emp_name"], [SourceColumnName = "rate", DestinationColumnName = "rate"], [SourceColumnName = "Units", DestinationColumnName = "Units"], [SourceColumnName = "signature_datetime", DestinationColumnName = "signature_datetime"], [SourceColumnName = "appr_date", DestinationColumnName = "appr_date"], [SourceColumnName = "program_id", DestinationColumnName = "program_id"], [SourceColumnName = "non_billable", DestinationColumnName = "non_billable"], [SourceColumnName = "contract_rate", DestinationColumnName = "contract_rate"], [SourceColumnName = "ins_paid_amount", DestinationColumnName = "ins_paid_amount"], [SourceColumnName = "ins_due", DestinationColumnName = "ins_due"], [SourceColumnName = "timeout", DestinationColumnName = "timeout"], [SourceColumnName = "is_late", DestinationColumnName = "is_late"], [SourceColumnName = "manual_update_to_modifier", DestinationColumnName = "manual_update_to_modifier"], [SourceColumnName = "auth_id", DestinationColumnName = "auth_id"], [SourceColumnName = "timein", DestinationColumnName = "timein"], [SourceColumnName = "manual_redx", DestinationColumnName = "manual_redx"], [SourceColumnName = "manual_redx_note", DestinationColumnName = "manual_redx_note"], [SourceColumnName = "appr", DestinationColumnName = "appr"], [SourceColumnName = "cotherapist", DestinationColumnName = "cotherapist"], [SourceColumnName = "comb_primary", DestinationColumnName = "comb_primary"], [SourceColumnName = "comb_secondary", DestinationColumnName = "comb_secondary"], [SourceColumnName = "comb_units", DestinationColumnName = "comb_units"], [SourceColumnName = "comb_rate", DestinationColumnName = "comb_rate"], [SourceColumnName = "episode_id", DestinationColumnName = "episode_id"], [SourceColumnName = "delayed_reason_code", DestinationColumnName = "delayed_reason_code"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared #"raw-credible-services-filtered-warehouse" = let
  Source = Xml.Tables(Web.Contents("https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2r5nsP4Xxsjat16wHQZAEU1Q__&start_date=&end_date=&custom_param1=&custom_param2=&custom_param3=")),
  Navigation = Source{1}[Table],
  #"Expanded Table" = Table.ExpandTableColumn(Navigation, "Table", {"Name", "Table"}, {"Name.1", "Table.1"}),
  #"Expanded Table.1 1" = Table.ExpandTableColumn(#"Expanded Table", "Table.1", {"Name", "Table"}, {"Name.2", "Table"}),
  #"Expanded Table 2" = Table.ExpandTableColumn(#"Expanded Table.1 1", "Table", {"Name", "Table"}, {"Name.3", "Table.1"}),
  #"Expanded Table.1 2" = Table.ExpandTableColumn(#"Expanded Table 2", "Table.1", {"clientvisit_id", "client_id", "emp_id", "visittype", "status", "duration", "clientfirstname", "clientlastname", "emp_name", "rate", "units_of_svc", "signature_datetime", "appr_date", "program_id", "non_billable", "contract_rate", "ins_paid_amount", "ins_due", "timeout", "is_late", "manual_update_to_modifier", "auth_id", "timein", "manual_redx", "manual_redx_note", "appr", "cotherapist", "comb_primary", "comb_secondary", "comb_units", "comb_rate", "episode_id", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "delayed_reason_code"}, {"clientvisit_id", "client_id", "emp_id", "visittype", "status", "duration", "clientfirstname", "clientlastname", "emp_name", "rate", "units_of_svc", "signature_datetime", "appr_date", "program_id", "non_billable", "contract_rate", "ins_paid_amount", "ins_due", "timeout", "is_late", "manual_update_to_modifier", "auth_id", "timein", "manual_redx", "manual_redx_note", "appr", "cotherapist", "comb_primary", "comb_secondary", "comb_units", "comb_rate", "episode_id", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "delayed_reason_code"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded Table.1 2", {{"clientvisit_id", "Service ID"}, {"client_id", "Client ID"}, {"emp_id", "Employee ID"}, {"visittype", "Service Type"}, {"status", "Status"}, {"duration", "Duration"}}),
  #"Changed column type 14" = Table.TransformColumnTypes(#"Renamed columns", {{"comb_units", type number}, {"comb_rate", type number}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type 14", {{"units_of_svc", "Units"}}),
  
  // FIX: Clean up data types and handle nulls
  #"Remove Problem Columns" = Table.RemoveColumns(#"Renamed columns 1", {"urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}),
  #"Replace Nulls" = Table.ReplaceValue(#"Remove Problem Columns", null, "", Replacer.ReplaceValue, Table.ColumnNames(#"Remove Problem Columns")),
  #"Convert All Text" = Table.TransformColumnTypes(#"Replace Nulls", List.Transform(Table.ColumnNames(#"Replace Nulls"), each {_, type text})),
  #"Final Type Cleanup" = Table.TransformColumnTypes(#"Convert All Text", {
    {"Service ID", type text},
    {"Client ID", type text}, 
    {"Employee ID", type text},
    {"Service Type", type text},
    {"Duration", type number},
    {"Units", type number},
    {"rate", type number}
  }),
  #"Sorted rows" = Table.Sort(#"Final Type Cleanup", {{"rate", Order.Descending}})
in
  #"Sorted rows";
shared #"raw-credible-services-filtered-warehouse_DataDestination" = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "e4d2de35-5f5d-457f-8e5b-43f758299057"]}[Data],
  TableNavigation = Navigation_2{[Item = "raw-credible-services-filtered", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
