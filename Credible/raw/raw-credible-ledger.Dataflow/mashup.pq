[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw-credible-ledger_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"raw-credible-ledger" = let
  Source = Xml.Tables(Web.Contents("https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2rJzEXOqMmJkA!Qc9fep!7lg__&start_date=&end_date=&custom_param1=&custom_param2=&custom_param3="), null, 65001),
  #"Expanded Table" = Table.ExpandTableColumn(Source, "Table", {"Name", "Table"}, {"Name.1", "Table.1"}),
  #"Expanded Table.1" = Table.ExpandTableColumn(#"Expanded Table", "Table.1", {"element", "Attribute:id", "Name", "Table"}, {"element", "Attribute:id", "Name.2", "Table"}),
  #"Expanded Table 1" = Table.ExpandTableColumn(#"Expanded Table.1", "Table", {"Name", "Table"}, {"Name.3", "Table.1"}),
  #"Expanded Table.1 2" = Table.ExpandTableColumn(#"Expanded Table 1", "Table.1", {"Name", "Table"}, {"Name.4", "Table"}),
  #"Expanded Table 3" = Table.ExpandTableColumn(#"Expanded Table.1 2", "Table", {"clientvisit_id", "claim_id", "visit_dateday", "last_name", "first_name", "visittype", "cptcode", "cpt_modifier1", "program_desc", "location_desc", "recipient_desc", "emp_name", "status", "rate", "payer_code", "appr_date", "revenue_code", "cur_batch_id", "external_claim_id", "balance", "date_batched", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}, {"clientvisit_id", "claim_id", "visit_dateday", "last_name", "first_name", "visittype", "cptcode", "cpt_modifier1", "program_desc", "location_desc", "recipient_desc", "emp_name", "status", "rate", "payer_code", "appr_date", "revenue_code", "cur_batch_id", "external_claim_id", "balance", "date_batched", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Table 3", {"Name", "Name.1", "element", "Attribute:id", "Name.2", "Name.3", "Name.4"}),
  #"Removed top rows" = Table.Skip(#"Removed columns", 1),
  #"Removed columns 1" = Table.RemoveColumns(#"Removed top rows", {"urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns 1", {{"claim_id", "Claim ID"}}),
  #"Extracted text range" = Table.TransformColumns(#"Renamed columns", {{"appr_date", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Extracted text range", {{"appr_date", type date}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type 2", {{"clientvisit_id", "Service ID"}, {"visit_dateday", "Service Date"}}),
  #"Merged columns" = Table.CombineColumns(Table.TransformColumnTypes(#"Renamed columns 1", {{"last_name", type text}, {"first_name", type text}}), {"last_name", "first_name"}, Combiner.CombineTextByDelimiter(", ", QuoteStyle.None), "Client Name"),
  #"Renamed columns 2" = Table.RenameColumns(#"Merged columns", {{"visittype", "Service Type"}, {"cptcode", "CPT Code"}, {"cpt_modifier1", "Modifier"}, {"program_desc", "Program"}, {"location_desc", "Location"}, {"recipient_desc", "Recipient"}, {"emp_name", "Employee Name"}, {"status", "Status"}, {"rate", "Rate"}, {"payer_code", "Pri Payer Code"}, {"appr_date", "Appr Date"}, {"revenue_code", "Revenue Code"}, {"cur_batch_id", "Current Billing Batch"}, {"external_claim_id", "ICN"}, {"balance", "Balance"}, {"date_batched", "Date Batched"}}),
  #"Extracted text range 1" = Table.TransformColumns(#"Renamed columns 2", {{"Date Batched", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Extracted text range 1", {{"Date Batched", type date}}),
  #"Reordered columns" = Table.ReorderColumns(#"Changed column type 3", {"Service ID", "Claim ID", "Service Date", "Client Name", "Service Type", "CPT Code", "Modifier", "Program", "Location", "Recipient", "Employee Name", "Status", "Rate", "Pri Payer Code", "Date Batched", "Balance", "Appr Date", "Revenue Code", "Current Billing Batch", "ICN"}),
  #"Transform columns" = Table.TransformColumnTypes(#"Reordered columns", {{"Service ID", type text}, {"Claim ID", type text}, {"Service Type", type text}, {"CPT Code", type text}, {"Modifier", type text}, {"Program", type text}, {"Location", type text}, {"Recipient", type text}, {"Employee Name", type text}, {"Status", type text}, {"Rate", type text}, {"Pri Payer Code", type text}, {"Balance", type text}, {"Revenue Code", type text}, {"Current Billing Batch", type text}, {"ICN", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"Service ID", null}, {"Claim ID", null}, {"Service Type", null}, {"CPT Code", null}, {"Modifier", null}, {"Program", null}, {"Location", null}, {"Recipient", null}, {"Employee Name", null}, {"Status", null}, {"Rate", null}, {"Pri Payer Code", null}, {"Balance", null}, {"Revenue Code", null}, {"Current Billing Batch", null}, {"ICN", null}}),
  #"Transform columns 1" = Table.TransformColumnTypes(#"Replace errors", {{"Service Date", type text}}),
  #"Replace errors 1" = Table.ReplaceErrorValues(#"Transform columns 1", {{"Service Date", null}}),
  #"Extracted text range 2" = Table.TransformColumns(#"Replace errors 1", {{"Service Date", each Text.Middle(_, 0, 10), type text}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Extracted text range 2", {{"Service Date", type date}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw-credible-ledger-warehouse_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Service ID", DestinationColumnName = "Service ID"], [SourceColumnName = "Claim ID", DestinationColumnName = "Claim ID"], [SourceColumnName = "Service Date", DestinationColumnName = "Service Date"], [SourceColumnName = "Client Name", DestinationColumnName = "Client Name"], [SourceColumnName = "Service Type", DestinationColumnName = "Service Type"], [SourceColumnName = "CPT Code", DestinationColumnName = "CPT Code"], [SourceColumnName = "Modifier", DestinationColumnName = "Modifier"], [SourceColumnName = "Program", DestinationColumnName = "Program"], [SourceColumnName = "Location", DestinationColumnName = "Location"], [SourceColumnName = "Recipient", DestinationColumnName = "Recipient"], [SourceColumnName = "Employee Name", DestinationColumnName = "Employee Name"], [SourceColumnName = "Status", DestinationColumnName = "Status"], [SourceColumnName = "Rate", DestinationColumnName = "Rate"], [SourceColumnName = "Pri Payer Code", DestinationColumnName = "Pri Payer Code"], [SourceColumnName = "Date Batched", DestinationColumnName = "Date Batched"], [SourceColumnName = "Balance", DestinationColumnName = "Balance"], [SourceColumnName = "Appr Date", DestinationColumnName = "Appr Date"], [SourceColumnName = "Revenue Code", DestinationColumnName = "Revenue Code"], [SourceColumnName = "Current Billing Batch", DestinationColumnName = "Current Billing Batch"], [SourceColumnName = "ICN", DestinationColumnName = "ICN"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"raw-credible-ledger-warehouse" = let
  Source = Xml.Tables(Web.Contents("https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2rJzEXOqMmJkA!Qc9fep!7lg__&start_date=&end_date=&custom_param1=&custom_param2=&custom_param3="), null, 65001),
  #"Expanded Table" = Table.ExpandTableColumn(Source, "Table", {"Name", "Table"}, {"Name.1", "Table.1"}),
  #"Expanded Table.1" = Table.ExpandTableColumn(#"Expanded Table", "Table.1", {"element", "Attribute:id", "Name", "Table"}, {"element", "Attribute:id", "Name.2", "Table"}),
  #"Expanded Table 1" = Table.ExpandTableColumn(#"Expanded Table.1", "Table", {"Name", "Table"}, {"Name.3", "Table.1"}),
  #"Expanded Table.1 2" = Table.ExpandTableColumn(#"Expanded Table 1", "Table.1", {"Name", "Table"}, {"Name.4", "Table"}),
  #"Expanded Table 3" = Table.ExpandTableColumn(#"Expanded Table.1 2", "Table", {"clientvisit_id", "claim_id", "visit_dateday", "last_name", "first_name", "visittype", "cptcode", "cpt_modifier1", "program_desc", "location_desc", "recipient_desc", "emp_name", "status", "rate", "payer_code", "appr_date", "revenue_code", "cur_batch_id", "external_claim_id", "balance", "date_batched", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}, {"clientvisit_id", "claim_id", "visit_dateday", "last_name", "first_name", "visittype", "cptcode", "cpt_modifier1", "program_desc", "location_desc", "recipient_desc", "emp_name", "status", "rate", "payer_code", "appr_date", "revenue_code", "cur_batch_id", "external_claim_id", "balance", "date_batched", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Table 3", {"Name", "Name.1", "element", "Attribute:id", "Name.2", "Name.3", "Name.4"}),
  #"Removed top rows" = Table.Skip(#"Removed columns", 1),
  #"Removed columns 1" = Table.RemoveColumns(#"Removed top rows", {"urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns 1", {{"claim_id", "Claim ID"}}),
  #"Extracted text range" = Table.TransformColumns(#"Renamed columns", {{"appr_date", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Extracted text range", {{"appr_date", type date}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type 2", {{"clientvisit_id", "Service ID"}, {"visit_dateday", "Service Date"}}),
  #"Merged columns" = Table.CombineColumns(Table.TransformColumnTypes(#"Renamed columns 1", {{"last_name", type text}, {"first_name", type text}}), {"last_name", "first_name"}, Combiner.CombineTextByDelimiter(", ", QuoteStyle.None), "Client Name"),
  #"Renamed columns 2" = Table.RenameColumns(#"Merged columns", {{"visittype", "Service Type"}, {"cptcode", "CPT Code"}, {"cpt_modifier1", "Modifier"}, {"program_desc", "Program"}, {"location_desc", "Location"}, {"recipient_desc", "Recipient"}, {"emp_name", "Employee Name"}, {"status", "Status"}, {"rate", "Rate"}, {"payer_code", "Pri Payer Code"}, {"appr_date", "Appr Date"}, {"revenue_code", "Revenue Code"}, {"cur_batch_id", "Current Billing Batch"}, {"external_claim_id", "ICN"}, {"balance", "Balance"}, {"date_batched", "Date Batched"}}),
  #"Extracted text range 1" = Table.TransformColumns(#"Renamed columns 2", {{"Date Batched", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Extracted text range 1", {{"Date Batched", type date}}),
  #"Reordered columns" = Table.ReorderColumns(#"Changed column type 3", {"Service ID", "Claim ID", "Service Date", "Client Name", "Service Type", "CPT Code", "Modifier", "Program", "Location", "Recipient", "Employee Name", "Status", "Rate", "Pri Payer Code", "Date Batched", "Balance", "Appr Date", "Revenue Code", "Current Billing Batch", "ICN"}),
  #"Transform columns" = Table.TransformColumnTypes(#"Reordered columns", {{"Service ID", type text}, {"Claim ID", type text}, {"Service Type", type text}, {"CPT Code", type text}, {"Modifier", type text}, {"Program", type text}, {"Location", type text}, {"Recipient", type text}, {"Employee Name", type text}, {"Status", type text}, {"Rate", type text}, {"Pri Payer Code", type text}, {"Balance", type text}, {"Revenue Code", type text}, {"Current Billing Batch", type text}, {"ICN", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"Service ID", null}, {"Claim ID", null}, {"Service Type", null}, {"CPT Code", null}, {"Modifier", null}, {"Program", null}, {"Location", null}, {"Recipient", null}, {"Employee Name", null}, {"Status", null}, {"Rate", null}, {"Pri Payer Code", null}, {"Balance", null}, {"Revenue Code", null}, {"Current Billing Batch", null}, {"ICN", null}}),
  #"Transform columns 1" = Table.TransformColumnTypes(#"Replace errors", {{"Service Date", type text}}),
  #"Replace errors 1" = Table.ReplaceErrorValues(#"Transform columns 1", {{"Service Date", null}}),
  #"Extracted text range 2" = Table.TransformColumns(#"Replace errors 1", {{"Service Date", each Text.Middle(_, 0, 10), type text}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Extracted text range 2", {{"Service Date", type date}})
in
  #"Changed column type";
shared #"raw-credible-ledger-warehouse_DataDestination" = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "e4d2de35-5f5d-457f-8e5b-43f758299057"]}[Data],
  TableNavigation = Navigation_2{[Item = "raw-credible-ledger-V2", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared #"raw-credible-ledger_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw-credible-ledger-V2", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
