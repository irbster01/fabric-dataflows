[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Query_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"raw-credible-ibhh" = let
  Source = Xml.Tables(Web.Contents("https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2rvfSwEVK9Ds!b66T9qUKKpg__&start_date=&end_date=&custom_param1=&custom_param2=&custom_param3="), null, 65001),
  #"Navigation 1" = Source{1}[Table],
  #"Expanded Table" = Table.ExpandTableColumn(#"Navigation 1", "Table", {"Name", "Table"}, {"Name.1", "Table.1"}),
  #"Expanded Table.1" = Table.ExpandTableColumn(#"Expanded Table", "Table.1", {"Name", "Table"}, {"Name.2", "Table"}),
  #"Expanded Table 1" = Table.ExpandTableColumn(#"Expanded Table.1", "Table", {"Name", "Table"}, {"Name.3", "Table.1"}),
  #"Expanded Table.1 1" = Table.ExpandTableColumn(#"Expanded Table 1", "Table.1", {"visittype", "location_id", "status", "program_id", "rev_timein", "emp_name", "clientlastname", "clientfirstname", "client_id", "clientins_id", "ins_id", "clientvisit_id", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "which_type_of_core_service_was_582371", "service_engagement_type_585413", "type_of_contact_585505"}, {"visittype", "location_id", "status", "program_id", "rev_timein", "emp_name", "clientlastname", "clientfirstname", "client_id", "clientins_id", "ins_id", "clientvisit_id", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "which_type_of_core_service_was_582371", "service_engagement_type_585413", "type_of_contact_585505"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Table.1 1", {"Name", "Name.1", "Name.2", "Name.3", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}),
  #"Filtered rows" = Table.SelectRows(#"Removed columns", each [visittype] = "BUN: ARST" or [visittype] = "BUN: ARST-G" or [visittype] = "BUN: IST" or [visittype] = "CPST" or [visittype] = "IBHH: CM" or [visittype] = "PSR" or [visittype] = "PSR Group"),
  #"Extracted text range" = Table.TransformColumns(#"Filtered rows", {{"rev_timein", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Extracted text range", {{"rev_timein", type date}}),
  #"Transform columns" = Table.TransformColumnTypes(#"Changed column type", {{"visittype", type text}, {"location_id", type text}, {"status", type text}, {"program_id", type text}, {"emp_name", type text}, {"clientlastname", type text}, {"clientfirstname", type text}, {"which_type_of_core_service_was_582371", type text}, {"service_engagement_type_585413", type text}, {"type_of_contact_585505", type text}}),
  #"Transform columns 1" = Table.TransformColumnTypes(#"Transform columns", {{"client_id", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns 1", {{"client_id", null}}),
  #"Transform columns 2" = Table.TransformColumnTypes(#"Replace errors", {{"clientins_id", type text}}),
  #"Replace errors 1" = Table.ReplaceErrorValues(#"Transform columns 2", {{"clientins_id", null}}),
  #"Transform columns 3" = Table.TransformColumnTypes(#"Replace errors 1", {{"ins_id", type text}}),
  #"Replace errors 2" = Table.ReplaceErrorValues(#"Transform columns 3", {{"ins_id", null}}),
  #"Renamed columns" = Table.RenameColumns(#"Replace errors 2", {{"clientvisit_id", "Service ID"}}),
  #"Transform columns 4" = Table.TransformColumnTypes(#"Renamed columns", {{"Service ID", type text}}),
  #"Replace errors 3" = Table.ReplaceErrorValues(#"Transform columns 4", {{"Service ID", null}})
in
  #"Replace errors 3";
shared Query_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw-credible-ibhh", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw-credible-ibhh (2)_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "visittype", DestinationColumnName = "visittype"], [SourceColumnName = "location_id", DestinationColumnName = "location_id"], [SourceColumnName = "status", DestinationColumnName = "status"], [SourceColumnName = "program_id", DestinationColumnName = "program_id"], [SourceColumnName = "rev_timein", DestinationColumnName = "rev_timein"], [SourceColumnName = "emp_name", DestinationColumnName = "emp_name"], [SourceColumnName = "clientlastname", DestinationColumnName = "clientlastname"], [SourceColumnName = "clientfirstname", DestinationColumnName = "clientfirstname"], [SourceColumnName = "client_id", DestinationColumnName = "client_id"], [SourceColumnName = "clientins_id", DestinationColumnName = "clientins_id"], [SourceColumnName = "ins_id", DestinationColumnName = "ins_id"], [SourceColumnName = "Service ID", DestinationColumnName = "Service ID"], [SourceColumnName = "which_type_of_core_service_was_582371", DestinationColumnName = "which_type_of_core_service_was_582371"], [SourceColumnName = "service_engagement_type_585413", DestinationColumnName = "service_engagement_type_585413"], [SourceColumnName = "type_of_contact_585505", DestinationColumnName = "type_of_contact_585505"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"raw-credible-ibhh-warehouse" = let
  Source = Xml.Tables(Web.Contents("https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2rvfSwEVK9Ds!b66T9qUKKpg__&start_date=&end_date=&custom_param1=&custom_param2=&custom_param3="), null, 65001),
  #"Navigation 1" = Source{1}[Table],
  #"Expanded Table" = Table.ExpandTableColumn(#"Navigation 1", "Table", {"Name", "Table"}, {"Name.1", "Table.1"}),
  #"Expanded Table.1" = Table.ExpandTableColumn(#"Expanded Table", "Table.1", {"Name", "Table"}, {"Name.2", "Table"}),
  #"Expanded Table 1" = Table.ExpandTableColumn(#"Expanded Table.1", "Table", {"Name", "Table"}, {"Name.3", "Table.1"}),
  #"Expanded Table.1 1" = Table.ExpandTableColumn(#"Expanded Table 1", "Table.1", {"visittype", "location_id", "status", "program_id", "rev_timein", "emp_name", "clientlastname", "clientfirstname", "client_id", "clientins_id", "ins_id", "clientvisit_id", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "which_type_of_core_service_was_582371", "service_engagement_type_585413", "type_of_contact_585505"}, {"visittype", "location_id", "status", "program_id", "rev_timein", "emp_name", "clientlastname", "clientfirstname", "client_id", "clientins_id", "ins_id", "clientvisit_id", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "which_type_of_core_service_was_582371", "service_engagement_type_585413", "type_of_contact_585505"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Table.1 1", {"Name", "Name.1", "Name.2", "Name.3", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}),
  #"Filtered rows" = Table.SelectRows(#"Removed columns", each [visittype] = "BUN: ARST" or [visittype] = "BUN: ARST-G" or [visittype] = "BUN: IST" or [visittype] = "CPST" or [visittype] = "IBHH: CM" or [visittype] = "PSR" or [visittype] = "PSR Group"),
  #"Extracted text range" = Table.TransformColumns(#"Filtered rows", {{"rev_timein", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Extracted text range", {{"rev_timein", type date}}),
  #"Transform columns" = Table.TransformColumnTypes(#"Changed column type", {{"visittype", type text}, {"location_id", type text}, {"status", type text}, {"program_id", type text}, {"emp_name", type text}, {"clientlastname", type text}, {"clientfirstname", type text}, {"which_type_of_core_service_was_582371", type text}, {"service_engagement_type_585413", type text}, {"type_of_contact_585505", type text}}),
  #"Transform columns 1" = Table.TransformColumnTypes(#"Transform columns", {{"client_id", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns 1", {{"client_id", null}}),
  #"Transform columns 2" = Table.TransformColumnTypes(#"Replace errors", {{"clientins_id", type text}}),
  #"Replace errors 1" = Table.ReplaceErrorValues(#"Transform columns 2", {{"clientins_id", null}}),
  #"Transform columns 3" = Table.TransformColumnTypes(#"Replace errors 1", {{"ins_id", type text}}),
  #"Replace errors 2" = Table.ReplaceErrorValues(#"Transform columns 3", {{"ins_id", null}}),
  #"Renamed columns" = Table.RenameColumns(#"Replace errors 2", {{"clientvisit_id", "Service ID"}}),
  #"Transform columns 4" = Table.TransformColumnTypes(#"Renamed columns", {{"Service ID", type text}}),
  #"Replace errors 3" = Table.ReplaceErrorValues(#"Transform columns 4", {{"Service ID", null}})
in
  #"Replace errors 3";
shared #"raw-credible-ibhh (2)_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "e4d2de35-5f5d-457f-8e5b-43f758299057"]}[Data],
  TableNavigation = Navigation_2{[Item = "raw-credible-ibhh", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
