[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_credible_lookup_dict_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared raw_credible_lookup_dict = let
  Source = Xml.Tables(Web.Contents("https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2r4Joz6AcsN5q-CqbVMVT5TQ__&start_date=&end_date=&custom_param1=&custom_param2=&custom_param3=")),
  #"Navigation 1" = Source{1}[Table],
  #"Expanded Table" = Table.ExpandTableColumn(#"Navigation 1", "Table", {"Name", "Table"}, {"Name.1", "Table.1"}),
  #"Expanded Table.1" = Table.ExpandTableColumn(#"Expanded Table", "Table.1", {"Name", "Table"}, {"Name.2", "Table"}),
  #"Expanded Table 1" = Table.ExpandTableColumn(#"Expanded Table.1", "Table", {"Name", "Table"}, {"Name.3", "Table.1"}),
  #"Expanded Table.1 1" = Table.ExpandTableColumn(#"Expanded Table 1", "Table.1", {"lookup_id", "lookup_category", "lookup_desc", "lookup_code1", "external_id1", "short_desc", "lookup_code", "external_id", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "date_updated", "updatedby_emp_id", "outcome_value", "hl7_code1", "hl7_code", "lookup_value"}, {"lookup_id", "lookup_category", "lookup_desc", "lookup_code1", "external_id1", "short_desc", "lookup_code", "external_id", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "date_updated", "updatedby_emp_id", "outcome_value", "hl7_code1", "hl7_code", "lookup_value"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Table.1 1", {"Name", "Name.1", "Name.2", "Name.3"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns", {{"lookup_id", type text}, {"lookup_category", type text}, {"lookup_desc", type text}, {"lookup_code1", type text}}),
  #"Expanded external_id1" = Table.ExpandTableColumn(#"Changed column type", "external_id1", {"Element:Text"}, {"Element:Text"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Expanded external_id1", {"Element:Text"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed columns 1", {{"short_desc", type text}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Changed column type 1", {"external_id", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}),
  #"Extracted text range" = Table.TransformColumns(#"Removed columns 2", {{"date_updated", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Extracted text range", {{"date_updated", type date}, {"updatedby_emp_id", type text}, {"outcome_value", type text}, {"hl7_code1", type text}, {"hl7_code", type text}, {"lookup_value", type text}, {"lookup_code", type text}}),
  #"Removed duplicates" = Table.Distinct(#"Changed column type 2", {"lookup_id"})
in
  #"Removed duplicates";
shared raw_credible_lookup_dict_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_credible_lookup_dict", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
