[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Append"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_credible_episodes_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared raw_credible_episodes = let
  Source = Xml.Tables(Web.Contents("https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2rMeUMv-ZFdO!IBc3hAanPCg__&start_date=&end_date=&custom_param1=&custom_param2=&custom_param3=")),
  Navigation = Source{1}[Table],
  #"Expanded Table" = Table.ExpandTableColumn(Navigation, "Table", {"Name", "Table"}, {"Name.1", "Table.1"}),
  #"Expanded Table.1" = Table.ExpandTableColumn(#"Expanded Table", "Table.1", {"Name", "Table"}, {"Name.2", "Table"}),
  #"Expanded Table 2" = Table.ExpandTableColumn(#"Expanded Table.1", "Table", {"Name", "Table"}, {"Name.3", "Table.1"}),
  #"Expanded Table.1 2" = Table.ExpandTableColumn(#"Expanded Table 2", "Table.1", {"episode_id", "client_id", "episode_status", "assessment_date1", "admission_date", "program_id1", "program_id", "assessment_date", "date_updated", "date_created", "length_of_stay", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "discharge_date", "discharge_date1", "discharge_outcome", "discharge_location", "date_closed_auto", "referral_date", "referred_by", "referred_source", "fep_first_contact"}, {"episode_id", "client_id", "episode_status", "assessment_date1", "admission_date", "program_id1", "program_id", "assessment_date", "date_updated", "date_created", "length_of_stay", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "discharge_date", "discharge_date1", "discharge_outcome", "discharge_location", "date_closed_auto", "referral_date", "referred_by", "referred_source", "fep_first_contact"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Table.1 2", {"Name", "Name.1", "Name.2", "Name.3"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"client_id", "Client ID"}, {"episode_status", "Episode Status"}, {"program_id", "Program ID"}, {"date_updated", "Date Episode Updated"}, {"date_created", "Date Episode Created"}}),
  #"Extracted text range" = Table.TransformColumns(#"Renamed columns", {{"Date Episode Updated", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Extracted text range 1" = Table.TransformColumns(#"Extracted text range", {{"Date Episode Created", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Extracted text range 1", {{"Date Episode Updated", type date}, {"Date Episode Created", type date}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type", {{"length_of_stay", "Length of Stay"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns 1", {"urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"discharge_outcome", "Discharge Outcome"}, {"discharge_date", "Discharge Date"}, {"discharge_location", "Discharge Location"}, {"date_closed_auto", "Date Auto Closed"}, {"referral_date", "Referral Date"}, {"referred_by", "Referred By"}, {"referred_source", "Referred Source"}, {"episode_id", "Episode ID"}, {"assessment_date1", "Assessment Date"}, {"admission_date", "Admission Date"}}),
  #"Extracted text range 2" = Table.TransformColumns(#"Renamed columns 2", {{"Assessment Date", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Extracted text range 2", {{"Assessment Date", type date}}),
  #"Extracted text range 3" = Table.TransformColumns(#"Changed column type 1", {{"Admission Date", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Extracted text range 3", {{"Admission Date", type date}}),
  #"Extracted text range 4" = Table.TransformColumns(#"Changed column type 2", {{"Discharge Date", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Extracted text range 4", {{"Discharge Date", type date}}),
  #"Transform columns" = Table.TransformColumnTypes(#"Changed column type 3", {{"Episode ID", type text}, {"Client ID", type text}, {"Episode Status", type text}, {"program_id1", type text}, {"Program ID", type text}, {"assessment_date", type text}, {"Length of Stay", type text}, {"discharge_date1", type text}, {"Discharge Outcome", type text}, {"Discharge Location", type text}, {"Date Auto Closed", type text}, {"Referral Date", type text}, {"Referred By", type text}, {"Referred Source", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"Episode ID", null}, {"Client ID", null}, {"Episode Status", null}, {"program_id1", null}, {"Program ID", null}, {"assessment_date", null}, {"Length of Stay", null}, {"discharge_date1", null}, {"Discharge Outcome", null}, {"Discharge Location", null}, {"Date Auto Closed", null}, {"Referral Date", null}, {"Referred By", null}, {"Referred Source", null}}),
  #"Sorted rows" = Table.Sort(#"Replace errors", {{"fep_first_contact", Order.Descending}}),
  #"Extracted text before delimiter" = Table.TransformColumns(#"Sorted rows", {{"fep_first_contact", each Text.BeforeDelimiter(Text.From(_), "T", 0), type text}}),
  #"Changed column type 4" = Table.TransformColumnTypes(#"Extracted text before delimiter", {{"fep_first_contact", type date}}),
  #"Extracted text before delimiter 1" = Table.TransformColumns(#"Changed column type 4", {{"Referral Date", each Text.BeforeDelimiter(_, "T", 0), type text}}),
  #"Changed column type 5" = Table.TransformColumnTypes(#"Extracted text before delimiter 1", {{"Referral Date", type date}}),
  #"Replaced value" = Table.ReplaceValue(#"Changed column type 5", "14", "Epicenter", Replacer.ReplaceValue, {"Program ID"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "16", "CHRP", Replacer.ReplaceValue, {"Program ID"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "6", "Adult Bundle", Replacer.ReplaceValue, {"Program ID"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "13", "6401", Replacer.ReplaceValue, {"Program ID"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "10", "Supervision", Replacer.ReplaceValue, {"Program ID"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "15", "IBHH", Replacer.ReplaceValue, {"Program ID"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "12", "6021", Replacer.ReplaceValue, {"Program ID"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", "9", "6024", Replacer.ReplaceValue, {"Program ID"}),
  #"Filtered rows" = Table.SelectRows(#"Replaced value 7", each ([Program ID] <> "17")),
  #"Renamed columns 3" = Table.RenameColumns(#"Filtered rows", {{"Episode ID", "entry_id"}, {"Client ID", "client_id"}, {"Episode Status", "entry_status"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns 3", {"program_id1", "assessment_date"}),
  #"Renamed columns 4" = Table.RenameColumns(#"Removed columns 2", {{"Date Episode Updated", "date_entry_created"}, {"Program ID", "program"}, {"Admission Date", "admission_date"}, {"Assessment Date", "assessment_date"}, {"Date Episode Created", "date_entry_updated"}, {"Length of Stay", "length_of_stay"}, {"Discharge Date", "discharge_date"}}),
  #"Removed columns 3" = Table.RemoveColumns(#"Renamed columns 4", {"discharge_date1"}),
  #"Renamed columns 5" = Table.RenameColumns(#"Removed columns 3", {{"Discharge Outcome", "discaharge_outcome"}, {"Discharge Location", "discharge_location"}, {"Date Auto Closed", "date_autoClosed"}}),
  #"Extracted text range 5" = Table.TransformColumns(#"Renamed columns 5", {{"date_autoClosed", each Text.Middle(_, 0, 10), type text}}),
  #"Changed column type 6" = Table.TransformColumnTypes(#"Extracted text range 5", {{"date_autoClosed", type date}}),
  #"Renamed columns 6" = Table.RenameColumns(#"Changed column type 6", {{"Referred Source", "ref_source"}, {"Referral Date", "ref_date"}, {"Referred By", "_ref_by"}})
in
  #"Renamed columns 6";
shared DefaultDestination = Fabric.Warehouse([CreateNavigationProperties = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[warehouseId = "bd5204a3-3830-4fef-9c81-e233ba283b8e"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_credible_episodes-warehouse_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Episode ID", DestinationColumnName = "Episode ID"], [SourceColumnName = "Client ID", DestinationColumnName = "Client ID"], [SourceColumnName = "Episode Status", DestinationColumnName = "Episode Status"], [SourceColumnName = "Assessment Date", DestinationColumnName = "Assessment Date"], [SourceColumnName = "Admission Date", DestinationColumnName = "Admission Date"], [SourceColumnName = "program_id1", DestinationColumnName = "program_id1"], [SourceColumnName = "Program ID", DestinationColumnName = "Program ID"], [SourceColumnName = "assessment_date", DestinationColumnName = "assessment_date"], [SourceColumnName = "Date Episode Updated", DestinationColumnName = "Date Episode Updated"], [SourceColumnName = "Date Episode Created", DestinationColumnName = "Date Episode Created"], [SourceColumnName = "Length of Stay", DestinationColumnName = "Length of Stay"], [SourceColumnName = "Discharge Date", DestinationColumnName = "Discharge Date"], [SourceColumnName = "discharge_date1", DestinationColumnName = "discharge_date1"], [SourceColumnName = "Discharge Outcome", DestinationColumnName = "Discharge Outcome"], [SourceColumnName = "Discharge Location", DestinationColumnName = "Discharge Location"], [SourceColumnName = "Date Auto Closed", DestinationColumnName = "Date Auto Closed"], [SourceColumnName = "Referral Date", DestinationColumnName = "Referral Date"], [SourceColumnName = "Referred By", DestinationColumnName = "Referred By"], [SourceColumnName = "Referred Source", DestinationColumnName = "Referred Source"], [SourceColumnName = "fep_first_contact", DestinationColumnName = "fep_first_contact"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared #"raw_credible_episodes-warehouse" = let
  Source = Xml.Tables(Web.Contents("https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2rMeUMv-ZFdO!IBc3hAanPCg__&start_date=&end_date=&custom_param1=&custom_param2=&custom_param3=")),
  Navigation = Source{1}[Table],
  #"Expanded Table" = Table.ExpandTableColumn(Navigation, "Table", {"Name", "Table"}, {"Name.1", "Table.1"}),
  #"Expanded Table.1" = Table.ExpandTableColumn(#"Expanded Table", "Table.1", {"Name", "Table"}, {"Name.2", "Table"}),
  #"Expanded Table 2" = Table.ExpandTableColumn(#"Expanded Table.1", "Table", {"Name", "Table"}, {"Name.3", "Table.1"}),
  #"Expanded Table.1 2" = Table.ExpandTableColumn(#"Expanded Table 2", "Table.1", {"episode_id", "client_id", "episode_status", "assessment_date1", "admission_date", "program_id1", "program_id", "assessment_date", "date_updated", "date_created", "length_of_stay", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "discharge_date", "discharge_date1", "discharge_outcome", "discharge_location", "date_closed_auto", "referral_date", "referred_by", "referred_source", "fep_first_contact"}, {"episode_id", "client_id", "episode_status", "assessment_date1", "admission_date", "program_id1", "program_id", "assessment_date", "date_updated", "date_created", "length_of_stay", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "discharge_date", "discharge_date1", "discharge_outcome", "discharge_location", "date_closed_auto", "referral_date", "referred_by", "referred_source", "fep_first_contact"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Table.1 2", {"Name", "Name.1", "Name.2", "Name.3"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"client_id", "Client ID"}, {"episode_status", "Episode Status"}, {"program_id", "Program ID"}, {"date_updated", "Date Episode Updated"}, {"date_created", "Date Episode Created"}}),
  #"Extracted text range" = Table.TransformColumns(#"Renamed columns", {{"Date Episode Updated", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Extracted text range 1" = Table.TransformColumns(#"Extracted text range", {{"Date Episode Created", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Extracted text range 1", {{"Date Episode Updated", type date}, {"Date Episode Created", type date}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type", {{"length_of_stay", "Length of Stay"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns 1", {"urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"discharge_outcome", "Discharge Outcome"}, {"discharge_date", "Discharge Date"}, {"discharge_location", "Discharge Location"}, {"date_closed_auto", "Date Auto Closed"}, {"referral_date", "Referral Date"}, {"referred_by", "Referred By"}, {"referred_source", "Referred Source"}, {"episode_id", "Episode ID"}, {"assessment_date1", "Assessment Date"}, {"admission_date", "Admission Date"}}),
  #"Extracted text range 2" = Table.TransformColumns(#"Renamed columns 2", {{"Assessment Date", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Extracted text range 2", {{"Assessment Date", type date}}),
  #"Extracted text range 3" = Table.TransformColumns(#"Changed column type 1", {{"Admission Date", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Extracted text range 3", {{"Admission Date", type date}}),
  #"Extracted text range 4" = Table.TransformColumns(#"Changed column type 2", {{"Discharge Date", each Text.Middle(Text.From(_), 0, 10), type text}}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Extracted text range 4", {{"Discharge Date", type date}}),
  #"Transform columns" = Table.TransformColumnTypes(#"Changed column type 3", {{"Episode ID", type text}, {"Client ID", type text}, {"Episode Status", type text}, {"program_id1", type text}, {"Program ID", type text}, {"assessment_date", type text}, {"Length of Stay", type text}, {"discharge_date1", type text}, {"Discharge Outcome", type text}, {"Discharge Location", type text}, {"Date Auto Closed", type text}, {"Referral Date", type text}, {"Referred By", type text}, {"Referred Source", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"Episode ID", null}, {"Client ID", null}, {"Episode Status", null}, {"program_id1", null}, {"Program ID", null}, {"assessment_date", null}, {"Length of Stay", null}, {"discharge_date1", null}, {"Discharge Outcome", null}, {"Discharge Location", null}, {"Date Auto Closed", null}, {"Referral Date", null}, {"Referred By", null}, {"Referred Source", null}}),
  #"Sorted rows" = Table.Sort(#"Replace errors", {{"fep_first_contact", Order.Descending}}),
  #"Extracted text before delimiter" = Table.TransformColumns(#"Sorted rows", {{"fep_first_contact", each Text.BeforeDelimiter(Text.From(_), "T", 0), type text}}),
  #"Changed column type 4" = Table.TransformColumnTypes(#"Extracted text before delimiter", {{"fep_first_contact", type date}}),
  #"Extracted text before delimiter 1" = Table.TransformColumns(#"Changed column type 4", {{"Referral Date", each Text.BeforeDelimiter(_, "T", 0), type text}}),
  #"Changed column type 5" = Table.TransformColumnTypes(#"Extracted text before delimiter 1", {{"Referral Date", type date}}),
  #"Replaced value" = Table.ReplaceValue(#"Changed column type 5", "14", "Epicenter", Replacer.ReplaceValue, {"Program ID"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "16", "CHRP", Replacer.ReplaceValue, {"Program ID"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "6", "Adult Bundle", Replacer.ReplaceValue, {"Program ID"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "13", "6401", Replacer.ReplaceValue, {"Program ID"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "10", "Supervision", Replacer.ReplaceValue, {"Program ID"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "15", "IBHH", Replacer.ReplaceValue, {"Program ID"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "12", "6021", Replacer.ReplaceValue, {"Program ID"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", "9", "6024", Replacer.ReplaceValue, {"Program ID"}),
  #"Filtered rows" = Table.SelectRows(#"Replaced value 7", each ([Program ID] <> "17"))
in
  #"Filtered rows";
shared #"raw_credible_episodes-warehouse_DataDestination" = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "e4d2de35-5f5d-457f-8e5b-43f758299057"]}[Data],
  TableNavigation = Navigation_2{[Item = "raw_credible_episodes", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared raw_credible_episodes_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_credible_episodes", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
