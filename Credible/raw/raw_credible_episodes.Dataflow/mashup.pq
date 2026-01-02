[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Append"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;

// ===== CONFIGURATION PARAMETERS =====
shared WorkspaceId = "4241c9c7-5818-4c56-9220-faf632741770" meta [Description = "Fabric workspace ID"];
shared LakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a" meta [Description = "Lakehouse ID"];
shared CredibleApiUrl = "https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2rMeUMv-ZFdO!IBc3hAanPCg__&start_date=&end_date=&custom_param1=&custom_param2=&custom_param3=" meta [Description = "Credible API endpoint URL"];

// ===== DATA QUERIES =====

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_credible_episodes_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared raw_credible_episodes = let
  Source = Xml.Tables(Web.Contents("https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2rMeUMv-ZFdO!IBc3hAanPCg__&start_date=&end_date=&custom_param1=&custom_param2=&custom_param3=")),
  Navigation = Source{1}[Table],
  #"Expanded Table" = Table.ExpandTableColumn(Navigation, "Table", {"Name", "Table"}, {"Name.1", "Table.1"}),
  #"Expanded Table.1" = Table.ExpandTableColumn(#"Expanded Table", "Table.1", {"Name", "Table"}, {"Name.2", "Table"}),
  #"Expanded Table 2" = Table.ExpandTableColumn(#"Expanded Table.1", "Table", {"Name", "Table"}, {"Name.3", "Table.1"}),
  #"Expanded Table.1 2" = Table.ExpandTableColumn(#"Expanded Table 2", "Table.1", {"episode_id", "client_id", "episode_status", "assessment_date1", "admission_date", "program_id1", "program_id", "assessment_date", "date_updated", "date_created", "length_of_stay", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "discharge_date", "discharge_date1", "discharge_outcome", "discharge_location", "date_closed_auto", "referral_date", "referred_by", "referred_source", "fep_first_contact"}, {"episode_id", "client_id", "episode_status", "assessment_date1", "admission_date", "program_id1", "program_id", "assessment_date", "date_updated", "date_created", "length_of_stay", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "discharge_date", "discharge_date1", "discharge_outcome", "discharge_location", "date_closed_auto", "referral_date", "referred_by", "referred_source", "fep_first_contact"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Table.1 2", {"Name", "Name.1", "Name.2", "Name.3"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Removed columns", {"urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata"}),
  #"Removed columns 2" = Table.RemoveColumns(#"Removed columns 1", {"program_id1", "assessment_date", "discharge_date1"}), // Remove duplicate/unused date and program ID columns
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 2", {{"episode_id", type text}, {"client_id", type text}, {"episode_status", type text}, {"program_id", type text}, {"length_of_stay", type text}, {"discharge_outcome", type text}, {"discharge_location", type text}, {"referred_by", type text}, {"referred_source", type text}}), // Convert episode fields to text type
  #"Extracted text range" = Table.TransformColumns(#"Changed column type", {{"date_updated", each Text.Middle(Text.From(_), 0, 10), type text}}), // Extract date portion from date_updated timestamp
  #"Changed column type 1" = Table.TransformColumnTypes(#"Extracted text range", {{"date_updated", type date}}), // Convert date_updated to date type
  #"Extracted text range 1" = Table.TransformColumns(#"Changed column type 1", {{"date_created", each Text.Middle(Text.From(_), 0, 10), type text}}), // Extract date portion from date_created timestamp
  #"Changed column type 2" = Table.TransformColumnTypes(#"Extracted text range 1", {{"date_created", type date}}), // Convert date_created to date type
  #"Extracted text range 2" = Table.TransformColumns(#"Changed column type 2", {{"assessment_date1", each Text.Middle(Text.From(_), 0, 10), type text}}), // Extract date portion from assessment_date1 timestamp
  #"Changed column type 3" = Table.TransformColumnTypes(#"Extracted text range 2", {{"assessment_date1", type date}}), // Convert assessment_date1 to date type
  #"Extracted text range 3" = Table.TransformColumns(#"Changed column type 3", {{"admission_date", each Text.Middle(Text.From(_), 0, 10), type text}}), // Extract date portion from admission_date timestamp
  #"Changed column type 4" = Table.TransformColumnTypes(#"Extracted text range 3", {{"admission_date", type date}}), // Convert admission_date to date type
  #"Extracted text range 4" = Table.TransformColumns(#"Changed column type 4", {{"discharge_date", each Text.Middle(Text.From(_), 0, 10), type text}}), // Extract date portion from discharge_date timestamp
  #"Changed column type 5" = Table.TransformColumnTypes(#"Extracted text range 4", {{"discharge_date", type date}}), // Convert discharge_date to date type
  #"Extracted text before delimiter" = Table.TransformColumns(#"Changed column type 5", {{"fep_first_contact", each Text.BeforeDelimiter(Text.From(_), "T", 0), type text}}), // Extract date from FEP first contact ISO timestamp (before T delimiter)
  #"Changed column type 6" = Table.TransformColumnTypes(#"Extracted text before delimiter", {{"fep_first_contact", type date}}), // Convert fep_first_contact to date type
  #"Extracted text before delimiter 1" = Table.TransformColumns(#"Changed column type 6", {{"date_closed_auto", each Text.BeforeDelimiter(Text.From(_), "T", 0), type text}}), // Extract date from auto-close ISO timestamp (before T delimiter)
  #"Changed column type 7" = Table.TransformColumnTypes(#"Extracted text before delimiter 1", {{"date_closed_auto", type date}}), // Convert date_closed_auto to date type
  #"Extracted text before delimiter 2" = Table.TransformColumns(#"Changed column type 7", {{"referral_date", each Text.BeforeDelimiter(Text.From(_), "T", 0), type text}}), // Extract date from referral ISO timestamp (before T delimiter)
  #"Changed column type 8" = Table.TransformColumnTypes(#"Extracted text before delimiter 2", {{"referral_date", type date}}), // Convert referral_date to date type
  #"Replace errors" = Table.ReplaceErrorValues(#"Changed column type 8", {{"episode_id", null}, {"client_id", null}, {"episode_status", null}, {"program_id", null}, {"length_of_stay", null}, {"discharge_outcome", null}, {"discharge_location", null}, {"date_closed_auto", null}, {"referral_date", null}, {"referred_by", null}, {"referred_source", null}}), // Replace any parsing errors with null values
  #"Replaced value" = Table.ReplaceValue(#"Replace errors", "14", "Epicenter", Replacer.ReplaceValue, {"program_id"}), // Map program ID 14 to Epicenter
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "16", "CHRP", Replacer.ReplaceValue, {"program_id"}), // Map program ID 16 to CHRP
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "6", "Adult Bundle", Replacer.ReplaceValue, {"program_id"}), // Map program ID 6 to Adult Bundle
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "13", "6401", Replacer.ReplaceValue, {"program_id"}), // Map program ID 13 to 6401
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "10", "Supervision", Replacer.ReplaceValue, {"program_id"}), // Map program ID 10 to Supervision
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "15", "IBHH", Replacer.ReplaceValue, {"program_id"}), // Map program ID 15 to IBHH
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "12", "6021", Replacer.ReplaceValue, {"program_id"}), // Map program ID 12 to 6021
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", "9", "6024", Replacer.ReplaceValue, {"program_id"}), // Map program ID 9 to 6024
  #"Filtered rows" = Table.SelectRows(#"Replaced value 7", each ([program_id] <> "17")), // Exclude program ID 17 from results
  #"Renamed columns" = Table.RenameColumns(#"Filtered rows", {{"episode_id", "entry_id"}, {"client_id", "client_id"}, {"episode_status", "entry_status"}, {"date_updated", "date_entry_updated"}, {"date_created", "date_entry_created"}, {"program_id", "program"}, {"admission_date", "admission_date"}, {"assessment_date1", "assessment_date"}, {"length_of_stay", "length_of_stay"}, {"discharge_date", "discharge_date"}, {"discharge_outcome", "discharge_outcome"}, {"discharge_location", "discharge_location"}, {"date_closed_auto", "date_auto_closed"}, {"referral_date", "ref_date"}, {"referred_by", "ref_by"}, {"referred_source", "ref_source"}, {"fep_first_contact", "fep_first_contact"}}), // Rename columns to standard field names
  #"Sorted rows" = Table.Sort(#"Renamed columns", {{"fep_first_contact", Order.Descending}})
in
  #"Sorted rows";

shared DefaultDestination = Fabric.Warehouse([CreateNavigationProperties = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[warehouseId = "bd5204a3-3830-4fef-9c81-e233ba283b8e"]}[Data];

shared raw_credible_episodes_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_credible_episodes", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
