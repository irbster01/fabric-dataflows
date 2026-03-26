[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_maint_incident_log_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared raw_maint_incident_log = let
  Source = Excel.Workbook(Web.Contents("https://voanorthlaorg.sharepoint.com/sites/TheVOAHub/Logistics/Safety/Incidents/000%20incident%20log%20and%20info/1%20Ongoing%20Current%20Incident%20Log%202024.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "Log", Kind = "Sheet"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", type text}, {"Column8", type text}, {"Column9", type text}}),
  #"Removed top rows" = Table.Skip(#"Changed column type", 2),
  #"Promoted headers" = Table.PromoteHeaders(#"Removed top rows", [PromoteAllScalars = true]),
  #"Transform columns" = Table.TransformColumnTypes(#"Promoted headers", {{"Location", type text}, {"Date of Incide", type text}, {"Program", type text}, {"Column10", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"Location", null}, {"Date of Incide", null}, {"Program", null}, {"Column10", null}}),
  #"Renamed columns" = Table.RenameColumns(#"Replace errors", {{"Location", "location"}, {"Date of Incide", "incident_date"}, {"Program", "program"}, {"Type", "incident_type"}, {"Employee", "employee"}, {"Supervisor", "supervisor"}, {"Further Action Required?", "further_action"}, {"Notes", "notes"}, {"Link To Incident Report", "incident_link"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"Column10"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed columns", {{"incident_date", type date}}),
  #"Replaced errors" = Table.ReplaceErrorValues(#"Changed column type 1", {{"incident_date", #date(2999, 1, 1)}}),
  #"Trimmed text" = Table.TransformColumns(#"Replaced errors", {{"location", each Text.Trim(_), type nullable text}}),
  #"Cleaned text" = Table.TransformColumns(#"Trimmed text", {{"location", each Text.Clean(_), type nullable text}}),
  #"Sorted rows" = Table.Sort(#"Cleaned text", {{"incident_date", Order.Descending}}),
  #"Capitalized each word" = Table.TransformColumns(#"Sorted rows", {{"location", each Text.Proper(_), type nullable text}}),
  #"Replaced value" = Table.ReplaceValue(#"Capitalized each word", "spp", "SPP", Replacer.ReplaceValue, {"location"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "850 Tx", "850 Texas St", Replacer.ReplaceValue, {"location"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "453 J", "453 Jordan", Replacer.ReplaceValue, {"location"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "453 j", "453 Jordan", Replacer.ReplaceValue, {"location"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "Macadoo", "McAdoo", Replacer.ReplaceValue, {"location"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "Spp", "SouthPointe", Replacer.ReplaceValue, {"location"})
in
  #"Replaced value 5";
shared raw_maint_incident_log_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "fccbdcf0-4c42-431d-8e07-ae4b00bda780"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_maint_incident_log", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
