[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_credible_brs_cross_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared raw_credible_brs_cross = let
  Source = Excel.Workbook(Web.Contents("https://voanorthlaorg-my.sharepoint.com/personal/russell_irby_voanorthla_org/Documents/Apps/Microsoft Power Query/Uploaded Files/Updated_Billing Review Status Template and Crosswalk 4 (1).xlsx"), null, false),
  Crosswalk_sheet = Source{[Item = "Crosswalk", Kind = "Sheet"]}[Data],
  #"Filter null and whitespace" = each List.Select(_, each _ <> null and (not (_ is text) or Text.Trim(_) <> "")),
  #"Transposed table" = Table.Transpose(Crosswalk_sheet),
  #"Removed bottom rows" = Table.RemoveLastN(#"Transposed table", each try List.IsEmpty(List.Skip(#"Filter null and whitespace"(Record.FieldValues(_)), 1)) otherwise false),
  #"Transposed table1" = Table.Transpose(#"Removed bottom rows"),
  #"Removed bottom rows1" = Table.RemoveLastN(#"Transposed table1", each try List.IsEmpty(List.Skip(#"Filter null and whitespace"(Record.FieldValues(_)), 1)) otherwise false),
  #"Promoted headers" = Table.PromoteHeaders(#"Removed bottom rows1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Description", type text}, {"Action", type text}, {"Billing Review Status", type text}, {"Notes", type text}, {"Adjustment Code", type text}}),
  #"Replaced value" = Table.ReplaceValue(#"Changed column type", "B7", "B07", Replacer.ReplaceValue, {"Adjustment Code"}),
  #"Removed duplicates" = Table.Distinct(#"Replaced value", {"Adjustment Code"})
in
  #"Removed duplicates";
shared raw_credible_brs_cross_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_credible_brs_cross", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
