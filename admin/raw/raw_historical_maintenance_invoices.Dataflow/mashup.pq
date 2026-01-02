[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"2023" = let
  Source = Excel.Workbook(Web.Contents("https://voanorthlaorg.sharepoint.com/sites/TheVOAHub/Logistics/Maintenance/McAdoo%20HVAC%20&%20Plumbing/Byrnes%202023-2025%20McAdoo%20(1).xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "2023", Kind = "Sheet"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Column3", type text}, {"Column6", type text}, {"Column7", type text}}),
  #"Removed top rows" = Table.Skip(#"Changed column type", 4),
  #"Promoted headers" = Table.PromoteHeaders(#"Removed top rows", [PromoteAllScalars = true]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Promoted headers", {{"Date Completed", type date}, {"Invoice # ", Int64.Type}, {"Cost", type number}}),
  #"Removed top rows 1" = Table.Skip(#"Changed column type 1", 1),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Removed top rows 1", {{"Invoice # ", type text}, {"Cost", Currency.Type}, {"Column8", type text}, {"Unit #/area", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type 2", {"Column8"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Invoice # ", "invoice_num"}}),
  #"Filtered rows" = Table.SelectRows(#"Renamed columns", each ([Date Completed] <> null))
in
  #"Filtered rows";
shared #"2024" = let
  Source = Excel.Workbook(Web.Contents("https://voanorthlaorg.sharepoint.com/sites/TheVOAHub/Logistics/Maintenance/McAdoo%20HVAC%20&%20Plumbing/Byrnes%202023-2025%20McAdoo%20(1).xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "2024", Kind = "Sheet"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Column3", type text}, {"Column6", type text}, {"Column7", type text}}),
  #"Removed top rows" = Table.Skip(#"Changed column type", 4),
  #"Promoted headers" = Table.PromoteHeaders(#"Removed top rows", [PromoteAllScalars = true]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Promoted headers", {{"Date Completed", type date}, {"Invoice # ", Int64.Type}, {"Cost", type number}}),
  #"Removed top rows 1" = Table.Skip(#"Changed column type 1", 1),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Removed top rows 1", {{"Invoice # ", type text}, {"Cost", Currency.Type}, {"Column8", type text}, {"Unit #/area", type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type 2", {{"Invoice # ", "invoice_num"}}),
  #"Filtered rows" = Table.SelectRows(#"Renamed columns", each ([Date Completed] <> null))
in
  #"Filtered rows";
shared #"2025" = let
  Source = Excel.Workbook(Web.Contents("https://voanorthlaorg.sharepoint.com/sites/TheVOAHub/Logistics/Maintenance/McAdoo%20HVAC%20&%20Plumbing/Byrnes%202023-2025%20McAdoo%20(1).xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "2025", Kind = "Sheet"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Column3", type text}, {"Column6", type text}, {"Column7", type text}}),
  #"Removed top rows" = Table.Skip(#"Changed column type", 4),
  #"Promoted headers" = Table.PromoteHeaders(#"Removed top rows", [PromoteAllScalars = true]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Promoted headers", {{"Date Completed", type date}, {"Invoice # ", Int64.Type}, {"Cost", type number}}),
  #"Removed top rows 1" = Table.Skip(#"Changed column type 1", 1),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Removed top rows 1", {{"Invoice # ", type text}, {"Cost", Currency.Type}, {"Column8", type text}, {"Unit #/area", type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type 2", {{"Invoice # ", "invoice_num"}}),
  #"Filtered rows" = Table.SelectRows(#"Renamed columns", each ([Date Completed] <> null))
in
  #"Filtered rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "appended_historical_maint_invoices_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared appended_historical_maint_invoices = let
  Source = Table.Combine({#"2025", #"2024", #"2023"}),
  #"Filtered rows" = Table.SelectRows(Source, each ([Date Completed] <> null)),
  #"Removed columns" = Table.RemoveColumns(#"Filtered rows", {"Column8"}),
  #"Inserted text after delimiter" = Table.AddColumn(#"Removed columns", "Text after delimiter", each Text.AfterDelimiter([#"Unit #/area"], ",", 0), type text),
  #"Extracted text before delimiter" = Table.TransformColumns(#"Inserted text after delimiter", {{"Unit #/area", each Text.BeforeDelimiter(_, ",", 0), type text}}),
  #"Inserted text after delimiter 1" = Table.AddColumn(#"Extracted text before delimiter", "Text after delimiter (2)", each Text.AfterDelimiter([Text after delimiter], ",", 0), type text),
  #"Extracted text before delimiter 1" = Table.TransformColumns(#"Inserted text after delimiter 1", {{"Text after delimiter", each Text.BeforeDelimiter(_, ",", 0), type text}}),
  #"Extracted text after delimiter" = Table.TransformColumns(#"Extracted text before delimiter 1", {{"Text after delimiter (2)", each Text.AfterDelimiter(_, ",", 0), type text}}),
  #"Extracted text after delimiter 1" = Table.TransformColumns(#"Extracted text after delimiter", {{"Text after delimiter (2)", each Text.AfterDelimiter(_, ",", 0), type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Extracted text after delimiter 1", {{"Unit #/area", "location1"}, {"Text after delimiter", "location2"}, {"Text after delimiter (2)", "location3"}}),
  #"Unpivoted only selected columns" = Table.Unpivot(#"Renamed columns", {"location1", "location2", "location3"}, "Attribute", "Value"),
  #"Capitalized each word" = Table.TransformColumns(#"Unpivoted only selected columns", {{"Value", each Text.Proper(_), type text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Capitalized each word", each ([Value] <> "")),
  #"Trimmed text" = Table.TransformColumns(#"Filtered rows 1", {{"Value", each Text.Trim(_), type text}}),
  #"Cleaned text" = Table.TransformColumns(#"Trimmed text", {{"Value", each Text.Clean(_), type text}}),
  #"Sorted rows" = Table.Sort(#"Cleaned text", {{"Date Completed", Order.Descending}}),
  #"Replaced value" = Table.ReplaceValue(#"Sorted rows", "plumbing", "Plumbing", Replacer.ReplaceValue, {"Type of Service"})
in
  #"Replaced value";
shared appended_historical_maint_invoices_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "fccbdcf0-4c42-431d-8e07-ae4b00bda780"]}[Data],
  TableNavigation = Navigation_2{[Id = "appended_historical_maint_invoices", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
