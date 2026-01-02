[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw-netsuite-gl-ssvf_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"raw-netsuite-gl-ssvf" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw-netsuite-gl-ssvf", ItemKind = "Table"]}[Data],
  #"Replaced value" = Table.ReplaceValue(#"Navigation 2", "=", "", Replacer.ReplaceText, {"Amount"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "=", "", Replacer.ReplaceText, {"Debit"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 1", {{"Debit", Currency.Type}, {"Amount", Currency.Type}}),
  #"Replaced value 2" = Table.ReplaceValue(#"Changed column type", "=", "", Replacer.ReplaceText, {"Amount Due"}),
  #"Renamed columns" = Table.RenameColumns(#"Replaced value 2", {{"Amount Due", "amount_due"}, {"Description", "description"}, {"Debit", "debit"}, {"Amount", "amount"}, {"Transaction ID", "trans_id"}, {"Message", "message"}, {"Memo", "memo"}, {"Name", "name"}, {"Document Number", "doc_number"}, {"Date Closed", "date_closed"}, {"Date Received", "deate_rcvd"}, {"Date", "date"}, {"Department: Dept #", "dept"}}),
  #"Inserted text after delimiter" = Table.AddColumn(#"Renamed columns", "Text after delimiter", each Text.AfterDelimiter([Account], ":", 0), type text),
  #"Extracted text before delimiter" = Table.TransformColumns(#"Inserted text after delimiter", {{"Account", each Text.BeforeDelimiter(_, ":", 0), type text}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Extracted text before delimiter", {{"Account", "account"}, {"Text after delimiter", "sub_account"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns 1", {{"amount_due", Currency.Type}})
in
  #"Changed column type 1";
shared #"raw-netsuite-gl-ssvf_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "trans_ssvf_gl", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
