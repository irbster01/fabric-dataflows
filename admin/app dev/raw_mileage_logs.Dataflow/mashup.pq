[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "mileage_accounting_submissions_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared mileage_accounting_submissions = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "raw-data"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "mileage_accounting_submissions"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"submissionDate", type datetime}, {"submissionType", type text}, {"submittedBy", type text}, {"totalEntries", Int64.Type}, {"totalMiles", type number}, {"totalReimbursement", Int64.Type}, {"entries.entryId", type text}, {"entries.date", type date}, {"entries.purpose", type text}, {"entries.route", type text}, {"entries.miles", type number}, {"entries.reimbursement", Int64.Type}, {"notes", type text}}),
  #"Sorted rows" = Table.Sort(#"Changed column type", {{"submissionDate", Order.Descending}})
in
  #"Sorted rows";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "raw-data"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "mileage_accounting_submissions"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Json.Document(Parameter),
  #"Converted to table" = Table.FromRecords({Source}),
  #"Expanded entries" = Table.ExpandListColumn(#"Converted to table", "entries"),
  #"Expanded entries1" = Table.ExpandRecordColumn(#"Expanded entries", "entries", {"entryId", "date", "purpose", "route", "miles", "reimbursement"}, {"entries.entryId", "entries.date", "entries.purpose", "entries.route", "entries.miles", "entries.reimbursement"})
in
  #"Expanded entries1";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Json.Document(Parameter),
  #"Converted to table" = Table.FromRecords({Source}),
  #"Expanded entries" = Table.ExpandListColumn(#"Converted to table", "entries"),
  #"Expanded entries1" = Table.ExpandRecordColumn(#"Expanded entries", "entries", {"entryId", "date", "purpose", "route", "miles", "reimbursement"}, {"entries.entryId", "entries.date", "entries.purpose", "entries.route", "entries.miles", "entries.reimbursement"})
in
  #"Expanded entries1";
shared mileage_accounting_submissions_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_mileage_accounting_submissions", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
