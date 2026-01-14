[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Append"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Fabric.Warehouse([CreateNavigationProperties = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[warehouseId = "bd5204a3-3830-4fef-9c81-e233ba283b8e"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "lsndc_entries_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared lsndc_entries = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "lsndc_entries"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Filtered rows" = Table.SelectRows(#"Filtered hidden files", let latest = List.Max(#"Filtered hidden files"[Date modified]) in each [Date modified] = latest),
  #"Invoke custom function" = Table.AddColumn(#"Filtered rows", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Client ID", type text}, {"First Name", type text}, {"Last Name", type text}, {"Entry Date", type date}, {"Entry Exit ID", Int64.Type}, {"Exit Date", type date}, {"Provider", type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"Client ID", "client_id"}, {"First Name", "client_first_name"}, {"Last Name", "client_last_name"}, {"Entry Date", "entry_date"}, {"Entry Exit ID", "entry_id"}, {"Exit Date", "exit_date"}, {"Provider", "program"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"entry_id", type text}}),
  #"Removed duplicates" = Table.Distinct(#"Changed column type 1", {"entry_id"}),
  #"Added suffix" = Table.TransformColumns(#"Removed duplicates", {{"client_id", each _ & "-LS", type text}}),
  #"Replaced value" = Table.ReplaceValue(#"Added suffix", "VOANLA 3033 – SSVF VASH RRH", "VOANLA 3033 - SSVF VASH RRH", Replacer.ReplaceValue, {"program"}),
  #"Extracted text after delimiter" = Table.TransformColumns(#"Replaced value", {{"program", each Text.AfterDelimiter(_, "- ", 0), type text}}),

  // ---- added step ----
  #"Added exit_quarter" = Table.AddColumn(
      #"Extracted text after delimiter",
      "exit_quarter",
      each
        if [exit_date] = null then null
        else
          let m = Date.Month(Date.From([exit_date]))
          in
            if m >= 7 and m <= 9 then "Q1"
            else if m >= 10 and m <= 12 then "Q2"
            else if m >= 1 and m <= 3 then "Q3"
            else "Q4",
      type text
  ),
  Custom = Table.TransformColumnTypes(Table.AddColumn(#"Added exit_quarter", "entry_quarter", each if [entry_date] = null then null
        else
          let m = Date.Month(Date.From([entry_date]))
          in
            if m >= 7 and m <= 9 then "Q1"
            else if m >= 10 and m <= 12 then "Q2"
            else if m >= 1 and m <= 3 then "Q3"
            else "Q4"), {{"entry_quarter", type text}})
in
  Custom;
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "lsndc_entries"]}[Content],
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
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 7, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 7, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared lsndc_entries_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_lsndc_entries", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
