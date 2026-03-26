[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared raw_sp_entries_new = let
    Source = Lakehouse.Contents(null),
    #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
    #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
    #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
    #"Navigation 4" = #"Navigation 3"{[Name = "servicepoint"]}[Content],
    #"Navigation 5" = #"Navigation 4"{[Name = "entries"]}[Content],

    #"Filtered hidden files" = Table.SelectRows(#"Navigation 5", each [Attributes]?[Hidden]? <> true),
    #"Filtered to files only" = Table.SelectRows(#"Filtered hidden files", each [Extension] <> null),

    #"Invoke custom function" = Table.AddColumn(#"Filtered to files only", "Transform file", each #"Transform file"([Content])),
    #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
    #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),

    #"Changed column type" = Table.TransformColumnTypes(
        #"Expanded table column",
        {
            {"Client ID", Int64.Type},
            {"Entry Date", type date},
            {"Entry Exit ID", Int64.Type},
            {"Exit Date", type date},
            {"Provider", type text}
        }
    ),

    #"Renamed columns" = Table.RenameColumns(
        #"Changed column type",
        {
            {"Client ID", "client_id"},
            {"Entry Date", "entry_date"},
            {"Entry Exit ID", "entry_id"},
            {"Exit Date", "exit_date"},
            {"Provider", "program"}
        }
    ),

    #"Changed column type 1" = Table.TransformColumnTypes(
        #"Renamed columns",
        {
            {"client_id", type text},
            {"entry_id", type text}
        }
    ),

    #"Sorted rows" = Table.Sort(#"Changed column type 1", {{"entry_date", Order.Descending}}),
    #"Removed duplicates" = Table.Distinct(#"Sorted rows"),
    #"Removed duplicates 1" = Table.Distinct(#"Removed duplicates", {"entry_id"})
in
    #"Removed duplicates 1";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "ServicePoint"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Entries"]}[Content],
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
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 5, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 5, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data];
