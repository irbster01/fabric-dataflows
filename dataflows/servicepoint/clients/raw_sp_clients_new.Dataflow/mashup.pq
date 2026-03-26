[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"Sample file" = let
    Source = Lakehouse.Contents(null),
    #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
    #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
    #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
    #"Navigation 4" = #"Navigation 3"{[Name = "servicepoint"]}[Content],
    #"Navigation 5" = #"Navigation 4"{[Name = "clients"]}[Content],

    // Keep only visible files
    #"Filtered hidden files" = Table.SelectRows(#"Navigation 5", each [Attributes]?[Hidden]? <> true),

    // Optional: keep only actual files, not subfolders
    #"Filtered to files only" = Table.SelectRows(#"Filtered hidden files", each [Extension] <> null),

    // Apply your existing file transform function to each file binary
    #"Invoke custom function" = Table.AddColumn(#"Filtered to files only", "Transform file (3)", each #"Transform file (3)"([Content])),

    // Keep file name too in case you want lineage
    #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Name", "Transform file (3)"}),

    // Expand transformed tables
    #"Expanded table column" = Table.ExpandTableColumn(
        #"Removed other columns",
        "Transform file (3)",
        Table.ColumnNames(#"Transform file (3)"(#"Sample file (3)"))
    ),

    #"Changed column type" = Table.TransformColumnTypes(
        #"Expanded table column",
        {
            {"Column1", type text},
            {"Column2", type text},
            {"Column3", type text},
            {"Column4", type text}
        }
    ),

    #"Promoted headers" = Table.PromoteHeaders(#"Changed column type", [PromoteAllScalars = true]),

    #"Changed column type 1" = Table.TransformColumnTypes(
        #"Promoted headers",
        {
            {"Date of Birth", type date},
            {"Client ID", Int64.Type}
        }
    ),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type 1", {"1.13_clients.csv"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Client ID", "client_id"}, {"First Name", "first_name"}, {"Last Name", "last_name"}, {"Date of Birth", "dob"}}),
  #"Duplicated column" = Table.DuplicateColumn(#"Renamed columns", "client_id", "client_id - Copy"),
  #"Renamed columns 1" = Table.RenameColumns(#"Duplicated column", {{"client_id - Copy", "system_client_id"}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Renamed columns 1", {{"system_client_id", type text}}),
  #"Added suffix" = Table.TransformColumns(#"Changed column type 2", {{"system_client_id", each _ & "-SP", type text}})
in
    #"Added suffix";
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Excel.Workbook(Parameter, null, true),
  Navigation = Source{[Item = "report_1737126477986", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Excel.Workbook(Parameter, null, true),
  Navigation = Source{[Item = "report_1737126477986", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true])
in
  #"Promoted headers";
[BindToDefaultDestination = true]
shared raw_sp_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "ServicePoint"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Clients"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(
    #"Navigation 4",
    "Attributes",
    {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"},
    {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}
  ),
  #"Filtered rows" = Table.SelectRows(
    #"Expanded Attributes",
    let latest = List.Max(#"Expanded Attributes"[Date modified]) in each [Date modified] = latest
  ),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (2)", each #"Transform file (2)"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (2)"}),
  #"Expanded table column" = Table.ExpandTableColumn(
    #"Removed other columns",
    "Transform file (2)",
    Table.ColumnNames(#"Transform file (2)"(#"Sample file (2)"))
  ),
  #"Changed column type" = Table.TransformColumnTypes(
    #"Expanded table column",
    {{"Client ID", Int64.Type}, {"First Name", type text}, {"Last Name", type text}, {"Date of Birth", type date}}
  ),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"Client ID", "client_id"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"client_id", type text}}),
  #"Renamed columns 1" = Table.RenameColumns(
    #"Changed column type 1",
    {{"First Name", "client_first_name"}, {"Last Name", "client_last_name"}, {"Date of Birth", "dob"}}
  ),

  // INTERNAL ID: first 4 letters of first name + full last name + DOB as int (YYYYMMDD); uppercase; no spaces/special chars
  #"Added internal id" = Table.AddColumn(
    #"Renamed columns 1",
    "int_id",
    each
      let
        f = [client_first_name],
        l = [client_last_name],
        d = [dob],

        // keep only A-Z/a-z/0-9 (drops spaces + special chars)
        fClean = if f = null then "" else Text.Select(f, {"A".."Z","a".."z","0".."9"}),
        lClean = if l = null then "" else Text.Select(l, {"A".."Z","a".."z","0".."9"}),

        f4 = Text.Start(Text.Upper(fClean), 4),
        lUp = Text.Upper(lClean),

        // YYYYMMDD as integer, then to text for concatenation
        dobInt = if d = null then null else Int64.From(Number.FromText(Date.ToText(d, "yyyyMMdd"))),
        dobTxt = if dobInt = null then "" else Text.From(dobInt)
      in
        f4 & lUp & dobTxt,
    type text
  ),
  #"Duplicated column" = Table.DuplicateColumn(#"Added internal id", "client_id", "client_id - Copy"),
  #"Renamed columns 2" = Table.RenameColumns(#"Duplicated column", {{"client_id - Copy", "system_client_id"}}),
  #"Added suffix" = Table.TransformColumns(#"Renamed columns 2", {{"system_client_id", each _ & "-SP", type text}})
in
  #"Added suffix";
shared #"Sample file (2)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "ServicePoint"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Clients"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", let latest = List.Max(#"Expanded Attributes"[Date modified]) in each [Date modified] = latest),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (2)" = let
  #"Parameter (2)" = #"Sample file (2)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (2)"]
in
  #"Parameter (2)";
shared #"Transform Sample file (2)" = let
  Source = Csv.Document(#"Parameter (2)", [Delimiter = ",", Columns = 4, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (2)""}"]
shared #"Transform file (2)" = (#"Parameter (2)" as binary) => let
  Source = Csv.Document(#"Parameter (2)", [Delimiter = ",", Columns = 4, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"Sample file (3)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "ServicePoint"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Clients"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Filtered hidden files 1" = Table.SelectRows(#"Filtered hidden files", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files 1"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (3)" = let
  #"Parameter (3)" = #"Sample file (3)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (3)"]
in
  #"Parameter (3)";
shared #"Transform Sample file (3)" = let
  Source = Csv.Document(#"Parameter (3)", [Delimiter = ",", Columns = 4, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (3)""}"]
shared #"Transform file (3)" = (#"Parameter (3)" as binary) => let
  Source = Csv.Document(#"Parameter (3)", [Delimiter = ",", Columns = 4, QuoteStyle = QuoteStyle.None])
in
  Source;
shared #"Sample file (4)" = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "servicepoint"]}[Content],
  #"Navigation 5" = #"Navigation 4"{[Name = "clients"]}[Content],
  // Keep only visible files
  #"Filtered hidden files" = Table.SelectRows(#"Navigation 5", each [Attributes]?[Hidden]? <> true),
  // Optional: keep only actual files, not subfolders
  #"Filtered to files only" = Table.SelectRows(#"Filtered hidden files", each [Extension] <> null),
  // Apply your existing file transform function to each file binary
  #"Invoke custom function" = Table.AddColumn(#"Filtered to files only", "Transform file (3)", each #"Transform file (3)"([Content])),
  #"Filtered hidden files 1" = Table.SelectRows(#"Invoke custom function", each [Attributes]?[Hidden]? <> true),
  Navigation = #"Filtered hidden files 1"{0}[Content]
in
  Navigation;
shared #"Parameter (4)" = let
  #"Parameter (4)" = #"Sample file (4)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (4)"]
in
  #"Parameter (4)";
shared #"Transform Sample file (4)" = let
  Source = Csv.Document(#"Parameter (4)", [Delimiter = ",", Columns = 4, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (4)""}"]
shared #"Transform file (4)" = (#"Parameter (4)" as binary) => let
  Source = Csv.Document(#"Parameter (4)", [Delimiter = ",", Columns = 4, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data]
in
  #"Navigation 2";
