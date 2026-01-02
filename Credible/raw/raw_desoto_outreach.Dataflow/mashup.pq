[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "desoto_outreach_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared desoto_outreach = let
    // ===== Lakehouse navigation =====
    Source         = Lakehouse.Contents(null),
    Navigation     = Source{[workspaceId="4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
    #"Navigation 1"= Navigation{[lakehouseId="1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
    #"Navigation 2"= #"Navigation 1"{[Id="Files", ItemKind="Folder"]}[Data],
    #"Navigation 3"= #"Navigation 2"{[Name="raw-data"]}[Content],
    #"Navigation 4"= #"Navigation 3"{[Name="desoto_outreach"]}[Content],

    // Filter BEFORE expanding Attributes; predicate returns logical only
    #"Filtered hidden files" =
        Table.SelectRows(#"Navigation 4", each try [Attributes][Hidden] <> true otherwise true),

    // Expand attributes we want to keep (optional)
    #"Expanded Attributes" =
        Table.ExpandRecordColumn(
            #"Filtered hidden files",
            "Attributes",
            {"Content Type","Kind","Size","Group","Owner","Permissions"},
            {"Content Type","Kind","Size","Group","Owner","Permissions"}
        ),

    // ===== Apply your file transform =====
    #"Invoke custom function" =
        Table.AddColumn(#"Expanded Attributes", "Transform file", each #"Transform file"([Content])),
    #"Removed other columns" =
        Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
    #"Expanded table column" =
        Table.ExpandTableColumn(
            #"Removed other columns",
            "Transform file",
            Table.ColumnNames(#"Transform file"(#"Sample file"))
        ),

    // ===== Types (keep yours) =====
    #"Changed column type" =
        Table.TransformColumnTypes(#"Expanded table column",
        {
            {"data.count", Int64.Type},
            {"data.lastPage", Int64.Type},
            {"data.page", Int64.Type},
            {"data.results.id", Int64.Type},
            {"data.results.submittedBy", type text},
            {"data.results.submittedAt", type datetime},
            {"data.results.responses.id", Int64.Type},
            {"data.results.responses.element.id", Int64.Type},
            {"data.results.responses.element.title", type text},
            {"data.results.responses.element.description", type text},
            {"data.results.responses.type", type text},
            {"data.results.responses.replied", type logical}
        }),

    // ===== Helpers that ALWAYS return a table =====
    // Expand a list column if it exists & contains at least one list; otherwise return the table unchanged
    ExpandListIfExists = (tbl as table, col as text) as table =>
        let
            hasCol  = List.Contains(Table.ColumnNames(tbl), col),
            vals    = if hasCol then try Table.Column(tbl, col) otherwise {} else {},
            hasList = if hasCol then List.AnyTrue(List.Transform(vals, each try Value.Is(_, type list) otherwise false)) else false,
            outTbl  = if hasCol and hasList then Table.ExpandListColumn(tbl, col) else tbl
        in
            outTbl,

    // Expand a record column if it exists; if no fields (all null), return unchanged
    ExpandRecordIfExists = (tbl as table, col as text, preferFields as list, newNames as list) as table =>
        let
            hasCol   = List.Contains(Table.ColumnNames(tbl), col),
            sample   = if hasCol then try Table.Column(tbl, col){0} otherwise null else null,
            fields   = if List.Count(preferFields) > 0 then preferFields
                       else if sample is record then Record.FieldNames(sample) else {},
            names    = if List.Count(preferFields) > 0 then newNames
                       else List.Transform(fields, each col & "." & _),
            outTbl   = if hasCol and List.Count(fields) > 0
                       then Table.ExpandRecordColumn(tbl, col, fields, names)
                       else tbl
        in
            outTbl,

    // ===== Explicit flattening to ...responses.response =====
    t0 = #"Changed column type",
    t1 = ExpandListIfExists(t0, "data.results"),

    t2 = ExpandRecordIfExists(
            t1, "data.results",
            {"id","submittedBy","submittedAt","responses"},
            {"result.id","result.submittedBy","result.submittedAt","result.responses"}),

    t3 = ExpandListIfExists(t2, "result.responses"),

    t4 = ExpandRecordIfExists(
            t3, "result.responses",
            {"id","element","type","replied","response"},
            {"resp.id","resp.element","resp.type","resp.replied","resp.response"}),

    t5 = ExpandRecordIfExists(
            t4, "resp.element",
            {"id","title","description"},
            {"elem.id","elem.title","elem.description"}),

    // Some rows still have lists (or lists-of-lists) in resp.response; expand a couple of times safely
    t6 = ExpandListIfExists(t5, "resp.response"),
    t7 = ExpandListIfExists(t6, "resp.response"),
    t8 = ExpandListIfExists(t7, "resp.response"),

    // If your earlier naming kept the long path instead, normalize the column name to one variable
    ResponseColName =
        if List.Contains(Table.ColumnNames(t8), "resp.response") then "resp.response"
        else if List.Contains(Table.ColumnNames(t8), "data.results.responses.response") then "data.results.responses.response"
        else "resp.response", // default; won’t be used if absent

    // Normalize any remaining non-records in the response column so record expansion never crashes
    t9 =
        if List.Contains(Table.ColumnNames(t8), ResponseColName) then
            Table.TransformColumns(
                t8,
                {
                    ResponseColName,
                    each if _ is record then _
                         else if _ is null then [value = null]
                         else if _ is list then [value = Text.Combine(List.Transform(_, each Text.From(_)), ", ")]
                         else [value = _],
                    type record
                }
            )
        else t8,

    // Expand union of all fields present in response records
    ResponseFields =
        if List.Contains(Table.ColumnNames(t9), ResponseColName) then
            List.Union(
                List.Transform(
                    Table.Column(t9, ResponseColName),
                    each if _ is record then Record.FieldNames(_) else {}
                )
            )
        else {},

    ResponseNewNames = List.Transform(ResponseFields, each ResponseColName & "." & _),

    t10 =
        if List.Count(ResponseFields) > 0
        then Table.ExpandRecordColumn(t9, ResponseColName, ResponseFields, ResponseNewNames)
        else t9,
  #"Removed columns" = Table.RemoveColumns(t10, {"data.count", "data.lastPage", "data.page", "data.results.id"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed columns", {{"data.results.responses.response.value", type text}})
in
    #"Changed column type 1";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "raw-data"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "desoto_outreach"]}[Content],
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
  #"Expanded data" = Table.ExpandRecordColumn(#"Converted to table", "data", {"count", "lastPage", "page", "results"}, {"data.count", "data.lastPage", "data.page", "data.results"}),
  #"Expanded data.results" = Table.ExpandListColumn(#"Expanded data", "data.results"),
  #"Expanded data.results1" = Table.ExpandRecordColumn(#"Expanded data.results", "data.results", {"id", "submittedBy", "submittedAt", "responses"}, {"data.results.id", "data.results.submittedBy", "data.results.submittedAt", "data.results.responses"}),
  #"Expanded data.results.responses" = Table.ExpandListColumn(#"Expanded data.results1", "data.results.responses"),
  #"Expanded data.results.responses1" = Table.ExpandRecordColumn(#"Expanded data.results.responses", "data.results.responses", {"id", "element", "type", "replied", "response"}, {"data.results.responses.id", "data.results.responses.element", "data.results.responses.type", "data.results.responses.replied", "data.results.responses.response"}),
  #"Expanded data.results.responses.element" = Table.ExpandRecordColumn(#"Expanded data.results.responses1", "data.results.responses.element", {"id", "title", "description"}, {"data.results.responses.element.id", "data.results.responses.element.title", "data.results.responses.element.description"})
in
  #"Expanded data.results.responses.element";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Json.Document(Parameter),
  #"Converted to table" = Table.FromRecords({Source}),
  #"Expanded data" = Table.ExpandRecordColumn(#"Converted to table", "data", {"count", "lastPage", "page", "results"}, {"data.count", "data.lastPage", "data.page", "data.results"}),
  #"Expanded data.results" = Table.ExpandListColumn(#"Expanded data", "data.results"),
  #"Expanded data.results1" = Table.ExpandRecordColumn(#"Expanded data.results", "data.results", {"id", "submittedBy", "submittedAt", "responses"}, {"data.results.id", "data.results.submittedBy", "data.results.submittedAt", "data.results.responses"}),
  #"Expanded data.results.responses" = Table.ExpandListColumn(#"Expanded data.results1", "data.results.responses"),
  #"Expanded data.results.responses1" = Table.ExpandRecordColumn(#"Expanded data.results.responses", "data.results.responses", {"id", "element", "type", "replied", "response"}, {"data.results.responses.id", "data.results.responses.element", "data.results.responses.type", "data.results.responses.replied", "data.results.responses.response"}),
  #"Expanded data.results.responses.element" = Table.ExpandRecordColumn(#"Expanded data.results.responses1", "data.results.responses.element", {"id", "title", "description"}, {"data.results.responses.element.id", "data.results.responses.element.title", "data.results.responses.element.description"})
in
  #"Expanded data.results.responses.element";
shared desoto_outreach_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "desoto_outreach", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
