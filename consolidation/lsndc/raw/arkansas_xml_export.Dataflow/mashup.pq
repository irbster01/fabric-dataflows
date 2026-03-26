[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared hmis_export_flat = let
    Source      = Lakehouse.Contents(null),
    Workspace   = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
    Lakehouse   = Workspace{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
    Files       = Lakehouse{[Id = "Files", ItemKind = "Folder"]}[Data],
    LSNDC       = Files{[Name = "LSNDC"]}[Content],
    ExportFolder = LSNDC{[Name = "hmis_export"]}[Content],

    // Expand file attributes
    WithAttrs =
        Table.ExpandRecordColumn(
            ExportFolder,
            "Attributes",
            {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"},
            {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}
        ),

    // Drop hidden files
    VisibleFiles =
        Table.SelectRows(
            WithAttrs,
            each [Attributes]?[Hidden]? <> true
        ),

    // Rename file-level Name to avoid collision with XML Name
    Renamed =
        Table.RenameColumns(
            VisibleFiles,
            {{"Name", "FileName"}}
        ),

    // Apply the XML transform to each file
    WithTransform =
        Table.AddColumn(
            Renamed,
            "Transform file",
            each #"Transform file"([Content])
        ),

    // Keep just file info + transformed table
    KeptCols =
        Table.SelectColumns(
            WithTransform,
            {"FileName", "Date modified", "Size", "Transform file"}
        ),

    // Expand the inner table; use the sample-file column list
    Expanded =
        Table.ExpandTableColumn(
            KeptCols,
            "Transform file",
            Table.ColumnNames(#"Transform file"(#"Sample file")),
            Table.ColumnNames(#"Transform file"(#"Sample file"))
        ),

    Typed =
        Table.TransformColumnTypes(
            Expanded,
            {
                {"FileName", type text},
                {"Date modified", type datetime}
            }
        )
in
    Typed;
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "hmis_export"]}[Content],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", let latest = List.Max(#"Navigation 4"[Date modified]) in each [Date modified] = latest),
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Filtered rows", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Xml.Tables(Parameter)
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Xml.Tables(Parameter)
in
  Source;
shared fxUnfurlRecord = (recordName as text) as table =>
let
    Source     = hmis_export_flat,

    // Latest file only; remove these two lines if you want all files
    LatestDate = List.Max(Source[Date modified]),
    LatestRows = Table.SelectRows(Source, each [Date modified] = LatestDate),

    // Keep only the requested record group
    Filtered   = Table.SelectRows(LatestRows, each [Name] = recordName),

    // Expand outer "Table" column: gives RecordName + RecordTable
    Expanded   =
        Table.ExpandTableColumn(
            Filtered,
            "Table",
            {"Name", "Table"},
            {"RecordName", "RecordTable"}
        ),

    // Drop any rows where RecordTable is null or not a table
    NonNull =
        Table.SelectRows(
            Expanded,
            each [RecordTable] <> null and Value.Is([RecordTable], type table)
        ),

    Result =
        if Table.RowCount(NonNull) = 0 then
            // ⚠ No usable rows for this record group:
            // return a table with *one known column* so Fabric's model builder is happy
            #table(
                type table [RecordName = text],
                {
                    { recordName }  // single row, just to define schema; you can filter it out later if you want truly empty
                }
            )
        else
            let
                // Use first inner table as sample for column names
                Sample = NonNull{0}[RecordTable]
            in
                Table.ExpandTableColumn(
                    NonNull,
                    "RecordTable",
                    Table.ColumnNames(Sample),
                    Table.ColumnNames(Sample)
                )
in
    Result;
[BindToDefaultDestination = true]
shared bronze_hmis_providers = let
    Raw = fxUnfurlRecord("providerRecords"),

    Output =
        if Table.ColumnCount(Raw) = 0 then
            #table(type table [NoData = text], {})
        else
            let
                Schema =
                    Table.Schema(Raw),

                ScalarRows =
                    Table.SelectRows(
                        Schema,
                        each
                            not Text.StartsWith([Name], "child")
                            and [Kind] <> "Table"
                            and [Kind] <> "List"
                            and [Kind] <> "Record"
                    ),

                ScalarCols = ScalarRows[Name],
                TypePairs  = List.Transform(ScalarCols, each { _, type text }),
                Selected   = Table.SelectColumns(Raw, ScalarCols, MissingField.Ignore),
                Typed      = Table.TransformColumnTypes(Selected, TypePairs, "en-US")
            in
                Typed
in
    Output;
[BindToDefaultDestination = true]
shared bronze_hmis_provider_service_units = let
    Raw = fxUnfurlRecord("providerServiceUnitRecords"),

    Output =
        if Table.ColumnCount(Raw) = 0 then
            #table(type table [NoData = text], {})
        else
            let
                Schema =
                    Table.Schema(Raw),

                ScalarRows =
                    Table.SelectRows(
                        Schema,
                        each
                            not Text.StartsWith([Name], "child")
                            and [Kind] <> "Table"
                            and [Kind] <> "List"
                            and [Kind] <> "Record"
                    ),

                ScalarCols                = ScalarRows[Name],
                TypePairs                 = List.Transform(ScalarCols, each { _, type text }),
                Selected                  = Table.SelectColumns(Raw, ScalarCols, MissingField.Ignore),
                ProviderServiceUnitsTyped = Table.TransformColumnTypes(Selected, TypePairs, "en-US")
            in
                ProviderServiceUnitsTyped
in
    Output;
[BindToDefaultDestination = true]
shared bronze_hmis_bed_unit_inventory = let
    Raw = fxUnfurlRecord("bedUnitInventoryRecords"),

    Output =
        if Table.ColumnCount(Raw) = 0 then
            #table(type table [NoData = text], {})
        else
            let
                Schema =
                    Table.Schema(Raw),

                ScalarRows =
                    Table.SelectRows(
                        Schema,
                        each
                            not Text.StartsWith([Name], "child")
                            and [Kind] <> "Table"
                            and [Kind] <> "List"
                            and [Kind] <> "Record"
                    ),

                ScalarCols             = ScalarRows[Name],
                TypePairs              = List.Transform(ScalarCols, each { _, type text }),
                Selected               = Table.SelectColumns(Raw, ScalarCols, MissingField.Ignore),
                BedUnitInventoryTyped  = Table.TransformColumnTypes(Selected, TypePairs, "en-US")
            in
                BedUnitInventoryTyped
in
    Output;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_hmis_clients_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "FileName", DestinationColumnName = "FileName"], [SourceColumnName = "Date modified", DestinationColumnName = "Date modified"], [SourceColumnName = "Size", DestinationColumnName = "Size"], [SourceColumnName = "Name", DestinationColumnName = "Name"], [SourceColumnName = "RecordName", DestinationColumnName = "RecordName"], [SourceColumnName = "firstName", DestinationColumnName = "firstName"], [SourceColumnName = "lastName", DestinationColumnName = "lastName"], [SourceColumnName = "socSecNoDashed", DestinationColumnName = "socSecNoDashed"], [SourceColumnName = "ssnDataQualityValue", DestinationColumnName = "ssnDataQualityValue"], [SourceColumnName = "veteranStatus", DestinationColumnName = "veteranStatus"], [SourceColumnName = "active", DestinationColumnName = "active"], [SourceColumnName = "providerCreating", DestinationColumnName = "providerCreating"], [SourceColumnName = "providerUpdating", DestinationColumnName = "providerUpdating"], [SourceColumnName = "assessmentData", DestinationColumnName = "assessmentData"], [SourceColumnName = "matrixData", DestinationColumnName = "matrixData"], [SourceColumnName = "Attribute:record_id", DestinationColumnName = "Attribute:record_id"], [SourceColumnName = "Attribute:date_added", DestinationColumnName = "Attribute:date_added"], [SourceColumnName = "Attribute:system_id", DestinationColumnName = "Attribute:system_id"], [SourceColumnName = "Attribute:date_updated", DestinationColumnName = "Attribute:date_updated"], [SourceColumnName = "alias", DestinationColumnName = "alias"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared bronze_hmis_clients = let
    Raw = fxUnfurlRecord("clientRecords"),

    Output =
        if Table.ColumnCount(Raw) = 0 then
            // harmless empty table with one text column
            #table(type table [NoData = text], {})
        else
            let
                AllCols = Table.ColumnNames(Raw),

                // For every column, if the value is table/list/record, set it to null
                Cleaned =
                    Table.TransformColumns(
                        Raw,
                        List.Transform(
                            AllCols,
                            (colName) =>
                                {
                                    colName,
                                    (v) =>
                                        if Value.Is(v, type table)
                                            or Value.Is(v, type list)
                                            or Value.Is(v, type record)
                                        then
                                            null
                                        else
                                            v
                                }
                        )
                    ),

                // Now force every column to text
                TypePairs = List.Transform(AllCols, each { _, type text }),

                ClientsTyped =
                    Table.TransformColumnTypes(
                        Cleaned,
                        TypePairs,
                        "en-US"
                    )
            in
                ClientsTyped
in
    Output;
[BindToDefaultDestination = true]
shared bronze_hmis_households = let
    Raw = fxUnfurlRecord("householdRecords"),

    Output =
        if Table.ColumnCount(Raw) = 0 then
            #table(type table [NoData = text], {})
        else
            let
                Schema =
                    Table.Schema(Raw),

                ScalarRows =
                    Table.SelectRows(
                        Schema,
                        each
                            not Text.StartsWith([Name], "child")
                            and [Kind] <> "Table"
                            and [Kind] <> "List"
                            and [Kind] <> "Record"
                    ),

                ScalarCols        = ScalarRows[Name],
                TypePairs         = List.Transform(ScalarCols, each { _, type text }),
                Selected          = Table.SelectColumns(Raw, ScalarCols, MissingField.Ignore),
                HouseholdsTyped   = Table.TransformColumnTypes(Selected, TypePairs, "en-US")
            in
                HouseholdsTyped
in
    Output;
[BindToDefaultDestination = true]
shared bronze_hmis_case_managers = let
    Raw = fxUnfurlRecord("caseManagerRecords"),

    Output =
        if Table.ColumnCount(Raw) = 0 then
            #table(type table [NoData = text], {})
        else
            let
                Schema =
                    Table.Schema(Raw),

                ScalarRows =
                    Table.SelectRows(
                        Schema,
                        each
                            not Text.StartsWith([Name], "child")
                            and [Kind] <> "Table"
                            and [Kind] <> "List"
                            and [Kind] <> "Record"
                    ),

                ScalarCols          = ScalarRows[Name],
                TypePairs           = List.Transform(ScalarCols, each { _, type text }),
                Selected            = Table.SelectColumns(Raw, ScalarCols, MissingField.Ignore),
                CaseManagersTyped   = Table.TransformColumnTypes(Selected, TypePairs, "en-US")
            in
                CaseManagersTyped
in
    Output;
[BindToDefaultDestination = true]
shared bronze_hmis_goals = let
    Raw = fxUnfurlRecord("goalRecords"),

    Output =
        if Table.ColumnCount(Raw) = 0 then
            #table(type table [NoData = text], {})
        else
            let
                Schema =
                    Table.Schema(Raw),

                ScalarRows =
                    Table.SelectRows(
                        Schema,
                        each
                            not Text.StartsWith([Name], "child")
                            and [Kind] <> "Table"
                            and [Kind] <> "List"
                            and [Kind] <> "Record"
                    ),

                ScalarCols  = ScalarRows[Name],
                TypePairs   = List.Transform(ScalarCols, each { _, type text }),
                Selected    = Table.SelectColumns(Raw, ScalarCols, MissingField.Ignore),
                GoalsTyped  = Table.TransformColumnTypes(Selected, TypePairs, "en-US")
            in
                GoalsTyped
in
    Output;
shared bronze_hmis_action_steps = let
    Raw = fxUnfurlRecord("actionstepRecords"),

    Output =
        if Table.ColumnCount(Raw) = 0 then
            #table(type table [NoData = text], {})
        else
            let
                Schema =
                    Table.Schema(Raw),

                ScalarRows =
                    Table.SelectRows(
                        Schema,
                        each
                            not Text.StartsWith([Name], "child")
                            and [Kind] <> "Table"
                            and [Kind] <> "List"
                            and [Kind] <> "Record"
                    ),

                ScalarCols        = ScalarRows[Name],
                TypePairs         = List.Transform(ScalarCols, each { _, type text }),
                Selected          = Table.SelectColumns(Raw, ScalarCols, MissingField.Ignore),
                ActionStepsTyped  = Table.TransformColumnTypes(Selected, TypePairs, "en-US")
            in
                ActionStepsTyped
in
    Output;
[BindToDefaultDestination = true]
shared bronze_hmis_case_notes = let
    Raw = fxUnfurlRecord("casenoteRecords"),

    Output =
        if Table.ColumnCount(Raw) = 0 then
            #table(type table [NoData = text], {})
        else
            let
                Schema =
                    Table.Schema(Raw),

                ScalarRows =
                    Table.SelectRows(
                        Schema,
                        each
                            not Text.StartsWith([Name], "child")
                            and [Kind] <> "Table"
                            and [Kind] <> "List"
                            and [Kind] <> "Record"
                    ),

                ScalarCols       = ScalarRows[Name],
                TypePairs        = List.Transform(ScalarCols, each { _, type text }),
                Selected         = Table.SelectColumns(Raw, ScalarCols, MissingField.Ignore),
                CaseNotesTyped   = Table.TransformColumnTypes(Selected, TypePairs, "en-US")
            in
                CaseNotesTyped
in
    Output;
shared bronze_hmis_infractions = let
    Raw = fxUnfurlRecord("infractionRecords"),

    Output =
        if Table.ColumnCount(Raw) = 0 then
            #table(type table [NoData = text], {})
        else
            let
                Schema =
                    Table.Schema(Raw),

                ScalarRows =
                    Table.SelectRows(
                        Schema,
                        each
                            not Text.StartsWith([Name], "child")
                            and [Kind] <> "Table"
                            and [Kind] <> "List"
                            and [Kind] <> "Record"
                    ),

                ScalarCols          = ScalarRows[Name],
                TypePairs           = List.Transform(ScalarCols, each { _, type text }),
                Selected            = Table.SelectColumns(Raw, ScalarCols, MissingField.Ignore),
                InfractionsTyped    = Table.TransformColumnTypes(Selected, TypePairs, "en-US")
            in
                InfractionsTyped
in
    Output;
[BindToDefaultDestination = true]
shared bronze_hmis_release_of_info = let
    Raw = fxUnfurlRecord("releaseOfInfoRecords"),

    Output =
        if Table.ColumnCount(Raw) = 0 then
            #table(type table [NoData = text], {})
        else
            let
                Schema =
                    Table.Schema(Raw),

                ScalarRows =
                    Table.SelectRows(
                        Schema,
                        each
                            not Text.StartsWith([Name], "child")
                            and [Kind] <> "Table"
                            and [Kind] <> "List"
                            and [Kind] <> "Record"
                    ),

                ScalarCols           = ScalarRows[Name],
                TypePairs            = List.Transform(ScalarCols, each { _, type text }),
                Selected             = Table.SelectColumns(Raw, ScalarCols, MissingField.Ignore),
                ReleaseOfInfoTyped   = Table.TransformColumnTypes(Selected, TypePairs, "en-US")
            in
                ReleaseOfInfoTyped
in
    Output;
[BindToDefaultDestination = true]
shared bronze_hmis_need_service_groups = let
    // Unfurl the needServiceGroupRecords group from the latest export
    Raw = fxUnfurlRecord("needServiceGroupRecords"),

    Output =
        // If there are literally no columns, return a harmless empty table
        if Table.ColumnCount(Raw) = 0 then
            #table(type table [NoData = text], {})
        else
            let
                // Get schema
                Schema =
                    Table.Schema(Raw),

                // Keep only "normal" columns:
                //  - not child*
                //  - not nested kinds (Table/List/Record)
                ScalarRows =
                    Table.SelectRows(
                        Schema,
                        each
                            not Text.StartsWith([Name], "child")
                            and [Kind] <> "Table"
                            and [Kind] <> "List"
                            and [Kind] <> "Record"
                    ),

                ScalarCols = ScalarRows[Name],

                // Keep only those columns
                Selected =
                    Table.SelectColumns(
                        Raw,
                        ScalarCols,
                        MissingField.Ignore
                    ),

                // Extra safety: if any remaining cell is a table/list/record at row level, set to null
                Cleaned =
                    Table.TransformColumns(
                        Selected,
                        List.Transform(
                            ScalarCols,
                            (colName) =>
                                {
                                    colName,
                                    (v) =>
                                        if Value.Is(v, type table)
                                            or Value.Is(v, type list)
                                            or Value.Is(v, type record)
                                        then
                                            null
                                        else
                                            v
                                }
                        )
                    ),

                // Now cast everything to text
                ColumnTypePairs = List.Transform(ScalarCols, each { _, type text }),

                NeedServiceGroupsTyped =
                    Table.TransformColumnTypes(
                        Cleaned,
                        ColumnTypePairs,
                        "en-US"
                    )
            in
                NeedServiceGroupsTyped
in
    Output;
[BindToDefaultDestination = true]
shared bronze_hmis_needs = let
    // Unfurl the needRecords group from the latest export
    Raw = fxUnfurlRecord("needRecords"),

    Output =
        // If there are literally no columns, return a harmless empty table
        if Table.ColumnCount(Raw) = 0 then
            #table(type table [NoData = text], {})
        else
            let
                // Get schema
                Schema =
                    Table.Schema(Raw),

                // Keep only "normal" columns:
                //  - not child*
                //  - not nested kinds (Table/List/Record)
                ScalarRows =
                    Table.SelectRows(
                        Schema,
                        each
                            not Text.StartsWith([Name], "child")
                            and [Kind] <> "Table"
                            and [Kind] <> "List"
                            and [Kind] <> "Record"
                    ),

                ScalarCols = ScalarRows[Name],

                // Keep only those columns
                Selected =
                    Table.SelectColumns(
                        Raw,
                        ScalarCols,
                        MissingField.Ignore
                    ),

                // Extra safety: if any remaining cell is a table/list/record at row level, set to null
                Cleaned =
                    Table.TransformColumns(
                        Selected,
                        List.Transform(
                            ScalarCols,
                            (colName) =>
                                {
                                    colName,
                                    (v) =>
                                        if Value.Is(v, type table)
                                            or Value.Is(v, type list)
                                            or Value.Is(v, type record)
                                        then
                                            null
                                        else
                                            v
                                }
                        )
                    ),

                // Now cast everything to text
                ColumnTypePairs = List.Transform(ScalarCols, each { _, type text }),

                NeedsTyped =
                    Table.TransformColumnTypes(
                        Cleaned,
                        ColumnTypePairs,
                        "en-US"
                    )
            in
                NeedsTyped
in
    Output;
[BindToDefaultDestination = true]
shared bronze_hmis_services = let
    // Unfurl the serviceRecords group from the latest export
    Raw = fxUnfurlRecord("serviceRecords"),

    Output =
        // If there are literally no columns, return a harmless empty table
        if Table.ColumnCount(Raw) = 0 then
            #table(type table [NoData = text], {})
        else
            let
                // Get schema
                Schema =
                    Table.Schema(Raw),

                // Keep only "normal" columns:
                //  - not child*
                //  - not known nested kinds (Table/List/Record)
                ScalarRows =
                    Table.SelectRows(
                        Schema,
                        each
                            not Text.StartsWith([Name], "child")
                            and [Kind] <> "Table"
                            and [Kind] <> "List"
                            and [Kind] <> "Record"
                    ),

                ScalarCols = ScalarRows[Name],

                // First, keep only those columns
                Selected =
                    Table.SelectColumns(
                        Raw,
                        ScalarCols,
                        MissingField.Ignore
                    ),

                // Extra safety: if any remaining cell is a table/list/record at row level, nuke it to null
                Cleaned =
                    Table.TransformColumns(
                        Selected,
                        List.Transform(
                            ScalarCols,
                            (colName) =>
                                {
                                    colName,
                                    (v) =>
                                        if Value.Is(v, type table)
                                            or Value.Is(v, type list)
                                            or Value.Is(v, type record)
                                        then
                                            null
                                        else
                                            v
                                }
                        )
                    ),

                // Now cast everything to text for Lakehouse
                ColumnTypePairs = List.Transform(ScalarCols, each { _, type text }),

                ServicesTyped =
                    Table.TransformColumnTypes(
                        Cleaned,
                        ColumnTypePairs,
                        "en-US"
                    )
            in
                ServicesTyped
in
    Output;
shared bronze_hmis_need_service_group_funds = let
    Raw = fxUnfurlRecord("needServiceGroupFundRecords"),

    Output =
        if Table.ColumnCount(Raw) = 0 then
            #table(type table [NoData = text], {})
        else
            let
                Schema =
                    Table.Schema(Raw),

                ScalarRows =
                    Table.SelectRows(
                        Schema,
                        each
                            not Text.StartsWith([Name], "child")
                            and [Kind] <> "Table"
                            and [Kind] <> "List"
                            and [Kind] <> "Record"
                    ),

                ScalarCols                 = ScalarRows[Name],
                TypePairs                  = List.Transform(ScalarCols, each { _, type text }),
                Selected                   = Table.SelectColumns(Raw, ScalarCols, MissingField.Ignore),
                NeedServiceGroupFundsTyped = Table.TransformColumnTypes(Selected, TypePairs, "en-US")
            in
                NeedServiceGroupFundsTyped
in
    Output;
[BindToDefaultDestination = true]
shared bronze_hmis_entry_exits = let
    Raw = fxUnfurlRecord("entryExitRecords"),

    Output =
        if Table.ColumnCount(Raw) = 0 then
            #table(type table [NoData = text], {})
        else
            let
                Schema =
                    Table.Schema(Raw),

                ScalarRows =
                    Table.SelectRows(
                        Schema,
                        each
                            not Text.StartsWith([Name], "child")
                            and [Kind] <> "Table"
                            and [Kind] <> "List"
                            and [Kind] <> "Record"
                    ),

                ScalarCols        = ScalarRows[Name],
                TypePairs         = List.Transform(ScalarCols, each { _, type text }),
                Selected          = Table.SelectColumns(Raw, ScalarCols, MissingField.Ignore),
                EntryExitsTyped   = Table.TransformColumnTypes(Selected, TypePairs, "en-US")
            in
                EntryExitsTyped
in
    Output;
shared DefaultDestination = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data]
in
  #"Navigation 2";
shared bronze_hmis_clients_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  TableNavigation = Navigation_2{[Id = "bronze_hmis_clients", ItemKind = "Table"]}[Data]
in
  TableNavigation;
