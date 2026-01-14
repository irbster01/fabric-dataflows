[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "cis_clients_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared cis_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "cis_clients"]}[Content],

  #"Expanded Attributes" =
    Table.ExpandRecordColumn(
      #"Navigation 4",
      "Attributes",
      {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"},
      {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}
    ),

  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Filtered hidden files 1" = Table.SelectRows(#"Filtered hidden files", each [Attributes]?[Hidden]? <> true),

  #"Invoke custom function 1" = Table.AddColumn(#"Filtered hidden files 1", "Transform file (2)", each #"Transform file (2)"([Content])),
  #"Removed other columns 2" = Table.SelectColumns(#"Invoke custom function 1", {"Transform file (2)"}),

  #"Expanded table column 1" =
    Table.ExpandTableColumn(
      #"Removed other columns 2",
      "Transform file (2)",
      Table.ColumnNames(#"Transform file (2)"(#"Sample file (2)"))
    ),

  #"Changed column type" =
    Table.TransformColumnTypes(
      #"Expanded table column 1",
      {
        {"Column1", type text}, {"Column2", type text}, {"Column3", type text}, {"Column4", type text}, {"Column5", type text},
        {"Column7", type text}, {"Column8", type text}, {"Column9", type text}, {"Column10", type text}, {"Column11", type text},
        {"Column12", type text}, {"Column13", type text}, {"Column14", type text}, {"Column15", type text},
        {"Column17", type text}, {"Column18", type text}, {"Column19", type text}, {"Column20", type text}, {"Column21", type text},
        {"Column26", type text}, {"Column27", type text}, {"Column28", type text}, {"Column29", type text}, {"Column30", type text},
        {"Column31", type text}, {"Column32", type text}, {"Column35", type text}
      }
    ),

  #"Removed top rows" = Table.Skip(#"Changed column type", 1),
  #"Promoted headers" = Table.PromoteHeaders(#"Removed top rows", [PromoteAllScalars = true]),

  // Keep your other type conversions, but make them tolerant (invalid -> null)
  #"Changed column type 1" =
    Table.TransformColumns(
      #"Promoted headers",
      {
        {"Student System ID", each Text.From(_), type text},
        {"Birth Date", each try Date.From(_) otherwise null, type date},
        {"Enrollment #(lf)Begin Date", each try Date.From(_) otherwise null, type date},
        {"First CIS Enrollment Date", each try Date.From(_) otherwise null, type date},
        {"Organization ID", each try Int64.From(_) otherwise null, Int64.Type},
        {"Site ID", each try Int64.From(_) otherwise null, Int64.Type},
        {"NCES ID", each try Int64.From(_) otherwise null, Int64.Type}
      }
    ),

  #"Removed columns" = Table.RemoveColumns(#"Changed column type 1", {"Organization ID", "Site ID", "NCES ID", "Initiatives"}),

  #"Removed other columns 1" =
    Table.SelectColumns(
      #"Removed columns",
      {
        "Home School",
        "Student System ID",
        "Student ID",
        "Student",
        "Birth Date",
        "Enrollment #(lf)Begin Date",
        "Enrollment#(lf)Exit Date",
        "First CIS Enrollment Date"
      }
    ),

  #"Removed duplicates" = Table.Distinct(#"Removed other columns 1", {"Student System ID"}),

  #"Renamed columns" =
    Table.RenameColumns(
      #"Removed duplicates",
      {
        {"Student System ID", "client_id"},
        {"Student ID", "sasid"},
        {"Student", "client_full_name"}
      }
    ),

  #"Renamed columns 1" =
    Table.RenameColumns(
      #"Renamed columns",
      {
        {"Home School", "school_name"},
        {"Birth Date", "client_dob"},
        {"Enrollment #(lf)Begin Date", "entry_start_date"}
      }
    ),

  #"Renamed columns 2" = Table.RenameColumns(#"Renamed columns 1", {{"First CIS Enrollment Date", "first_cis_enrollment_date"}}),

  // Make Exit Date null-safe before converting to date
  #"Cleaned Exit Date" =
    Table.TransformColumns(
      #"Renamed columns 2",
      {
        {
          "Enrollment#(lf)Exit Date",
          each
            let
              t = Text.Trim(Text.From(_)),
              v = if t = "" or t = "-" then null else (try Date.From(t) otherwise null)
            in
              v,
          type date
        }
      }
    )
in
  #"Cleaned Exit Date";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "cis_clients"]}[Content],
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
  Source = Excel.Workbook(Parameter, null, true),
  Navigation = Source{[Item = "Caselist Details", Kind = "Sheet"]}[Data]
in
  Navigation;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Excel.Workbook(Parameter, null, true),
  Navigation = Source{[Item = "Caselist Details", Kind = "Sheet"]}[Data]
in
  Navigation;
shared cis_clients_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_cisdm_clients", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Sample file (2)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "cis_clients"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Filtered hidden files 1" = Table.SelectRows(#"Filtered hidden files", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files 1"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (2)" = let
  #"Parameter (2)" = #"Sample file (2)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (2)"]
in
  #"Parameter (2)";
shared #"Transform Sample file (2)" = let
  Source = Excel.Workbook(#"Parameter (2)", null, true),
  Navigation = Source{[Item = "Caselist Details", Kind = "Sheet"]}[Data]
in
  Navigation;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (2)""}"]
shared #"Transform file (2)" = (#"Parameter (2)" as binary) => let
  Source = Excel.Workbook(#"Parameter (2)", null, true),
  Navigation = Source{[Item = "Caselist Details", Kind = "Sheet"]}[Data]
in
  Navigation;
