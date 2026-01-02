[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "punches_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared punches = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "Paycor"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "punches"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (2)", each #"Transform file (2)"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (2)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (2)", Table.ColumnNames(#"Transform file (2)"(#"Sample file (2)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Client ID", type text}, {"For", type text}, {"Full Name", type text}, {"Employee #", Int64.Type}, {"Badge #", Int64.Type}, {"Home Department", type text}, {"Supervisor", type text}, {"Pay Group", type text}, {"Date", type date}, {"In Punch", type time}, {"Out Punch", type time}, {"Hours", type number}, {"Worked Department", type text}, {"Earnings Code", type text}, {"Deduction", Int64.Type}, {"Pay Item", Int64.Type}, {"Daily Note", type text}, {"Exceptions", type text}}),
  #"Removed errors" = Table.RemoveRowsWithErrors(#"Changed column type", {"Client ID"}),
  #"Removed columns" = Table.RemoveColumns(#"Removed errors", {"Client ID"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"For", "report_timeframe"}, {"Full Name", "employee_name"}, {"Employee #", "employee_num"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"Badge #"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed columns 1", {{"employee_num", type text}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type 1", {{"Home Department", "dept"}, {"Supervisor", "manager"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns 1", {"Pay Group"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 2", {{"Date", "date"}, {"In Punch", "punch_in"}, {"Out Punch", "punch_out"}, {"Hours", "hours"}}),
  #"Removed columns 3" = Table.RemoveColumns(#"Renamed columns 2", {"Worked Department", "Deduction", "Pay Item", "Daily Note"}),
  #"Renamed columns 3" = Table.RenameColumns(#"Removed columns 3", {{"Exceptions", "exceptions"}, {"Earnings Code", "earning_code"}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Renamed columns 3", {{"punch_in", type datetime}, {"punch_out", type datetime}})
in
  #"Changed column type 2";
shared #"Sample file (2)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "Paycor"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "punches"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (2)" = let
  #"Parameter (2)" = #"Sample file (2)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (2)"]
in
  #"Parameter (2)";
shared #"Transform Sample file (2)" = let
  Source = Csv.Document(#"Parameter (2)", [Delimiter = ",", Columns = 18, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (2)""}"]
shared #"Transform file (2)" = (#"Parameter (2)" as binary) => let
  Source = Csv.Document(#"Parameter (2)", [Delimiter = ",", Columns = 18, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared punches_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "fccbdcf0-4c42-431d-8e07-ae4b00bda780"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_paycor_punches", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "FEP Punches_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"FEP Punches" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "Paycor"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "punches"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (2)", each #"Transform file (2)"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (2)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (2)", Table.ColumnNames(#"Transform file (2)"(#"Sample file (2)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Client ID", type text}, {"For", type text}, {"Full Name", type text}, {"Employee #", Int64.Type}, {"Badge #", Int64.Type}, {"Home Department", type text}, {"Supervisor", type text}, {"Pay Group", type text}, {"Date", type date}, {"In Punch", type time}, {"Out Punch", type time}, {"Hours", type number}, {"Worked Department", type text}, {"Earnings Code", type text}, {"Deduction", Int64.Type}, {"Pay Item", Int64.Type}, {"Daily Note", type text}, {"Exceptions", type text}}),
  #"Removed errors" = Table.RemoveRowsWithErrors(#"Changed column type", {"Client ID"}),
  #"Removed columns" = Table.RemoveColumns(#"Removed errors", {"Client ID"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"For", "report_timeframe"}, {"Full Name", "employee_name"}, {"Employee #", "employee_num"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"Badge #"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed columns 1", {{"employee_num", type text}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type 1", {{"Home Department", "dept"}, {"Supervisor", "manager"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns 1", {"Pay Group"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 2", {{"Date", "date"}, {"In Punch", "punch_in"}, {"Out Punch", "punch_out"}, {"Hours", "hours"}}),
  #"Removed columns 3" = Table.RemoveColumns(#"Renamed columns 2", {"Worked Department", "Deduction", "Pay Item", "Daily Note"}),
  #"Renamed columns 3" = Table.RenameColumns(#"Removed columns 3", {{"Exceptions", "exceptions"}, {"Earnings Code", "earning_code"}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Renamed columns 3", {{"punch_in", type datetime}, {"punch_out", type datetime}}),
  #"Filtered rows" = Table.SelectRows(#"Changed column type 2", each ([employee_name] = "Allyson Ebey" or [employee_name] = "Brittney Dukes" or [employee_name] = "Claire Shidler" or [employee_name] = "Elizabeth Luent" or [employee_name] = "Jacinta Lafleur" or [employee_name] = "Justin Davis" or [employee_name] = "Kelley Baker" or [employee_name] = "Kristal Poland" or [employee_name] = "Michelle Merello" or [employee_name] = "Russell Irby" or [employee_name] = "Tammy Polke" or [employee_name] = "Tina Hall"))
in
  #"Filtered rows";
shared #"FEP Punches_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_fep_punches", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
