[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_lsndc_entries_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared gpd_exit_destinations = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([program] = "Veterans Transitional Living Program") and ([exit_date] <> null)),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each [exit_date] >= #date(2025, 7, 1) and [exit_date] <= #date(2026, 6, 30)),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows 1", {"entry_id"}, lsndc_exits, {"entry_id"}, "lsndc_exits", JoinKind.LeftOuter),
  #"Expanded lsndc_exits" = Table.ExpandTableColumn(#"Merged queries", "lsndc_exits", {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income", "entry_id"}, {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income", "entry_id.1"}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Expanded lsndc_exits", "client_counter", each 1), {{"client_counter", Int64.Type}}),
  #"Inserted conditional column" = Table.AddColumn(#"Added custom", "positive_counter", each if Text.Contains([exit_destination], "permanent") then 1 else if Text.Contains([exit_destination], "Rental by client") then 1 else if Text.Contains([exit_destination], "Owned by") then 1 else if Text.Contains([exit_destination], "Emergency") then 0 else if Text.Contains([exit_destination], "Psychiatric") then 0 else if Text.Contains([exit_destination], "temporary") then 0 else if Text.Contains([exit_destination], "Other") then 1 else if Text.Contains([exit_destination], "Hotel") then 1 else ""),
  #"Filtered rows 2" = Table.SelectRows(#"Inserted conditional column", each ([exit_destination] <> "Hospital or other residential non-psychiatric medical facility (HUD)" and [exit_destination] <> "Long-term care facility or nursing home (HUD)")),
  #"Filtered rows 3" = Table.SelectRows(#"Filtered rows 2", each ([exit_destination] <> "Hospital or other residential non-psychiatric medical facility (HUD)" and [exit_destination] <> "Psychiatric hospital or other psychiatric facility (HUD)")),
  #"Grouped rows" = Table.Group(#"Filtered rows 3", {"program", "exit_quarter"}, {{"positive", each List.Sum([positive_counter]), type any}, {"total", each List.Sum([client_counter]), type nullable number}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Grouped rows", {{"positive", type number}}),
  #"Inserted division" = Table.AddColumn(#"Changed column type", "Division", each [positive] / [total], type number),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted division", {{"Division", Percentage.Type}})
in
  #"Changed column type 1";
shared lsndc_exits = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "lsndc_exits", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared raw_lsndc_entries_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "gpd_exit_destinations", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[BindToDefaultDestination = true]
shared gpd_positive_exits = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([program] = "Veterans Transitional Living Program") and ([exit_date] <> null)),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each [exit_date] >= #date(2025, 7, 1) and [exit_date] <= #date(2026, 6, 30)),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows 1", {"entry_id"}, lsndc_exits, {"entry_id"}, "lsndc_exits", JoinKind.LeftOuter),
  #"Expanded lsndc_exits" = Table.ExpandTableColumn(#"Merged queries", "lsndc_exits", {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income", "entry_id"}, {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income", "entry_id.1"}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Expanded lsndc_exits", "client_counter", each 1), {{"client_counter", type number}}),
  #"Inserted conditional column" = Table.AddColumn(#"Added custom", "positive", each if Text.Contains([reason_forLeaving], "Completed") then 1 else if Text.Contains([reason_forLeaving], "Full Met") then 1 else if Text.Contains([reason_forLeaving], "Other") then 1 else 0),
  #"Grouped rows" = Table.Group(#"Inserted conditional column", {"program", "exit_quarter"}, {{"total", each List.Sum([client_counter]), type nullable number}, {"positive", each List.Sum([positive]), type any}}),
  #"Reordered columns" = Table.ReorderColumns(#"Grouped rows", {"program", "exit_quarter", "positive", "total"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Reordered columns", {{"positive", type number}}),
  #"Inserted division" = Table.AddColumn(#"Changed column type", "Division", each [positive] / [total], type number),
  #"Renamed columns" = Table.RenameColumns(#"Inserted division", {{"Division", "percentage"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"percentage", Percentage.Type}})
in
  #"Changed column type 1";
[BindToDefaultDestination = true]
shared gpd_income_exits = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([program] = "Veterans Transitional Living Program") and ([exit_date] <> null)),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each [exit_date] >= #date(2025, 7, 1) and [exit_date] <= #date(2026, 6, 30)),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows 1", {"entry_id"}, lsndc_exits, {"entry_id"}, "lsndc_exits", JoinKind.LeftOuter),
  #"Expanded lsndc_exits" = Table.ExpandTableColumn(#"Merged queries", "lsndc_exits", {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income", "entry_id"}, {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income", "entry_id.1"}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Expanded lsndc_exits", "client_counter", each 1), {{"client_counter", type number}}),
  #"Inserted conditional column" = Table.AddColumn(#"Added custom", "positive", each if Text.Contains([income_flag], "Yes") then 1 else 0),
  #"Grouped rows" = Table.Group(#"Inserted conditional column", {"program", "exit_quarter"}, {{"positive", each List.Sum([positive]), type any}, {"total", each List.Sum([client_counter]), type nullable number}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Grouped rows", {{"positive", type number}}),
  #"Inserted division" = Table.AddColumn(#"Changed column type", "Division", each [positive] / [total], type number),
  #"Renamed columns" = Table.RenameColumns(#"Inserted division", {{"Division", "percentage"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"percentage", Percentage.Type}})
in
  #"Changed column type 1";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "safeHaven_exit_destinations_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared safeHaven_exit_destinations = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([program] = "Safe Haven")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each [exit_date] >= #date(2025, 7, 1) and [exit_date] <= #date(2026, 6, 30)),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows 1", {"entry_id"}, lsndc_exits, {"entry_id"}, "lsndc_exits", JoinKind.LeftOuter),
  #"Expanded lsndc_exits" = Table.ExpandTableColumn(#"Merged queries", "lsndc_exits", {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income", "nonCash_flag"}, {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income", "nonCash_flag"}),

  #"Added days_between" = Table.AddColumn(
      #"Expanded lsndc_exits",
      "days_between",
      each
        if [entry_date] = null or [exit_date] = null then null
        else Duration.Days(Date.From([exit_date]) - Date.From([entry_date])),
      Int64.Type
  ),
  #"Filtered rows 2" = Table.SelectRows(#"Added days_between", each [days_between] > 14),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Filtered rows 2", "counter", each 1), {{"counter", type number}}),
  #"Inserted conditional column" = Table.AddColumn(#"Added custom", "positive", each if Text.Contains([exit_destination], "by client") then 1 else if Text.Contains([exit_destination], "with family") then 1 else if Text.Contains([exit_destination], "No exit") then 0 else 0),
  #"Filtered rows 3" = Table.SelectRows(#"Inserted conditional column", each ([exit_destination] <> "Psychiatric hospital or other psychiatric facility (HUD)")),
  #"Grouped rows" = Table.Group(#"Filtered rows 3", {"program", "exit_quarter"}, {{"positive", each List.Sum([positive]), type any}, {"total", each List.Sum([counter]), type nullable number}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Grouped rows", {{"positive", type number}}),
  #"Inserted division" = Table.AddColumn(#"Changed column type", "Division", each [positive] / [total], type number),
  #"Renamed columns" = Table.RenameColumns(#"Inserted division", {{"Division", "percentage"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"percentage", Percentage.Type}})
in
  #"Changed column type 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "safeHaven_average_lengthOfStay_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared safeHaven_average_lengthOfStay = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([program] = "Safe Haven")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each [exit_date] >= #date(2025, 7, 1) and [exit_date] <= #date(2026, 6, 30)),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows 1", {"entry_id"}, lsndc_exits, {"entry_id"}, "lsndc_exits", JoinKind.LeftOuter),
  #"Expanded lsndc_exits" = Table.ExpandTableColumn(#"Merged queries", "lsndc_exits", {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income", "nonCash_flag"}, {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income", "nonCash_flag"}),

  #"Added days_between" = Table.AddColumn(
      #"Expanded lsndc_exits",
      "days_between",
      each
        if [entry_date] = null or [exit_date] = null then null
        else Duration.Days(Date.From([exit_date]) - Date.From([entry_date])),
      Int64.Type
  ),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Added days_between", "counter", each 1), {{"counter", type number}}),
  #"Inserted conditional column" = Table.AddColumn(#"Added custom", "positive", each if Text.Contains([exit_destination], "by client") then 1 else if Text.Contains([exit_destination], "with family") then 1 else if Text.Contains([exit_destination], "No exit") then 0 else 0),
  #"Grouped rows" = Table.Group(#"Inserted conditional column", {"program", "exit_quarter"}, {{"avg_stay", each List.Average([days_between]), type nullable number}})
in
  #"Grouped rows";
[BindToDefaultDestination = true]
shared ssvfLa_exit_housedTiming = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([program] = "SSVF Homeless Prevention" or [program] = "SSVF Rapid Re-Housing")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each [exit_date] >= #date(2025, 7, 1) and [exit_date] <= #date(2026, 6, 30)),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows 1", {"entry_id"}, lsndc_exits, {"entry_id"}, "lsndc_exits", JoinKind.LeftOuter),
  #"Expanded lsndc_exits" = Table.ExpandTableColumn(#"Merged queries", "lsndc_exits", {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income", "nonCash_flag"}, {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income", "nonCash_flag"}),

  #"Added days_between" = Table.AddColumn(
      #"Expanded lsndc_exits",
      "days_between",
      each
        if [entry_date] = null or [exit_date] = null then null
        else Duration.Days(Date.From([exit_date]) - Date.From([entry_date])),
      Int64.Type
  ),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Added days_between", "counter", each 1), {{"counter", type number}}),
  #"Inserted conditional column" = Table.AddColumn(#"Added custom", "positive", each if Text.Contains([exit_destination], "Rental") then 1 else if Text.Contains([exit_destination], "Staying") then 1 else if Text.Contains([exit_destination], "by client") then 1 else 0),
  #"Filtered rows 2" = Table.SelectRows(#"Inserted conditional column", each [days_between] <= 90),
  #"Grouped rows" = Table.Group(#"Filtered rows 2", {"program", "exit_quarter"}, {{"positive", each List.Sum([positive]), type any}, {"total", each List.Sum([counter]), type nullable number}}),
  #"Merged queries 1" = Table.NestedJoin(#"Grouped rows", {"program", "exit_quarter"}, ssvf_housed_staging, {"program", "exit_quarter"}, "raw_lsndc_entries (2)", JoinKind.LeftOuter),
  #"Expanded raw_lsndc_entries (2)" = Table.ExpandTableColumn(#"Merged queries 1", "raw_lsndc_entries (2)", {"stage_total"}, {"stage_total"}),
  #"Replaced value" = Table.ReplaceValue(#"Expanded raw_lsndc_entries (2)", null, 0, Replacer.ReplaceValue, {"stage_total"}),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(#"Replaced value", "true_total", each [total] + [stage_total]), {{"true_total", type number}}),
  #"Removed columns" = Table.RemoveColumns(#"Added custom 1", {"total", "stage_total"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns", {{"positive", type number}}),
  #"Inserted division" = Table.AddColumn(#"Changed column type", "Division", each [positive] / [true_total], type number),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted division", {{"Division", Percentage.Type}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type 1", {{"Division", "percentage"}})
in
  #"Renamed columns";
shared ssvf_housed_staging = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([program] = "SSVF Homeless Prevention" or [program] = "SSVF Rapid Re-Housing")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each [exit_date] >= #date(2025, 7, 1) and [exit_date] <= #date(2026, 6, 30)),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows 1", {"entry_id"}, lsndc_exits, {"entry_id"}, "lsndc_exits", JoinKind.LeftOuter),
  #"Expanded lsndc_exits" = Table.ExpandTableColumn(#"Merged queries", "lsndc_exits", {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income", "nonCash_flag"}, {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income", "nonCash_flag"}),

  #"Added days_between" = Table.AddColumn(
      #"Expanded lsndc_exits",
      "days_between",
      each
        if [entry_date] = null or [exit_date] = null then null
        else Duration.Days(Date.From([exit_date]) - Date.From([entry_date])),
      Int64.Type
  ),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Added days_between", "counter", each 1), {{"counter", type number}}),
  #"Inserted conditional column" = Table.AddColumn(#"Added custom", "positive", each if Text.Contains([exit_destination], "Rental") then 1 else if Text.Contains([exit_destination], "Staying") then 1 else if Text.Contains([exit_destination], "by client") then 1 else 0),
  #"Filtered rows 2" = Table.SelectRows(#"Inserted conditional column", each [days_between] > 90),
  #"Grouped rows" = Table.Group(#"Filtered rows 2", {"program", "exit_quarter"}, {{"positive", each List.Sum([positive]), type any}, {"total", each List.Sum([counter]), type nullable number}}),
  #"Renamed columns" = Table.RenameColumns(#"Grouped rows", {{"positive", "stage_pos"}, {"total", "stage_total"}})
in
  #"Renamed columns";
[BindToDefaultDestination = true]
shared ssvfLa_exit_housed = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([program] = "SSVF Homeless Prevention" or [program] = "SSVF Rapid Re-Housing")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each [exit_date] >= #date(2025, 7, 1) and [exit_date] <= #date(2026, 6, 30)),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows 1", {"entry_id"}, lsndc_exits, {"entry_id"}, "lsndc_exits", JoinKind.LeftOuter),
  #"Expanded lsndc_exits" = Table.ExpandTableColumn(#"Merged queries", "lsndc_exits", {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income", "nonCash_flag"}, {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income", "nonCash_flag"}),

  #"Added days_between" = Table.AddColumn(
      #"Expanded lsndc_exits",
      "days_between",
      each
        if [entry_date] = null or [exit_date] = null then null
        else Duration.Days(Date.From([exit_date]) - Date.From([entry_date])),
      Int64.Type
  ),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Added days_between", "counter", each 1), {{"counter", type number}}),
  Custom = Table.AddColumn(#"Added custom", "positive", each if Text.Contains([exit_destination], "Rental") then 1 else if Text.Contains([exit_destination], "Staying") then 1 else if Text.Contains([exit_destination], "by client") then 1 else 0),
  #"Grouped rows" = Table.Group(Custom, {"program", "exit_quarter"}, {{"positive", each List.Sum([positive]), type any}, {"total", each List.Sum([counter]), type nullable number}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Grouped rows", {{"positive", type number}}),
  #"Inserted division" = Table.AddColumn(#"Changed column type", "Division", each [positive] / [total], type number),
  #"Renamed columns" = Table.RenameColumns(#"Inserted division", {{"Division", "percentage"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"percentage", Percentage.Type}})
in
  #"Changed column type 1";
shared raw_lsndc_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_services", ItemKind = "Table"]}[Data],
  #"Grouped rows" = Table.Group(#"Navigation 2", {"client_id", "program"}, {{"first_service", each List.Min([service_date]), type nullable date}}),
  #"Extracted text after delimiter" = Table.TransformColumns(#"Grouped rows", {{"program", each Text.AfterDelimiter(_, "- ", 0), type text}})
in
  #"Extracted text after delimiter";
[BindToDefaultDestination = true]
shared ssvfLa_tfa_efficiency = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each [entry_date] >= #date(2025, 7, 1) and [entry_date] <= #date(2026, 6, 30)),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each ([program] = "SSVF Homeless Prevention" or [program] = "SSVF Rapid Re-Housing" or [program] = "SSVF Shallow Subsidy" or [program] = "SSVF VASH RRH")),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows 1", {"client_id", "program"}, raw_lsndc_services, {"client_id", "program"}, "raw_lsndc_services", JoinKind.LeftOuter),
  #"Expanded raw_lsndc_services" = Table.ExpandTableColumn(#"Merged queries", "raw_lsndc_services", {"program", "first_service"}, {"program.1", "first_service"}),
  #"Replaced value" = Table.ReplaceValue(#"Expanded raw_lsndc_services", "SSVF", "", Replacer.ReplaceValue, {"program.1"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "SSVF Homeless Prevention", "SSVF", Replacer.ReplaceValue, {"program.1"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "SSVF Rapid Re-Housing", "SSVF", Replacer.ReplaceValue, {"program.1"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "SSVF Shallow Subsidy", "SSVF", Replacer.ReplaceValue, {"program.1"}),
  #"Filtered rows 2" = Table.SelectRows(#"Replaced value 3", each ([program.1] = "SSVF")),

  #"Added days_to_first_service" = Table.AddColumn(
      #"Filtered rows 2",
      "days_to_first_service",
      each
        if [entry_date] = null or [first_service] = null then null
        else Duration.Days(Date.From([first_service]) - Date.From([entry_date])),
      Int64.Type
  ),
  #"Inserted conditional column" = Table.AddColumn(#"Added days_to_first_service", "good_service_flag", each if [days_to_first_service] < 31 then 1 else 0),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted conditional column", {{"good_service_flag", type number}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "counter", each 1), {{"counter", type number}}),
  #"Grouped rows" = Table.Group(#"Added custom", {"program.1", "entry_quarter"}, {{"positive", each List.Sum([good_service_flag]), type nullable number}, {"total", each List.Sum([counter]), type nullable number}}),
  #"Inserted division" = Table.AddColumn(#"Grouped rows", "Division", each [positive] / [total], type number),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted division", {{"Division", Percentage.Type}})
in
  #"Changed column type 1";
shared safeHaven_average_lengthOfStay_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "safeHaven_average_lengthOfStay", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared safeHaven_exit_destinations_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "safeHaven_exit_destinations", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
