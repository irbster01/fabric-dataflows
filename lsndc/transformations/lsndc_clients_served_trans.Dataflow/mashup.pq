[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_lsndc_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_services", ItemKind = "Table"]}[Data],

  // Fiscal year starts Oct 1:
  // FY = Year(service_date) + 1 when Month >= 10, else Year(service_date)
  #"Added Fiscal Year" =
    Table.AddColumn(
      #"Navigation 2",
      "fiscal_year",
      each
        let d = try Date.From([service_date]) otherwise null
        in if d = null then null
           else if Date.Month(d) >= 10 then Date.Year(d) + 1 else Date.Year(d),
      Int64.Type
    ),

  // Filter programs first
  #"Filtered programs" =
    Table.SelectRows(#"Added Fiscal Year", each ([program_creating] = "VOANLA 3001 - Veterans Transitional Living Program" or [program_creating] = "VOANLA 3013 - Safe Haven" or [program_creating] = "VOANLA 3031 - SSVF Homeless Prevention" or [program_creating] = "VOANLA 3031 - SSVF Rapid Re-Housing" or [program_creating] = "VOANLA 3032 - SSVF Shallow Subsidy" or [program_creating] = "VOANLA 3033 – SSVF VASH RRH" or [program_creating] = "VOANLA Medical Respite Program - Hospital to Housing")),

  // Remove null FYs (prevents pivot/header issues)
  #"Filtered FY Not Null" =
    Table.SelectRows(#"Filtered programs", each [fiscal_year] <> null),

  // Create text version for pivot headers
  #"Added FY Text" =
    Table.AddColumn(
      #"Filtered FY Not Null",
      "fiscal_year_text",
      each Text.From([fiscal_year]),
      type text
    ),
  #"Replaced value" = Table.ReplaceValue(#"Added FY Text", "VOANLA 3032 - SSVF Shallow Subsidy", "VOANLA 3031 - SSVF Homeless Prevention", Replacer.ReplaceValue, {"program_creating"}),

  // Distinct-client counts by Program + FY(text)
  #"Grouped rows" =
    Table.Group(
      #"Replaced value",
      {"program_creating", "fiscal_year_text"},
      {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}
    ),

  // Pivot FY(text) into columns
  Years = List.Sort(List.Distinct(#"Grouped rows"[fiscal_year_text])),
  #"Pivoted Fiscal Years" =
    Table.Pivot(
      #"Grouped rows",
      Years,
      "fiscal_year_text",
      "Count",
      List.Sum
    ),

  // Replace nulls with 0 in year columns
  YearCols = List.RemoveItems(Table.ColumnNames(#"Pivoted Fiscal Years"), {"program_creating"}),
  #"Nulls to Zero" =
    Table.ReplaceValue(
      #"Pivoted Fiscal Years",
      null,
      0,
      Replacer.ReplaceValue,
      YearCols
    )
in
  #"Nulls to Zero";
shared raw_lsndc_entries = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_entries", ItemKind = "Table"]}[Data],

  #"Filtered rows" =
    Table.SelectRows(
      #"Navigation 2",
      each
        ([program] = "Hospital to Housing"
          or [program] = "Safe Haven"
          or [program] = "SSVF Homeless Prevention"
          or [program] = "SSVF Rapid Re-Housing"
          or [program] = "Veterans Transitional Living Program")
    ),

  // Normalize to Date
  #"Added Entry Date (Date)" =
    Table.AddColumn(
      #"Filtered rows",
      "entry_date_d",
      each try Date.From([entry_date]) otherwise null,
      type date
    ),

  #"Added Exit Date (Date)" =
    Table.AddColumn(
      #"Added Entry Date (Date)",
      "exit_date_d",
      each try Date.From([exit_date]) otherwise null,
      type date
    ),

  // If exit is blank, treat as active through today's refresh date
  #"Added Active End Date" =
    Table.AddColumn(
      #"Added Exit Date (Date)",
      "active_end_d",
      each if [exit_date_d] <> null then [exit_date_d] else Date.From(DateTime.LocalNow()),
      type date
    ),

  // FY starts Oct 1
  FyFromDate = (d as date) as number =>
    if Date.Month(d) >= 10 then Date.Year(d) + 1 else Date.Year(d),

  // List of fiscal years active (inclusive)
  #"Added Active Fiscal Year List" =
    Table.AddColumn(
      #"Added Active End Date",
      "active_fiscal_year_list",
      each
        let
          s = [entry_date_d],
          e = [active_end_d]
        in
          if s = null or e = null then {}
          else if e < s then {}
          else { FyFromDate(s) .. FyFromDate(e) },
      type list
    ),

  // Expand to one row per fiscal year
  #"Expanded Active Fiscal Years" =
    Table.ExpandListColumn(#"Added Active Fiscal Year List", "active_fiscal_year_list"),

  #"Renamed FY Column" =
    Table.RenameColumns(#"Expanded Active Fiscal Years", {{"active_fiscal_year_list", "active_fiscal_year"}}),

  #"Typed FY" =
    Table.TransformColumnTypes(#"Renamed FY Column", {{"active_fiscal_year", Int64.Type}}),

  // Distinct-client counts by program + FY
  #"Grouped rows" =
    Table.Group(
      #"Typed FY",
      {"program", "active_fiscal_year"},
      {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}
    ),

  // Convert FY to text for pivot headers (prevents 10277)
  #"Added FY Text" =
    Table.AddColumn(
      #"Grouped rows",
      "active_fiscal_year_text",
      each Text.From([active_fiscal_year]),
      type text
    ),

  Years = List.Sort(List.Distinct(#"Added FY Text"[active_fiscal_year_text])),

  // Pivot FY into columns
  #"Pivoted Fiscal Years" =
    Table.Pivot(
      #"Added FY Text",
      Years,
      "active_fiscal_year_text",
      "Count",
      List.Sum
    ),

  // Replace nulls with 0 in year columns
  YearCols = List.RemoveItems(Table.ColumnNames(#"Pivoted Fiscal Years"), {"program"}),
  #"Nulls to Zero" =
    Table.ReplaceValue(
      #"Pivoted Fiscal Years",
      null,
      0,
      Replacer.ReplaceValue,
      YearCols
    ),
  #"Removed columns" = Table.RemoveColumns(#"Nulls to Zero", {"2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022"}),
  #"Filtered rows 1" = Table.SelectRows(#"Removed columns", each ([active_fiscal_year] = 2023 or [active_fiscal_year] = 2024 or [active_fiscal_year] = 2025)),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows 1", {"active_fiscal_year"}),
  #"Grouped rows 1" = Table.Group(#"Removed columns 1", {"program"}, {{"23_ct", each List.Max([2023]), type nullable number}, {"24_ct", each List.Max([2024]), type nullable number}, {"25_ct", each List.Max([2025]), type nullable number}, {"26_ct", each List.Max([2026]), type nullable number}})
in
  #"Grouped rows 1";
shared #"raw_lsndc_entries (2)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_entries", ItemKind = "Table"]}[Data],

  #"Filtered rows" =
    Table.SelectRows(
      #"Navigation 2",
      each
        ([program] = "Hospital to Housing"
          or [program] = "Safe Haven"
          or [program] = "SSVF Homeless Prevention"
          or [program] = "SSVF Rapid Re-Housing"
          or [program] = "Veterans Transitional Living Program")
    ),

  // Normalize to Date
  #"Added Entry Date (Date)" =
    Table.AddColumn(
      #"Filtered rows",
      "entry_date_d",
      each try Date.From([entry_date]) otherwise null,
      type date
    ),

  #"Added Exit Date (Date)" =
    Table.AddColumn(
      #"Added Entry Date (Date)",
      "exit_date_d",
      each try Date.From([exit_date]) otherwise null,
      type date
    ),

  // If exit is blank, treat as active through today's refresh date
  #"Added Active End Date" =
    Table.AddColumn(
      #"Added Exit Date (Date)",
      "active_end_d",
      each if [exit_date_d] <> null then [exit_date_d] else Date.From(DateTime.LocalNow()),
      type date
    ),

  // FY starts Oct 1
  FyFromDate = (d as date) as number =>
    if Date.Month(d) >= 10 then Date.Year(d) + 1 else Date.Year(d),

  // List of fiscal years active (inclusive)
  #"Added Active Fiscal Year List" =
    Table.AddColumn(
      #"Added Active End Date",
      "active_fiscal_year_list",
      each
        let
          s = [entry_date_d],
          e = [active_end_d]
        in
          if s = null or e = null then {}
          else if e < s then {}
          else { FyFromDate(s) .. FyFromDate(e) },
      type list
    ),

  // Expand to one row per fiscal year
  #"Expanded Active Fiscal Years" =
    Table.ExpandListColumn(#"Added Active Fiscal Year List", "active_fiscal_year_list"),

  #"Renamed FY Column" =
    Table.RenameColumns(#"Expanded Active Fiscal Years", {{"active_fiscal_year_list", "active_fiscal_year"}}),

  #"Typed FY" =
    Table.TransformColumnTypes(#"Renamed FY Column", {{"active_fiscal_year", Int64.Type}}),
  #"Filtered rows 2" = Table.SelectRows(#"Typed FY", each ([program] = "Safe Haven") and ([active_fiscal_year] = 2025)),
  #"Removed duplicates 1" = Table.Distinct(#"Filtered rows 2", {"entry_id"}),
  #"Merged queries" = Table.NestedJoin(#"Removed duplicates 1", {"entry_id"}, lsndc_exits, {"entry_id"}, "lsndc_exits", JoinKind.Inner),
  #"Removed duplicates" = Table.Distinct(#"Merged queries", {"client_id"}),
  #"Expanded lsndc_exits" = Table.ExpandTableColumn(#"Removed duplicates", "lsndc_exits", {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income"}, {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income"}),

  // Distinct-client counts by program + FY
  #"Grouped rows" =
    Table.Group(#"Expanded lsndc_exits", {"program", "active_fiscal_year", "reason_forLeaving"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}),
  #"Filtered rows 1" = Table.SelectRows(#"Grouped rows", each ([active_fiscal_year] = 2025)),

  // Convert FY to text for pivot headers (prevents 10277)
  #"Added FY Text" =
    Table.AddColumn(
      #"Filtered rows 1",
      "active_fiscal_year_text",
      each Text.From([active_fiscal_year]),
      type text
    ),
  #"Filtered rows 3" = Table.SelectRows(#"Added FY Text", each ([reason_forLeaving] = "Completed program" or [reason_forLeaving] = "Need Full Met"))
in
  #"Filtered rows 3";
shared lsndc_exits = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "lsndc_exits", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared #"raw_lsndc_entries (3)" = let
  Source = #"raw_lsndc_entries (2)"
in
  Source;
shared #"raw_lsndc_entries (4)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_entries", ItemKind = "Table"]}[Data],

  #"Filtered rows" =
    Table.SelectRows(
      #"Navigation 2",
      each
        ([program] = "Hospital to Housing"
          or [program] = "Safe Haven"
          or [program] = "SSVF Homeless Prevention"
          or [program] = "SSVF Rapid Re-Housing"
          or [program] = "Veterans Transitional Living Program")
    ),

  // Normalize to Date
  #"Added Entry Date (Date)" =
    Table.AddColumn(
      #"Filtered rows",
      "entry_date_d",
      each try Date.From([entry_date]) otherwise null,
      type date
    ),

  #"Added Exit Date (Date)" =
    Table.AddColumn(
      #"Added Entry Date (Date)",
      "exit_date_d",
      each try Date.From([exit_date]) otherwise null,
      type date
    ),

  // If exit is blank, treat as active through today's refresh date
  #"Added Active End Date" =
    Table.AddColumn(
      #"Added Exit Date (Date)",
      "active_end_d",
      each if [exit_date_d] <> null then [exit_date_d] else Date.From(DateTime.LocalNow()),
      type date
    ),

  // FY starts Oct 1
  FyFromDate = (d as date) as number =>
    if Date.Month(d) >= 10 then Date.Year(d) + 1 else Date.Year(d),

  // List of fiscal years active (inclusive)
  #"Added Active Fiscal Year List" =
    Table.AddColumn(
      #"Added Active End Date",
      "active_fiscal_year_list",
      each
        let
          s = [entry_date_d],
          e = [active_end_d]
        in
          if s = null or e = null then {}
          else if e < s then {}
          else { FyFromDate(s) .. FyFromDate(e) },
      type list
    ),

  // Expand to one row per fiscal year
  #"Expanded Active Fiscal Years" =
    Table.ExpandListColumn(#"Added Active Fiscal Year List", "active_fiscal_year_list"),

  #"Renamed FY Column" =
    Table.RenameColumns(#"Expanded Active Fiscal Years", {{"active_fiscal_year_list", "active_fiscal_year"}}),

  #"Typed FY" =
    Table.TransformColumnTypes(#"Renamed FY Column", {{"active_fiscal_year", Int64.Type}}),
  #"Filtered rows 2" = Table.SelectRows(#"Typed FY", each ([program] = "SSVF Homeless Prevention" or [program] = "SSVF Rapid Re-Housing")),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows 2", {"entry_id"}, lsndc_exits, {"entry_id"}, "lsndc_exits", JoinKind.Inner),
  #"Expanded lsndc_exits" = Table.ExpandTableColumn(#"Merged queries", "lsndc_exits", {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income"}, {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income"}),

  // Distinct-client counts by program + FY
  #"Grouped rows" =
    Table.Group(#"Expanded lsndc_exits", {"program", "active_fiscal_year", "exit_destination"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}),
  #"Filtered rows 1" = Table.SelectRows(#"Grouped rows", each ([active_fiscal_year] = 2025)),

  // Convert FY to text for pivot headers (prevents 10277)
  #"Added FY Text" =
    Table.AddColumn(
      #"Filtered rows 1",
      "active_fiscal_year_text",
      each Text.From([active_fiscal_year]),
      type text
    ),

  Years = List.Sort(List.Distinct(#"Added FY Text"[active_fiscal_year_text])),

  // Pivot FY into columns
  #"Pivoted Fiscal Years" =
    Table.Pivot(
      #"Added FY Text",
      Years,
      "active_fiscal_year_text",
      "Count",
      List.Sum
    ),

  // Replace nulls with 0 in year columns
  YearCols = List.RemoveItems(Table.ColumnNames(#"Pivoted Fiscal Years"), {"program"}),
  #"Nulls to Zero" =
    Table.ReplaceValue(
      #"Pivoted Fiscal Years",
      null,
      0,
      Replacer.ReplaceValue,
      YearCols
    ),
  #"Filtered rows 3" = Table.SelectRows(#"Nulls to Zero", each ([exit_destination] = "Data not collected (HUD)" or [exit_destination] = "Emergency shelter, including hotel or motel paid for with emergency shelter voucher, Host Home shelter (HUD)" or [exit_destination] = "Hospital or other residential non-psychiatric medical facility (HUD)" or [exit_destination] = "Hotel or motel paid for without emergency shelter voucher (HUD)" or [exit_destination] = "Other (HUD)" or [exit_destination] = "Psychiatric hospital or other psychiatric facility (HUD)"))
in
  #"Filtered rows 3";
shared #"raw_lsndc_entries (5)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lsndc_entries", ItemKind = "Table"]}[Data],

  #"Filtered rows" =
    Table.SelectRows(
      #"Navigation 2",
      each
        ([program] = "Hospital to Housing"
          or [program] = "Safe Haven"
          or [program] = "SSVF Homeless Prevention"
          or [program] = "SSVF Rapid Re-Housing"
          or [program] = "Veterans Transitional Living Program")
    ),

  // Normalize to Date
  #"Added Entry Date (Date)" =
    Table.AddColumn(
      #"Filtered rows",
      "entry_date_d",
      each try Date.From([entry_date]) otherwise null,
      type date
    ),

  #"Added Exit Date (Date)" =
    Table.AddColumn(
      #"Added Entry Date (Date)",
      "exit_date_d",
      each try Date.From([exit_date]) otherwise null,
      type date
    ),

  // If exit is blank, treat as active through today's refresh date
  #"Added Active End Date" =
    Table.AddColumn(
      #"Added Exit Date (Date)",
      "active_end_d",
      each if [exit_date_d] <> null then [exit_date_d] else Date.From(DateTime.LocalNow()),
      type date
    ),

  // FY starts Oct 1
  FyFromDate = (d as date) as number =>
    if Date.Month(d) >= 10 then Date.Year(d) + 1 else Date.Year(d),

  // List of fiscal years active (inclusive)
  #"Added Active Fiscal Year List" =
    Table.AddColumn(
      #"Added Active End Date",
      "active_fiscal_year_list",
      each
        let
          s = [entry_date_d],
          e = [active_end_d]
        in
          if s = null or e = null then {}
          else if e < s then {}
          else { FyFromDate(s) .. FyFromDate(e) },
      type list
    ),

  // Expand to one row per fiscal year
  #"Expanded Active Fiscal Years" =
    Table.ExpandListColumn(#"Added Active Fiscal Year List", "active_fiscal_year_list"),

  #"Renamed FY Column" =
    Table.RenameColumns(#"Expanded Active Fiscal Years", {{"active_fiscal_year_list", "active_fiscal_year"}}),

  #"Typed FY" =
    Table.TransformColumnTypes(#"Renamed FY Column", {{"active_fiscal_year", Int64.Type}}),
  #"Filtered rows 2" = Table.SelectRows(#"Typed FY", each ([program] = "Safe Haven")),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows 2", {"entry_id"}, lsndc_exits, {"entry_id"}, "lsndc_exits", JoinKind.Inner),
  #"Removed duplicates" = Table.Distinct(#"Merged queries", {"client_id"}),
  #"Expanded lsndc_exits" = Table.ExpandTableColumn(#"Removed duplicates", "lsndc_exits", {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income"}, {"exit_destination", "reason_forLeaving", "income_flag", "monthly_income"}),

  // Distinct-client counts by program + FY
  #"Grouped rows" =
    Table.Group(#"Expanded lsndc_exits", {"program", "active_fiscal_year", "reason_forLeaving"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}),
  #"Filtered rows 4" = Table.SelectRows(#"Grouped rows", each ([reason_forLeaving] <> "Completed program" and [reason_forLeaving] <> "Need Full Met")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows 4", each ([active_fiscal_year] = 2025)),

  // Convert FY to text for pivot headers (prevents 10277)
  #"Added FY Text" =
    Table.AddColumn(
      #"Filtered rows 1",
      "active_fiscal_year_text",
      each Text.From([active_fiscal_year]),
      type text
    ),
  #"Filtered rows 3" = Table.SelectRows(#"Added FY Text", each ([exit_destination] <> "Rental by client, no ongoing housing subsidy (HUD)" and [exit_destination] <> "Rental by client, with ongoing housing subsidy (HUD)" and [exit_destination] <> "Staying or living with family, permanent tenure (HUD)" and [exit_destination] <> "Staying or living with friends, temporary tenure (e.g., room, apartment, or house) (HUD)"))
in
  #"Filtered rows 3";
