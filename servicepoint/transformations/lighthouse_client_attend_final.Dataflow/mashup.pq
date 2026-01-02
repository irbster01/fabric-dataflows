[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"2526_lh_clients" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "2526_lh_clients", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each [days_enrolled] > 7)
in
  #"Filtered rows";
shared raw_lighthouse_registration = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_lighthouse_registration", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared raw_sp_demographics = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_demographics", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared raw_sp_entries = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_sp_entries", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each [exit_date] > #date(2025, 9, 1) or [exit_date] = null),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each ([program] = "VOANLA 4002 -The LightHouse - School Sites" or [program] = "VOANLA 4080 -The LightHouse - Community Sites" or [program] = "VOANLA 4080a - Teen Club")),
  #"Grouped rows" = Table.Group(#"Filtered rows 1", {"client_id"}, {{"Count", each List.Max([entry_date]), type nullable date}, {"exit_date", each List.Max([exit_date]), type nullable date}, {"program", each List.Max([program]), type nullable text}})
in
  #"Grouped rows";
shared #"silver_lighthouse client_month_possible_vs_actual" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "silver_lighthouse.client_month_possible_vs_actual", ItemKind = "Table"]}[Data],
  #"Grouped rows" = Table.Group(#"Navigation 2", {"client_id"}, {{"Count", each List.Max([possible_sessions_in_month]), type nullable Int64.Type}})
in
  #"Grouped rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "v_client_month_rollups_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared v_client_month_rollups = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "v_client_month_rollups", ItemKind = "View"]}[Data],
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Navigation 2", "today", each DateTime.Date(DateTimeZone.LocalNow())), {{"today", type datetime}}),
  #"Filtered rows" = Table.SelectRows(#"Added custom", each ([month_name] = "August" or [month_name] = "October" or [month_name] = "September")),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows", {"client_id"}, raw_lighthouse_registration, {"client_id"}, "raw_lighthouse_registration", JoinKind.LeftOuter),
  #"Added custom 1" = Table.AddColumn(#"Merged queries", "Custom", each "1st 9 Weeks"),
  #"Expanded raw_lighthouse_registration" = Table.ExpandTableColumn(#"Added custom 1", "raw_lighthouse_registration", {"app_date", "lighthouse_location_enrolling", "school_name"}, {"app_date", "lighthouse_location_enrolling", "school_name"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded raw_lighthouse_registration", {"max830", "max92", "max98"}),
  #"Removed duplicates" = Table.Distinct(#"Removed columns", {"client_id"}),
  #"Grouped rows" = Table.Group(#"Removed duplicates", {"client_id", "client_last_name", "client_first_name"}, {{"Count", each List.Sum([Count]), type nullable number}, {"days poss", each List.Sum([max99]), type nullable number}}),
  #"Sorted rows" = Table.Sort(#"Grouped rows", {{"Count", Order.Descending}}),
  #"Inserted multiplication" = Table.AddColumn(#"Sorted rows", "Multiplication", each [days poss] * 0.8, type number),
  #"Renamed columns" = Table.RenameColumns(#"Inserted multiplication", {{"Multiplication", "80 Check"}}),
  #"Inserted multiplication 1" = Table.AddColumn(#"Renamed columns", "Multiplication", each [days poss] * 0.6, type number),
  #"Renamed columns 1" = Table.RenameColumns(#"Inserted multiplication 1", {{"Multiplication", "60 Check"}}),
  #"Inserted conditional column" = Table.AddColumn(#"Renamed columns 1", "attend_flag", each if [Count] >= [80 Check] then "80% GOOD" else if [Count] >= [60 Check] then "60% GOOD" else "BAD ATTENDANCE"),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted conditional column", {{"attend_flag", type text}}),
  #"Merged queries 1" = Table.NestedJoin(#"Changed column type", {"client_id"}, raw_lighthouse_registration, {"client_id"}, "raw_lighthouse_registration", JoinKind.LeftOuter),
  #"Expanded raw_lighthouse_registration 1" = Table.ExpandTableColumn(#"Merged queries 1", "raw_lighthouse_registration", {"lighthouse_location_enrolling", "school_name"}, {"lighthouse_location_enrolling", "school_name"}),
  #"Grouped rows 1" = Table.Group(#"Expanded raw_lighthouse_registration 1", {"lighthouse_location_enrolling"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}})

in
  #"Grouped rows 1";
shared v_client_month_rollups_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  TableNavigation = Navigation_2{[Id = "lghthouse_attend_rollups_w_aggregates", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "80 sites_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"80 sites" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "v_client_month_rollups", ItemKind = "View"]}[Data],
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Navigation 2", "today", each DateTime.Date(DateTimeZone.LocalNow())), {{"today", type datetime}}),
  #"Filtered rows" = Table.SelectRows(#"Added custom", each ([month_name] = "August" or [month_name] = "October" or [month_name] = "September")),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows", {"client_id"}, raw_lighthouse_registration, {"client_id"}, "raw_lighthouse_registration", JoinKind.LeftOuter),
  #"Added custom 1" = Table.AddColumn(#"Merged queries", "Custom", each "1st 9 Weeks"),
  #"Expanded raw_lighthouse_registration" = Table.ExpandTableColumn(#"Added custom 1", "raw_lighthouse_registration", {"app_date", "lighthouse_location_enrolling", "school_name"}, {"app_date", "lighthouse_location_enrolling", "school_name"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded raw_lighthouse_registration", {"max830", "max92", "max98"}),
  #"Removed duplicates" = Table.Distinct(#"Removed columns", {"client_id"}),
  #"Grouped rows" = Table.Group(#"Removed duplicates", {"client_id", "client_last_name", "client_first_name", "lighthouse_location_enrolling"}, {{"Count", each List.Sum([Count]), type nullable number}, {"days poss", each List.Sum([max99]), type nullable number}}),
  #"Sorted rows" = Table.Sort(#"Grouped rows", {{"Count", Order.Descending}}),
  #"Inserted multiplication" = Table.AddColumn(#"Sorted rows", "Multiplication", each [days poss] * 0.8, type number),
  #"Renamed columns" = Table.RenameColumns(#"Inserted multiplication", {{"Multiplication", "80 Check"}}),
  #"Inserted multiplication 1" = Table.AddColumn(#"Renamed columns", "Multiplication", each [days poss] * 0.6, type number),
  #"Renamed columns 1" = Table.RenameColumns(#"Inserted multiplication 1", {{"Multiplication", "60 Check"}}),
  #"Inserted conditional column" = Table.AddColumn(#"Renamed columns 1", "attend_flag", each if [Count] >= [80 Check] then "80% GOOD" else if [Count] >= [60 Check] then "60% GOOD" else "BAD ATTENDANCE"),
  #"Subtracted from column" = Table.TransformColumns(#"Inserted conditional column", {{"80 Check", each _ - 1, type number}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Subtracted from column", {{"attend_flag", type text}}),
  #"Merged queries 1" = Table.NestedJoin(#"Changed column type", {"client_id"}, #"silver_lighthouse Lighthouse Matched with JCampus", {"client_id"}, "silver_lighthouse Lighthouse Matched with JCampus", JoinKind.LeftOuter),
  #"Expanded silver_lighthouse Lighthouse Matched with JCampus" = Table.ExpandTableColumn(#"Merged queries 1", "silver_lighthouse Lighthouse Matched with JCampus", {"client_dob", "int_id", "SIDNO", "School Name", "entry_date", "exit_date"}, {"client_dob", "int_id", "SIDNO", "School Name", "entry_date", "exit_date"}),
  #"Filtered rows 1" = Table.SelectRows(#"Expanded silver_lighthouse Lighthouse Matched with JCampus", each ([School Name] = "Bethune/Oak Park" or [School Name] = "Broadmoor STEM" or [School Name] = "Creswell Elementary")),
  #"Removed duplicates 1" = Table.Distinct(#"Filtered rows 1", {"client_id"}),
  #"Filtered rows 2" = Table.SelectRows(#"Removed duplicates 1", each ([attend_flag] = "80% GOOD")),
  #"Grouped rows 1" = Table.Group(#"Filtered rows 2", {"School Name"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}),
  #"Merged queries 2" = Table.NestedJoin(#"Grouped rows 1", {"School Name"}, #"80 sites (2)", {"School Name"}, "80 sites (2)", JoinKind.LeftOuter),
  #"Expanded 80 sites (2)" = Table.ExpandTableColumn(#"Merged queries 2", "80 sites (2)", {"total"}, {"total"}),
  #"Inserted division" = Table.AddColumn(#"Expanded 80 sites (2)", "Division", each [Count] / [total], type number),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted division", {{"Division", Percentage.Type}})

in
  #"Changed column type 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "not 80 sites_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"not 80 sites" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "v_client_month_rollups", ItemKind = "View"]}[Data],
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Navigation 2", "today", each DateTime.Date(DateTimeZone.LocalNow())), {{"today", type datetime}}),
  #"Filtered rows" = Table.SelectRows(#"Added custom", each ([month_name] = "August" or [month_name] = "October" or [month_name] = "September")),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows", {"client_id"}, raw_lighthouse_registration, {"client_id"}, "raw_lighthouse_registration", JoinKind.LeftOuter),
  #"Added custom 1" = Table.AddColumn(#"Merged queries", "Custom", each "1st 9 Weeks"),
  #"Expanded raw_lighthouse_registration" = Table.ExpandTableColumn(#"Added custom 1", "raw_lighthouse_registration", {"app_date", "lighthouse_location_enrolling", "school_name"}, {"app_date", "lighthouse_location_enrolling", "school_name"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded raw_lighthouse_registration", {"max830", "max92", "max98"}),
  #"Removed duplicates" = Table.Distinct(#"Removed columns", {"client_id"}),
  #"Grouped rows" = Table.Group(#"Removed duplicates", {"client_id", "client_last_name", "client_first_name"}, {{"Count", each List.Sum([Count]), type nullable number}, {"days poss", each List.Sum([max99]), type nullable number}}),
  #"Sorted rows" = Table.Sort(#"Grouped rows", {{"Count", Order.Descending}}),
  #"Inserted multiplication" = Table.AddColumn(#"Sorted rows", "Multiplication", each [days poss] * 0.8, type number),
  #"Renamed columns" = Table.RenameColumns(#"Inserted multiplication", {{"Multiplication", "80 Check"}}),
  #"Inserted multiplication 1" = Table.AddColumn(#"Renamed columns", "Multiplication", each [days poss] * 0.6, type number),
  #"Renamed columns 1" = Table.RenameColumns(#"Inserted multiplication 1", {{"Multiplication", "60 Check"}}),
  #"Inserted conditional column" = Table.AddColumn(#"Renamed columns 1", "attend_flag", each if [Count] >= [80 Check] then "80% GOOD" else if [Count] >= [60 Check] then "60% GOOD" else "BAD ATTENDANCE"),
  #"Subtracted from column" = Table.TransformColumns(#"Inserted conditional column", {{"80 Check", each _ - 1, type number}}),
  #"Subtracted from column 1" = Table.TransformColumns(#"Subtracted from column", {{"60 Check", each _ - 1, type number}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Subtracted from column 1", {{"attend_flag", type text}}),
  #"Merged queries 1" = Table.NestedJoin(#"Changed column type", {"client_id"}, raw_lighthouse_registration, {"client_id"}, "raw_lighthouse_registration", JoinKind.LeftOuter),
  #"Expanded raw_lighthouse_registration 1" = Table.ExpandTableColumn(#"Merged queries 1", "raw_lighthouse_registration", {"lighthouse_location_enrolling", "school_name"}, {"lighthouse_location_enrolling", "school_name"}),
  #"Filtered rows 1" = Table.SelectRows(#"Expanded raw_lighthouse_registration 1", each ([attend_flag] <> "BAD ATTENDANCE")),
  #"Grouped rows 1" = Table.Group(#"Filtered rows 1", {"lighthouse_location_enrolling"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}),
  #"Filtered rows 2" = Table.SelectRows(#"Grouped rows 1", each ([lighthouse_location_enrolling] <> "Highland" and [lighthouse_location_enrolling] <> "School Site - Broadmoor" and [lighthouse_location_enrolling] <> "School Site - Creswell" and [lighthouse_location_enrolling] <> "School Site - Oak Park")),
  #"Merged queries 2" = Table.NestedJoin(#"Filtered rows 2", {"lighthouse_location_enrolling"}, v_client_month_rollups, {"lighthouse_location_enrolling"}, "v_client_month_rollups", JoinKind.LeftOuter),
  #"Expanded v_client_month_rollups" = Table.ExpandTableColumn(#"Merged queries 2", "v_client_month_rollups", {"Count"}, {"Count.1"}),
  #"Inserted division" = Table.AddColumn(#"Expanded v_client_month_rollups", "Division", each [Count] / [Count.1], type number),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted division", {{"Division", Percentage.Type}}),
  #"Filtered rows 3" = Table.SelectRows(#"Changed column type 1", each ([lighthouse_location_enrolling] <> "Teen Club"))

in
  #"Filtered rows 3";
shared #"not 80 sites_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  TableNavigation = Navigation_2{[Id = "lighthouse_not_80_table", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"80 sites_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  TableNavigation = Navigation_2{[Id = "lighthouse_80_site_table", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"silver_lighthouse Lighthouse Matched with JCampus" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "silver_lighthouse.Lighthouse Matched with JCampus", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared #"80 sites (2)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "v_client_month_rollups", ItemKind = "View"]}[Data],
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Navigation 2", "today", each DateTime.Date(DateTimeZone.LocalNow())), {{"today", type datetime}}),
  #"Filtered rows" = Table.SelectRows(#"Added custom", each ([month_name] = "August" or [month_name] = "October" or [month_name] = "September")),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows", {"client_id"}, raw_lighthouse_registration, {"client_id"}, "raw_lighthouse_registration", JoinKind.LeftOuter),
  #"Added custom 1" = Table.AddColumn(#"Merged queries", "Custom", each "1st 9 Weeks"),
  #"Expanded raw_lighthouse_registration" = Table.ExpandTableColumn(#"Added custom 1", "raw_lighthouse_registration", {"app_date", "lighthouse_location_enrolling", "school_name"}, {"app_date", "lighthouse_location_enrolling", "school_name"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded raw_lighthouse_registration", {"max830", "max92", "max98"}),
  #"Removed duplicates" = Table.Distinct(#"Removed columns", {"client_id"}),
  #"Grouped rows" = Table.Group(#"Removed duplicates", {"client_id", "client_last_name", "client_first_name", "lighthouse_location_enrolling"}, {{"Count", each List.Sum([Count]), type nullable number}, {"days poss", each List.Sum([max99]), type nullable number}}),
  #"Sorted rows" = Table.Sort(#"Grouped rows", {{"Count", Order.Descending}}),
  #"Inserted multiplication" = Table.AddColumn(#"Sorted rows", "Multiplication", each [days poss] * 0.8, type number),
  #"Renamed columns" = Table.RenameColumns(#"Inserted multiplication", {{"Multiplication", "80 Check"}}),
  #"Inserted multiplication 1" = Table.AddColumn(#"Renamed columns", "Multiplication", each [days poss] * 0.6, type number),
  #"Renamed columns 1" = Table.RenameColumns(#"Inserted multiplication 1", {{"Multiplication", "60 Check"}}),
  #"Inserted conditional column" = Table.AddColumn(#"Renamed columns 1", "attend_flag", each if [Count] >= [80 Check] then "80% GOOD" else if [Count] >= [60 Check] then "60% GOOD" else "BAD ATTENDANCE"),
  #"Subtracted from column" = Table.TransformColumns(#"Inserted conditional column", {{"80 Check", each _ - 1, type number}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Subtracted from column", {{"attend_flag", type text}}),
  #"Merged queries 1" = Table.NestedJoin(#"Changed column type", {"client_id"}, #"silver_lighthouse Lighthouse Matched with JCampus", {"client_id"}, "silver_lighthouse Lighthouse Matched with JCampus", JoinKind.LeftOuter),
  #"Expanded silver_lighthouse Lighthouse Matched with JCampus" = Table.ExpandTableColumn(#"Merged queries 1", "silver_lighthouse Lighthouse Matched with JCampus", {"client_dob", "int_id", "SIDNO", "School Name", "entry_date", "exit_date"}, {"client_dob", "int_id", "SIDNO", "School Name", "entry_date", "exit_date"}),
  #"Filtered rows 1" = Table.SelectRows(#"Expanded silver_lighthouse Lighthouse Matched with JCampus", each ([School Name] = "Bethune/Oak Park" or [School Name] = "Broadmoor STEM" or [School Name] = "Creswell Elementary")),
  #"Removed duplicates 1" = Table.Distinct(#"Filtered rows 1", {"client_id"}),
  #"Grouped rows 1" = Table.Group(#"Removed duplicates 1", {"School Name"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}),
  #"Renamed columns 2" = Table.RenameColumns(#"Grouped rows 1", {{"Count", "total"}})

in
  #"Renamed columns 2";
