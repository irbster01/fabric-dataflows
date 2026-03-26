[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_credible_brs = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_brs", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Rate", Currency.Type}, {"Balance", Currency.Type}, {"Days Since Approval", Int64.Type}, {"Corrected Aging Days", Int64.Type}, {"Days since last Kati Action", Int64.Type}, {"Days since Service", Int64.Type}}),
  #"Inserted start of month" = Table.AddColumn(#"Changed column type", "Start of month", each Date.StartOfMonth([Service Date]), type nullable date),
  #"Renamed columns" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns", "1740-1770", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "1830-1860", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "1800-1830", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "1710-1740", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "1680-1710", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "1770-1800", "365+", Replacer.ReplaceValue, {"Buckets"})
in
  #"Replaced value 5";
shared raw_credible_brs_cross = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_brs_cross", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared raw_credible_lookup_dict = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_lookup_dict", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared brs_w_balance = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_brs", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Rate", Currency.Type}, {"Balance", Currency.Type}, {"Days Since Approval", Int64.Type}, {"Corrected Aging Days", Int64.Type}, {"Days since last Kati Action", Int64.Type}, {"Days since Service", Int64.Type}}),
  #"Inserted start of month" = Table.AddColumn(#"Changed column type", "Start of month", each Date.StartOfMonth([Service Date]), type nullable date),
  #"Renamed columns" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns", "1740-1770", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "1830-1860", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "1800-1830", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "1710-1740", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "1680-1710", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "1770-1800", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Filtered rows" = Table.SelectRows(#"Replaced value 5", each ([Balance] <> 0) and ([CPT Code] <> "NB")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows", {"Service ID"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared brs_balance_by_prog = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_brs", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Rate", Currency.Type}, {"Balance", Currency.Type}, {"Days Since Approval", Int64.Type}, {"Corrected Aging Days", Int64.Type}, {"Days since last Kati Action", Int64.Type}, {"Days since Service", Int64.Type}}),
  #"Inserted start of month" = Table.AddColumn(#"Changed column type", "Start of month", each Date.StartOfMonth([Service Date]), type nullable date),
  #"Renamed columns" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns", "1740-1770", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "1830-1860", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "1800-1830", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "1710-1740", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "1680-1710", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "1770-1800", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Filtered rows" = Table.SelectRows(#"Replaced value 5", each ([Balance] <> 0) and ([CPT Code] <> "NB")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows", {"Service ID"}),
  #"Grouped rows" = Table.Group(#"Removed duplicates", {"Program"}, {{"total", each List.Sum([Balance]), type nullable number}})
in
  #"Grouped rows";
[BindToDefaultDestination = true]
shared brs_balance_by_brsStatus = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_brs", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Rate", Currency.Type}, {"Balance", Currency.Type}, {"Days Since Approval", Int64.Type}, {"Corrected Aging Days", Int64.Type}, {"Days since last Kati Action", Int64.Type}, {"Days since Service", Int64.Type}}),
  #"Inserted start of month" = Table.AddColumn(#"Changed column type", "Start of month", each Date.StartOfMonth([Service Date]), type nullable date),
  #"Renamed columns" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns", "1740-1770", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "1830-1860", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "1800-1830", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "1710-1740", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "1680-1710", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "1770-1800", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Filtered rows" = Table.SelectRows(#"Replaced value 5", each ([Balance] <> 0) and ([CPT Code] <> "NB")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows", {"Service ID"}),
  #"Grouped rows" = Table.Group(#"Removed duplicates", {"Billing Review Status"}, {{"total", each List.Sum([Balance]), type nullable number}})
in
  #"Grouped rows";
[BindToDefaultDestination = true]
shared brs_balance_by_bucket = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_brs", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Rate", Currency.Type}, {"Balance", Currency.Type}, {"Days Since Approval", Int64.Type}, {"Corrected Aging Days", Int64.Type}, {"Days since last Kati Action", Int64.Type}, {"Days since Service", Int64.Type}}),
  #"Inserted start of month" = Table.AddColumn(#"Changed column type", "Start of month", each Date.StartOfMonth([Service Date]), type nullable date),
  #"Renamed columns" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns", "1740-1770", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "1830-1860", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "1800-1830", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "1710-1740", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "1680-1710", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "1770-1800", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Filtered rows" = Table.SelectRows(#"Replaced value 5", each ([Balance] <> 0) and ([CPT Code] <> "NB")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows", {"Service ID"}),
  #"Grouped rows" = Table.Group(#"Removed duplicates", {"Buckets"}, {{"total", each List.Sum([Balance]), type nullable number}}),
  #"Inserted conditional column" = Table.AddColumn(#"Grouped rows", "bucket_id", each if [Buckets] = "0-30" then 1 else if [Buckets] = "30-60" then 2 else if [Buckets] = "60-90" then 3 else if [Buckets] = "90-120" then 4 else if [Buckets] = "120-180" then 5 else if [Buckets] = "180-365" then 6 else if [Buckets] = "365+" then 7 else ""),
  #"Sorted rows" = Table.Sort(#"Inserted conditional column", {{"bucket_id", Order.Ascending}})
in
  #"Sorted rows";
[BindToDefaultDestination = true]
shared brs_balance_by_servType = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_brs", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Rate", Currency.Type}, {"Balance", Currency.Type}, {"Days Since Approval", Int64.Type}, {"Corrected Aging Days", Int64.Type}, {"Days since last Kati Action", Int64.Type}, {"Days since Service", Int64.Type}}),
  #"Inserted start of month" = Table.AddColumn(#"Changed column type", "Start of month", each Date.StartOfMonth([Service Date]), type nullable date),
  #"Renamed columns" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns", "1740-1770", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "1830-1860", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "1800-1830", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "1710-1740", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "1680-1710", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "1770-1800", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Filtered rows" = Table.SelectRows(#"Replaced value 5", each ([Balance] <> 0) and ([CPT Code] <> "NB")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows", {"Service ID"}),
  #"Grouped rows" = Table.Group(#"Removed duplicates", {"Visit Type", "CPT Code"}, {{"total", each List.Sum([Balance]), type nullable number}})
in
  #"Grouped rows";
[BindToDefaultDestination = true]
shared brs_balance_by_servStatus = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_brs", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Rate", Currency.Type}, {"Balance", Currency.Type}, {"Days Since Approval", Int64.Type}, {"Corrected Aging Days", Int64.Type}, {"Days since last Kati Action", Int64.Type}, {"Days since Service", Int64.Type}}),
  #"Inserted start of month" = Table.AddColumn(#"Changed column type", "Start of month", each Date.StartOfMonth([Service Date]), type nullable date),
  #"Renamed columns" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns", "1740-1770", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "1830-1860", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "1800-1830", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "1710-1740", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "1680-1710", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "1770-1800", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Filtered rows" = Table.SelectRows(#"Replaced value 5", each ([Balance] <> 0) and ([CPT Code] <> "NB")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows", {"Service ID"}),
  #"Grouped rows" = Table.Group(#"Removed duplicates", {"Status"}, {{"total", each List.Sum([Balance]), type nullable number}})
in
  #"Grouped rows";
[BindToDefaultDestination = true]
shared brs_balance_by_Payer = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_brs", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Rate", Currency.Type}, {"Balance", Currency.Type}, {"Days Since Approval", Int64.Type}, {"Corrected Aging Days", Int64.Type}, {"Days since last Kati Action", Int64.Type}, {"Days since Service", Int64.Type}}),
  #"Inserted start of month" = Table.AddColumn(#"Changed column type", "Start of month", each Date.StartOfMonth([Service Date]), type nullable date),
  #"Renamed columns" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns", "1740-1770", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "1830-1860", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "1800-1830", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "1710-1740", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "1680-1710", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "1770-1800", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Filtered rows" = Table.SelectRows(#"Replaced value 5", each ([Balance] <> 0) and ([CPT Code] <> "NB")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows", {"Service ID"}),
  #"Grouped rows" = Table.Group(#"Removed duplicates", {"Payer"}, {{"total", each List.Sum([Balance]), type nullable number}})
in
  #"Grouped rows";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data];
[BindToDefaultDestination = true]
shared brs_count_bucket = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_brs", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Rate", Currency.Type}, {"Balance", Currency.Type}, {"Days Since Approval", Int64.Type}, {"Corrected Aging Days", Int64.Type}, {"Days since last Kati Action", Int64.Type}, {"Days since Service", Int64.Type}}),
  #"Inserted start of month" = Table.AddColumn(#"Changed column type", "Start of month", each Date.StartOfMonth([Service Date]), type nullable date),
  #"Renamed columns" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns", "1740-1770", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "1830-1860", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "1800-1830", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "1710-1740", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "1680-1710", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "1770-1800", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Filtered rows" = Table.SelectRows(#"Replaced value 5", each ([Balance] <> 0) and ([CPT Code] <> "NB")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows", {"Service ID"}),
  #"Grouped rows" = Table.Group(#"Removed duplicates", {"Buckets"}, {{"Count", each List.Count(List.Distinct([Service ID])), Int64.Type}})
in
  #"Grouped rows";
[BindToDefaultDestination = true]
shared brs_count_brsStatus = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_brs", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Rate", Currency.Type}, {"Balance", Currency.Type}, {"Days Since Approval", Int64.Type}, {"Corrected Aging Days", Int64.Type}, {"Days since last Kati Action", Int64.Type}, {"Days since Service", Int64.Type}}),
  #"Inserted start of month" = Table.AddColumn(#"Changed column type", "Start of month", each Date.StartOfMonth([Service Date]), type nullable date),
  #"Renamed columns" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns", "1740-1770", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "1830-1860", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "1800-1830", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "1710-1740", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "1680-1710", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "1770-1800", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Filtered rows" = Table.SelectRows(#"Replaced value 5", each ([Balance] <> 0) and ([CPT Code] <> "NB")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows", {"Service ID"}),
  #"Grouped rows" = Table.Group(#"Removed duplicates", {"Billing Review Status"}, {{"Count", each List.Count(List.Distinct([Service ID])), Int64.Type}})
in
  #"Grouped rows";
[BindToDefaultDestination = true]
shared brs_count_servType = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_brs", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Rate", Currency.Type}, {"Balance", Currency.Type}, {"Days Since Approval", Int64.Type}, {"Corrected Aging Days", Int64.Type}, {"Days since last Kati Action", Int64.Type}, {"Days since Service", Int64.Type}}),
  #"Inserted start of month" = Table.AddColumn(#"Changed column type", "Start of month", each Date.StartOfMonth([Service Date]), type nullable date),
  #"Renamed columns" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "service_month"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns", "1740-1770", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "1830-1860", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "1800-1830", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "1710-1740", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "1680-1710", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "1770-1800", "365+", Replacer.ReplaceValue, {"Buckets"}),
  #"Filtered rows" = Table.SelectRows(#"Replaced value 5", each ([Balance] <> 0) and ([CPT Code] <> "NB")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows", {"Service ID"}),
  #"Grouped rows" = Table.Group(#"Removed duplicates", {"Visit Type"}, {{"Count", each List.Count(List.Distinct([Service ID])), Int64.Type}})
in
  #"Grouped rows";
