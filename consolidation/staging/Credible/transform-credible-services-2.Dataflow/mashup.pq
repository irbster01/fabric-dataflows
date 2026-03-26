[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"transform-credible-services-1" = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "raw_credible_services", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bundle-secondaries_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"bundle-secondaries" = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "raw_credible_services", ItemKind = "Table"]}[Data],
  #"Renamed columns 1" = Table.RenameColumns(#"Navigation 3", {{"service_date", "Service Date"}}),
  #"Inserted start of month" = Table.AddColumn(#"Renamed columns 1", "Start of month", each Date.StartOfMonth([Service Date]), type nullable date),
  #"Filtered rows 1" = Table.SelectRows(#"Inserted start of month", each Text.Contains([service_type], "BUN")),
  #"Sorted rows 1" = Table.Sort(#"Filtered rows 1", {{"Service Date", Order.Descending}}),
  #"Renamed columns" = Table.RenameColumns(#"Sorted rows 1", {{"Start of month", "Month"}}),
  #"Filtered rows" = Table.SelectRows(#"Renamed columns", each [comb_primary] = "false"),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"client_id", "Month", "comb_primary"}, {{"Count", each List.Count(List.Distinct([service_id])), Int64.Type}}),
  #"Sorted rows" = Table.Sort(#"Grouped rows", {{"Month", Order.Descending}}),
  #"Renamed columns 2" = Table.RenameColumns(#"Sorted rows", {{"client_id", "Client ID"}})
in
  #"Renamed columns 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "transform-credible-bundle-primary_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"transform-credible-bundle-primary" = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "raw_credible_services", ItemKind = "Table"]}[Data],
  #"Renamed columns 1" = Table.RenameColumns(#"Navigation 3", {{"service_date", "Service Date"}}),
  #"Inserted start of month" = Table.AddColumn(#"Renamed columns 1", "Start of month", each Date.StartOfMonth([Service Date]), type nullable date),
  #"Filtered rows 1" = Table.SelectRows(#"Inserted start of month", each Text.Contains([service_type], "BUN")),
  #"Sorted rows 1" = Table.Sort(#"Filtered rows 1", {{"Service Date", Order.Descending}}),
  #"Renamed columns" = Table.RenameColumns(#"Sorted rows 1", {{"Start of month", "Month"}}),
  #"Filtered rows" = Table.SelectRows(#"Renamed columns", each [comb_primary] = "true"),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"client_id", "Month", "service_id"}, {{"Count", each List.Count(List.Distinct([service_id])), Int64.Type}}),
  #"Sorted rows" = Table.Sort(#"Grouped rows", {{"Month", Order.Descending}}),
  #"Renamed columns 2" = Table.RenameColumns(#"Sorted rows", {{"client_id", "Client ID"}})
in
  #"Renamed columns 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "division-bundle-counter_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Month", DestinationColumnName = "Month"], [SourceColumnName = "bundle-flag", DestinationColumnName = "bundle-flag"], [SourceColumnName = "bundle-count", DestinationColumnName = "bundle-count"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"division-bundle-counter" = let
  Source = Table.NestedJoin(#"transform-credible-bundle-primary", {"Month", "Client ID"}, #"bundle-secondaries", {"Month", "Client ID"}, "bundle-secondaries", JoinKind.Inner),
  #"Expanded bundle-secondaries 1" = Table.ExpandTableColumn(Source, "bundle-secondaries", {"Count"}, {"Count.1"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Expanded bundle-secondaries 1", {{"Count.1", "sec-count"}}),
  #"Inserted addition" = Table.AddColumn(#"Renamed columns 1", "Addition", each [Count] + [#"sec-count"], Int64.Type),
  #"Removed columns" = Table.RemoveColumns(#"Inserted addition", {"Count", "sec-count"}),
  #"Sorted rows" = Table.Sort(#"Removed columns", {{"Month", Order.Descending}}),
  #"Inserted conditional column" = Table.AddColumn(#"Sorted rows", "Custom", each if [Addition] >= 4 then "BUN" else "NO"),
  #"Grouped rows" = Table.Group(#"Inserted conditional column", {"Month", "Custom"}, {{"Count", each Table.RowCount(_), Int64.Type}}),
  #"Sorted rows 1" = Table.Sort(#"Grouped rows", {{"Month", Order.Descending}}),
  #"Renamed columns" = Table.RenameColumns(#"Sorted rows 1", {{"Count", "bundle-count"}, {"Custom", "bundle-flag"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"bundle-flag", type text}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "client-bundle-counter_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"client-bundle-counter" = let
  Source = Table.NestedJoin(#"transform-credible-bundle-primary", {"Client ID", "Month"}, #"bundle-secondaries", {"Client ID", "Month"}, "bundle-secondaries", JoinKind.Inner),
  #"Expanded bundle-secondaries" = Table.ExpandTableColumn(Source, "bundle-secondaries", {"Count"}, {"Count.1"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Expanded bundle-secondaries", {{"Count.1", "sec-count"}}),
  #"Inserted addition" = Table.AddColumn(#"Renamed columns 2", "Addition", each [Count] + [#"sec-count"], Int64.Type),
  #"Removed columns" = Table.RemoveColumns(#"Inserted addition", {"Count", "sec-count"}),
  #"Sorted rows" = Table.Sort(#"Removed columns", {{"Month", Order.Descending}}),
  #"Replaced value" = Table.ReplaceValue(#"Sorted rows", null, 0, Replacer.ReplaceValue, {"Addition"}),
  #"Inserted conditional column" = Table.AddColumn(#"Replaced value", "Custom", each if [Addition] >= 4 then "BUN" else "NO"),
  #"Sorted rows 1" = Table.Sort(#"Inserted conditional column", {{"Month", Order.Descending}}),
  #"Renamed columns" = Table.RenameColumns(#"Sorted rows 1", {{"Custom", "bundle-flag"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"bundle-flag", type text}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type", {{"Addition", "bundle-count"}})
in
  #"Renamed columns 1";
shared #"division-bundle-counter_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c46fe4a6-e106-4e77-993b-efc9ce22e551"]}[Data],
  TableNavigation = Navigation_2{[Id = "division-bundle-counter", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"client-bundle-counter_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c46fe4a6-e106-4e77-993b-efc9ce22e551"]}[Data],
  TableNavigation = Navigation_2{[Id = "client-bundle-counter", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"bundle-secondaries_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c46fe4a6-e106-4e77-993b-efc9ce22e551"]}[Data],
  TableNavigation = Navigation_2{[Id = "bundle-secondaries", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"transform-credible-bundle-primary_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c46fe4a6-e106-4e77-993b-efc9ce22e551"]}[Data],
  TableNavigation = Navigation_2{[Id = "transform-credible-bundle-primary", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
