[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared active_choice_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "active_choice_clients", ItemKind = "Table"]}[Data],
  #"Grouped rows" = Table.Group(#"Navigation 2", {"program"}, {{"Count", each Table.RowCount(Table.Distinct(_)), Int64.Type}})
in
  #"Grouped rows";
shared active_corbitt_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "active_corbitt_clients", ItemKind = "Table"]}[Data],
  #"Grouped rows" = Table.Group(#"Navigation 2", {"program"}, {{"Count", each Table.RowCount(Table.Distinct(_)), Int64.Type}})
in
  #"Grouped rows";
shared active_greenwood_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "active_greenwood_clients", ItemKind = "Table"]}[Data],
  #"Grouped rows" = Table.Group(#"Navigation 2", {"program"}, {{"Count", each Table.RowCount(Table.Distinct(_)), Int64.Type}})
in
  #"Grouped rows";
shared active_independence_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "active_independence_clients", ItemKind = "Table"]}[Data],
  #"Grouped rows" = Table.Group(#"Navigation 2", {"program"}, {{"Count", each Table.RowCount(Table.Distinct(_)), Int64.Type}})
in
  #"Grouped rows";
shared active_indMead_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "active_indMead_clients", ItemKind = "Table"]}[Data],
  #"Grouped rows" = Table.Group(#"Navigation 2", {"program"}, {{"Count", each Table.RowCount(Table.Distinct(_)), Int64.Type}})
in
  #"Grouped rows";
shared active_lh_comm_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "active_lh_comm_clients", ItemKind = "Table"]}[Data],
  #"Grouped rows" = Table.Group(#"Navigation 2", {"program"}, {{"Count", each Table.RowCount(Table.Distinct(_)), Int64.Type}})
in
  #"Grouped rows";
shared active_lh_school_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "active_lh_school_clients", ItemKind = "Table"]}[Data],
  #"Grouped rows" = Table.Group(#"Navigation 2", {"program"}, {{"Count", each Table.RowCount(Table.Distinct(_)), Int64.Type}})
in
  #"Grouped rows";
shared active_newHaven_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "active_newHaven_clients", ItemKind = "Table"]}[Data],
  #"Grouped rows" = Table.Group(#"Navigation 2", {"program"}, {{"Count", each Table.RowCount(Table.Distinct(_)), Int64.Type}})
in
  #"Grouped rows";
shared active_now_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "active_now_clients", ItemKind = "Table"]}[Data],
  #"Grouped rows" = Table.Group(#"Navigation 2", {"program"}, {{"Count", each Table.RowCount(Table.Distinct(_)), Int64.Type}})
in
  #"Grouped rows";
shared active_pineHaven = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "active_pineHaven", ItemKind = "Table"]}[Data],
  #"Grouped rows" = Table.Group(#"Navigation 2", {"program"}, {{"Count", each Table.RowCount(Table.Distinct(_)), Int64.Type}})
in
  #"Grouped rows";
shared active_southPointe_clients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "active_southPointe_clients", ItemKind = "Table"]}[Data],
  #"Grouped rows" = Table.Group(#"Navigation 2", {"program"}, {{"Count", each Table.RowCount(Table.Distinct(_)), Int64.Type}})
in
  #"Grouped rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "active_sp_counts_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared active_sp_counts = let
  Source = Table.Combine({active_southPointe_clients, active_pineHaven, active_now_clients, active_newHaven_clients, active_lh_school_clients, active_lh_comm_clients, active_independence_clients, active_indMead_clients, active_greenwood_clients, active_corbitt_clients, active_choice_clients}),
  #"Replaced value" = Table.ReplaceValue(Source, "VOANLA", "", Replacer.ReplaceText, {"program"}),
  #"Trimmed text" = Table.TransformColumns(#"Replaced value", {{"program", each Text.Trim(_), type nullable text}}),
  #"Cleaned text" = Table.TransformColumns(#"Trimmed text", {{"program", each Text.Clean(_), type nullable text}})
in
  #"Cleaned text";
shared active_sp_counts_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  TableNavigation = Navigation_2{[Id = "metrics.active_sp_counts", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
