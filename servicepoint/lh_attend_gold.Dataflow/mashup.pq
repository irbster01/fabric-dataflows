[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"silver_lighthouse new_monthly_client_attend" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "silver_lighthouse.new_monthly_client_attend", ItemKind = "View"]}[Data],
  #"Inserted multiplication" = Table.AddColumn(#"Navigation 2", "Multiplication", each [possible_sessions_in_month] * 0.6, Int64.Type),
  #"Inserted multiplication 1" = Table.AddColumn(#"Inserted multiplication", "Multiplication (2)", each [possible_sessions_in_month] * 0.8, Int64.Type),
  #"Renamed columns 1" = Table.RenameColumns(#"Inserted multiplication 1", {{"Multiplication (2)", "80_thresh"}}),
  #"Renamed columns" = Table.RenameColumns(#"Renamed columns 1", {{"Multiplication", "60_thresh"}}),
  #"Rounded up" = Table.TransformColumns(#"Renamed columns", {{"60_thresh", each Number.RoundUp(_), Int64.Type}}),
  #"Rounded up 1" = Table.TransformColumns(#"Rounded up", {{"80_thresh", each Number.RoundUp(_), Int64.Type}}),
  #"Inserted conditional column" = Table.AddColumn(#"Rounded up 1", "60_thresh_check", each if [service_count] >= [60_thresh] then "GOOD" else "BAD"),
  #"Inserted conditional column 1" = Table.AddColumn(#"Inserted conditional column", "80_thresh_check", each if [service_count] >= [80_thresh] then "GOOD" else "BAD")
in
  #"Inserted conditional column 1";
shared #"60_monthly_attend_good" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "silver_lighthouse.new_monthly_client_attend", ItemKind = "View"]}[Data],
  #"Inserted multiplication" = Table.AddColumn(#"Navigation 2", "Multiplication", each [possible_sessions_in_month] * 0.6, Int64.Type),
  #"Inserted multiplication 1" = Table.AddColumn(#"Inserted multiplication", "Multiplication (2)", each [possible_sessions_in_month] * 0.8, Int64.Type),
  #"Renamed columns 1" = Table.RenameColumns(#"Inserted multiplication 1", {{"Multiplication (2)", "80_thresh"}}),
  #"Renamed columns" = Table.RenameColumns(#"Renamed columns 1", {{"Multiplication", "60_thresh"}}),
  #"Rounded up" = Table.TransformColumns(#"Renamed columns", {{"60_thresh", each Number.RoundUp(_), Int64.Type}}),
  #"Rounded up 1" = Table.TransformColumns(#"Rounded up", {{"80_thresh", each Number.RoundUp(_), Int64.Type}}),
  #"Inserted conditional column" = Table.AddColumn(#"Rounded up 1", "60_thresh_check", each if [service_count] >= [60_thresh] then "GOOD" else "BAD"),
  #"Inserted conditional column 1" = Table.AddColumn(#"Inserted conditional column", "80_thresh_check", each if [service_count] >= [80_thresh] then "GOOD" else "BAD"),
  #"Filtered rows" = Table.SelectRows(#"Inserted conditional column 1", each ([60_thresh_check] = "GOOD")),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"lighthouse_location_enrolling", "month_name"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}}),
  #"Filtered rows 1" = Table.SelectRows(#"Grouped rows", each ([lighthouse_location_enrolling] <> "School Site - Broadmoor" and [lighthouse_location_enrolling] <> "School Site - Creswell" and [lighthouse_location_enrolling] <> "School Site - Oak Park"))
in
  #"Filtered rows 1";
shared #"monthly_attend_good (2)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "silver_lighthouse.new_monthly_client_attend", ItemKind = "View"]}[Data],
  #"Renamed columns 3" = Table.RenameColumns(#"Navigation 2", {{"possible_sessions_in_month_client", "possible_sessions_in_month"}}),
  #"Inserted multiplication" = Table.AddColumn(#"Renamed columns 3", "Multiplication", each [possible_sessions_in_month] * 0.6, Int64.Type),
  #"Inserted multiplication 1" = Table.AddColumn(#"Inserted multiplication", "Multiplication (2)", each [possible_sessions_in_month] * 0.8, Int64.Type),
  #"Renamed columns 1" = Table.RenameColumns(#"Inserted multiplication 1", {{"Multiplication (2)", "80_thresh"}}),
  #"Renamed columns" = Table.RenameColumns(#"Renamed columns 1", {{"Multiplication", "60_thresh"}}),
  #"Rounded up" = Table.TransformColumns(#"Renamed columns", {{"60_thresh", each Number.RoundUp(_), Int64.Type}}),
  #"Rounded up 1" = Table.TransformColumns(#"Rounded up", {{"80_thresh", each Number.RoundUp(_), Int64.Type}}),
  #"Inserted conditional column" = Table.AddColumn(#"Rounded up 1", "60_thresh_check", each if [service_count] >= [60_thresh] then "GOOD" else "BAD"),
  #"Inserted conditional column 1" = Table.AddColumn(#"Inserted conditional column", "80_thresh_check", each if [service_count] >= [80_thresh] then "GOOD" else "BAD"),
  #"Filtered rows" = Table.SelectRows(#"Inserted conditional column 1", each [possible_sessions_in_month] > 0),
  #"Inserted division" = Table.AddColumn(#"Filtered rows", "Division", each [service_count] / [possible_sessions_in_month], type number),
  #"Renamed columns 2" = Table.RenameColumns(#"Inserted division", {{"Division", "percentage_attended"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns 2", {{"percentage_attended", Percentage.Type}}),
  #"Grouped rows" = Table.Group(#"Changed column type", {"60_thresh_check"}, {{"Count", each List.Count(List.Distinct([client_id])), Int64.Type}})
in
  #"Grouped rows";
shared #"monthly_attend_good (3)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "silver_lighthouse.new_monthly_client_attend", ItemKind = "View"]}[Data],
  #"Inserted multiplication" = Table.AddColumn(#"Navigation 2", "Multiplication", each [possible_sessions_in_month] * 0.6, Int64.Type),
  #"Inserted multiplication 1" = Table.AddColumn(#"Inserted multiplication", "Multiplication (2)", each [possible_sessions_in_month] * 0.8, Int64.Type),
  #"Renamed columns 1" = Table.RenameColumns(#"Inserted multiplication 1", {{"Multiplication (2)", "80_thresh"}}),
  #"Renamed columns" = Table.RenameColumns(#"Renamed columns 1", {{"Multiplication", "60_thresh"}}),
  #"Rounded up" = Table.TransformColumns(#"Renamed columns", {{"60_thresh", each Number.RoundUp(_), Int64.Type}}),
  #"Rounded up 1" = Table.TransformColumns(#"Rounded up", {{"80_thresh", each Number.RoundUp(_), Int64.Type}}),
  #"Inserted conditional column" = Table.AddColumn(#"Rounded up 1", "60_thresh_check", each if [service_count] >= [60_thresh] then "GOOD" else "BAD"),
  #"Inserted conditional column 1" = Table.AddColumn(#"Inserted conditional column", "80_thresh_check", each if [service_count] >= [80_thresh] then "GOOD" else "BAD"),
  #"Filtered rows" = Table.SelectRows(#"Inserted conditional column 1", each ([month_name] = "October")),
  #"Sorted rows" = Table.Sort(#"Filtered rows", {{"service_count", Order.Descending}})
in
  #"Sorted rows";
shared #"silver_lighthouse lh_client_month_agg" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "silver_lighthouse.lh_client_month_agg", ItemKind = "View"]}[Data]
in
  #"Navigation 2";
shared #"silver_lighthouse v3_monthly_client_attend" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "silver_lighthouse.v3_monthly_client_attend", ItemKind = "View"]}[Data]
in
  #"Navigation 2";
