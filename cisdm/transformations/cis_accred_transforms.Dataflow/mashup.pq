[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_tier1_accred = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_tier1_accred", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared tier1_ratio = let
  Source = raw_tier1_accred,
  #"Removed columns" = Table.RemoveColumns(Source, {"num_school_basic_needs", "total_served_dupe", "avg_served", "min_served", "max_served", "num_tier1_supports", "total_students_dupe", "tier1_avg_served", "tier1_min_served", "num_tier1_and_needs"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed columns", {"school_name", "school_type", "tier1_max_served", "school_enrollment"}),
  #"Inserted division" = Table.AddColumn(#"Reordered columns", "Division", each [tier1_max_served] / [school_enrollment], type number),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted division", {{"Division", Percentage.Type}})
in
  #"Changed column type";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data];
[BindToDefaultDestination = true]
shared raw_cisdm_accred_case_manage = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_accred_case_manage", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared raw_cisdm_accred_drilldown = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_accred_drilldown", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared raw_cisdm_accred_site_coord = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_cisdm_accred_site_coord", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared first_9weeks_tier1 = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "first_9weeks_tier1", ItemKind = "Table"]}[Data],
  #"Removed other columns" = Table.SelectColumns(#"Navigation 2", {"School", "School Type", "Total School Enrollment", "Tier I - _Max Served __at any one support_"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed other columns", {"School", "School Type", "Tier I - _Max Served __at any one support_", "Total School Enrollment"}),
  #"Inserted division" = Table.AddColumn(#"Reordered columns", "Division", each [#"Tier I - _Max Served __at any one support_"] / [Total School Enrollment], type number),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted division", {{"Division", Percentage.Type}})
in
  #"Changed column type";
