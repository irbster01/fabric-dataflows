[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared clean_lighthouse_services = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "clean_lighthouse_services", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "lighthouse_sessions_attended_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared lighthouse_sessions_attended = let
  Source = clean_lighthouse_services,
  #"Grouped by client and period" = Table.Group(Source, {"client_id", "nine_week_period"}, {
    {"sessions_attended", each Table.RowCount(_), Int64.Type}
  }),
  // Set types
  #"Changed types" = Table.TransformColumnTypes(#"Grouped by client and period", {
    {"client_id", Int64.Type},
    {"nine_week_period", type text},
    {"sessions_attended", Int64.Type}
  })
in
  #"Changed types";
shared lighthouse_sessions_attended_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "lighthouse_sessions_attended", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
