[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw-netsuite-fullregister_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"raw-netsuite-fullregister" = let
    Source = Web.Contents("https://9060623.app.netsuite.com/app/reporting/webquery.nl?compid=9060623&entity=24720&email=Russell.irby@voanorthla.org&role=1180&cr=466&hash=AAEJ7tMQ9LNM48XAqY0QZbdykzesLLdGPMIrK60A0emARMNbrsc
"),
    #"Extracted Table From Html" = Html.Table(Source, {{"Column1", "TABLE > * > TR > :nth-child(1)"}, {"Column2", "TABLE > * > TR > :nth-child(2)"}, {"Column3", "TABLE > * > TR > :nth-child(3)"}, {"Column4", "TABLE > * > TR > :nth-child(4)"}, {"Column5", "TABLE > * > TR > :nth-child(5)"}, {"Column6", "TABLE > * > TR > :nth-child(6)"}, {"Column7", "TABLE > * > TR > :nth-child(7)"}, {"Column8", "TABLE > * > TR > :nth-child(8)"}, {"Column9", "TABLE > * > TR > :nth-child(9)"}, {"Column10", "TABLE > * > TR > :nth-child(10)"}, {"Column11", "TABLE > * > TR > :nth-child(11)"}, {"Column12", "TABLE > * > TR > :nth-child(12)"}}, [RowSelector="TABLE > * > TR"]),
  #"Promoted headers" = Table.PromoteHeaders(#"Extracted Table From Html", [PromoteAllScalars = true])
in
    #"Promoted headers";
shared #"raw-netsuite-fullregister_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw-netsuite-fullregister", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
