[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared Query = let
  // --- Request ---
  BaseUrl = "https://9060623.app.netsuite.com/app/reporting/webquery.nl",
  Source0 =
    Web.Contents(
      BaseUrl,
      [
        Query = [
          compid = "9060623",
          entity = "24720",
          email  = "Russell.irby@voanorthla.org",
          role   = "1180",
          cr     = "481",
          hash   = "AAEJ7tMQJkvGSS-HfIijLh6Cfs596PNQ5OBXbaWmJdfKnBRDrTI"
        ],
        Headers = [
          #"User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
          Accept = "text/html"
        ]
      ]
    ),

  HtmlText = Text.FromBinary(Source0, TextEncoding.Utf8),

  // --- Sanity check for auth/blocked HTML ---
  Probe = Text.Upper(Text.Start(HtmlText, 2000)),
  _ = if Text.Contains(Probe, "LOGIN") or Text.Contains(Probe, "SIGN IN") or Text.Contains(Probe, "UNAUTHORIZED")
      then error "NetSuite returned a login/blocked page to the service. The webquery link must be tokenized and accessible without interactive sign-in."
      else null,

  // --- Parse table: prefer <tbody>, fallback to bare <tr> ---
  Columns = {
    {"Account",   "td:nth-child(1)"},
    {"Date",      "td:nth-child(2)"},
    {"Memo",      "td:nth-child(3)"},
    {"DistMemo",  "td:nth-child(4)"},
    {"VendorMsg", "td:nth-child(5)"},
    {"Note1",     "td:nth-child(6)"},
    {"Note2",     "td:nth-child(7)"},
    {"DocNum",    "td:nth-child(8)"},
    {"Vendor",    "td:nth-child(9)"},
    {"Status",    "td:nth-child(10)"}
  },

  TryTbody = Html.Table(HtmlText, Columns, [RowSelector = "table tbody tr"]),
  Parsed   = if Table.RowCount(TryTbody) > 0
             then TryTbody
             else Html.Table(HtmlText, Columns, [RowSelector = "table tr"]),

  // Remove empty/header rows
  NonEmpty =
    Table.SelectRows(
      Parsed,
      each List.NonNullCount(Record.FieldValues(_)) > 0
    ),
  #"Promoted headers" = Table.PromoteHeaders(NonEmpty, [PromoteAllScalars = true]),

  // Optional: basic typing
  WithTypes = Table.TransformColumnTypes(#"Promoted headers", {{"Date", type date}}),
  #"Renamed columns" = Table.RenameColumns(WithTypes, {{"Account", "account"}, {"Date", "date"}, {"Memo", "memo"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"Dist. Related Journal: Memo", "Dist. Related Journal: Vendor Message", "Custodial Client: Note 1", "Custodial Client: Note 2"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"Document Number", "doc_num"}, {"Vendor", "vendor"}, {"Status", "status"}}),
  #"Inserted start of month" = Table.AddColumn(#"Renamed columns 1", "Start of month", each Date.StartOfMonth([date]), type nullable date),
  #"Renamed columns 2" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "month"}}),
  #"Inserted conditional column" = Table.AddColumn(#"Renamed columns 2", "Custom", each if Text.Contains([memo], "McAdoo") then 1 else if Text.Contains([memo], "Mcadoo") then 1 else if Text.Contains([memo], "1002") then 1 else if Text.Contains([memo], "3504") then 1 else if Text.Contains([account], "304") then 1 else ""),
  #"Filtered rows" = Table.SelectRows(#"Inserted conditional column", each Text.Contains([memo], "AC") or Text.Contains([memo], "A/C"))
in
  #"Filtered rows";
