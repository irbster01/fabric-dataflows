[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "lighthouse_dates_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared lighthouse_dates = let
    // calendar bounds
    StartDate = #date(2025,8,30),
    EndDate   = #date(2026,5,31),

    // blackout/excluded dates (your list)
    ExcludedDates = {
      #date(2025,10,6), #date(2025,10,7), #date(2025,10,8), #date(2025,10,9), #date(2025,10,10),
      #date(2025,11,24), #date(2025,11,25), #date(2025,11,26), #date(2025,11,27), #date(2025,11,28),
      #date(2025,12,18), #date(2025,12,19), #date(2025,12,20), #date(2025,12,21), #date(2025,12,22),
      #date(2025,12,23), #date(2025,12,24), #date(2025,12,25), #date(2025,12,26), #date(2025,12,27),
      #date(2025,12,28), #date(2025,12,29), #date(2025,12,30), #date(2025,12,31),
      #date(2026,1,1), #date(2026,1,2),
      #date(2026,2,13), #date(2026,2,14), #date(2026,2,15), #date(2026,2,16),
      #date(2026,3,9),  #date(2026,3,10), #date(2026,3,11), #date(2026,3,12), #date(2026,3,13),
      #date(2026,4,3),  #date(2026,4,4),  #date(2026,4,5),  #date(2026,4,6)
    },

    // anchors you asked for (start-of-count dates)
    AnchorSept2 = #date(2025,9,2),
    AnchorSept8 = #date(2025,9,8),
    AnchorSept9 = #date(2025,9,9),

    // weekday mapping: use Day.Monday so Mon=0, Tue=1, Wed=2, Thu=3
    AllowedWeekdays = {1,2,3}, // Tue/Wed/Thu

    // full date list
    DaysTotal = Duration.Days(EndDate - StartDate) + 1,
    DateList = List.Dates(StartDate, DaysTotal, #duration(1,0,0,0)),

    // precompute the list of session dates (Tue/Wed/Thu minus exclusions) for efficient reuse
    SessionDates = List.Select(DateList, (d) => (List.Contains(AllowedWeekdays, Date.DayOfWeek(d, Day.Monday)) and not List.Contains(ExcludedDates, d))),

    // make a table of every date and add flags
    DateTable = Table.FromList(DateList, Splitter.SplitByNothing(), {"Date"}, null, ExtraValues.Error),
    #"Added Columns" = Table.TransformColumns(DateTable, {{"Date", each _, type date}}),

    #"Added WeekdayNum" = Table.AddColumn(#"Added Columns", "WeekdayNum", each Date.DayOfWeek([Date], Day.Monday), Int64.Type),
    #"Added Weekday" = Table.AddColumn(#"Added WeekdayNum", "Weekday", each Date.ToText([Date], "dddd"), type text),

    //"Is it Tue/Wed/Thu?"
    #"Added IsTueWedThu" = Table.AddColumn(#"Added Weekday", "IsTueWedThu", each List.Contains(AllowedWeekdays, [WeekdayNum]), type logical),

    // "Is it on the exclusion list?"
    #"Added IsExcluded" = Table.AddColumn(#"Added IsTueWedThu", "IsExcluded", each List.Contains(ExcludedDates, [Date]), type logical),

    // "Is session day" = Tue/Wed/Thu and not excluded
    #"Added IsSessionDay" = Table.AddColumn(#"Added IsExcluded", "IsSessionDay", each ([IsTueWedThu] and (not [IsExcluded])), type logical),

    // Running/cumulative counts up to and including current date, starting at each anchor.
    // We use the precomputed SessionDates list and count those <= current date and >= anchor.
    AddCumCounts = Table.AddColumn(#"Added IsSessionDay", "CumFromSept2", each
        let cur = [Date]
        in List.Count(List.Select(SessionDates, (d) => d >= AnchorSept2 and d <= cur)), Int64.Type),

    AddCumCounts2 = Table.AddColumn(AddCumCounts, "CumFromSept8", each
        let cur = [Date]
        in List.Count(List.Select(SessionDates, (d) => d >= AnchorSept8 and d <= cur)), Int64.Type),

    AddCumCounts3 = Table.AddColumn(AddCumCounts2, "CumFromSept9", each
        let cur = [Date]
        in List.Count(List.Select(SessionDates, (d) => d >= AnchorSept9 and d <= cur)), Int64.Type),

    // Optional: cumulative count from the start of the calendar (Aug 30)
    AddCumFromStart = Table.AddColumn(AddCumCounts3, "CumFromStart", each
        let cur = [Date]
        in List.Count(List.Select(SessionDates, (d) => d >= StartDate and d <= cur)), Int64.Type),

    // Clean up columns ordering
    #"Reordered Columns" = Table.ReorderColumns(AddCumFromStart,
        {"Date","Weekday","WeekdayNum","IsTueWedThu","IsExcluded","IsSessionDay","CumFromStart","CumFromSept2","CumFromSept8","CumFromSept9"}),

    // Optional: keep only session rows (if you prefer a calendar of only possible dates uncomment next line)
    // #"Filtered Only Sessions" = Table.SelectRows(#"Reordered Columns", each [IsSessionDay] = true)
    Result = #"Reordered Columns",
  #"Sorted rows" = Table.Sort(Result, {{"Date", Order.Descending}}),
  #"Renamed columns" = Table.RenameColumns(#"Sorted rows", {{"Date", "date"}, {"Weekday", "weekday"}, {"WeekdayNum", "weekday_num"}, {"CumFromStart", "cume_8_30"}, {"CumFromSept2", "cum_sept_2"}, {"CumFromSept8", "cum_sept_8"}, {"CumFromSept9", "cum_sept_9"}}),
  #"Inserted start of month" = Table.AddColumn(#"Renamed columns", "Start of month", each Date.StartOfMonth([date]), type date),
  #"Renamed columns 1" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "month_start"}}),
  #"Inserted month name" = Table.AddColumn(#"Renamed columns 1", "Month name", each Date.MonthName([month_start]), type text),
  #"Renamed columns 2" = Table.RenameColumns(#"Inserted month name", {{"Month name", "month_name"}}),
  #"Added nine_week_period" = Table.AddColumn(#"Renamed columns 2", "nine_week_period", each 
    if [date] >= #date(2025, 8, 13) and [date] <= #date(2025, 10, 17) then "1st 9 Weeks"
    else if [date] >= #date(2025, 10, 20) and [date] <= #date(2025, 12, 17) then "2nd 9 Weeks"
    else if [date] >= #date(2026, 1, 6) and [date] <= #date(2026, 3, 6) then "3rd 9 Weeks"
    else if [date] >= #date(2026, 3, 16) and [date] <= #date(2026, 5, 21) then "4th 9 Weeks"
    else null, type text),
  #"Removed duplicates" = Table.Distinct(#"Added nine_week_period", {"date"})
in
    #"Removed duplicates";
shared lighthouse_dates_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"]}[Data],
  TableNavigation = Navigation_2{[Id = "lighthouse_dates", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
