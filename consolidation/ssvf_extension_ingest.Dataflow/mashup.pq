[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared lsndc_middleman = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "lsndc_middleman"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Column1", type text}, {"Column2", type text}, {"Column3", type text}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", type text}, {"Column8", type text}, {"Column9", type text}, {"Column10", type text}, {"Column11", type text}, {"Column12", type text}, {"Column13", type text}, {"Column14", type text}, {"Column15", type text}, {"Column16", type text}, {"Column17", type text}, {"Column18", type text}, {"Column19", type text}, {"Column20", type text}, {"Column21", type text}, {"Column22", type text}, {"Column23", type text}, {"Column24", type text}, {"Column25", type text}, {"Column26", type text}, {"Column27", type text}, {"Column28", type text}, {"Column29", type text}, {"Column30", type text}, {"Column31", type text}, {"Column32", type text}, {"Column33", type text}, {"Column34", type text}, {"Column35", type text}, {"Column36", type text}, {"Column37", type text}, {"Column38", type text}, {"Column39", type text}, {"Column40", type text}, {"Column41", type text}, {"Column42", type text}, {"Column43", type text}, {"Column44", type text}, {"Column45", type text}, {"Column46", type text}, {"Column47", type text}, {"Column48", type text}, {"Column49", type text}, {"Column50", type text}, {"Column51", type text}, {"Column52", type text}, {"Column53", type text}, {"Column54", type text}, {"Column55", type text}, {"Column56", type text}, {"Column57", type text}, {"Column58", type text}, {"Column59", type text}, {"Column60", type text}, {"Column61", type text}, {"Column62", type text}, {"Column63", type text}, {"Column64", type text}, {"Column65", type text}, {"Column66", type text}, {"Column67", type text}, {"Column68", type text}, {"Column69", type text}, {"Column70", type text}, {"Column71", type text}, {"Column72", type text}, {"Column73", type text}, {"Column74", type text}, {"Column75", type text}, {"Column76", type text}, {"Column77", type text}, {"Column78", type text}, {"Column79", type text}, {"Column80", type text}, {"Column81", type text}, {"Column82", type text}, {"Column83", type text}, {"Column84", type text}, {"Column85", type text}, {"Column86", type text}, {"Column87", type text}, {"Column88", type text}, {"Column89", type text}, {"Column90", type text}, {"Column91", type text}, {"Column92", type text}, {"Column93", type text}, {"Column94", type text}, {"Column95", type text}, {"Column96", type text}, {"Column97", type text}, {"Column98", type text}, {"Column99", type text}, {"Column100", type text}, {"Column101", type text}, {"Column102", type text}, {"Column103", type text}, {"Column104", type text}, {"Column105", type text}, {"Column106", type text}, {"Column107", type text}, {"Column108", type text}, {"Column109", type text}, {"Column110", type text}, {"Column111", type text}, {"Column112", type text}, {"Column113", type text}, {"Column114", type text}, {"Column115", type text}, {"Column116", type text}, {"Column117", type text}, {"Column118", type text}, {"Column119", type text}, {"Column120", type text}, {"Column121", type text}, {"Column122", type text}, {"Column123", type text}, {"Column124", type text}, {"Column125", type text}, {"Column126", type text}, {"Column127", type text}, {"Column128", type text}, {"Column129", type text}, {"Column130", type text}, {"Column131", type text}, {"Column132", type text}, {"Column133", type text}, {"Column134", type text}, {"Column135", type text}, {"Column136", type text}, {"Column137", type text}, {"Column138", type text}, {"Column139", type text}, {"Column140", type text}, {"Column141", type text}, {"Column142", type text}, {"Column143", type text}, {"Column144", type text}, {"Column145", type text}, {"Column146", type text}, {"Column147", type text}, {"Column148", type text}, {"Column149", type text}, {"Column150", type text}, {"Column151", type text}, {"Column152", type text}, {"Column153", type text}, {"Column154", type text}, {"Column155", type text}, {"Column156", type text}, {"Column157", type text}, {"Column158", type text}, {"Column159", type text}, {"Column160", type text}, {"Column161", type text}, {"Column162", type text}, {"Column163", type text}, {"Column164", type text}, {"Column165", type text}, {"Column166", type text}, {"Column167", type text}, {"Column168", type text}, {"Column169", type text}, {"Column170", type text}, {"Column171", type text}, {"Column172", type text}, {"Column173", type text}, {"Column174", type text}, {"Column175", type text}, {"Column176", type text}, {"Column177", type text}, {"Column178", type text}, {"Column179", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"Column179"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Column178", "cost"}}),
  #"Extracted text before delimiter" = Table.TransformColumns(#"Renamed columns", {{"cost", each Text.BeforeDelimiter(_, "}", 0), type text}}),
  #"Replaced value" = Table.ReplaceValue(#"Extracted text before delimiter", "2025-12-04T16:27:57.917Z", "", Replacer.ReplaceValue, {"cost"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "2025-12-04T16:23:31.314Z", "", Replacer.ReplaceValue, {"cost"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Replaced value 1", {{"cost", Currency.Type}}),
  #"Extracted text before delimiter 1" = Table.TransformColumns(#"Changed column type 1", {{"Column177", each Text.BeforeDelimiter(_, "}", 0), type text}}),
  #"Extracted text before delimiter 2" = Table.TransformColumns(#"Extracted text before delimiter 1", {{"Column177", each Text.BeforeDelimiter(_, ",", 0), type text}}),
  #"Replaced value 2" = Table.ReplaceValue(#"Extracted text before delimiter 2", "35", "", Replacer.ReplaceValue, {"Column177"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "2025-12-04T16:16:09.362Z", "", Replacer.ReplaceValue, {"Column177"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Replaced value 3", {{"Column177", "units"}}),
  #"Replaced value 5" = Table.ReplaceValue(#"Renamed columns 1", "2025-12-04T17:01:53.221Z", "", Replacer.ReplaceValue, {"units"}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Replaced value 5", {{"units", Int64.Type}}),
  #"Renamed columns 2" = Table.RenameColumns(#"Changed column type 2", {{"Column10", "end_month"}, {"Column9", "end_year"}}),
  #"Extracted text before delimiter 3" = Table.TransformColumns(#"Renamed columns 2", {{"end_year", each Text.BeforeDelimiter(_, ",", 0), type text}}),
  #"Extracted text before delimiter 4" = Table.TransformColumns(#"Extracted text before delimiter 3", {{"end_month", each Text.BeforeDelimiter(_, ",", 0), type text}}),
  #"Renamed columns 3" = Table.RenameColumns(#"Extracted text before delimiter 4", {{"Column11", "end_day"}, {"end_year", "start_year"}, {"Column8", "start_day"}, {"Column7", "start_month"}}),
  #"Extracted text before delimiter 5" = Table.TransformColumns(#"Renamed columns 3", {{"start_month", each Text.BeforeDelimiter(_, ",", 0), type text}}),
  #"Extracted text before delimiter 6" = Table.TransformColumns(#"Extracted text before delimiter 5", {{"start_day", each Text.BeforeDelimiter(_, ",", 0), type text}}),
  #"Merged columns" = Table.CombineColumns(#"Extracted text before delimiter 6", {"start_month", "start_day", "start_year"}, Combiner.CombineTextByDelimiter("/", QuoteStyle.None), "start_date"),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Merged columns", {{"start_date", type date}}),
  #"Renamed columns 4" = Table.RenameColumns(#"Changed column type 3", {{"Column12", "end_year"}}),
  #"Extracted text before delimiter 7" = Table.TransformColumns(#"Renamed columns 4", {{"end_day", each Text.BeforeDelimiter(_, ",", 0), type text}, {"end_year", each Text.BeforeDelimiter(_, ",", 0), type text}, {"Column13", each Text.BeforeDelimiter(_, ",", 0), type text}}),
  #"Merged columns 1" = Table.CombineColumns(#"Extracted text before delimiter 7", {"end_month", "end_day", "end_year"}, Combiner.CombineTextByDelimiter("/", QuoteStyle.None), "end_date"),
  #"Replaced value 4" = Table.ReplaceValue(#"Merged columns 1", "//", "", Replacer.ReplaceValue, {"end_date"}),
  #"Changed column type 4" = Table.TransformColumnTypes(#"Replaced value 4", {{"end_date", type date}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Changed column type 4", {"Column3", "Column4", "Column5"}),
  #"Extracted text before delimiter 8" = Table.TransformColumns(#"Removed columns 1", {{"Column6", each Text.BeforeDelimiter(_, ",", 0), type text}}),
  #"Renamed columns 5" = Table.RenameColumns(#"Extracted text before delimiter 8", {{"Column6", "user"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns 5", {"Column2", "Column1"}),
  #"Renamed columns 6" = Table.RenameColumns(#"Removed columns 2", {{"Column176", "notes"}}),
  #"Extracted text before delimiter 9" = Table.TransformColumns(#"Renamed columns 6", {{"notes", each Text.BeforeDelimiter(_, ",", 0), type text}}),
  #"Replaced value 6" = Table.ReplaceValue(#"Extracted text before delimiter 9", "}", "", Replacer.ReplaceValue, {"notes"}),
  #"Renamed columns 7" = Table.RenameColumns(#"Replaced value 6", {{"Column174", "unmet_reason"}}),
  #"Extracted text before delimiter 10" = Table.TransformColumns(#"Renamed columns 7", {{"unmet_reason", each Text.BeforeDelimiter(_, ",", 0), type text}}),
  #"Removed columns 3" = Table.RemoveColumns(#"Extracted text before delimiter 10", {"Column173", "Column28", "Column29", "Column30", "Column31", "Column32", "Column33", "Column34", "Column35", "Column36", "Column37", "Column38", "Column39", "Column40", "Column41", "Column42", "Column43", "Column44", "Column45", "Column46", "Column47", "Column48", "Column49", "Column50", "Column51", "Column52", "Column53", "Column54", "Column55", "Column56", "Column57", "Column58", "Column59", "Column60", "Column61", "Column62", "Column63", "Column64", "Column65", "Column66", "Column67", "Column68", "Column69", "Column70", "Column71", "Column72", "Column73", "Column74", "Column75", "Column76", "Column77", "Column78", "Column79", "Column80", "Column81", "Column82", "Column83", "Column84", "Column85", "Column86", "Column87", "Column88", "Column89", "Column90", "Column91", "Column92", "Column93", "Column94", "Column95", "Column96", "Column97", "Column98", "Column99", "Column100", "Column101", "Column102", "Column103", "Column104", "Column105", "Column106", "Column107", "Column108", "Column109", "Column110", "Column111", "Column112", "Column113", "Column114", "Column115", "Column116", "Column117", "Column118", "Column119", "Column120", "Column121", "Column122", "Column123", "Column124", "Column125", "Column126", "Column127", "Column128", "Column129", "Column130", "Column131", "Column132", "Column133", "Column134", "Column135", "Column136", "Column137", "Column138", "Column139", "Column140", "Column141", "Column142", "Column143", "Column144", "Column145", "Column146", "Column147", "Column148", "Column149", "Column150", "Column151", "Column152", "Column153", "Column154", "Column155", "Column27", "Column26", "Column25", "Column24", "Column23", "Column22", "Column21"})
in
  #"Removed columns 3";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "lsndc_middleman"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Csv.Document(Parameter, [Delimiter = ":", Columns = 179, QuoteStyle = QuoteStyle.None])
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ":", Columns = 179, QuoteStyle = QuoteStyle.None])
in
  Source;
