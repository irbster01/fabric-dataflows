[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared lsndc_tfa = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "lsndc_tfa"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(
      #"Navigation 4",
      "Attributes",
      {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"},
      {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}
  ),
  #"Filtered hidden files" =
      Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" =
      Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" =
      Table.SelectColumns(#"Invoke custom function", {"Date modified", "Transform file"}),
  #"Expanded table column" =
      Table.ExpandTableColumn(
          #"Removed other columns",
          "Transform file",
          Table.ColumnNames(#"Transform file"(#"Sample file"))
      ),
  #"Changed column type" =
      Table.TransformColumnTypes(
          #"Expanded table column",
          {
              {"Client ID", type text},
              {"First Name", type text},
              {"Last Name", type text},
              {"Service Start Date", type date},
              {"SSVF Financial Assistance Amount", type number},
              {"SSVF Financial Assistance Type", type text},
              {"Entry Date", type date},
              {"Exit Date", type date},
              {"Service Group ID", type text},
              {"Service ID", Int64.Type},
              {"Service End Date", type date},
              {"Service Provider", type text},
              {"Service Notes", type text},
              {"Service Provided", type text},
              {"SSVF Financial Assistance End Date", type date},
              {"SSVF Financial Assistance Start Date", type date},
              {"SSVF Other Public Benefits", type text},
              {"SSVF Other Supportive Service", type text},
              {"SSVF Service Type", type text},
              {"SSVF VA Benefits", type text},
              {"Code", type text},
              {"Moving On Other", type text},
              {"Moving On Assistance", type text},
              {"Need ID", Int64.Type},
              {"Service Location", type text},
              {"Active", type text},
              {"Completed Follow Up Date", type text},
              {"Call Record ID", type text},
              {"Client ID_1", Int64.Type},
              {"Date Added", type date},
              {"Date Updated", type date},
              {"Destination Other", type text},
              {"Destination", type text},
              {"Follow Up Made", type text},
              {"Follow Up User", type text},
              {"Group ID", Int64.Type},
              {"HOPWA Financial Assistance Amount", type text},
              {"HOPWA Financial Assistance Type", type text},
              {"HOPWA Service Type", type text},
              {"Household ID", Int64.Type},
              {"HPRP Financial Assistance Type (Retired)", type text},
              {"HPRP Financial Assistance End Date (Retired)", type text},
              {"HPRP Financial Assistance Start Date (Retired)", type text},
              {"HPRP Housing Relocation And Stabilization Service (Retired)", type text},
              {"MAP Call Recordset ID", type text},
              {"PATH Referral Outcome", type text},
              {"PATH Referral Type", type text},
              {"PATH Service Type", type text},
              {"Projected Follow Up Date", type date},
              {"Provider Specific Service", type text},
              {"Provider Creating", type text},
              {"Provider Updating", type text},
              {"Referral Completed Follow Up Date", type text},
              {"Needs Referral Date", type text},
              {"Referral Follow Up Made", type text},
              {"Referral Follow Up User", type text},
              {"Referral Projected Follow Up Date", type text},
              {"Reason Referral Canceled or Declined", type text},
              {"Referral Subgroup ID", type text},
              {"Referred-From Provider", type text},
              {"Referral Outcome", type text},
              {"Referral Ranking", type text},
              {"Referred-To Provider", type text},
              {"RHYMIS Referral Type", type text},
              {"RHYMIS Service Type", type text},
              {"Service Staff", type text},
              {"Shelter Bed", type text},
              {"Shelter Bedlist", type text},
              {"Shelter Behavior Notes", type text},
              {"Shelter Confirmed", type text},
              {"Shelter Exempt from Curfew", type text},
              {"Shelter Entry Exit ID", type text},
              {"Shelter Item", type text},
              {"Shelter Locker No", type text},
              {"Shelter On Premises", type text},
              {"Shelter Overflow Name", type text},
              {"Shelter Reserve Date", type text},
              {"Shelter Supplies Given", type text},
              {"Shelter Supplies Returned", type text},
              {"SSVF Direct Public Benefits", type text},
              {"Subgroup ID", type text},
              {"TAY-VI-SPDAT Call Recordset ID", type text},
              {"User Creating", type text},
              {"User Updating", type text},
              {"Active_2", type text},
              {"Assignable", type text},
              {"Code_3", type text},
              {"Code Date Added", type date},
              {"Code Date Updated", type date},
              {"Code Level", Int64.Type},
              {"Date Added_4", type date},
              {"Date Updated_5", type date},
              {"Description", type text},
              {"Facet", type text},
              {"Has Children", type text},
              {"How Used", type text},
              {"Long Description", type text},
              {"Non Selectable", type text},
              {"Parent Service Code", type text},
              {"Provider Creating_6", type text},
              {"Provider Updating_7", type text},
              {"Number of Providers Using Service Code", Int64.Type},
              {"Service Code ID", Int64.Type},
              {"Service Code Type", type text},
              {"Sort Order", type text},
              {"User Creating_8", type text},
              {"User Updating_9", type text}
          }
      ),
  #"Renamed columns" =
      Table.RenameColumns(
          #"Changed column type",
          {
              {"Client ID", "client_id"},
              {"First Name", "first_name"},
              {"Last Name", "last_name"},
              {"Service Start Date", "service_date"},
              {"SSVF Financial Assistance Amount", "amount"},
              {"SSVF Financial Assistance Type", "assist_type"},
              {"Entry Date", "entry_date"},
              {"Exit Date", "exit_date"}
          }
      ),
  #"Filtered rows" =
      Table.SelectRows(
          #"Renamed columns",
          each [exit_date] >= #date(2025, 1, 1)
            and [exit_date] <= #date(2025, 10, 31)
            or [exit_date] = null
      ),
  #"Removed columns" =
      Table.RemoveColumns(#"Filtered rows", {"Service Group ID"}),
  #"Renamed columns 1" =
      Table.RenameColumns(#"Removed columns", {{"Service ID", "service_id"}}),
  #"Removed columns 1" =
      Table.RemoveColumns(#"Renamed columns 1", {"Service End Date"}),
  #"Renamed columns 2" =
      Table.RenameColumns(
          #"Removed columns 1",
          {
              {"Service Provider", "provider"},
              {"Service Notes", "notes"},
              {"Service Provided", "service_provided"}
          }
      ),
  #"Removed columns 2" =
      Table.RemoveColumns(
          #"Renamed columns 2",
          {
              "SSVF Financial Assistance End Date",
              "SSVF Financial Assistance Start Date",
              "SSVF Other Public Benefits",
              "SSVF Other Supportive Service",
              "SSVF Service Type",
              "SSVF VA Benefits"
          }
      ),
  #"Renamed columns 3" =
      Table.RenameColumns(#"Removed columns 2", {{"Code", "code"}}),
  #"Removed columns 3" =
      Table.RemoveColumns(
          #"Renamed columns 3",
          {
              "Moving On Other",
              "Moving On Assistance",
              "Need ID",
              "Service Location",
              "Active",
              "Completed Follow Up Date",
              "Call Record ID",
              "Client ID_1",
              "Date Added",
              "Date Updated",
              "Destination Other",
              "Destination",
              "Follow Up Made",
              "Follow Up User",
              "Group ID",
              "HOPWA Financial Assistance Amount",
              "HOPWA Financial Assistance Type",
              "HOPWA Service Type",
              "Household ID",
              "HPRP Financial Assistance Type (Retired)",
              "HPRP Financial Assistance End Date (Retired)",
              "HPRP Financial Assistance Start Date (Retired)",
              "HPRP Housing Relocation And Stabilization Service (Retired)",
              "MAP Call Recordset ID",
              "PATH Referral Outcome",
              "PATH Referral Type",
              "PATH Service Type",
              "Projected Follow Up Date",
              "Provider Specific Service",
              "provider"
          }
      ),
  #"Renamed columns 4" =
      Table.RenameColumns(#"Removed columns 3", {{"Provider Creating", "provider"}}),
  #"Removed columns 4" =
      Table.RemoveColumns(
          #"Renamed columns 4",
          {
              "Provider Updating",
              "Referral Completed Follow Up Date",
              "Needs Referral Date",
              "Referral Follow Up Made",
              "Referral Follow Up User",
              "Referral Projected Follow Up Date",
              "Reason Referral Canceled or Declined",
              "Referral Subgroup ID",
              "Referred-From Provider",
              "Referral Outcome",
              "Referral Ranking",
              "Referred-To Provider",
              "RHYMIS Referral Type",
              "RHYMIS Service Type",
              "Service Staff",
              "Shelter Bed",
              "Shelter Bedlist",
              "Shelter Behavior Notes",
              "Shelter Confirmed",
              "Shelter Exempt from Curfew",
              "Shelter Entry Exit ID",
              "Shelter Item",
              "Shelter Locker No",
              "Shelter On Premises",
              "Shelter Overflow Name",
              "Shelter Reserve Date",
              "Shelter Supplies Given",
              "Shelter Supplies Returned",
              "SSVF Direct Public Benefits",
              "User Updating_9",
              "User Creating_8",
              "Sort Order",
              "Service Code Type",
              "Service Code ID",
              "Number of Providers Using Service Code",
              "Provider Updating_7",
              "Provider Creating_6",
              "Non Selectable",
              "How Used",
              "Has Children",
              "Facet",
              "Date Added_4",
              "Date Updated_5",
              "Code Level",
              "Active_2",
              "Assignable",
              "Code_3",
              "Code Date Added",
              "Code Date Updated",
              "TAY-VI-SPDAT Call Recordset ID",
              "Subgroup ID",
              "User Creating",
              "User Updating",
              "Long Description"
          }
      ),
  #"Renamed columns 5" =
      Table.RenameColumns(
          #"Removed columns 4",
          {
              {"Description", "description"},
              {"Parent Service Code", "parent_code"}
          }
      ),
  #"Changed column type 1" =
      Table.TransformColumnTypes(#"Renamed columns 5", {{"amount", Currency.Type}}),
  #"Inserted merged column" =
      Table.AddColumn(
          #"Changed column type 1",
          "client_name",
          each Text.Combine({[last_name], [first_name]}, ", "),
          type text
      ),
  #"Reordered columns" =
      Table.ReorderColumns(
          #"Inserted merged column",
          {
              "Date modified",
              "client_id",
              "client_name",
              "first_name",
              "last_name",
              "service_date",
              "amount",
              "assist_type",
              "entry_date",
              "exit_date",
              "service_id",
              "notes",
              "service_provided",
              "code",
              "provider",
              "description",
              "parent_code"
          }
      ),
  #"Removed columns 5" =
      Table.RemoveColumns(#"Reordered columns", {"first_name", "last_name"}),
  #"Changed column type 2" =
      Table.TransformColumnTypes(#"Removed columns 5", {{"service_id", type text}}),

  // NEW: evaluate [notes] for specific business names / keywords
  #"Added NoteTag" =
      Table.TransformColumnTypes(Table.AddColumn(#"Changed column type 2", "note_tag", each let
                  txt =
                      if [notes] = null
                      then ""
                      else Text.Lower(Text.Trim(Text.From([notes])))
              in
                  if Text.Contains(txt, "swepco") then "SWEPCO"
                  else if Text.Contains(txt, "sterling") then "Sterling Crest One"
                  else if Text.Contains(txt, "jewelers") then "United Jewelers Apartments"
                  else if Text.Contains(txt, "TNT", Comparer.OrdinalIgnoreCase) then "TNT Rental"
                  else if Text.Contains(txt, "DOWUS", Comparer.OrdinalIgnoreCase) then "DOWUS"
                  else if Text.Contains(txt, "mattress", Comparer.OrdinalIgnoreCase) then "Mattress Plus"

                  else if Text.Contains(txt, "Manager", Comparer.OrdinalIgnoreCase) then "My Property Manager"

                  else if Text.Contains(txt, "Utilit", Comparer.OrdinalIgnoreCase) then "SWEPCO"

                  else if Text.Contains(txt, "MAX", Comparer.OrdinalIgnoreCase) then "REMAX"

                  else if Text.Contains(txt, "MAX", Comparer.OrdinalIgnoreCase) then "REMAX"

else if Text.Contains(txt, "Ogilvie", Comparer.OrdinalIgnoreCase) then "Oglvie Hardware"

else if Text.Contains(txt, "TR Managment", Comparer.OrdinalIgnoreCase) then "TR Managment LLC"

else if Text.Contains(txt, "Lighthouse", Comparer.OrdinalIgnoreCase) then "Lighthouse"

else if Text.Contains(txt, "Strongman", Comparer.OrdinalIgnoreCase) then "Strongman Movers"


else if Text.Contains(txt, "Salem", Comparer.OrdinalIgnoreCase) then "Salem Village"

else if Text.Contains(txt, "Ruben", Comparer.OrdinalIgnoreCase) then "Ruben Realty"

else if Text.Contains(txt, "Lifestyle", Comparer.OrdinalIgnoreCase) then "Lifestyle Apartments"
else if Text.Contains(txt, "Phoenix", Comparer.OrdinalIgnoreCase) then "Phoenix-Fairmont"
else if Text.Contains(txt, "mart", Comparer.OrdinalIgnoreCase) then "Walmart"
else if Text.Contains(txt, "Loft", Comparer.OrdinalIgnoreCase) then "Ogilvie Lofts"
else if Text.Contains(txt, "Embassy", Comparer.OrdinalIgnoreCase) then "Embassy"
else if Text.Contains(txt, "water", Comparer.OrdinalIgnoreCase) then "City of Shreveport"
else if Text.Contains(txt, "Northwest Louisiana", Comparer.OrdinalIgnoreCase) then "Northwest Louisiana Properties LLC"



                  else null), {{"note_tag", type text}}),
  #"Renamed columns 6" = Table.RenameColumns(#"Added NoteTag", {{"note_tag", "vendor"}}),

  #"Removed duplicates" =
      Table.Distinct(#"Renamed columns 6", {"service_id"})
in
  #"Removed duplicates";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "LSNDC"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "lsndc_tfa"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
[BindToDefaultDestination = true]
shared #"Transform Sample file" = let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 107, QuoteStyle = QuoteStyle.Csv]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 107, QuoteStyle = QuoteStyle.Csv]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[BindToDefaultDestination = true]
shared hp_client_list = let
  Source = lsndc_tfa,
  #"Removed columns" = Table.RemoveColumns(Source, {"service_date", "amount", "assist_type", "service_id", "notes", "service_provided", "code"}),
  #"Filtered rows" = Table.SelectRows(#"Removed columns", each ([provider] = "VOANLA 3031 - SSVF Homeless Prevention")),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows", {"description", "parent_code"}),
  #"Removed columns 2" = Table.RemoveColumns(#"Removed columns 1", {"vendor"}),
  #"Removed duplicates" = Table.Distinct(#"Removed columns 2", {"client_id", "client_name", "entry_date", "exit_date", "provider"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed duplicates", "VOANLA 3031 - SSVF Homeless Prevention", "HP", Replacer.ReplaceValue, {"provider"})
in
  #"Replaced value";
[BindToDefaultDestination = true]
shared rrh_client_list = let
  Source = lsndc_tfa,
  #"Removed columns" = Table.RemoveColumns(Source, {"service_date", "amount", "assist_type", "service_id", "notes", "service_provided", "code"}),
  #"Filtered rows" = Table.SelectRows(#"Removed columns", each [provider] = "VOANLA 3031 - SSVF Rapid Re-Housing"),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows", {"description", "parent_code"}),
  #"Removed columns 2" = Table.RemoveColumns(#"Removed columns 1", {"vendor"}),
  #"Removed duplicates" = Table.Distinct(#"Removed columns 2", {"client_id", "client_name", "entry_date", "exit_date", "provider"}),
  #"Removed duplicates 1" = Table.Distinct(#"Removed duplicates", {"client_id"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed duplicates 1", "VOANLA 3031 - SSVF Rapid Re-Housing", "RRH", Replacer.ReplaceValue, {"provider"})
in
  #"Replaced value";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data];
[BindToDefaultDestination = true]
shared appended_ssvf_clientList = let
  Source = Table.Combine({hp_client_list, rrh_client_list})
in
  Source;
