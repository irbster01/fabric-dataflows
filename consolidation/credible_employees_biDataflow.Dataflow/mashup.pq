[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "API - Employees_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"API - Employees" = let
  Source = PowerPlatform.Dataflows([]),
  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[workspaceId = "2e078b2f-481a-42df-8fee-6d81b3c2dac1"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[dataflowId = "14444098-6553-4c81-9fbd-a2a48b9ff763"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[entity = "API - Employees", version = ""]}[Data],
  Custom = let
    Source = #"Navigation 4",
    RenameColumns = Table.TransformColumnNames(
        Source,
        each Text.Lower(Text.Replace(_, " ", "_"))
    )
in
    RenameColumns,
  #"Removed columns" = Table.RemoveColumns(Custom, {"text3", "text1", "text2", "text4"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns", {{"effective_dates", type date}, {"license_expire", type date}}),
  #"Sorted rows" = Table.Sort(#"Changed column type", {{"effective_dates", Order.Descending}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Sorted rows", {"is_billing_supervisor", "is_mu_provider", "is_licensed_health_prof", "is_incident_to", "is_nurse", "is_doctor", "is_supervisor", "username"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed columns 1", "13", "6401", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "6", "Adult BH", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "12", "6021", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "5", "Adult BH", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "10", "Supervisor", Replacer.ReplaceValue, {"program_id"}),
  #"Filtered rows" = Table.SelectRows(#"Replaced value 4", each ([role] <> "MASTER")),
  #"Trimmed text" = Table.TransformColumns(#"Filtered rows", {{"npi", each Text.Trim(_), type nullable text}}),
  #"Cleaned text" = Table.TransformColumns(#"Trimmed text", {{"npi", each Text.Clean(_), type nullable text}}),
  #"Extracted text range" = Table.TransformColumns(#"Cleaned text", {{"hire_date", each Text.Middle(_, 0, 10), type text}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Extracted text range", {{"hire_date", type date}}),
  #"Extracted text range 1" = Table.TransformColumns(#"Changed column type 1", {{"termination_date", each Text.Middle(_, 0, 10), type text}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Extracted text range 1", {{"termination_date", type date}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type 2", each ([role] <> ""))

in
  #"Filtered rows 1";
shared #"API - Employees_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "API - Employees", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "license expiration_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"license expiration" = let
  Source = PowerPlatform.Dataflows([]),
  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[workspaceId = "2e078b2f-481a-42df-8fee-6d81b3c2dac1"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[dataflowId = "14444098-6553-4c81-9fbd-a2a48b9ff763"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[entity = "API - Employees", version = ""]}[Data],
  Custom = let
    Source = #"Navigation 4",
    RenameColumns = Table.TransformColumnNames(
        Source,
        each Text.Lower(Text.Replace(_, " ", "_"))
    )
in
    RenameColumns,
  #"Removed columns" = Table.RemoveColumns(Custom, {"text3", "text1", "text2", "text4"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns", {{"effective_dates", type date}, {"license_expire", type date}}),
  #"Sorted rows" = Table.Sort(#"Changed column type", {{"effective_dates", Order.Descending}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Sorted rows", {"is_billing_supervisor", "is_mu_provider", "is_licensed_health_prof", "is_incident_to", "is_nurse", "is_doctor", "is_supervisor", "username"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed columns 1", "13", "6401", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "6", "Adult BH", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "12", "6021", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "5", "Adult BH", Replacer.ReplaceValue, {"program_id"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "10", "Supervisor", Replacer.ReplaceValue, {"program_id"}),
  #"Filtered rows" = Table.SelectRows(#"Replaced value 4", each ([role] <> "MASTER")),
  #"Trimmed text" = Table.TransformColumns(#"Filtered rows", {{"npi", each Text.Trim(_), type nullable text}}),
  #"Cleaned text" = Table.TransformColumns(#"Trimmed text", {{"npi", each Text.Clean(_), type nullable text}}),
  #"Extracted text range" = Table.TransformColumns(#"Cleaned text", {{"hire_date", each Text.Middle(_, 0, 10), type text}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Extracted text range", {{"hire_date", type date}}),
  #"Extracted text range 1" = Table.TransformColumns(#"Changed column type 1", {{"termination_date", each Text.Middle(_, 0, 10), type text}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Extracted text range 1", {{"termination_date", type date}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type 2", each ([role] <> "" and [role] <> "Billing" and [role] <> "Billing Sup")),
  #"Removed columns 2" = Table.RemoveColumns(#"Filtered rows 1", {"email", "employee_status", "client_id", "work_phone", "first_name", "email_office", "internal_team", "mobile_phone", "home_phone", "termination_date"}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Removed columns 2", {{"license_expire", type datetime}}),
  #"Custom 1" = Table.AddColumn(
   #"Changed column type 3",
    "LicenseStatus",
    each 
        if [license_expire] = null then null
        else 
            let
                daysUntilExpire = Duration.Days([license_expire] - DateTime.LocalNow())
            in
                if daysUntilExpire < 0 then "Expired"
                else if daysUntilExpire <= 30 then "Expires within 30 days"
                else if daysUntilExpire <= 90 then "Expires within 90 days"
                else if daysUntilExpire <= 180 then "Expires within 6 months"
                else if daysUntilExpire <= 365 then "Expires within 1 year"
                else "Valid (over 1 year left)"
),
  #"Filtered rows 2" = Table.SelectRows(#"Custom 1", each ([license_expire] <> null)),
  #"Changed column type 4" = Table.TransformColumnTypes(#"Filtered rows 2", {{"LicenseStatus", type text}})

in
  #"Changed column type 4";
shared #"license expiration_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  TableNavigation = Navigation_2{[Id = "credible_license_expiration", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
