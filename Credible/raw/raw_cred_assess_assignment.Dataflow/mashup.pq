[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_cred_assess_assignment_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared raw_cred_assess_assignment = let
  Source = Xml.Tables(Web.Contents("https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet?connection=N6wxVSQqPQ90HFOKAHmM!4HkQk7KeoTxbDm6GvoVakMDpuCQwjSxuu7oMphvGM2rafcuIMjlPuAzTvOQJJ2UUQ__&start_date=&end_date=&custom_param1=&custom_param2=&custom_param3=")),
  #"Navigation 1" = Source{1}[Table],
  #"Expanded Table" = Table.ExpandTableColumn(#"Navigation 1", "Table", {"Name", "Table"}, {"Name.1", "Table.1"}),
  #"Expanded Table.1" = Table.ExpandTableColumn(#"Expanded Table", "Table.1", {"Name", "Table"}, {"Name.2", "Table"}),
  #"Expanded Table 1" = Table.ExpandTableColumn(#"Expanded Table.1", "Table", {"Name", "Table"}, {"Name.3", "Table.1"}),
  #"Expanded Table.1 2" = Table.ExpandTableColumn(#"Expanded Table 1", "Table.1", {"clientvisit_id", "font_colornavyprimary_insuranc_594556", "font_colornavytype_of_assessme_594557", "font_colornavylmhp_assignedfon_594558", "font_colorredauthorization_due_594560", "font_colorredauthorization_exp_594559", "clientvisit_id1", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "font_colorgreenreassessmentrea_594886", "font_colorgreencompletion_date_594563", "font_colorpurpleissues_impacti_594561", "font_colorpurpleadditional_not_594565", "font_colorpurpleadditional_not_594565_notes", "font_colorpurpleissues_impacti_594561_notes", "font_colorpurpleoutcome_of_ref_594746", "font_colornavyreferral_datefon_594566"}, {"clientvisit_id", "font_colornavyprimary_insuranc_594556", "font_colornavytype_of_assessme_594557", "font_colornavylmhp_assignedfon_594558", "font_colorredauthorization_due_594560", "font_colorredauthorization_exp_594559", "clientvisit_id1", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "font_colorgreenreassessmentrea_594886", "font_colorgreencompletion_date_594563", "font_colorpurpleissues_impacti_594561", "font_colorpurpleadditional_not_594565", "font_colorpurpleadditional_not_594565_notes", "font_colorpurpleissues_impacti_594561_notes", "font_colorpurpleoutcome_of_ref_594746", "font_colornavyreferral_datefon_594566"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Table.1 2", {"Name", "Name.1", "Name.2", "Name.3", "urn:schemas-microsoft-com:xml-diffgram-v1", "urn:schemas-microsoft-com:xml-msdata", "clientvisit_id1"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"clientvisit_id", "Service ID"}, {"font_colornavytype_of_assessme_594557", "Assess Type"}, {"font_colornavylmhp_assignedfon_594558", "LMHP Assigned"}, {"font_colorredauthorization_due_594560", "Auth due"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"Auth due", type date}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type", {{"Service ID", "service_id"}, {"font_colornavyprimary_insuranc_594556", "primary_insurance"}, {"Assess Type", "assess_type"}, {"LMHP Assigned", "lmhp_assigned"}, {"Auth due", "auth_due"}, {"font_colorredauthorization_exp_594559", "auth_exp"}, {"font_colorgreenreassessmentrea_594886", "assess_reason"}, {"font_colorgreencompletion_date_594563", "completion_date"}, {"font_colorpurpleissues_impacti_594561", "issues_impacting"}, {"font_colorpurpleadditional_not_594565_notes", "notes"}, {"font_colorpurpleissues_impacti_594561_notes", "impact_notes"}, {"font_colorpurpleoutcome_of_ref_594746", "ref_outcome"}}),
  #"Transform columns" = Table.TransformColumnTypes(#"Renamed columns 1", {{"service_id", type text}, {"primary_insurance", type text}, {"assess_type", type text}, {"lmhp_assigned", type text}, {"auth_exp", type text}, {"assess_reason", type text}, {"completion_date", type text}, {"issues_impacting", type text}, {"font_colorpurpleadditional_not_594565", type text}, {"notes", type text}, {"impact_notes", type text}, {"ref_outcome", type text}, {"font_colornavyreferral_datefon_594566", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"service_id", null}, {"primary_insurance", null}, {"assess_type", null}, {"lmhp_assigned", null}, {"auth_exp", null}, {"assess_reason", null}, {"completion_date", null}, {"issues_impacting", null}, {"font_colorpurpleadditional_not_594565", null}, {"notes", null}, {"impact_notes", null}, {"ref_outcome", null}, {"font_colornavyreferral_datefon_594566", null}}),
  #"Merged queries" = Table.NestedJoin(#"Replace errors", {"service_id"}, raw_credible_services, {"service_id"}, "raw_credible_services", JoinKind.Inner),
  #"Expanded raw_credible_services" = Table.ExpandTableColumn(#"Merged queries", "raw_credible_services", {"service_date"}, {"service_date"}),
  #"Sorted rows" = Table.Sort(#"Expanded raw_credible_services", {{"service_date", Order.Descending}})
in
  #"Sorted rows";
shared raw_cred_assess_assignment_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_cred_assess_assignment", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared raw_credible_services = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "74be8ffb-fa88-4278-9a30-358e88a4f13a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credible_services", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
