[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared needs_assessment = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "needs_assessment"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Total number of years working with youth", type text}, {"Highest level of education completed:", type text}, {"CIS Organization Name", type text}, {"Relationship skills", type text}, {"Communication", type text}, {"Time management", type text}, {"Problem-solving", type text}, {"Conflict resolution", type text}, {"Leadership", type text}, {"Adaptability", type text}, {"Empathy", type text}, {"Relationship skills2", type text}, {"Communication3", type text}, {"Time management4", type text}, {"Problem-solving5", type text}, {"Conflict resolution6", type text}, {"Leadership7", type text}, {"Adaptability8", type text}, {"Empathy9", type text}, {"Conducting a school needs assessment", type text}, {"Creating a school support plan", type text}, {"Setting school goals", type text}, {"Creating a school support team", type text}, {"Facilitating school support team meetings", type text}, {"Identifying students in need of case-management", type text}, {"Conducting a student needs assessment", type text}, {"Creating a student support plan", type text}, {"Setting student goals", type text}, {"Identifying tiered supports aligned with school and individual student needs", type text}, {"Implementing Tier 1 (schoolwide/systems) supports", type text}, {"Implementing Tier 2 (small group) supports", type text}, {"Implementing Tier 3 (individualized) supports", type text}, {"Connecting with partners/other service providers to broker supports to students and the school", type text}, {"Implementing school support plan", type text}, {"Implementing student support plans", type text}, {"Using data to monitor and make adjustments to student supports", type text}, {"Assessing progress towards goals", type text}, {"Working with school staff and leadership", type text}, {"Building relationships with students and families", type text}, {"Partnership development", type text}, {"Developmental Relationships", type text}, {"Trauma-Informed Care", type text}, {"Youth mental health", type text}, {"Adverse Childhood Experiences (ACEs)", type text}, {"Restorative Practices", type text}, {"Creating a safe and welcoming school environment", type text}, {"Ensuring fair and respectful treatment of students", type text}, {"Fostering belonging and student engagement", type text}, {"Data-driven decision making", type text}, {"Partnership development10", type text}, {"Developmental Relationships11", type text}, {"Trauma-Informed Care12", type text}, {"Youth mental health13", type text}, {"Adverse Childhood Experiences (ACEs)14", type text}, {"Restorative Practices15", type text}, {"Creating a safe and welcoming school environment16", type text}, {"Ensuring fair and respectful treatment of students17", type text}, {"Fostering belonging and student engagement18", type text}, {"Data-driven decision making19", type text}, {"Attendance", type text}, {"Behavior", type text}, {"Coursework/Academics", type text}, {"Life Skills", type text}, {"College and Career Readiness (CCR)", type text}, {"Family Engagement", type text}, {"School Climate", type text}, {"Live, instructor-led in person", type text}, {"Live, instructor-led virtual", type text}, {"Self-paced, on your own time", type text}, {"Hybrid", type text}, {"I see something in action", type text}, {"I hear someone explain it", type text}, {"I have time to read and process", type text}, {"I have time to practice", type text}, {"Before school year begins", type text}, {"Towards the end of school year", type text}, {"During summer break", type text}, {"Throughout the school year, as needed", type text}, {"Response", type text}, {"Morning", type text}, {"Afternoon", type text}, {"Evening", type text}, {"Response20", type text}, {"Response21", type text}, {"Professional development funds", type text}, {"Dedicated time for training and learning needs", type text}, {"Information about training opportunities", type text}, {"Support from your supervisor", type text}, {"Support from your organization", type text}, {"Mentors/coaches", type text}, {"Professional learning communities", type text}, {"Onboarding training and resources for site coordinators - Spanish", type text}, {"Onboarding training and resources for site coordinators - Not needed", type text}, {"CIS National learning events (live interpretation) - Spanish", type text}, {"CIS National learning events (live interpretation) - Other (please specify)", type text}, {"CIS National learning events (live interpretation) - Not needed", type text}, {"Student-facing resources - Spanish", type text}, {"Student-facing resources - Other (please specify)", type text}, {"Student-facing resources - Not needed", type text}, {"Family-facing resources - Spanish", type text}, {"Family-facing resources - Not needed", type text}, {"Open-Ended Response", type text}, {"In-person conference", type text}, {"Live virtual workshop (interactive)", type text}, {"Resource hub", type text}, {"One-time training/discussion", type text}, {"Ongoing topical convening", type text}, {"Ongoing informal conversation(s)", type text}, {"Project based leadership experience/work group", type text}, {"Webinar/live virtual presentation (noninteractive)", type text}, {"Short learning videos (TikTok style or YouTube shorts format)", type text}, {"Podcast", type text}, {"Other (please specify)", type text}, {"I know how to access training resources from CIS National.", type text}, {"I know how to access training resources from my organization.", type text}, {"CIS Community", type text}, {"CIS University (CISU)", type text}, {"National Resource Center (NRC)", type text}, {"CIS Connect", type text}, {"Learning & Practice Monthly Newsletter", type text}, {"Community Briefing Newsletter", type text}, {"CIS Events Calendar", type text}, {"CIS Community22", type text}, {"CIS University (CISU)23", type text}, {"National Resource Center (NRC)24", type text}, {"CIS Connect25", type text}, {"Learning & Practice Monthly Newsletter26", type text}, {"Community Briefing Newsletter27", type text}, {"CIS Events Calendar28", type text}, {"Response29", type text}, {"Response31", type text}, {"Response33", type text}, {"Response35", type text}, {"Open-Ended Response36", type text}, {"Equipping students to meet their goals", type text}, {"Serving families and meeting their needs", type text}, {"Ensuring equitable access to supports and resources", type text}, {"Impacting school climate", type text}, {"Meeting required caseload", type text}, {"Meeting schoolwide goals", type text}, {"Ensuring integration and coordination of supports across providers within the school", type text}, {"Ensuring the work of CIS is integrated with the broader system(s) of support within the school", type text}, {"Other (please describe)", type text}, {"Response37", type text}, {"Response38", type text}, {"Limited time", type text}, {"Burnout", type text}, {"Compassion fatigue", type text}, {"Difficulty disconnecting from work", type text}, {"High caseload or workload demands", type text}, {"Rigid schedules or limited flexibility during the school day", type text}, {"Difficulty prioritizing personal needs", type text}, {"Financial strain (such as cost of care or cost of living)", type text}, {"Limited access to mental health supports", type text}, {"Other (please specify)39", type text}, {"Open-Ended Response40", type text}, {"Open-Ended Response41", type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"Relationship skills", "Pro Strength: Relationship skills"}, {"Communication", "Pro Strength: Communication"}, {"Time management", "Pro Strength: Time Management"}, {"Problem-solving", "Pro Strength: Problem-solving"}, {"Conflict resolution", "Pro Strength: Conflict resolution"}, {"Leadership", "Pro Strength: Leadership"}, {"Adaptability", "Pro Strength: Adaptability"}, {"Empathy", "Pro Strength: Empathy"}, {"Relationship skills2", "Pro Weakness: Relationship skills2"}, {"Communication3", "Pro Weakness: Communication3"}, {"Time management4", "Pro Weakness: Time management4"}, {"Problem-solving5", "Pro Weakness: Problem-solving5"}, {"Conflict resolution6", "Pro Weakness: Conflict resolution6"}, {"Leadership7", "Pro Weakness: Leadership"}, {"Adaptability8", "Pro Weakness: Adaptability"}, {"Empathy9", "Pro Weakness: Empathy"}, {"Conducting a school needs assessment", "Confidence: Conducting a school needs assessment"}, {"Creating a school support plan", "Confidence: Creating a school support plan"}, {"Setting school goals", "Confidence: Setting school goals"}, {"Creating a school support team", "Confidence: Creating a school support team"}, {"Facilitating school support team meetings", "Confidence: Facilitating school support team meetings"}, {"Identifying students in need of case-management", "Confidence: Identifying students in need of case-management"}, {"Conducting a student needs assessment", "Confidence: Conducting a student needs assessment"}, {"Creating a student support plan", "Confidence: Creating a student support plan"}, {"Setting student goals", "Confidence: Setting student goals"}, {"Identifying tiered supports aligned with school and individual student needs", "Confidence: Identifying tiered supports aligned with school and individual student needs"}, {"Implementing Tier 1 (schoolwide/systems) supports", "Confidence: Implementing Tier 1 (schoolwide/systems) supports"}, {"Implementing Tier 2 (small group) supports", "Confidence: Implementing Tier 2 (small group) supports"}, {"Implementing Tier 3 (individualized) supports", "Confidence: Implementing Tier 3 (individualized) supports"}, {"Connecting with partners/other service providers to broker supports to students and the school", "Confidence: Connecting with partners/other service providers to broker supports to students and the school"}, {"Implementing school support plan", "Confidence: Implementing school support plan"}, {"Implementing student support plans", "Confidence: Implementing student support plans"}, {"Using data to monitor and make adjustments to student supports", "Confidence: Using data to monitor and make adjustments to student supports"}, {"Assessing progress towards goals", "Confidence: Assessing progress towards goals"}, {"Working with school staff and leadership", "Confidence: Working with school staff and leadership"}, {"Building relationships with students and families", "Confidence: Building relationships with students and families"}, {"Partnership development", "Knowledge: Partnership development"}, {"Developmental Relationships", "Knowledge: Developmental Relationships"}, {"Trauma-Informed Care", "Knowlege: Trauma-Informed Care"}, {"Youth mental health", "Knowledge: Youth mental health"}, {"Adverse Childhood Experiences (ACEs)", "Knowledge: Adverse Childhood Experiences (ACEs)"}, {"Restorative Practices", "Knowledge: Restorative Practices"}, {"Creating a safe and welcoming school environment", "Knowledge: Creating a safe and welcoming school environment"}, {"Ensuring fair and respectful treatment of students", "Knowledge: Ensuring fair and respectful treatment of students"}, {"Fostering belonging and student engagement", "Knowledge: Fostering belonging and student engagement"}, {"Data-driven decision making", "Knowledge: Data-driven decision making"}, {"Partnership development10", "Relevance: Partnership development10"}, {"Developmental Relationships11", "Relevance: Developmental Relationships11"}, {"Trauma-Informed Care12", "Relevance: Trauma-Informed Care12"}, {"Youth mental health13", "Relevance: Youth mental health13"}, {"Adverse Childhood Experiences (ACEs)14", "Relevance: Adverse Childhood Experiences (ACEs)14"}, {"Restorative Practices15", "Relevance: Restorative Practices15"}, {"Creating a safe and welcoming school environment16", "Relevance: Creating a safe and welcoming school environment16"}, {"Ensuring fair and respectful treatment of students17", "Relevance: Ensuring fair and respectful treatment of students17"}, {"Fostering belonging and student engagement18", "Relevance: Fostering belonging and student engagement18"}, {"Data-driven decision making19", "Relevance: Data-driven decision making19"}, {"Attendance", "Confdence: Attendance"}, {"Behavior", "Confidence: Behavior"}, {"Coursework/Academics", "Confidence: Coursework/Academics"}, {"Life Skills", "Confidence: Life Skills"}, {"College and Career Readiness (CCR)", "Confidence: College and Career Readiness (CCR)"}, {"Family Engagement", "Confidence: Family Engagement"}, {"School Climate", "Confidence: School Climate"}, {"Live, instructor-led in person", "Learning: Live, instructor-led in person"}, {"Live, instructor-led virtual", "Learning: Live, instructor-led virtual"}, {"Self-paced, on your own time", "Learning: Self-paced, on your own time"}, {"Hybrid", "Learning: Hybrid"}, {"I see something in action", "Learn Best: I see something in action"}, {"I hear someone explain it", "Learn Best: I hear someone explain it"}, {"I have time to read and process", "Learn Best: I have time to read and process"}, {"I have time to practice", "Learn Best: I have time to practice"}, {"Before school year begins", "When: Before school year begins"}, {"Towards the end of school year", "When: Towards the end of school year"}, {"During summer break", "When: During summer break"}, {"Throughout the school year, as needed", "When: Throughout the school year, as needed"}, {"Response", "When: Frequency Response"}, {"Morning", "Training: Time of Day Morning"}, {"Afternoon", "Training: Time of Day Afternoon"}, {"Evening", "Training: Time of Day Evening"}, {"Response20", "Training : Length"}, {"Response21", "Training: Total length"}, {"Professional development funds", "Access: Professional development funds"}, {"Dedicated time for training and learning needs", "Access: Dedicated time for training and learning needs"}, {"Information about training opportunities", "Access: Information about training opportunities"}, {"Support from your supervisor", "Access: Support from your supervisor"}, {"Support from your organization", "Access: Support from your organization"}, {"Mentors/coaches", "Access: Mentors/coaches"}, {"Professional learning communities", "Access: Professional learning communities"}, {"Onboarding training and resources for site coordinators - Spanish", "ESL: Onboarding training and resources for site coordinators - Spanish"}, {"Onboarding training and resources for site coordinators - Not needed", "ESL: Onboarding training and resources for site coordinators - Not needed"}, {"CIS National learning events (live interpretation) - Spanish", "ESL: CIS National learning events (live interpretation) - Spanish"}, {"CIS National learning events (live interpretation) - Other (please specify)", "ESL: CIS National learning events (live interpretation) - Other (please specify)"}, {"CIS National learning events (live interpretation) - Not needed", "ESL: CIS National learning events (live interpretation) - Not needed"}, {"Student-facing resources - Spanish", "ESL: Student-facing resources - Spanish"}, {"Student-facing resources - Other (please specify)", "ESL: Student-facing resources - Other (please specify)"}, {"Student-facing resources - Not needed", "ESL: Student-facing resources - Not needed"}, {"Family-facing resources - Spanish", "ESL: Family-facing resources - Spanish"}, {"Family-facing resources - Other (please specify)", "ESL: Family-facing resources - Other (please specify)"}, {"Family-facing resources - Not needed", "ESL: Family-facing resources - Not needed"}, {"Open-Ended Response", "ESL: Open-Ended Response"}, {"In-person conference", "Likely to Participate: In-person conference"}, {"Live virtual workshop (interactive)", "Likely to Participate: Live virtual workshop (interactive)"}, {"Resource hub", "Likely to Participate: Resource hub"}, {"One-time training/discussion", "Likely to Participate: Likely to Participate: One-time training/discussion"}, {"Ongoing topical convening", "Likely to Participate: Ongoing topical convening"}, {"Ongoing informal conversation(s)", "Likely to Participate: Ongoing informal conversation(s)"}, {"Project based leadership experience/work group", "Likely to Participate: Project based leadership experience/work group"}, {"Webinar/live virtual presentation (noninteractive)", "Likely to Participate: Webinar/live virtual presentation (noninteractive)"}, {"Short learning videos (TikTok style or YouTube shorts format)", "Likely to Participate: Short learning videos (TikTok style or YouTube shorts format)"}, {"Podcast", "Likely to Participate: Podcast"}, {"Other (please specify)", "Likely to Participate: Other (please specify)"}, {"I know how to access training resources from CIS National.", "Agree Level: I know how to access training resources from CIS National."}, {"I know how to access training resources from my organization.", "Agree Level: I know how to access training resources from my organization."}, {"CIS Community", "Help: CIS Community"}, {"CIS University (CISU)", "Help: CIS University (CISU)"}, {"National Resource Center (NRC)", "Help: National Resource Center (NRC)"}, {"CIS Connect", "Help: CIS Connect"}, {"Learning & Practice Monthly Newsletter", "Help: Learning & Practice Monthly Newsletter"}, {"Community Briefing Newsletter", "Help: Community Briefing Newsletter"}, {"CIS Events Calendar", "Help: CIS Events Calendar"}, {"CIS Community22", "Often: CIS Community22"}, {"CIS University (CISU)23", "Often: CIS University (CISU)23"}, {"National Resource Center (NRC)24", "Often: National Resource Center (NRC)24"}, {"CIS Connect25", "Often: CIS Connect25"}, {"Learning & Practice Monthly Newsletter26", "Often: Learning & Practice Monthly Newsletter26"}, {"Community Briefing Newsletter27", "Often: Community Briefing Newsletter27"}, {"CIS Events Calendar28", "Often: CIS Events Calendar28"}, {"Ease of Access", "Learning Rating: Ease of Access"}, {"Content Relevance", "Learning Rating: Content Relevance"}, {"Subject Matter Experts/Trainers", "Learning Rating: Subject Matter Experts/Trainers"}, {"Time Commitment", "Learning Rating: Time Commitment"}, {"Response29", "Organization Priority"}, {"Open-Ended Response30", "Orginization Priority Open Response"}, {"Response31", "Learning Importance"}, {"Open-Ended Response32", "Learning Importance Open Ended Response"}, {"Response33", "Shared Learning Experiences"}, {"Open-Ended Response34", "Shared Learning Experiences Open-Ended Response"}, {"Response35", "Learning Needs Met"}, {"Open-Ended Response36", "Learning Needs Met Open-Ended Response"}, {"Equipping students to meet their goals", "Define Success: Equipping students to meet their goals"}, {"Serving families and meeting their needs", "Define Success: Serving families and meeting their needs"}, {"Ensuring equitable access to supports and resources", "Define Success: Ensuring equitable access to supports and resources"}, {"Impacting school climate", "Define Success: Impacting school climate"}, {"Meeting required caseload", "Define Success: Meeting required caseload"}, {"Meeting schoolwide goals", "Define Success: Meeting schoolwide goals"}, {"Ensuring integration and coordination of supports across providers within the school", "Define Success: Ensuring integration and coordination of supports across providers within the school"}, {"Ensuring the work of CIS is integrated with the broader system(s) of support within the school", "Define Success: Ensuring the work of CIS is integrated with the broader system(s) of support within the school"}, {"Other (please describe)", "Define Success: Other (please describe)"}, {"Response37", "Self care: How often"}, {"Response38", "Self Care: Interest in Resources"}, {"Limited time", "Self Care Barrier: Limited time"}, {"Burnout", "Self Care Barrier: Burnout"}, {"Secondary or vicarious trauma", "Self Care Barrier: Secondary or vicarious trauma"}, {"Compassion fatigue", "Self Care Barrier: Compassion fatigue"}, {"Difficulty disconnecting from work", "Self Care Barrier: Difficulty disconnecting from work"}, {"High caseload or workload demands", "Self Care Barrier: High caseload or workload demands"}, {"Rigid schedules or limited flexibility during the school day", "Self Care Barrier: Rigid schedules or limited flexibility during the school day"}, {"Difficulty prioritizing personal needs", "Self Care Barrier: Difficulty prioritizing personal needs"}, {"Financial strain (such as cost of care or cost of living)", "Self Care Barrier: Financial strain (such as cost of care or cost of living)"}, {"Limited access to mental health supports", "Self Care Barrier: Limited access to mental health supports"}, {"Insufficient paid time off", "Self Care Barrier: Insufficient paid time off"}, {"Other (please specify)39", "Self Care Barrier: Other (please specify)39"}, {"Open-Ended Response40", "Learn Next"}, {"Open-Ended Response41", "Additional"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"Learning Rating: Ease of Access", type text}, {"Learning Rating: Content Relevance", type text}, {"Learning Rating: Subject Matter Experts/Trainers", type text}, {"Learning Rating: Time Commitment", type text}, {"Organization Priority", type text}, {"Orginization Priority Open Response", type text}, {"Learning Importance", type text}, {"Learning Importance Open Ended Response", type text}, {"Shared Learning Experiences", type text}, {"Shared Learning Experiences Open-Ended Response", type text}, {"Learning Needs Met", type text}, {"Learning Needs Met Open-Ended Response", type text}, {"Define Success: Equipping students to meet their goals", type text}, {"Define Success: Serving families and meeting their needs", type text}, {"Define Success: Ensuring equitable access to supports and resources", type text}, {"Define Success: Impacting school climate", type text}, {"Define Success: Meeting required caseload", type text}, {"Define Success: Meeting schoolwide goals", type text}, {"Define Success: Ensuring integration and coordination of supports across providers within the school", type text}, {"Define Success: Ensuring the work of CIS is integrated with the broader system(s) of support within the school", type text}, {"Define Success: Other (please describe)", type text}, {"Self care: How often", type text}, {"Self Care: Interest in Resources", type text}, {"Self Care Barrier: Limited time", type text}, {"Self Care Barrier: Burnout", type text}, {"Self Care Barrier: Secondary or vicarious trauma", type text}, {"Self Care Barrier: Compassion fatigue", type text}, {"Self Care Barrier: Difficulty disconnecting from work", type text}, {"Self Care Barrier: High caseload or workload demands", type text}, {"Self Care Barrier: Rigid schedules or limited flexibility during the school day", type text}, {"Self Care Barrier: Difficulty prioritizing personal needs", type text}, {"Self Care Barrier: Financial strain (such as cost of care or cost of living)", type text}, {"Self Care Barrier: Limited access to mental health supports", type text}, {"Self Care Barrier: Insufficient paid time off", type text}, {"Self Care Barrier: Other (please specify)39", type text}, {"Learn Next", type text}, {"Additional", type text}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type 1", {{"Self Care Barrier: Other (please specify)39", "Self Care Barrier: Other (please specify)"}, {"Often: CIS Events Calendar28", "Often: CIS Events Calendar"}, {"Often: Community Briefing Newsletter27", "Often: Community Briefing Newsletter2"}, {"Often: Learning & Practice Monthly Newsletter26", "Often: Learning & Practice Monthly Newsletter"}, {"Often: CIS Connect25", "Often: CIS Connect"}, {"Often: National Resource Center (NRC)24", "Often: National Resource Center (NRC)"}, {"Often: CIS University (CISU)23", "Often: CIS University (CISU)"}, {"Often: CIS Community22", "Often: CIS Community"}, {"Pro Weakness: Relationship skills2", "Pro Weakness: Relationship skills"}, {"Pro Weakness: Communication3", "Pro Weakness: Communication"}, {"Pro Weakness: Time management4", "Pro Weakness: Time management"}, {"Pro Weakness: Problem-solving5", "Pro Weakness: Problem-solving"}, {"Pro Weakness: Conflict resolution6", "Pro Weakness: Conflict resolution"}})
in
  #"Renamed columns 1";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "needs_assessment"]}[Content],
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
  Source = Excel.Workbook(Parameter, null, true),
  Navigation = Source{[Item = "Table1", Kind = "Table"]}[Data]
in
  Navigation;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Excel.Workbook(Parameter, null, true),
  Navigation = Source{[Item = "Table1", Kind = "Table"]}[Data]
in
  Navigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "strengths_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared strengths = let
  Source = needs_assessment,
  #"Removed other columns" = Table.SelectColumns(Source, {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name", "Pro Strength: Relationship skills", "Pro Strength: Communication", "Pro Strength: Time Management", "Pro Strength: Problem-solving", "Pro Strength: Conflict resolution", "Pro Strength: Leadership", "Pro Strength: Adaptability", "Pro Strength: Empathy"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed other columns", "Time management", "1", Replacer.ReplaceValue, {"Pro Strength: Time Management"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "Problem-solving", "1", Replacer.ReplaceValue, {"Pro Strength: Problem-solving"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "Conflict resolution", "1", Replacer.ReplaceValue, {"Pro Strength: Conflict resolution"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "Leadership", "1", Replacer.ReplaceValue, {"Pro Strength: Leadership"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "Adaptability", "1", Replacer.ReplaceValue, {"Pro Strength: Adaptability"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "Empathy", "1", Replacer.ReplaceValue, {"Pro Strength: Empathy"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "Communication", "1", Replacer.ReplaceValue, {"Pro Strength: Communication"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", "Relationship skills", "1", Replacer.ReplaceValue, {"Pro Strength: Relationship skills"}),
  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 7", null, "0", Replacer.ReplaceValue, {"Pro Strength: Relationship skills"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 8", null, "0", Replacer.ReplaceValue, {"Pro Strength: Communication"}),
  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 9", null, "0", Replacer.ReplaceValue, {"Pro Strength: Time Management"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", null, "0", Replacer.ReplaceValue, {"Pro Strength: Problem-solving"}),
  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", null, "0", Replacer.ReplaceValue, {"Pro Strength: Conflict resolution"}),
  #"Replaced value 13" = Table.ReplaceValue(#"Replaced value 12", null, "0", Replacer.ReplaceValue, {"Pro Strength: Leadership"}),
  #"Replaced value 14" = Table.ReplaceValue(#"Replaced value 13", null, "0", Replacer.ReplaceValue, {"Pro Strength: Adaptability"}),
  #"Replaced value 15" = Table.ReplaceValue(#"Replaced value 14", null, "0", Replacer.ReplaceValue, {"Pro Strength: Empathy"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 15", {{"Pro Strength: Empathy", Int64.Type}, {"Pro Strength: Adaptability", Int64.Type}, {"Pro Strength: Leadership", Int64.Type}, {"Pro Strength: Conflict resolution", Int64.Type}, {"Pro Strength: Problem-solving", Int64.Type}, {"Pro Strength: Time Management", Int64.Type}, {"Pro Strength: Communication", Int64.Type}, {"Pro Strength: Relationship skills", Int64.Type}}),
  #"Unpivoted columns" = Table.UnpivotOtherColumns(#"Changed column type", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}, "Attribute", "Value")
in
  #"Unpivoted columns";
shared strengths_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "strengths_needs_assessment", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[BindToDefaultDestination = true]
shared weaknesses = let
  Source = needs_assessment,
  #"Removed other columns 1" = Table.SelectColumns(Source, {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name", "Pro Weakness: Relationship skills", "Pro Weakness: Communication", "Pro Weakness: Time management", "Pro Weakness: Problem-solving", "Pro Weakness: Conflict resolution", "Pro Weakness: Leadership", "Pro Weakness: Adaptability", "Pro Weakness: Empathy"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed other columns 1", "Relationship skills", "1", Replacer.ReplaceValue, {"Pro Weakness: Relationship skills"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", null, "0", Replacer.ReplaceValue, {"Pro Weakness: Relationship skills"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", null, "0", Replacer.ReplaceValue, {"Pro Weakness: Communication"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "Communication", "1", Replacer.ReplaceValue, {"Pro Weakness: Communication"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "Time management", "1", Replacer.ReplaceValue, {"Pro Weakness: Time management"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", null, "0", Replacer.ReplaceValue, {"Pro Weakness: Time management"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "Problem-solving", "1", Replacer.ReplaceValue, {"Pro Weakness: Problem-solving"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", null, "0", Replacer.ReplaceValue, {"Pro Weakness: Problem-solving"}),
  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 7", "Conflict resolution", "1", Replacer.ReplaceValue, {"Pro Weakness: Conflict resolution"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 8", null, "0", Replacer.ReplaceValue, {"Pro Weakness: Conflict resolution"}),
  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 9", "Leadership", "1", Replacer.ReplaceValue, {"Pro Weakness: Leadership"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", null, "0", Replacer.ReplaceValue, {"Pro Weakness: Leadership"}),
  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", "Adaptability", "1", Replacer.ReplaceValue, {"Pro Weakness: Adaptability"}),
  #"Replaced value 13" = Table.ReplaceValue(#"Replaced value 12", null, "0", Replacer.ReplaceValue, {"Pro Weakness: Adaptability"}),
  #"Replaced value 14" = Table.ReplaceValue(#"Replaced value 13", "Empathy", "1", Replacer.ReplaceValue, {"Pro Weakness: Empathy"}),
  #"Replaced value 15" = Table.ReplaceValue(#"Replaced value 14", null, "0", Replacer.ReplaceValue, {"Pro Weakness: Empathy"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 15", {{"Pro Weakness: Empathy", Int64.Type}, {"Pro Weakness: Adaptability", Int64.Type}, {"Pro Weakness: Leadership", Int64.Type}, {"Pro Weakness: Conflict resolution", Int64.Type}, {"Pro Weakness: Problem-solving", Int64.Type}, {"Pro Weakness: Time management", Int64.Type}, {"Pro Weakness: Communication", Int64.Type}, {"Pro Weakness: Relationship skills", Int64.Type}}),
  #"Unpivoted columns" = Table.UnpivotOtherColumns(#"Changed column type", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}, "Attribute", "Value")
in
  #"Unpivoted columns";
[BindToDefaultDestination = true]
shared strengths_aggregated = let
  Source = needs_assessment,
  #"Removed other columns" = Table.SelectColumns(Source, {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name", "Pro Strength: Relationship skills", "Pro Strength: Communication", "Pro Strength: Time Management", "Pro Strength: Problem-solving", "Pro Strength: Conflict resolution", "Pro Strength: Leadership", "Pro Strength: Adaptability", "Pro Strength: Empathy"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed other columns", "Time management", "1", Replacer.ReplaceValue, {"Pro Strength: Time Management"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "Problem-solving", "1", Replacer.ReplaceValue, {"Pro Strength: Problem-solving"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "Conflict resolution", "1", Replacer.ReplaceValue, {"Pro Strength: Conflict resolution"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "Leadership", "1", Replacer.ReplaceValue, {"Pro Strength: Leadership"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "Adaptability", "1", Replacer.ReplaceValue, {"Pro Strength: Adaptability"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "Empathy", "1", Replacer.ReplaceValue, {"Pro Strength: Empathy"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "Communication", "1", Replacer.ReplaceValue, {"Pro Strength: Communication"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", "Relationship skills", "1", Replacer.ReplaceValue, {"Pro Strength: Relationship skills"}),
  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 7", null, "0", Replacer.ReplaceValue, {"Pro Strength: Relationship skills"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 8", null, "0", Replacer.ReplaceValue, {"Pro Strength: Communication"}),
  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 9", null, "0", Replacer.ReplaceValue, {"Pro Strength: Time Management"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", null, "0", Replacer.ReplaceValue, {"Pro Strength: Problem-solving"}),
  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", null, "0", Replacer.ReplaceValue, {"Pro Strength: Conflict resolution"}),
  #"Replaced value 13" = Table.ReplaceValue(#"Replaced value 12", null, "0", Replacer.ReplaceValue, {"Pro Strength: Leadership"}),
  #"Replaced value 14" = Table.ReplaceValue(#"Replaced value 13", null, "0", Replacer.ReplaceValue, {"Pro Strength: Adaptability"}),
  #"Replaced value 15" = Table.ReplaceValue(#"Replaced value 14", null, "0", Replacer.ReplaceValue, {"Pro Strength: Empathy"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 15", {{"Pro Strength: Empathy", Int64.Type}, {"Pro Strength: Adaptability", Int64.Type}, {"Pro Strength: Leadership", Int64.Type}, {"Pro Strength: Conflict resolution", Int64.Type}, {"Pro Strength: Problem-solving", Int64.Type}, {"Pro Strength: Time Management", Int64.Type}, {"Pro Strength: Communication", Int64.Type}, {"Pro Strength: Relationship skills", Int64.Type}}),
  #"Unpivoted columns" = Table.UnpivotOtherColumns(#"Changed column type", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}, "Attribute", "Value"),
  #"Grouped rows" = Table.Group(#"Unpivoted columns", {"Attribute"}, {{"Count", each List.Sum([Value]), type nullable number}}),
  #"Extracted text after delimiter" = Table.TransformColumns(#"Grouped rows", {{"Attribute", each Text.AfterDelimiter(_, ": ", 0), type text}})
in
  #"Extracted text after delimiter";
[BindToDefaultDestination = true]
shared weaknesses_aggregated = let
  Source = weaknesses,
  #"Grouped rows" = Table.Group(Source, {"Attribute"}, {{"Count", each List.Sum([Value]), type nullable number}}),
  #"Extracted text after delimiter" = Table.TransformColumns(#"Grouped rows", {{"Attribute", each Text.AfterDelimiter(_, ": ", 0), type text}})
in
  #"Extracted text after delimiter";
[BindToDefaultDestination = true]
shared confidence = let
  Source = needs_assessment,
  #"Removed other columns" = Table.SelectColumns(Source, {"Confidence: Conducting a school needs assessment", "Confidence: Creating a school support plan", "Confidence: Setting school goals", "Confidence: Creating a school support team", "Confidence: Facilitating school support team meetings", "Confidence: Identifying students in need of case-management", "Confidence: Conducting a student needs assessment", "Confidence: Creating a student support plan", "Confidence: Setting student goals", "Confidence: Identifying tiered supports aligned with school and individual student needs", "Confidence: Implementing Tier 1 (schoolwide/systems) supports", "Confidence: Implementing Tier 2 (small group) supports", "Confidence: Implementing Tier 3 (individualized) supports", "Confidence: Connecting with partners/other service providers to broker supports to students and the school", "Confidence: Implementing school support plan", "Confidence: Implementing student support plans", "Confidence: Using data to monitor and make adjustments to student supports", "Confidence: Assessing progress towards goals", "Confidence: Working with school staff and leadership", "Confidence: Building relationships with students and families", "Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed other columns", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name", "Confidence: Conducting a school needs assessment", "Confidence: Creating a school support plan", "Confidence: Setting school goals", "Confidence: Creating a school support team", "Confidence: Facilitating school support team meetings", "Confidence: Identifying students in need of case-management", "Confidence: Conducting a student needs assessment", "Confidence: Creating a student support plan", "Confidence: Setting student goals", "Confidence: Identifying tiered supports aligned with school and individual student needs", "Confidence: Implementing Tier 1 (schoolwide/systems) supports", "Confidence: Implementing Tier 2 (small group) supports", "Confidence: Implementing Tier 3 (individualized) supports", "Confidence: Connecting with partners/other service providers to broker supports to students and the school", "Confidence: Implementing school support plan", "Confidence: Implementing student support plans", "Confidence: Using data to monitor and make adjustments to student supports", "Confidence: Assessing progress towards goals", "Confidence: Working with school staff and leadership", "Confidence: Building relationships with students and families"}),
  #"Replaced value" = Table.ReplaceValue(#"Reordered columns", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Conducting a school needs assessment"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Conducting a school needs assessment"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Conducting a school needs assessment"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Creating a school support plan"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Creating a school support plan"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Creating a school support plan"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "Somewhat confident", "2", Replacer.ReplaceValue, {"Confidence: Creating a school support plan"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Setting school goals"}),
  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 7", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Setting school goals"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 8", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Setting school goals"}),
  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 9", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Creating a school support team"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Creating a school support team"}),
  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Creating a school support team"}),
  #"Replaced value 13" = Table.ReplaceValue(#"Replaced value 12", "Somewhat confident", "2", Replacer.ReplaceValue, {"Confidence: Creating a school support team"}),
  #"Replaced value 14" = Table.ReplaceValue(#"Replaced value 13", "Somewhat confident", "2", Replacer.ReplaceValue, {"Confidence: Facilitating school support team meetings"}),
  #"Replaced value 15" = Table.ReplaceValue(#"Replaced value 14", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Facilitating school support team meetings"}),
  #"Replaced value 16" = Table.ReplaceValue(#"Replaced value 15", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Facilitating school support team meetings"}),
  #"Replaced value 17" = Table.ReplaceValue(#"Replaced value 16", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Facilitating school support team meetings"}),
  #"Replaced value 18" = Table.ReplaceValue(#"Replaced value 17", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Identifying students in need of case-management"}),
  #"Replaced value 19" = Table.ReplaceValue(#"Replaced value 18", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Identifying students in need of case-management"}),
  #"Replaced value 20" = Table.ReplaceValue(#"Replaced value 19", "Moderately confident", "", Replacer.ReplaceValue, {"Confidence: Identifying students in need of case-management"}),
  #"Replaced value 21" = Table.ReplaceValue(#"Replaced value 20", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Conducting a student needs assessment"}),
  #"Replaced value 22" = Table.ReplaceValue(#"Replaced value 21", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Conducting a student needs assessment"}),
  #"Replaced value 23" = Table.ReplaceValue(#"Replaced value 22", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Conducting a student needs assessment"}),
  #"Replaced value 24" = Table.ReplaceValue(#"Replaced value 23", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Creating a student support plan"}),
  #"Replaced value 25" = Table.ReplaceValue(#"Replaced value 24", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Creating a student support plan"}),
  #"Replaced value 26" = Table.ReplaceValue(#"Replaced value 25", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Setting student goals"}),
  #"Replaced value 27" = Table.ReplaceValue(#"Replaced value 26", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Setting student goals"}),
  #"Replaced value 28" = Table.ReplaceValue(#"Replaced value 27", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Setting student goals"}),
  #"Replaced value 29" = Table.ReplaceValue(#"Replaced value 28", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Identifying tiered supports aligned with school and individual student needs"}),
  #"Replaced value 30" = Table.ReplaceValue(#"Replaced value 29", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Identifying tiered supports aligned with school and individual student needs"}),
  #"Replaced value 31" = Table.ReplaceValue(#"Replaced value 30", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Identifying tiered supports aligned with school and individual student needs"}),
  #"Replaced value 32" = Table.ReplaceValue(#"Replaced value 31", "Somewhat confident", "2", Replacer.ReplaceValue, {"Confidence: Identifying tiered supports aligned with school and individual student needs"}),
  #"Replaced value 33" = Table.ReplaceValue(#"Replaced value 32", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Implementing Tier 1 (schoolwide/systems) supports"}),
  #"Replaced value 34" = Table.ReplaceValue(#"Replaced value 33", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Implementing Tier 1 (schoolwide/systems) supports"}),
  #"Replaced value 35" = Table.ReplaceValue(#"Replaced value 34", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Implementing Tier 1 (schoolwide/systems) supports"}),
  #"Replaced value 36" = Table.ReplaceValue(#"Replaced value 35", "Not confident", "1", Replacer.ReplaceValue, {"Confidence: Implementing Tier 1 (schoolwide/systems) supports"}),
  #"Replaced value 37" = Table.ReplaceValue(#"Replaced value 36", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Implementing Tier 2 (small group) supports"}),
  #"Replaced value 38" = Table.ReplaceValue(#"Replaced value 37", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Implementing Tier 2 (small group) supports"}),
  #"Replaced value 39" = Table.ReplaceValue(#"Replaced value 38", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Implementing Tier 2 (small group) supports"}),
  #"Replaced value 40" = Table.ReplaceValue(#"Replaced value 39", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Implementing Tier 3 (individualized) supports"}),
  #"Replaced value 41" = Table.ReplaceValue(#"Replaced value 40", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Implementing Tier 3 (individualized) supports"}),
  #"Replaced value 42" = Table.ReplaceValue(#"Replaced value 41", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Implementing Tier 3 (individualized) supports"}),
  #"Replaced value 43" = Table.ReplaceValue(#"Replaced value 42", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Connecting with partners/other service providers to broker supports to students and the school"}),
  #"Replaced value 44" = Table.ReplaceValue(#"Replaced value 43", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Connecting with partners/other service providers to broker supports to students and the school"}),
  #"Replaced value 45" = Table.ReplaceValue(#"Replaced value 44", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Connecting with partners/other service providers to broker supports to students and the school"}),
  #"Replaced value 46" = Table.ReplaceValue(#"Replaced value 45", "Somewhat confident", "2", Replacer.ReplaceValue, {"Confidence: Connecting with partners/other service providers to broker supports to students and the school"}),
  #"Replaced value 47" = Table.ReplaceValue(#"Replaced value 46", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Implementing school support plan"}),
  #"Replaced value 48" = Table.ReplaceValue(#"Replaced value 47", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Implementing school support plan"}),
  #"Replaced value 49" = Table.ReplaceValue(#"Replaced value 48", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Implementing school support plan"}),
  #"Replaced value 50" = Table.ReplaceValue(#"Replaced value 49", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Implementing student support plans"}),
  #"Replaced value 51" = Table.ReplaceValue(#"Replaced value 50", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Implementing student support plans"}),
  #"Replaced value 52" = Table.ReplaceValue(#"Replaced value 51", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Implementing student support plans"}),
  #"Replaced value 53" = Table.ReplaceValue(#"Replaced value 52", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Using data to monitor and make adjustments to student supports"}),
  #"Replaced value 54" = Table.ReplaceValue(#"Replaced value 53", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Using data to monitor and make adjustments to student supports"}),
  #"Replaced value 55" = Table.ReplaceValue(#"Replaced value 54", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Using data to monitor and make adjustments to student supports"}),
  #"Replaced value 56" = Table.ReplaceValue(#"Replaced value 55", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Assessing progress towards goals"}),
  #"Replaced value 57" = Table.ReplaceValue(#"Replaced value 56", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Assessing progress towards goals"}),
  #"Replaced value 58" = Table.ReplaceValue(#"Replaced value 57", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Assessing progress towards goals"}),
  #"Replaced value 59" = Table.ReplaceValue(#"Replaced value 58", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Working with school staff and leadership"}),
  #"Replaced value 60" = Table.ReplaceValue(#"Replaced value 59", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Working with school staff and leadership"}),
  #"Replaced value 61" = Table.ReplaceValue(#"Replaced value 60", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Working with school staff and leadership"}),
  #"Replaced value 62" = Table.ReplaceValue(#"Replaced value 61", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Building relationships with students and families"}),
  #"Replaced value 63" = Table.ReplaceValue(#"Replaced value 62", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Building relationships with students and families"}),
  #"Replaced value 64" = Table.ReplaceValue(#"Replaced value 63", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Building relationships with students and families"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 64", {{"Confidence: Building relationships with students and families", Int64.Type}, {"Confidence: Working with school staff and leadership", Int64.Type}, {"Confidence: Assessing progress towards goals", Int64.Type}, {"Confidence: Using data to monitor and make adjustments to student supports", Int64.Type}, {"Confidence: Implementing student support plans", Int64.Type}, {"Confidence: Implementing school support plan", Int64.Type}, {"Confidence: Connecting with partners/other service providers to broker supports to students and the school", Int64.Type}, {"Confidence: Implementing Tier 3 (individualized) supports", Int64.Type}, {"Confidence: Implementing Tier 2 (small group) supports", Int64.Type}, {"Confidence: Implementing Tier 1 (schoolwide/systems) supports", Int64.Type}, {"Confidence: Identifying tiered supports aligned with school and individual student needs", Int64.Type}, {"Confidence: Setting student goals", Int64.Type}, {"Confidence: Creating a student support plan", Int64.Type}, {"Confidence: Conducting a student needs assessment", Int64.Type}, {"Confidence: Identifying students in need of case-management", Int64.Type}, {"Confidence: Facilitating school support team meetings", Int64.Type}, {"Confidence: Creating a school support team", Int64.Type}, {"Confidence: Setting school goals", Int64.Type}, {"Confidence: Creating a school support plan", Int64.Type}, {"Confidence: Conducting a school needs assessment", Int64.Type}}),
  #"Unpivoted columns" = Table.UnpivotOtherColumns(#"Changed column type", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}, "Attribute", "Value")
in
  #"Unpivoted columns";
[BindToDefaultDestination = true]
shared confidence_aggregated = let
  Source = confidence,
  #"Grouped rows" = Table.Group(Source, {"Attribute"}, {{"average score", each List.Average([Value]), type nullable number}}),
  #"Extracted text after delimiter" = Table.TransformColumns(#"Grouped rows", {{"Attribute", each Text.AfterDelimiter(_, ": ", 0), type text}})
in
  #"Extracted text after delimiter";
[BindToDefaultDestination = true]
shared knowledge = let
  Source = needs_assessment,
  #"Removed other columns" = Table.SelectColumns(Source, {"Knowledge: Partnership development", "Knowledge: Developmental Relationships", "Knowlege: Trauma-Informed Care", "Knowledge: Youth mental health", "Knowledge: Adverse Childhood Experiences (ACEs)", "Knowledge: Restorative Practices", "Knowledge: Creating a safe and welcoming school environment", "Knowledge: Ensuring fair and respectful treatment of students", "Knowledge: Fostering belonging and student engagement", "Knowledge: Data-driven decision making", "Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed other columns", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name", "Knowledge: Partnership development", "Knowledge: Developmental Relationships", "Knowlege: Trauma-Informed Care", "Knowledge: Youth mental health", "Knowledge: Adverse Childhood Experiences (ACEs)", "Knowledge: Restorative Practices", "Knowledge: Creating a safe and welcoming school environment", "Knowledge: Ensuring fair and respectful treatment of students", "Knowledge: Fostering belonging and student engagement", "Knowledge: Data-driven decision making"}),
  #"Replaced value" = Table.ReplaceValue(#"Reordered columns", "Extremely knowledgeable", "5", Replacer.ReplaceValue, {"Knowledge: Partnership development"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "Moderately knowledgeable", "3", Replacer.ReplaceValue, {"Knowledge: Partnership development"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "Very knowledgeable", "4", Replacer.ReplaceValue, {"Knowledge: Partnership development"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "Somewhat knowledgeable", "2", Replacer.ReplaceValue, {"Knowledge: Partnership development"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "Very knowledgeable", "4", Replacer.ReplaceValue, {"Knowledge: Developmental Relationships"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "Moderately knowledgeable", "3", Replacer.ReplaceValue, {"Knowledge: Developmental Relationships"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "Extremely knowledgeable", "5", Replacer.ReplaceValue, {"Knowledge: Developmental Relationships"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", "Somewhat knowledgeable", "2", Replacer.ReplaceValue, {"Knowledge: Developmental Relationships"}),
  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 7", "Very knowledgeable", "4", Replacer.ReplaceValue, {"Knowlege: Trauma-Informed Care"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 8", "Moderately knowledgeable", "3", Replacer.ReplaceValue, {"Knowlege: Trauma-Informed Care"}),
  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 9", "Extremely knowledgeable", "5", Replacer.ReplaceValue, {"Knowlege: Trauma-Informed Care"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", "Very knowledgeable", "4", Replacer.ReplaceValue, {"Knowledge: Youth mental health"}),
  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", "Extremely knowledgeable", "5", Replacer.ReplaceValue, {"Knowledge: Youth mental health"}),
  #"Replaced value 13" = Table.ReplaceValue(#"Replaced value 12", "Moderately knowledgeable", "3", Replacer.ReplaceValue, {"Knowledge: Youth mental health"}),
  #"Replaced value 14" = Table.ReplaceValue(#"Replaced value 13", "Somewhat knowledgeable", "2", Replacer.ReplaceValue, {"Knowledge: Youth mental health"}),
  #"Replaced value 15" = Table.ReplaceValue(#"Replaced value 14", "Very knowledgeable", "4", Replacer.ReplaceValue, {"Knowledge: Adverse Childhood Experiences (ACEs)"}),
  #"Replaced value 16" = Table.ReplaceValue(#"Replaced value 15", "Moderately knowledgeable", "3", Replacer.ReplaceValue, {"Knowledge: Adverse Childhood Experiences (ACEs)"}),
  #"Replaced value 17" = Table.ReplaceValue(#"Replaced value 16", "Extremely knowledgeable", "5", Replacer.ReplaceValue, {"Knowledge: Adverse Childhood Experiences (ACEs)"}),
  #"Replaced value 18" = Table.ReplaceValue(#"Replaced value 17", "Somewhat knowledgeable", "2", Replacer.ReplaceValue, {"Knowledge: Adverse Childhood Experiences (ACEs)"}),
  #"Replaced value 19" = Table.ReplaceValue(#"Replaced value 18", "Very knowledgeable", "4", Replacer.ReplaceValue, {"Knowledge: Restorative Practices"}),
  #"Replaced value 20" = Table.ReplaceValue(#"Replaced value 19", "Very knowledgeable", "4", Replacer.ReplaceValue, {"Knowledge: Creating a safe and welcoming school environment"}),
  #"Replaced value 21" = Table.ReplaceValue(#"Replaced value 20", "Extremely knowledgeable", "5", Replacer.ReplaceValue, {"Knowledge: Creating a safe and welcoming school environment"}),
  #"Replaced value 22" = Table.ReplaceValue(#"Replaced value 21", "Moderately knowledgeable", "3", Replacer.ReplaceValue, {"Knowledge: Restorative Practices"}),
  #"Replaced value 23" = Table.ReplaceValue(#"Replaced value 22", "Extremely knowledgeable", "5", Replacer.ReplaceValue, {"Knowledge: Restorative Practices"}),
  #"Replaced value 24" = Table.ReplaceValue(#"Replaced value 23", "Somewhat knowledgeable", "2", Replacer.ReplaceValue, {"Knowledge: Restorative Practices"}),
  #"Replaced value 25" = Table.ReplaceValue(#"Replaced value 24", "Moderately knowledgeable", "3", Replacer.ReplaceValue, {"Knowledge: Creating a safe and welcoming school environment"}),
  #"Replaced value 26" = Table.ReplaceValue(#"Replaced value 25", "Extremely knowledgeable", "5", Replacer.ReplaceValue, {"Knowledge: Ensuring fair and respectful treatment of students"}),
  #"Replaced value 27" = Table.ReplaceValue(#"Replaced value 26", "Very knowledgeable", "4", Replacer.ReplaceValue, {"Knowledge: Ensuring fair and respectful treatment of students"}),
  #"Replaced value 28" = Table.ReplaceValue(#"Replaced value 27", "Moderately knowledgeable", "3", Replacer.ReplaceValue, {"Knowledge: Ensuring fair and respectful treatment of students"}),
  #"Replaced value 29" = Table.ReplaceValue(#"Replaced value 28", "Extremely knowledgeable", "5", Replacer.ReplaceValue, {"Knowledge: Fostering belonging and student engagement"}),
  #"Replaced value 30" = Table.ReplaceValue(#"Replaced value 29", "Moderately knowledgeable", "3", Replacer.ReplaceValue, {"Knowledge: Fostering belonging and student engagement"}),
  #"Replaced value 31" = Table.ReplaceValue(#"Replaced value 30", "Very knowledgeable", "4", Replacer.ReplaceValue, {"Knowledge: Fostering belonging and student engagement"}),
  #"Replaced value 32" = Table.ReplaceValue(#"Replaced value 31", "Very knowledgeable", "4", Replacer.ReplaceValue, {"Knowledge: Data-driven decision making"}),
  #"Replaced value 33" = Table.ReplaceValue(#"Replaced value 32", "Moderately knowledgeable", "3", Replacer.ReplaceValue, {"Knowledge: Data-driven decision making"}),
  #"Replaced value 34" = Table.ReplaceValue(#"Replaced value 33", "Somewhat knowledgeable", "2", Replacer.ReplaceValue, {"Knowledge: Data-driven decision making"}),
  #"Replaced value 35" = Table.ReplaceValue(#"Replaced value 34", "Extremely knowledgeable", "5", Replacer.ReplaceValue, {"Knowledge: Data-driven decision making"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 35", {{"Knowledge: Partnership development", type number}, {"Knowledge: Developmental Relationships", type number}, {"Knowlege: Trauma-Informed Care", type number}, {"Knowledge: Youth mental health", type number}, {"Knowledge: Adverse Childhood Experiences (ACEs)", type number}, {"Knowledge: Restorative Practices", type number}, {"Knowledge: Creating a safe and welcoming school environment", type number}, {"Knowledge: Ensuring fair and respectful treatment of students", type number}, {"Knowledge: Fostering belonging and student engagement", type number}, {"Knowledge: Data-driven decision making", type number}}),
  #"Unpivoted columns" = Table.UnpivotOtherColumns(#"Changed column type", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}, "Attribute", "Value")
in
  #"Unpivoted columns";
[BindToDefaultDestination = true]
shared relevance = let
  Source = needs_assessment,
  #"Removed other columns" = Table.SelectColumns(Source, {"Relevance: Developmental Relationships11", "Relevance: Trauma-Informed Care12", "Relevance: Youth mental health13", "Relevance: Adverse Childhood Experiences (ACEs)14", "Relevance: Restorative Practices15", "Relevance: Creating a safe and welcoming school environment16", "Relevance: Ensuring fair and respectful treatment of students17", "Relevance: Fostering belonging and student engagement18", "Relevance: Data-driven decision making19", "Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed other columns", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name", "Relevance: Developmental Relationships11", "Relevance: Trauma-Informed Care12", "Relevance: Youth mental health13", "Relevance: Adverse Childhood Experiences (ACEs)14", "Relevance: Restorative Practices15", "Relevance: Creating a safe and welcoming school environment16", "Relevance: Ensuring fair and respectful treatment of students17", "Relevance: Fostering belonging and student engagement18", "Relevance: Data-driven decision making19"}),
  #"Replaced value" = Table.ReplaceValue(#"Reordered columns", "Extremely relevant", "5", Replacer.ReplaceValue, {"Relevance: Developmental Relationships11"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "Moderately relevant", "3", Replacer.ReplaceValue, {"Relevance: Developmental Relationships11"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "Very relevant", "4", Replacer.ReplaceValue, {"Relevance: Developmental Relationships11"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "Somewhat relevant", "2", Replacer.ReplaceValue, {"Relevance: Developmental Relationships11"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "Very relevant", "4", Replacer.ReplaceValue, {"Relevance: Trauma-Informed Care12"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "Moderately relevant", "3", Replacer.ReplaceValue, {"Relevance: Trauma-Informed Care12"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "Extremely relevant", "5", Replacer.ReplaceValue, {"Relevance: Trauma-Informed Care12"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", "Very relevant", "4", Replacer.ReplaceValue, {"Relevance: Youth mental health13"}),
  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 7", "Extremely relevant", "5", Replacer.ReplaceValue, {"Relevance: Youth mental health13"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 8", "Moderately relevant", "3", Replacer.ReplaceValue, {"Relevance: Youth mental health13"}),
  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 9", "Moderately relevant", "3", Replacer.ReplaceValue, {"Relevance: Adverse Childhood Experiences (ACEs)14"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", "Extremely relevant", "5", Replacer.ReplaceValue, {"Relevance: Adverse Childhood Experiences (ACEs)14"}),
  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", "Very relevant", "4", Replacer.ReplaceValue, {"Relevance: Adverse Childhood Experiences (ACEs)14"}),
  #"Renamed columns" = Table.RenameColumns(#"Replaced value 12", {{"Relevance: Youth mental health13", "Relevance: Youth mental health"}, {"Relevance: Trauma-Informed Care12", "Relevance: Trauma-Informed Care"}, {"Relevance: Adverse Childhood Experiences (ACEs)14", "Relevance: Adverse Childhood Experiences (ACEs)"}, {"Relevance: Restorative Practices15", "Relevance: Restorative Practices"}, {"Relevance: Creating a safe and welcoming school environment16", "Relevance: Creating a safe and welcoming school environment"}, {"Relevance: Ensuring fair and respectful treatment of students17", "Relevance: Ensuring fair and respectful treatment of students"}, {"Relevance: Fostering belonging and student engagement18", "Relevance: Fostering belonging and student engagement"}, {"Relevance: Data-driven decision making19", "Relevance: Data-driven decision making"}}),
  #"Replaced value 13" = Table.ReplaceValue(#"Renamed columns", "Very relevant", "4", Replacer.ReplaceValue, {"Relevance: Data-driven decision making"}),
  #"Replaced value 14" = Table.ReplaceValue(#"Replaced value 13", "Moderately relevant", "3", Replacer.ReplaceValue, {"Relevance: Data-driven decision making"}),
  #"Replaced value 15" = Table.ReplaceValue(#"Replaced value 14", "Extremely relevant", "5", Replacer.ReplaceValue, {"Relevance: Data-driven decision making"}),
  #"Replaced value 16" = Table.ReplaceValue(#"Replaced value 15", "Extremely relevant", "5", Replacer.ReplaceValue, {"Relevance: Fostering belonging and student engagement"}),
  #"Replaced value 17" = Table.ReplaceValue(#"Replaced value 16", "Moderately relevant", "3", Replacer.ReplaceValue, {"Relevance: Fostering belonging and student engagement"}),
  #"Replaced value 18" = Table.ReplaceValue(#"Replaced value 17", "Very relevant", "4", Replacer.ReplaceValue, {"Relevance: Fostering belonging and student engagement"}),
  #"Replaced value 19" = Table.ReplaceValue(#"Replaced value 18", "Extremely relevant", "5", Replacer.ReplaceValue, {"Relevance: Ensuring fair and respectful treatment of students"}),
  #"Replaced value 20" = Table.ReplaceValue(#"Replaced value 19", "Very relevant", "4", Replacer.ReplaceValue, {"Relevance: Ensuring fair and respectful treatment of students"}),
  #"Replaced value 21" = Table.ReplaceValue(#"Replaced value 20", "Very relevant", "4", Replacer.ReplaceValue, {"Relevance: Creating a safe and welcoming school environment"}),
  #"Replaced value 22" = Table.ReplaceValue(#"Replaced value 21", "Extremely relevant", "5", Replacer.ReplaceValue, {"Relevance: Creating a safe and welcoming school environment"}),
  #"Replaced value 23" = Table.ReplaceValue(#"Replaced value 22", "Moderately relevant", "3", Replacer.ReplaceValue, {"Relevance: Creating a safe and welcoming school environment"}),
  #"Replaced value 24" = Table.ReplaceValue(#"Replaced value 23", "Very relevant", "4", Replacer.ReplaceValue, {"Relevance: Restorative Practices"}),
  #"Replaced value 25" = Table.ReplaceValue(#"Replaced value 24", "Extremely relevant", "5", Replacer.ReplaceValue, {"Relevance: Restorative Practices"}),
  #"Replaced value 26" = Table.ReplaceValue(#"Replaced value 25", "Moderately relevant", "3", Replacer.ReplaceValue, {"Relevance: Restorative Practices"}),
  #"Replaced value 27" = Table.ReplaceValue(#"Replaced value 26", "Somewhat relevant", "2", Replacer.ReplaceValue, {"Relevance: Restorative Practices"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 27", {{"Relevance: Developmental Relationships11", Int64.Type}, {"Relevance: Trauma-Informed Care", Int64.Type}, {"Relevance: Youth mental health", Int64.Type}, {"Relevance: Adverse Childhood Experiences (ACEs)", Int64.Type}, {"Relevance: Restorative Practices", Int64.Type}, {"Relevance: Creating a safe and welcoming school environment", Int64.Type}, {"Relevance: Ensuring fair and respectful treatment of students", Int64.Type}, {"Relevance: Fostering belonging and student engagement", Int64.Type}, {"Relevance: Data-driven decision making", Int64.Type}}),
  #"Unpivoted columns" = Table.UnpivotOtherColumns(#"Changed column type", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}, "Attribute", "Value")
in
  #"Unpivoted columns";
[BindToDefaultDestination = true]
shared relevance_aggregated = let
  Source = relevance,
  #"Grouped rows" = Table.Group(Source, {"Attribute"}, {{"average rating", each List.Average([Value]), type nullable number}})
in
  #"Grouped rows";
[BindToDefaultDestination = true]
shared knowledge_aggregated = let
  Source = knowledge,
  #"Grouped rows" = Table.Group(Source, {"Attribute"}, {{"average rating", each List.Average([Value]), type nullable number}}),
  #"Extracted text after delimiter" = Table.TransformColumns(#"Grouped rows", {{"Attribute", each Text.AfterDelimiter(_, ": ", 0), type text}})
in
  #"Extracted text after delimiter";
[BindToDefaultDestination = true]
shared #"confidence providing" = let
  Source = needs_assessment,
  #"Removed other columns" = Table.SelectColumns(Source, {"Confdence: Attendance", "Confidence: Behavior", "Confidence: Coursework/Academics", "Confidence: Life Skills", "Confidence: College and Career Readiness (CCR)", "Confidence: Family Engagement", "Confidence: School Climate", "Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed other columns", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name", "Confdence: Attendance", "Confidence: Behavior", "Confidence: Coursework/Academics", "Confidence: Life Skills", "Confidence: College and Career Readiness (CCR)", "Confidence: Family Engagement", "Confidence: School Climate"}),
  #"Replaced value" = Table.ReplaceValue(#"Reordered columns", "Extremely confident", "5", Replacer.ReplaceValue, {"Confdence: Attendance"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Behavior"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "Very confident", "4", Replacer.ReplaceValue, {"Confdence: Attendance"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Behavior"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "Somewhat confident", "2", Replacer.ReplaceValue, {"Confidence: Behavior"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Coursework/Academics"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Coursework/Academics"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Coursework/Academics"}),
  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 7", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Life Skills"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 8", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: College and Career Readiness (CCR)"}),
  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 9", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Life Skills"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: College and Career Readiness (CCR)"}),
  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Life Skills"}),
  #"Replaced value 13" = Table.ReplaceValue(#"Replaced value 12", "Not confident", "1", Replacer.ReplaceValue, {"Confidence: College and Career Readiness (CCR)"}),
  #"Replaced value 14" = Table.ReplaceValue(#"Replaced value 13", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: College and Career Readiness (CCR)"}),
  #"Replaced value 15" = Table.ReplaceValue(#"Replaced value 14", "Somewhat confident", "2", Replacer.ReplaceValue, {"Confidence: College and Career Readiness (CCR)"}),
  #"Replaced value 16" = Table.ReplaceValue(#"Replaced value 15", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: Family Engagement"}),
  #"Replaced value 17" = Table.ReplaceValue(#"Replaced value 16", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: School Climate"}),
  #"Replaced value 18" = Table.ReplaceValue(#"Replaced value 17", "Very confident", "4", Replacer.ReplaceValue, {"Confidence: Family Engagement"}),
  #"Replaced value 19" = Table.ReplaceValue(#"Replaced value 18", "Moderately confident", "3", Replacer.ReplaceValue, {"Confidence: Family Engagement"}),
  #"Replaced value 20" = Table.ReplaceValue(#"Replaced value 19", "Extremely confident", "5", Replacer.ReplaceValue, {"Confidence: School Climate"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 20", {{"Confidence: School Climate", type number}, {"Confidence: Family Engagement", type number}, {"Confidence: College and Career Readiness (CCR)", type number}, {"Confidence: Life Skills", type number}, {"Confidence: Coursework/Academics", type number}, {"Confidence: Behavior", type number}, {"Confdence: Attendance", type number}}),
  #"Unpivoted columns" = Table.UnpivotOtherColumns(#"Changed column type", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}, "Attribute", "Value")
in
  #"Unpivoted columns";
[BindToDefaultDestination = true]
shared #"confidence providing aggregated" = let
  Source = #"confidence providing",
  #"Grouped rows" = Table.Group(Source, {"Attribute"}, {{"average rating", each List.Average([Value]), type nullable number}}),
  #"Extracted text after delimiter" = Table.TransformColumns(#"Grouped rows", {{"Attribute", each Text.AfterDelimiter(_, ": ", 0), type text}})
in
  #"Extracted text after delimiter";
[BindToDefaultDestination = true]
shared learning_style = let
  Source = needs_assessment,
  #"Removed other columns" = Table.SelectColumns(Source, {"Learning: Live, instructor-led in person", "Learning: Live, instructor-led virtual", "Learning: Self-paced, on your own time", "Learning: Hybrid", "Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed other columns", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name", "Learning: Live, instructor-led in person", "Learning: Live, instructor-led virtual", "Learning: Self-paced, on your own time", "Learning: Hybrid"}),
  #"Replaced value" = Table.ReplaceValue(#"Reordered columns", "Live, instructor-led in person", "1", Replacer.ReplaceValue, {"Learning: Live, instructor-led in person"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "Live, instructor-led virtual", "1", Replacer.ReplaceValue, {"Learning: Live, instructor-led virtual"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "Self-paced, on your own time", "1", Replacer.ReplaceValue, {"Learning: Self-paced, on your own time"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "Hybrid", "1", Replacer.ReplaceValue, {"Learning: Hybrid"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", null, "0", Replacer.ReplaceValue, {"Learning: Hybrid"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", null, "0", Replacer.ReplaceValue, {"Learning: Self-paced, on your own time"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", null, "0", Replacer.ReplaceValue, {"Learning: Live, instructor-led virtual"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", null, "0", Replacer.ReplaceValue, {"Learning: Live, instructor-led in person"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 7", {{"Learning: Live, instructor-led in person", type number}, {"Learning: Live, instructor-led virtual", type number}, {"Learning: Self-paced, on your own time", type number}, {"Learning: Hybrid", type number}}),
  #"Unpivoted columns" = Table.UnpivotOtherColumns(#"Changed column type", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}, "Attribute", "Value")
in
  #"Unpivoted columns";
[BindToDefaultDestination = true]
shared learning_style_aggregated = let
  Source = learning_style,
  #"Grouped rows" = Table.Group(Source, {"Attribute"}, {{"total", each List.Sum([Value]), type nullable number}}),
  #"Extracted text after delimiter" = Table.TransformColumns(#"Grouped rows", {{"Attribute", each Text.AfterDelimiter(_, ": ", 0), type text}})
in
  #"Extracted text after delimiter";
[BindToDefaultDestination = true]
shared learn_best = let
  Source = needs_assessment,
  #"Removed other columns" = Table.SelectColumns(Source, {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name", "Learn Best: I see something in action", "Learn Best: I hear someone explain it", "Learn Best: I have time to read and process", "Learn Best: I have time to practice"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed other columns", "I hear someone explain it", "1", Replacer.ReplaceValue, {"Learn Best: I hear someone explain it"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "I have time to practice", "1", Replacer.ReplaceValue, {"Learn Best: I have time to practice"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "I have time to read and process", "1", Replacer.ReplaceValue, {"Learn Best: I have time to read and process"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", null, "0", Replacer.ReplaceValue, {"Learn Best: I hear someone explain it"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", null, "0", Replacer.ReplaceValue, {"Learn Best: I have time to read and process"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", null, "0", Replacer.ReplaceValue, {"Learn Best: I have time to practice"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 5", {{"Learn Best: I have time to practice", type number}, {"Learn Best: I have time to read and process", type number}, {"Learn Best: I hear someone explain it", type number}}),
  #"Replaced value 6" = Table.ReplaceValue(#"Changed column type", "I see something in action", "1", Replacer.ReplaceValue, {"Learn Best: I see something in action"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Replaced value 6", {{"Learn Best: I see something in action", type number}}),
  #"Unpivoted columns" = Table.UnpivotOtherColumns(#"Changed column type 1", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}, "Attribute", "Value")
in
  #"Unpivoted columns";
[BindToDefaultDestination = true]
shared learn_best_aggregated = let
  Source = learn_best,
  #"Grouped rows" = Table.Group(Source, {"Attribute"}, {{"total", each List.Sum([Value]), type nullable number}}),
  #"Extracted text after delimiter" = Table.TransformColumns(#"Grouped rows", {{"Attribute", each Text.AfterDelimiter(_, ": ", 0), type text}})
in
  #"Extracted text after delimiter";
[BindToDefaultDestination = true]
shared training_when = let
  Source = needs_assessment,
  #"Removed other columns" = Table.SelectColumns(Source, {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name", "When: Before school year begins", "When: Towards the end of school year", "When: During summer break", "When: Throughout the school year, as needed"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed other columns", "Before school year begins", "1", Replacer.ReplaceValue, {"When: Before school year begins"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "Towards the end of school year", "1", Replacer.ReplaceValue, {"When: Towards the end of school year"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "During summer break", "1", Replacer.ReplaceValue, {"When: During summer break"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "Throughout the school year, as needed", "1", Replacer.ReplaceValue, {"When: Throughout the school year, as needed"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", null, "0", Replacer.ReplaceValue, {"When: Throughout the school year, as needed"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", null, "0", Replacer.ReplaceValue, {"When: During summer break"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", null, "0", Replacer.ReplaceValue, {"When: Towards the end of school year"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", null, "0", Replacer.ReplaceValue, {"When: Before school year begins"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 7", {{"When: Before school year begins", type number}, {"When: Towards the end of school year", type number}, {"When: During summer break", type number}, {"When: Throughout the school year, as needed", type number}}),
  #"Unpivoted columns" = Table.UnpivotOtherColumns(#"Changed column type", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}, "Attribute", "Value")
in
  #"Unpivoted columns";
[BindToDefaultDestination = true]
shared training_when_aggregated = let
  Source = training_when,
  #"Grouped rows" = Table.Group(Source, {"Attribute"}, {{"total", each List.Sum([Value]), type nullable number}}),
  #"Extracted text after delimiter" = Table.TransformColumns(#"Grouped rows", {{"Attribute", each Text.AfterDelimiter(_, ": ", 0), type text}})
in
  #"Extracted text after delimiter";
[BindToDefaultDestination = true]
shared training_freq = let
  Source = needs_assessment,
  #"Removed other columns" = Table.SelectColumns(Source, {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name", "When: Frequency Response"})
in
  #"Removed other columns";
[BindToDefaultDestination = true]
shared training_freq_aggregated = let
  Source = training_freq,
  #"Grouped rows" = Table.Group(Source, {"When: Frequency Response"}, {{"total", each Table.RowCount(_), Int64.Type}})
in
  #"Grouped rows";
[BindToDefaultDestination = true]
shared training_time = let
  Source = needs_assessment,
  #"Removed other columns" = Table.SelectColumns(Source, {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name", "Training: Time of Day Morning", "Training: Time of Day Afternoon", "Training: Time of Day Evening"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed other columns", "Morning", "1", Replacer.ReplaceValue, {"Training: Time of Day Morning"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "Afternoon", "1", Replacer.ReplaceValue, {"Training: Time of Day Afternoon"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "Evening", "1", Replacer.ReplaceValue, {"Training: Time of Day Evening"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", null, "0", Replacer.ReplaceValue, {"Training: Time of Day Afternoon"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", null, "0", Replacer.ReplaceValue, {"Training: Time of Day Evening"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", null, "0", Replacer.ReplaceValue, {"Training: Time of Day Morning"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 5", {{"Training: Time of Day Morning", type number}, {"Training: Time of Day Afternoon", type number}, {"Training: Time of Day Evening", type number}}),
  #"Unpivoted columns" = Table.UnpivotOtherColumns(#"Changed column type", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}, "Attribute", "Value")
in
  #"Unpivoted columns";
[BindToDefaultDestination = true]
shared training_time_aggregated = let
  Source = training_time,
  #"Grouped rows" = Table.Group(Source, {"Attribute"}, {{"total", each List.Sum([Value]), type nullable number}}),
  #"Extracted text after delimiter" = Table.TransformColumns(#"Grouped rows", {{"Attribute", each Text.AfterDelimiter(_, ": ", 0), type text}})
in
  #"Extracted text after delimiter";
[BindToDefaultDestination = true]
shared #"training length" = let
  Source = needs_assessment,
  #"Removed other columns" = Table.SelectColumns(Source, {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name", "Training : Length"})
in
  #"Removed other columns";
[BindToDefaultDestination = true]
shared training_length_aggregated = let
  Source = #"training length",
  #"Grouped rows" = Table.Group(Source, {"Training : Length"}, {{"total", each Table.RowCount(_), Int64.Type}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Grouped rows", "total_surveyed", each 16), {{"total_surveyed", type number}}),
  #"Inserted division" = Table.AddColumn(#"Added custom", "Division", each [total] / [total_surveyed], type number),
  #"Renamed columns" = Table.RenameColumns(#"Inserted division", {{"Division", "percentage"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"percentage", Percentage.Type}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared access = let
  Source = needs_assessment,
  #"Removed other columns" = Table.SelectColumns(Source, {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name", "Access: Professional development funds", "Access: Dedicated time for training and learning needs", "Access: Information about training opportunities", "Access: Support from your supervisor", "Access: Support from your organization", "Access: Mentors/coaches", "Access: Professional learning communities"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed other columns", "Yes", "1", Replacer.ReplaceValue, {"Access: Professional development funds"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "Yes", "1", Replacer.ReplaceValue, {"Access: Dedicated time for training and learning needs"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "No", "0", Replacer.ReplaceValue, {"Access: Professional development funds"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "Yes", "1", Replacer.ReplaceValue, {"Access: Information about training opportunities"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "No", "0", Replacer.ReplaceValue, {"Access: Information about training opportunities"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "Yes", "1", Replacer.ReplaceValue, {"Access: Support from your supervisor"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "No", "0", Replacer.ReplaceValue, {"Access: Support from your supervisor"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", "Yes", "1", Replacer.ReplaceValue, {"Access: Support from your organization"}),
  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 7", "Yes", "1", Replacer.ReplaceValue, {"Access: Mentors/coaches"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 8", "Yes", "1", Replacer.ReplaceValue, {"Access: Professional learning communities"}),
  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 9", "No", "0", Replacer.ReplaceValue, {"Access: Mentors/coaches"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", "No", "0", Replacer.ReplaceValue, {"Access: Professional learning communities"}),
  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", "No", "0", Replacer.ReplaceValue, {"Access: Support from your organization"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 12", {{"Access: Professional development funds", type number}, {"Access: Dedicated time for training and learning needs", type number}, {"Access: Information about training opportunities", type number}, {"Access: Support from your supervisor", type number}, {"Access: Support from your organization", type number}, {"Access: Mentors/coaches", type number}, {"Access: Professional learning communities", type number}}),
  #"Unpivoted columns" = Table.UnpivotOtherColumns(#"Changed column type", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}, "Attribute", "Value")
in
  #"Unpivoted columns";
[BindToDefaultDestination = true]
shared access_aggregated = let
  Source = access,
  #"Grouped rows" = Table.Group(Source, {"Attribute"}, {{"total", each List.Sum([Value]), type nullable number}}),
  #"Extracted text after delimiter" = Table.TransformColumns(#"Grouped rows", {{"Attribute", each Text.AfterDelimiter(_, ": ", 0), type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Extracted text after delimiter", {{"total", "yes"}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Renamed columns", "total surveyed", each 16), {{"total surveyed", type number}}),
  #"Inserted division" = Table.AddColumn(#"Added custom", "Division", each [yes] / [total surveyed], type number),
  #"Renamed columns 1" = Table.RenameColumns(#"Inserted division", {{"Division", "percentage with access"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns 1", {{"percentage with access", Percentage.Type}})
in
  #"Changed column type";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data];
[BindToDefaultDestination = true]
shared self_care = let
  Source = needs_assessment,
  #"Removed other columns" = Table.SelectColumns(Source, {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name", "Self care: How often", "Self Care: Interest in Resources", "Self Care Barrier: Limited time", "Self Care Barrier: Burnout", "Self Care Barrier: Secondary or vicarious trauma", "Self Care Barrier: Compassion fatigue", "Self Care Barrier: Difficulty disconnecting from work", "Self Care Barrier: High caseload or workload demands", "Self Care Barrier: Rigid schedules or limited flexibility during the school day", "Self Care Barrier: Difficulty prioritizing personal needs", "Self Care Barrier: Financial strain (such as cost of care or cost of living)", "Self Care Barrier: Limited access to mental health supports", "Self Care Barrier: Insufficient paid time off", "Self Care Barrier: Other (please specify)"}),
  #"Removed columns" = Table.RemoveColumns(#"Removed other columns", {"Self care: How often", "Self Care: Interest in Resources"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed columns", "Limited time", "1", Replacer.ReplaceValue, {"Self Care Barrier: Limited time"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", null, "0", Replacer.ReplaceValue, {"Self Care Barrier: Limited time"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "Burnout", "1", Replacer.ReplaceValue, {"Self Care Barrier: Burnout"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", null, "0", Replacer.ReplaceValue, {"Self Care Barrier: Burnout"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", null, "0", Replacer.ReplaceValue, {"Self Care Barrier: Secondary or vicarious trauma"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "Compassion fatigue", "1", Replacer.ReplaceValue, {"Self Care Barrier: Compassion fatigue"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", null, "0", Replacer.ReplaceValue, {"Self Care Barrier: Compassion fatigue"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", "Difficulty disconnecting from work", "1", Replacer.ReplaceValue, {"Self Care Barrier: Difficulty disconnecting from work"}),
  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 7", "High caseload or workload demands", "1", Replacer.ReplaceValue, {"Self Care Barrier: High caseload or workload demands"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 8", null, "0", Replacer.ReplaceValue, {"Self Care Barrier: Difficulty disconnecting from work"}),
  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 9", null, "0", Replacer.ReplaceValue, {"Self Care Barrier: High caseload or workload demands"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", "Rigid schedules or limited flexibility during the school day", "1", Replacer.ReplaceValue, {"Self Care Barrier: Rigid schedules or limited flexibility during the school day"}),
  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", null, "0", Replacer.ReplaceValue, {"Self Care Barrier: Rigid schedules or limited flexibility during the school day"}),
  #"Replaced value 13" = Table.ReplaceValue(#"Replaced value 12", null, "0", Replacer.ReplaceValue, {"Self Care Barrier: Difficulty prioritizing personal needs"}),
  #"Replaced value 14" = Table.ReplaceValue(#"Replaced value 13", "Difficulty prioritizing personal needs", "1", Replacer.ReplaceValue, {"Self Care Barrier: Difficulty prioritizing personal needs"}),
  #"Replaced value 15" = Table.ReplaceValue(#"Replaced value 14", "Financial strain (such as cost of care or cost of living)", "1", Replacer.ReplaceValue, {"Self Care Barrier: Financial strain (such as cost of care or cost of living)"}),
  #"Replaced value 16" = Table.ReplaceValue(#"Replaced value 15", null, "0", Replacer.ReplaceValue, {"Self Care Barrier: Financial strain (such as cost of care or cost of living)"}),
  #"Replaced value 17" = Table.ReplaceValue(#"Replaced value 16", "Limited access to mental health supports", "1", Replacer.ReplaceValue, {"Self Care Barrier: Limited access to mental health supports"}),
  #"Replaced value 18" = Table.ReplaceValue(#"Replaced value 17", null, "0", Replacer.ReplaceValue, {"Self Care Barrier: Limited access to mental health supports"}),
  #"Replaced value 19" = Table.ReplaceValue(#"Replaced value 18", null, "0", Replacer.ReplaceValue, {"Self Care Barrier: Insufficient paid time off"}),
  #"Replaced value 20" = Table.ReplaceValue(#"Replaced value 19", "Sometimes I have a lot to juggle but for the most part I have enough self care time.", "1", Replacer.ReplaceValue, {"Self Care Barrier: Other (please specify)"}),
  #"Replaced value 21" = Table.ReplaceValue(#"Replaced value 20", "Death of family member/mental health", "1", Replacer.ReplaceValue, {"Self Care Barrier: Other (please specify)"}),
  #"Replaced value 22" = Table.ReplaceValue(#"Replaced value 21", null, "0", Replacer.ReplaceValue, {"Self Care Barrier: Other (please specify)"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 22", {{"Self Care Barrier: Other (please specify)", type number}, {"Self Care Barrier: Insufficient paid time off", type number}, {"Self Care Barrier: Limited access to mental health supports", type number}, {"Self Care Barrier: Financial strain (such as cost of care or cost of living)", type number}, {"Self Care Barrier: Difficulty prioritizing personal needs", type number}, {"Self Care Barrier: Rigid schedules or limited flexibility during the school day", type number}, {"Self Care Barrier: High caseload or workload demands", type number}, {"Self Care Barrier: Difficulty disconnecting from work", type number}, {"Self Care Barrier: Compassion fatigue", type number}, {"Self Care Barrier: Secondary or vicarious trauma", type number}, {"Self Care Barrier: Burnout", type number}, {"Self Care Barrier: Limited time", type number}}),
  #"Unpivoted columns" = Table.UnpivotOtherColumns(#"Changed column type", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}, "Attribute", "Value")
in
  #"Unpivoted columns";
[BindToDefaultDestination = true]
shared self_care_aggregated = let
  Source = self_care,
  #"Grouped rows" = Table.Group(Source, {"Attribute"}, {{"Count", each List.Sum([Value]), type nullable number}}),
  #"Extracted text after delimiter" = Table.TransformColumns(#"Grouped rows", {{"Attribute", each Text.AfterDelimiter(_, ": ", 0), type text}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Extracted text after delimiter", "total_surveyed", each 16), {{"total_surveyed", type number}}),
  #"Inserted division" = Table.AddColumn(#"Added custom", "Division", each [Count] / [total_surveyed], type number),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted division", {{"Division", Percentage.Type}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared define_success = let
  Source = needs_assessment,
  #"Removed other columns" = Table.SelectColumns(Source, {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name", "Define Success: Equipping students to meet their goals", "Define Success: Serving families and meeting their needs", "Define Success: Ensuring equitable access to supports and resources", "Define Success: Impacting school climate", "Define Success: Meeting required caseload", "Define Success: Meeting schoolwide goals", "Define Success: Ensuring integration and coordination of supports across providers within the school", "Define Success: Ensuring the work of CIS is integrated with the broader system(s) of support within the school", "Define Success: Other (please describe)"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed other columns", "Equipping students to meet their goals", "1", Replacer.ReplaceValue, {"Define Success: Equipping students to meet their goals"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "Serving families and meeting their needs", "1", Replacer.ReplaceValue, {"Define Success: Serving families and meeting their needs"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "Ensuring equitable access to supports and resources", "1", Replacer.ReplaceValue, {"Define Success: Ensuring equitable access to supports and resources"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "Impacting school climate", "1", Replacer.ReplaceValue, {"Define Success: Impacting school climate"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "Meeting required caseload", "1", Replacer.ReplaceValue, {"Define Success: Meeting required caseload"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "Meeting schoolwide goals", "1", Replacer.ReplaceValue, {"Define Success: Meeting schoolwide goals"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "Ensuring integration and coordination of supports across providers within the school", "1", Replacer.ReplaceValue, {"Define Success: Ensuring integration and coordination of supports across providers within the school"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", "Ensuring the work of CIS is integrated with the broader system(s) of support within the school", "1", Replacer.ReplaceValue, {"Define Success: Ensuring the work of CIS is integrated with the broader system(s) of support within the school"}),
  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 7", "Goals necessarily do not have to be met to define success, as long as they are improving to reach the goal. ", "1", Replacer.ReplaceValue, {"Define Success: Other (please describe)"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 8", null, "0", Replacer.ReplaceValue, {"Define Success: Other (please describe)"}),
  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 9", null, "0", Replacer.ReplaceValue, {"Define Success: Ensuring the work of CIS is integrated with the broader system(s) of support within the school"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", null, "0", Replacer.ReplaceValue, {"Define Success: Ensuring integration and coordination of supports across providers within the school"}),
  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", null, "0", Replacer.ReplaceValue, {"Define Success: Meeting schoolwide goals"}),
  #"Replaced value 13" = Table.ReplaceValue(#"Replaced value 12", null, "0", Replacer.ReplaceValue, {"Define Success: Meeting required caseload"}),
  #"Replaced value 14" = Table.ReplaceValue(#"Replaced value 13", null, "0", Replacer.ReplaceValue, {"Define Success: Impacting school climate"}),
  #"Replaced value 15" = Table.ReplaceValue(#"Replaced value 14", null, "0", Replacer.ReplaceValue, {"Define Success: Ensuring equitable access to supports and resources"}),
  #"Replaced value 16" = Table.ReplaceValue(#"Replaced value 15", null, "0", Replacer.ReplaceValue, {"Define Success: Serving families and meeting their needs"}),
  #"Replaced value 17" = Table.ReplaceValue(#"Replaced value 16", null, "0", Replacer.ReplaceValue, {"Define Success: Equipping students to meet their goals"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 17", {{"Define Success: Equipping students to meet their goals", type number}, {"Define Success: Serving families and meeting their needs", type number}, {"Define Success: Ensuring equitable access to supports and resources", type number}, {"Define Success: Impacting school climate", type number}, {"Define Success: Meeting required caseload", type number}, {"Define Success: Meeting schoolwide goals", type number}, {"Define Success: Ensuring integration and coordination of supports across providers within the school", type number}, {"Define Success: Ensuring the work of CIS is integrated with the broader system(s) of support within the school", type number}, {"Define Success: Other (please describe)", type number}}),
  #"Unpivoted columns" = Table.UnpivotOtherColumns(#"Changed column type", {"Total number of years working with youth", "Highest level of education completed:", "CIS Organization Name"}, "Attribute", "Value"),
  #"Extracted text after delimiter" = Table.TransformColumns(#"Unpivoted columns", {{"Attribute", each Text.AfterDelimiter(_, ": ", 0), type text}})
in
  #"Extracted text after delimiter";
[BindToDefaultDestination = true]
shared define_success_aggregated = let
  Source = define_success,
  #"Grouped rows" = Table.Group(Source, {"Attribute"}, {{"Count", each List.Sum([Value]), type nullable number}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Grouped rows", "total_surveyed", each 16), {{"total_surveyed", type number}}),
  #"Inserted division" = Table.AddColumn(#"Added custom", "Division", each [Count] / [total_surveyed], type number),
  #"Renamed columns" = Table.RenameColumns(#"Inserted division", {{"Division", "percent"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"percent", Percentage.Type}})
in
  #"Changed column type";
