[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared raw_paf_submissions = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "raw-data"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "raw_data"]}[Content],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", each ([Name] = "paf_submissions")),
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Filtered rows", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Content", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"paF_ID", type text}, {"submissionDate", type datetime}, {"submittedBy", type text}, {"employeeData.name", type text}, {"employeeData.employeeID", type text}, {"employeeData.department", type text}, {"employeeData.currentPosition", type text}, {"employeeData.newPosition", type text}, {"employeeData.currentSalary", type text}, {"employeeData.newSalary", type text}, {"employeeData.supervisor", type text}, {"actionDetails.actionType", type text}, {"actionDetails.actionTypeName", type text}, {"actionDetails.effectiveDate", type date}, {"actionDetails.justification", type text}, {"actionDetails.budgetImpact", type text}, {"actionDetails.hrApprovalAcknowledged", type logical}, {"status", type text}, {"processingNotes", type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"actionDetails.actionType", "paf_type"}, {"employeeData.supervisor", "supervisor"}, {"actionDetails.actionTypeName", "paf_type_name"}, {"actionDetails.effectiveDate", "effective_date"}, {"actionDetails.justification", "justification"}, {"actionDetails.budgetImpact", "budget_impact"}, {"actionDetails.hrApprovalAcknowledged", "hr_approval"}, {"processingNotes", "processing_notes"}, {"submittedBy", "submitted_by"}, {"employeeData.name", "emp_name"}, {"employeeData.employeeID", "emp_id"}, {"employeeData.department", "department"}, {"employeeData.currentPosition", "current_position"}, {"employeeData.newPosition", "new_position"}, {"employeeData.currentSalary", "current_salary"}, {"employeeData.newSalary", "new_salary"}})
in
  #"Renamed columns";
[BindToDefaultDestination = true]
shared raw_po_submissions = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "raw-data"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "raw_data"]}[Content],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", each ([Name] = "po_submissions")),
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Filtered rows", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Content", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (2)", each #"Transform file (2)"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (2)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (2)", Table.ColumnNames(#"Transform file (2)"(#"Sample file (2)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"submissionDate", type datetime}, {"submittedBy", type text}, {"poNumber", type text}, {"orderData.department", type text}, {"orderData.digitalSignatureData", type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"submissionDate", "submission_date"}, {"submittedBy", "submitted_by"}, {"poNumber", "po_number"}, {"orderData.poDate", "po_date"}, {"orderData.dueDate", "due_date"}, {"orderData.department", "department"}, {"orderData.requestedBy", "requested_by"}, {"orderData.approvedBy", "approved_by"}, {"orderData.vendorName", "vendor_name"}, {"orderData.vendorId", "vendor_id"}, {"orderData.vendorEmail", "vendor_email"}, {"orderData.vendorPhone", "vendor_phone"}, {"orderData.vendorAddress", "vendor_address"}}),
  #"Expanded orderData.lineItems" = Table.ExpandListColumn(#"Renamed columns", "orderData.lineItems"),
  #"Renamed columns 1" = Table.RenameColumns(#"Expanded orderData.lineItems", {{"orderData.lineItems", "line_items"}, {"orderData.subTotal", "subtotal"}, {"orderData.taxAmount", "tax_amount"}, {"orderData.shippingAmount", "shipping_amount"}, {"orderData.totalAmount", "total_amount"}, {"orderData.digitalSignatureData", "digital_signature"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns 1", {"attachments"})
in
  #"Removed columns";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "raw-data"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "raw_data"]}[Content],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", each [Name] = "paf_submissions"),
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Filtered rows", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Content", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Json.Document(Parameter),
  #"Converted to table" = Table.FromRecords({Source}),
  #"Expanded employeeData" = Table.ExpandRecordColumn(#"Converted to table", "employeeData", {"name", "employeeID", "department", "currentPosition", "newPosition", "currentSalary", "newSalary", "supervisor"}, {"employeeData.name", "employeeData.employeeID", "employeeData.department", "employeeData.currentPosition", "employeeData.newPosition", "employeeData.currentSalary", "employeeData.newSalary", "employeeData.supervisor"}),
  #"Expanded actionDetails" = Table.ExpandRecordColumn(#"Expanded employeeData", "actionDetails", {"actionType", "actionTypeName", "effectiveDate", "justification", "budgetImpact", "hrApprovalAcknowledged"}, {"actionDetails.actionType", "actionDetails.actionTypeName", "actionDetails.effectiveDate", "actionDetails.justification", "actionDetails.budgetImpact", "actionDetails.hrApprovalAcknowledged"}),
  #"Expanded processingNotes" = Table.ExpandListColumn(#"Expanded actionDetails", "processingNotes")
in
  #"Expanded processingNotes";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Json.Document(Parameter),
  #"Converted to table" = Table.FromRecords({Source}),
  #"Expanded employeeData" = Table.ExpandRecordColumn(#"Converted to table", "employeeData", {"name", "employeeID", "department", "currentPosition", "newPosition", "currentSalary", "newSalary", "supervisor"}, {"employeeData.name", "employeeData.employeeID", "employeeData.department", "employeeData.currentPosition", "employeeData.newPosition", "employeeData.currentSalary", "employeeData.newSalary", "employeeData.supervisor"}),
  #"Expanded actionDetails" = Table.ExpandRecordColumn(#"Expanded employeeData", "actionDetails", {"actionType", "actionTypeName", "effectiveDate", "justification", "budgetImpact", "hrApprovalAcknowledged"}, {"actionDetails.actionType", "actionDetails.actionTypeName", "actionDetails.effectiveDate", "actionDetails.justification", "actionDetails.budgetImpact", "actionDetails.hrApprovalAcknowledged"}),
  #"Expanded processingNotes" = Table.ExpandListColumn(#"Expanded actionDetails", "processingNotes")
in
  #"Expanded processingNotes";
shared #"Sample file (2)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "raw-data"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "raw_data"]}[Content],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", each [Name] = "po_submissions"),
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Filtered rows", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Expanded Content" = Table.ExpandTableColumn(#"Expanded Attributes", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes", "Folder Path.1"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Content", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 5";
shared #"Parameter (2)" = let
  #"Parameter (2)" = #"Sample file (2)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (2)"]
in
  #"Parameter (2)";
shared #"Transform Sample file (2)" = let
  Source = Json.Document(#"Parameter (2)"),
  #"Converted to table" = Table.FromRecords({Source}),
  #"Expanded orderData" = Table.ExpandRecordColumn(#"Converted to table", "orderData", {"poDate", "dueDate", "department", "requestedBy", "approvedBy", "vendorName", "vendorId", "vendorEmail", "vendorPhone", "vendorAddress", "lineItems", "subTotal", "taxAmount", "shippingAmount", "totalAmount", "digitalSignatureData", "signedBy", "signatureDate", "signatureConsent", "specialInstructions", "paymentTerms", "shippingMethod"}, {"orderData.poDate", "orderData.dueDate", "orderData.department", "orderData.requestedBy", "orderData.approvedBy", "orderData.vendorName", "orderData.vendorId", "orderData.vendorEmail", "orderData.vendorPhone", "orderData.vendorAddress", "orderData.lineItems", "orderData.subTotal", "orderData.taxAmount", "orderData.shippingAmount", "orderData.totalAmount", "orderData.digitalSignatureData", "orderData.signedBy", "orderData.signatureDate", "orderData.signatureConsent", "orderData.specialInstructions", "orderData.paymentTerms", "orderData.shippingMethod"})
in
  #"Expanded orderData";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (2)""}"]
shared #"Transform file (2)" = (#"Parameter (2)" as binary) => let
  Source = Json.Document(#"Parameter (2)"),
  #"Converted to table" = Table.FromRecords({Source}),
  #"Expanded orderData" = Table.ExpandRecordColumn(#"Converted to table", "orderData", {"poDate", "dueDate", "department", "requestedBy", "approvedBy", "vendorName", "vendorId", "vendorEmail", "vendorPhone", "vendorAddress", "lineItems", "subTotal", "taxAmount", "shippingAmount", "totalAmount", "digitalSignatureData", "signedBy", "signatureDate", "signatureConsent", "specialInstructions", "paymentTerms", "shippingMethod"}, {"orderData.poDate", "orderData.dueDate", "orderData.department", "orderData.requestedBy", "orderData.approvedBy", "orderData.vendorName", "orderData.vendorId", "orderData.vendorEmail", "orderData.vendorPhone", "orderData.vendorAddress", "orderData.lineItems", "orderData.subTotal", "orderData.taxAmount", "orderData.shippingAmount", "orderData.totalAmount", "orderData.digitalSignatureData", "orderData.signedBy", "orderData.signatureDate", "orderData.signatureConsent", "orderData.specialInstructions", "orderData.paymentTerms", "orderData.shippingMethod"})
in
  #"Expanded orderData";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data];
