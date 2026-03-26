[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared #"Behavioral Health Docs" = let
    Source = SharePoint.Contents("https://voanorthlaorg.sharepoint.com/sites/TheVOAHub/"),
    Library = Source{[Name="Committees"]}[Content],
    Folder1 = Library{[Name="QA Committee"]}[Content],
  #"Filtered rows" = Table.SelectRows(Folder1, each ([Name] = "Program Documentation")),
  Custom = #"Filtered rows"{[Name="Program Documentation"]}[Content],
  #"Custom 1" = Custom{[Name="Behavioral Health"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Custom 1", "Attributes", {"Content Type", "Kind", "Size", "ContentTypeID", "Name", "ColorTag", "ComplianceAssetId", "Title", "Description", "SharedWithDetails", "ExtractedText", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "Id", "ContentType", "Created", "CreatedById", "ModifiedById", "CopySource", "ApprovalStatus", "Path", "CheckedOutToId", "VirusStatus", "IsCurrentVersion", "Owshiddenversion", "Version", "SharedWith", "Division", "DocumentationType", "ProgramS", "CreatedBy", "ModifiedBy", "CheckedOutTo"}, {"Content Type", "Kind", "Size", "ContentTypeID", "Name.1", "ColorTag", "ComplianceAssetId", "Title", "Description", "SharedWithDetails", "ExtractedText", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "Id", "ContentType", "Created", "CreatedById", "ModifiedById", "CopySource", "ApprovalStatus", "Path", "CheckedOutToId", "VirusStatus", "IsCurrentVersion", "Owshiddenversion", "Version", "SharedWith", "Division", "DocumentationType", "ProgramS", "CreatedBy", "ModifiedBy", "CheckedOutTo"}),
  #"Removed other columns" = Table.SelectColumns(#"Expanded Attributes", {"Name", "Date modified", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "ProgramS", "Folder Path"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed other columns", {{"Modified", type datetime}, {"DivisionValue", type text}, {"DocumentationTypeValue", type text}, {"Details", type text}, {"RevisionDate", type date}}),
  #"Expanded ProgramS" = Table.ExpandTableColumn(#"Changed column type", "ProgramS", {"Value"}, {"programs"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Expanded ProgramS", {{"programs", type text}})
in
    #"Changed column type 1";
[BindToDefaultDestination = true]
shared #"CF Docs" = let
    Source = SharePoint.Contents("https://voanorthlaorg.sharepoint.com/sites/TheVOAHub/"),
    Library = Source{[Name="Committees"]}[Content],
    Folder1 = Library{[Name="QA Committee"]}[Content],
  #"Filtered rows" = Table.SelectRows(Folder1, each ([Name] = "Program Documentation")),
  Custom = #"Filtered rows"{[Name="Program Documentation"]}[Content],
  #"Custom 1" = Custom{[Name="Children and Families"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Custom 1", "Attributes", {"Content Type", "Kind", "Size", "ContentTypeID", "Name", "ColorTag", "ComplianceAssetId", "Title", "Description", "SharedWithDetails", "ExtractedText", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "Id", "ContentType", "Created", "CreatedById", "ModifiedById", "CopySource", "ApprovalStatus", "Path", "CheckedOutToId", "VirusStatus", "IsCurrentVersion", "Owshiddenversion", "Version", "SharedWith", "Division", "DocumentationType", "ProgramS", "CreatedBy", "ModifiedBy", "CheckedOutTo"}, {"Content Type", "Kind", "Size", "ContentTypeID", "Name.1", "ColorTag", "ComplianceAssetId", "Title", "Description", "SharedWithDetails", "ExtractedText", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "Id", "ContentType", "Created", "CreatedById", "ModifiedById", "CopySource", "ApprovalStatus", "Path", "CheckedOutToId", "VirusStatus", "IsCurrentVersion", "Owshiddenversion", "Version", "SharedWith", "Division", "DocumentationType", "ProgramS", "CreatedBy", "ModifiedBy", "CheckedOutTo"}),
  #"Removed other columns" = Table.SelectColumns(#"Expanded Attributes", {"Name", "Date modified", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "ProgramS", "Folder Path"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed other columns", {{"Modified", type datetime}, {"DivisionValue", type text}, {"DocumentationTypeValue", type text}, {"Details", type text}, {"RevisionDate", type date}}),
  #"Custom 2" = Table.ExpandTableColumn(#"Changed column type", "ProgramS", {"Value"}, {"programs"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Custom 2", {{"programs", type text}})
in
    #"Changed column type 1";
[BindToDefaultDestination = true]
shared #"Disability Docs" = let
    Source = SharePoint.Contents("https://voanorthlaorg.sharepoint.com/sites/TheVOAHub/"),
    Library = Source{[Name="Committees"]}[Content],
    Folder1 = Library{[Name="QA Committee"]}[Content],
  #"Filtered rows" = Table.SelectRows(Folder1, each ([Name] = "Program Documentation")),
  Custom = #"Filtered rows"{[Name="Program Documentation"]}[Content],
  #"Custom 1" = Custom{[Name="Disability Services"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Custom 1", "Attributes", {"Content Type", "Kind", "Size", "ContentTypeID", "Name", "ColorTag", "ComplianceAssetId", "Title", "Description", "SharedWithDetails", "ExtractedText", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "Id", "ContentType", "Created", "CreatedById", "ModifiedById", "CopySource", "ApprovalStatus", "Path", "CheckedOutToId", "VirusStatus", "IsCurrentVersion", "Owshiddenversion", "Version", "SharedWith", "Division", "DocumentationType", "ProgramS", "CreatedBy", "ModifiedBy", "CheckedOutTo"}, {"Content Type", "Kind", "Size", "ContentTypeID", "Name.1", "ColorTag", "ComplianceAssetId", "Title", "Description", "SharedWithDetails", "ExtractedText", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "Id", "ContentType", "Created", "CreatedById", "ModifiedById", "CopySource", "ApprovalStatus", "Path", "CheckedOutToId", "VirusStatus", "IsCurrentVersion", "Owshiddenversion", "Version", "SharedWith", "Division", "DocumentationType", "ProgramS", "CreatedBy", "ModifiedBy", "CheckedOutTo"}),
  #"Removed other columns" = Table.SelectColumns(#"Expanded Attributes", {"Name", "Date modified", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "ProgramS", "Folder Path"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed other columns", {{"Modified", type datetime}, {"DivisionValue", type text}, {"DocumentationTypeValue", type text}, {"Details", type text}, {"RevisionDate", type date}}),
  #"Custom 2" = Table.ExpandTableColumn(#"Changed column type", "ProgramS", {"Value"}, {"programs"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Custom 2", {{"programs", type text}})
in
    #"Changed column type 1";
[BindToDefaultDestination = true]
shared #"Case Management Docs" = let
    Source = SharePoint.Contents("https://voanorthlaorg.sharepoint.com/sites/TheVOAHub/"),
    Library = Source{[Name="Committees"]}[Content],
    Folder1 = Library{[Name="QA Committee"]}[Content],
  #"Filtered rows" = Table.SelectRows(Folder1, each ([Name] = "Program Documentation")),
  Custom = #"Filtered rows"{[Name="Program Documentation"]}[Content],
  #"Custom 1" = Custom{[Name="Case Management"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Custom 1", "Attributes", {"Content Type", "Kind", "Size", "ContentTypeID", "Name", "ColorTag", "ComplianceAssetId", "Title", "Description", "SharedWithDetails", "ExtractedText", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "Id", "ContentType", "Created", "CreatedById", "ModifiedById", "CopySource", "ApprovalStatus", "Path", "CheckedOutToId", "VirusStatus", "IsCurrentVersion", "Owshiddenversion", "Version", "SharedWith", "Division", "DocumentationType", "ProgramS", "CreatedBy", "ModifiedBy", "CheckedOutTo"}, {"Content Type", "Kind", "Size", "ContentTypeID", "Name.1", "ColorTag", "ComplianceAssetId", "Title", "Description", "SharedWithDetails", "ExtractedText", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "Id", "ContentType", "Created", "CreatedById", "ModifiedById", "CopySource", "ApprovalStatus", "Path", "CheckedOutToId", "VirusStatus", "IsCurrentVersion", "Owshiddenversion", "Version", "SharedWith", "Division", "DocumentationType", "ProgramS", "CreatedBy", "ModifiedBy", "CheckedOutTo"}),
  #"Removed other columns" = Table.SelectColumns(#"Expanded Attributes", {"Name", "Date modified", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "ProgramS", "Folder Path"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed other columns", {{"Modified", type datetime}, {"DivisionValue", type text}, {"DocumentationTypeValue", type text}, {"Details", type text}, {"RevisionDate", type date}}),
  #"Custom 2" = Table.ExpandTableColumn(#"Changed column type", "ProgramS", {"Value"}, {"programs"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Custom 2", {{"programs", type text}})
in
    #"Changed column type 1";
[BindToDefaultDestination = true]
shared #"Housing Services Docs" = let
    Source = SharePoint.Contents("https://voanorthlaorg.sharepoint.com/sites/TheVOAHub/"),
    Library = Source{[Name="Committees"]}[Content],
    Folder1 = Library{[Name="QA Committee"]}[Content],
  #"Filtered rows" = Table.SelectRows(Folder1, each ([Name] = "Program Documentation")),
  Custom = #"Filtered rows"{[Name="Program Documentation"]}[Content],
  #"Custom 1" = Custom{[Name="Housing"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Custom 1", "Attributes", {"Content Type", "Kind", "Size", "ContentTypeID", "Name", "ColorTag", "ComplianceAssetId", "Title", "Description", "SharedWithDetails", "ExtractedText", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "Id", "ContentType", "Created", "CreatedById", "ModifiedById", "CopySource", "ApprovalStatus", "Path", "CheckedOutToId", "VirusStatus", "IsCurrentVersion", "Owshiddenversion", "Version", "SharedWith", "Division", "DocumentationType", "ProgramS", "CreatedBy", "ModifiedBy", "CheckedOutTo"}, {"Content Type", "Kind", "Size", "ContentTypeID", "Name.1", "ColorTag", "ComplianceAssetId", "Title", "Description", "SharedWithDetails", "ExtractedText", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "Id", "ContentType", "Created", "CreatedById", "ModifiedById", "CopySource", "ApprovalStatus", "Path", "CheckedOutToId", "VirusStatus", "IsCurrentVersion", "Owshiddenversion", "Version", "SharedWith", "Division", "DocumentationType", "ProgramS", "CreatedBy", "ModifiedBy", "CheckedOutTo"}),
  #"Removed other columns" = Table.SelectColumns(#"Expanded Attributes", {"Name", "Date modified", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "ProgramS", "Folder Path"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed other columns", {{"Modified", type datetime}, {"DivisionValue", type text}, {"DocumentationTypeValue", type text}, {"Details", type text}, {"RevisionDate", type date}}),
  #"Custom 2" = Table.ExpandTableColumn(#"Changed column type", "ProgramS", {"Value"}, {"programs"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Custom 2", {{"programs", type text}})
in
    #"Changed column type 1";
[BindToDefaultDestination = true]
shared #"HUD Operations Docs" = let
    Source = SharePoint.Contents("https://voanorthlaorg.sharepoint.com/sites/TheVOAHub/"),
    Library = Source{[Name="Committees"]}[Content],
    Folder1 = Library{[Name="QA Committee"]}[Content],
  #"Filtered rows" = Table.SelectRows(Folder1, each ([Name] = "Program Documentation")),
  Custom = #"Filtered rows"{[Name="Program Documentation"]}[Content],
  #"Custom 1" = Custom{[Name="HUD Operations"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Custom 1", "Attributes", {"Content Type", "Kind", "Size", "ContentTypeID", "Name", "ColorTag", "ComplianceAssetId", "Title", "Description", "SharedWithDetails", "ExtractedText", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "Id", "ContentType", "Created", "CreatedById", "ModifiedById", "CopySource", "ApprovalStatus", "Path", "CheckedOutToId", "VirusStatus", "IsCurrentVersion", "Owshiddenversion", "Version", "SharedWith", "Division", "DocumentationType", "ProgramS", "CreatedBy", "ModifiedBy", "CheckedOutTo"}, {"Content Type", "Kind", "Size", "ContentTypeID", "Name.1", "ColorTag", "ComplianceAssetId", "Title", "Description", "SharedWithDetails", "ExtractedText", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "Id", "ContentType", "Created", "CreatedById", "ModifiedById", "CopySource", "ApprovalStatus", "Path", "CheckedOutToId", "VirusStatus", "IsCurrentVersion", "Owshiddenversion", "Version", "SharedWith", "Division", "DocumentationType", "ProgramS", "CreatedBy", "ModifiedBy", "CheckedOutTo"}),
  #"Removed other columns" = Table.SelectColumns(#"Expanded Attributes", {"Name", "Date modified", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "ProgramS", "Folder Path"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed other columns", {{"Modified", type datetime}, {"DivisionValue", type text}, {"DocumentationTypeValue", type text}, {"Details", type text}, {"RevisionDate", type date}}),
  #"Custom 2" = Table.ExpandTableColumn(#"Changed column type", "ProgramS", {"Value"}, {"programs"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Custom 2", {{"programs", type text}})
in
    #"Changed column type 1";
[BindToDefaultDestination = true]
shared #"Veterans Docs" = let
    Source = SharePoint.Contents("https://voanorthlaorg.sharepoint.com/sites/TheVOAHub/"),
    Library = Source{[Name="Committees"]}[Content],
    Folder1 = Library{[Name="QA Committee"]}[Content],
  #"Filtered rows" = Table.SelectRows(Folder1, each ([Name] = "Program Documentation")),
  Custom = #"Filtered rows"{[Name="Program Documentation"]}[Content],
  #"Custom 1" = Custom{[Name="Veterans"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Custom 1", "Attributes", {"Content Type", "Kind", "Size", "ContentTypeID", "Name", "ColorTag", "ComplianceAssetId", "Title", "Description", "SharedWithDetails", "ExtractedText", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "Id", "ContentType", "Created", "CreatedById", "ModifiedById", "CopySource", "ApprovalStatus", "Path", "CheckedOutToId", "VirusStatus", "IsCurrentVersion", "Owshiddenversion", "Version", "SharedWith", "Division", "DocumentationType", "ProgramS", "CreatedBy", "ModifiedBy", "CheckedOutTo"}, {"Content Type", "Kind", "Size", "ContentTypeID", "Name.1", "ColorTag", "ComplianceAssetId", "Title", "Description", "SharedWithDetails", "ExtractedText", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "Id", "ContentType", "Created", "CreatedById", "ModifiedById", "CopySource", "ApprovalStatus", "Path", "CheckedOutToId", "VirusStatus", "IsCurrentVersion", "Owshiddenversion", "Version", "SharedWith", "Division", "DocumentationType", "ProgramS", "CreatedBy", "ModifiedBy", "CheckedOutTo"}),
  #"Removed other columns" = Table.SelectColumns(#"Expanded Attributes", {"Name", "Date modified", "Modified", "DivisionValue", "RevisionDate", "DocumentationTypeValue", "Details", "ProgramS", "Folder Path"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed other columns", {{"Modified", type datetime}, {"DivisionValue", type text}, {"DocumentationTypeValue", type text}, {"Details", type text}, {"RevisionDate", type date}}),
  #"Custom 2" = Table.ExpandTableColumn(#"Changed column type", "ProgramS", {"Value"}, {"programs"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Custom 2", {{"programs", type text}})
in
    #"Changed column type 1";
shared DefaultDestination = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared #"Appended Proram Docs" = let
  Source = Table.Combine({#"Veterans Docs", #"Behavioral Health Docs", #"CF Docs", #"Case Management Docs", #"Disability Docs", #"HUD Operations Docs", #"Housing Services Docs"})
in
  Source;
