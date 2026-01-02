[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "SSVF Workflow_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"SSVF Workflow" = let
  Source = SharePoint.Tables("https://voanorthlaorg.sharepoint.com/sites/TheVOAHub", [ApiVersion = 15]),
  #"Navigation 1" = Source{[Id = "353ee74d-3bbb-44f8-b7cc-f3e59a3957c0"]}[Items],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 1", {"ServerRedirectedEmbedUrl", "FileSystemObjectType", "ServerRedirectedEmbedUri", "ContentTypeId", "OData__ColorTag", "ComplianceAssetId"}),
  #"Sorted rows" = Table.Sort(#"Removed columns", {{"DateCompleted", Order.Descending}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Sorted rows", {{"DateCompleted", type datetime}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Changed column type", {{"DateCompleted", type date}, {"Amount", Currency.Type}, {"AssistanceMonth", type text}, {"ClientCategory", type text}, {"ClientType", type text}, {"ClientLastName", type text}, {"ClientFirstName", type text}, {"Vendor", type text}, {"Title", type text}, {"DateSubmitted", type datetime}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Changed column type 1", {{"DateSubmitted", type date}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type 2", {{"ClientFirstName", "first_name"}, {"ClientLastName", "last_name"}, {"ClientType", "client_type"}, {"ClientCategory", "client_cat"}, {"Amount", "amount"}, {"AssistanceMonth", "month"}, {"DateCompleted", "date_completed"}, {"Status", "status"}}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Renamed columns", {{"status", type text}, {"Requester", type text}, {"LSNDCClientID", type text}, {"CheckNumber", type text}, {"Entered", type text}, {"FinancialAssistanceType1", type text}, {"ServicePointClientID", type text}, {"ID", type text}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Changed column type 3", {"Modified", "Created", "AuthorId", "EditorId", "OData__UIVersionString", "GUID"}),
  #"Expanded AttachmentFiles" = Table.ExpandTableColumn(#"Removed columns 1", "AttachmentFiles", {"FileName", "FileNameAsPath", "ServerRelativePath", "ServerRelativeUrl"}, {"FileName", "FileNameAsPath", "ServerRelativePath", "ServerRelativeUrl"}),
  #"Expanded FileNameAsPath" = Table.ExpandRecordColumn(#"Expanded AttachmentFiles", "FileNameAsPath", {"DecodedUrl"}, {"DecodedUrl"}),
  #"Expanded ServerRelativePath" = Table.ExpandRecordColumn(#"Expanded FileNameAsPath", "ServerRelativePath", {"DecodedUrl"}, {"DecodedUrl.1"}),
  #"Removed columns 2" = Table.RemoveColumns(#"Expanded ServerRelativePath", {"ContentType", "GetDlpPolicyTip", "FieldValuesAsHtml", "FieldValuesAsText", "FieldValuesForEdit", "File", "Folder", "LikedByInformation", "ParentList", "Properties", "Versions", "Author", "Editor"}),
  #"Changed column type 4" = Table.TransformColumnTypes(#"Removed columns 2", {{"ServerRelativeUrl", type text}, {"DecodedUrl.1", type text}, {"DecodedUrl", type text}, {"FileName", type text}}),
  #"Removed columns 3" = Table.RemoveColumns(#"Changed column type 4", {"RoleAssignments", "FirstUniqueAncestorSecurableObject"}),
  #"Inserted merged column" = Table.AddColumn(#"Removed columns 3", "client_name", each Text.Combine({[last_name], [first_name]}, ", "), type text),
  #"Reordered columns" = Table.ReorderColumns(#"Inserted merged column", {"client_name", "Id", "Title", "Vendor", "DateSubmitted", "first_name", "last_name", "client_type", "client_cat", "amount", "month", "date_completed", "status", "Requester", "LSNDCClientID", "FinancialAssistanceType1", "Entered", "CheckNumber", "ServicePointClientID", "ID", "Attachments", "FileName", "DecodedUrl", "DecodedUrl.1", "ServerRelativeUrl"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Reordered columns", {{"LSNDCClientID", "lsndc_client_id"}, {"ServicePointClientID", "sp_client_id"}, {"ID", "workflow_id"}, {"CheckNumber", "check_num"}}),
  #"Removed columns 4" = Table.RemoveColumns(#"Renamed columns 1", {"Entered"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 4", {{"FinancialAssistanceType1", "financial_assist_type"}, {"Requester", "requester"}, {"FileName", "file_name"}}),
  #"Removed columns 5" = Table.RemoveColumns(#"Renamed columns 2", {"DecodedUrl"}),
  #"Renamed columns 3" = Table.RenameColumns(#"Removed columns 5", {{"DecodedUrl.1", "file_relative_path"}}),
  #"Changed column type 5" = Table.TransformColumnTypes(#"Renamed columns 3", {{"Attachments", type logical}}),
  #"Removed columns 6" = Table.RemoveColumns(#"Changed column type 5", {"ServerRelativeUrl"}),
  #"Changed column type 6" = Table.TransformColumnTypes(#"Removed columns 6", {{"Id", type text}})
in
  #"Changed column type 6";
shared #"SSVF Workflow_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0a6e0b32-26e7-4e4e-891d-833423d2e910"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_ssvf_workflow", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"SSVF Workflow (2)" = let
    // 1. Start from your existing query
    Source = #"SSVF Workflow",

    // 2. Get distinct vendor names only
    DistinctVendors =
        Table.Distinct(
            Table.SelectColumns(Source, {"Vendor"})
        ),

    // 3. Fuzzy-group vendor names into clusters
    //    Each row here is a cluster with a representative Vendor and a nested table of group members
    GroupedVendors =
        Table.FuzzyGroup(
            DistinctVendors,
            {"Vendor"},
            {{"AllVendors", each _, type table [Vendor = text]}},
            [
                IgnoreCase = true,
                IgnoreSpace = true,
                Threshold  = 0.7      // tweak: higher = stricter, lower = looser
            ]
        ),

    // 4. Expand to get mapping: CanonicalVendor -> OriginalVendor
    ExpandedVendorGroups =
        Table.ExpandTableColumn(
            GroupedVendors,
            "AllVendors",
            {"Vendor"},
            {"OriginalVendor"}
        ),

    // 5. Rename representative column to CanonicalVendor
    VendorMapping =
        Table.RenameColumns(
            ExpandedVendorGroups,
            {{"Vendor", "CanonicalVendor"}}
        ),

    // 6. Merge mapping back to the original Source on Vendor
    MergedVendors =
        Table.NestedJoin(
            Source,
            {"Vendor"},
            VendorMapping,
            {"OriginalVendor"},
            "VendorMap",
            JoinKind.LeftOuter
        ),

    ExpandedVendorMap =
        Table.ExpandTableColumn(
            MergedVendors,
            "VendorMap",
            {"CanonicalVendor"},
            {"CanonicalVendor"}
        ),
  #"Capitalized each word" = Table.TransformColumns(ExpandedVendorMap, {{"CanonicalVendor", each Text.Proper(_), type nullable text}}),
  #"Replaced value 1" = Table.ReplaceValue(#"Capitalized each word", "encore on kingston", "Encore at Kingston", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value" = Table.ReplaceValue(#"Replaced value 1", "Encore At kingston", "Encore At Kingston", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value", "DOWUS CoS Water", "DOWUS", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "links at texarkana", "Links at Texarkana", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "nantuckett", "Nantuckett", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "Loan oak oasis", "Lone Oak Oasis", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "MacAdoo Apartments - VOA ", "McAdoo", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", "Lifestyle apartments", "Lifestyle Apartments", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 7", "MacAdoo ", "McAdoo", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 9", "ul coleman", "UL Coleman", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 8", "United jewerls", "United Jewelers", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", "Lesson real estate firm", "Lesson", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", "River run", "River Run", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 13" = Table.ReplaceValue(#"Replaced value 12", "lankmark realty", "Landmark Realty", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Sorted rows" = Table.Sort(#"Replaced value 13", {{"CanonicalVendor", Order.Ascending}}),
  #"Replaced value 14" = Table.ReplaceValue(#"Sorted rows", "80 Eight Llc", "80 Eight LLC", Replacer.ReplaceValue, {"CanonicalVendor"})

in
    #"Replaced value 14";
shared #"SSVF Workflow (3)" = let
  // 1. Start from your existing query
  Source = #"SSVF Workflow",
  // 2. Get distinct vendor names only
  DistinctVendors = Table.Distinct(
            Table.SelectColumns(Source, {"Vendor"})
        ),
  // 3. Fuzzy-group vendor names into clusters
  // Each row here is a cluster with a representative Vendor and a nested table of group members
  GroupedVendors = Table.FuzzyGroup(DistinctVendors, {"Vendor"}, {{"AllVendors", each _, type table[Vendor = text]}}, [IgnoreCase = true, IgnoreSpace = true, Threshold = 0.7]),
  // 4. Expand to get mapping: CanonicalVendor -> OriginalVendor
  ExpandedVendorGroups = Table.ExpandTableColumn(GroupedVendors, "AllVendors", {"Vendor"}, {"OriginalVendor"}),
  // 5. Rename representative column to CanonicalVendor
  VendorMapping = Table.RenameColumns(ExpandedVendorGroups, {{"Vendor", "CanonicalVendor"}}),
  // 6. Merge mapping back to the original Source on Vendor
  MergedVendors = Table.NestedJoin(Source, {"Vendor"}, VendorMapping, {"OriginalVendor"}, "VendorMap", JoinKind.LeftOuter),
  ExpandedVendorMap = Table.ExpandTableColumn(MergedVendors, "VendorMap", {"CanonicalVendor"}, {"CanonicalVendor"}),
  #"Capitalized each word" = Table.TransformColumns(ExpandedVendorMap, {{"CanonicalVendor", each Text.Proper(_), type nullable text}}),
  #"Replaced value 1" = Table.ReplaceValue(#"Capitalized each word", "encore on kingston", "Encore at Kingston", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value" = Table.ReplaceValue(#"Replaced value 1", "Encore At kingston", "Encore At Kingston", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value", "DOWUS CoS Water", "DOWUS", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 14" = Table.ReplaceValue(#"Replaced value 2", "80 Eight Llc", "80 Eight LLC", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 14", "links at texarkana", "Links at Texarkana", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "nantuckett", "Nantuckett", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", "Loan oak oasis", "Lone Oak Oasis", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", "MacAdoo Apartments - VOA ", "McAdoo", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", "Lifestyle apartments", "Lifestyle Apartments", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 7", "MacAdoo ", "McAdoo", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 9", "ul coleman", "UL Coleman", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 8", "United jewerls", "United Jewelers", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", "Lesson real estate firm", "Lesson", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", "River run", "River Run", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 13" = Table.ReplaceValue(#"Replaced value 12", "lankmark realty", "Landmark Realty", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Sorted rows" = Table.Sort(#"Replaced value 13", {{"CanonicalVendor", Order.Ascending}}),
  #"Replaced value 15" = Table.ReplaceValue(#"Sorted rows", "Aep Swepco", "SWEPCO", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 16" = Table.ReplaceValue(#"Replaced value 15", "American Electric Power Company", "SWEPCO", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 17" = Table.ReplaceValue(#"Replaced value 16", "Aj Realestate Holdings", "AJ Realestate Holdings", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 19" = Table.ReplaceValue(#"Replaced value 17", "Dowus Cos Water", "DOWUS", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 18" = Table.ReplaceValue(#"Replaced value 19", "Dowus", "DOWUS", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 20" = Table.ReplaceValue(#"Replaced value 18", "Loan Oak Oasis", "Lone Oak Oasis", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 21" = Table.ReplaceValue(#"Replaced value 20", "Fairmont Housing Associates", "Fairmont Apartment", Replacer.ReplaceValue, {"CanonicalVendor"}),
  #"Replaced value 22" = Table.ReplaceValue(#"Replaced value 21", "Llc ", "LLC", Replacer.ReplaceText, {"CanonicalVendor"})
in
  #"Replaced value 22";
