[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared cleaned_caddo_grades = let
    Source = Lakehouse.Contents(null),
    #"Navigation 1" = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
    #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data],
    #"Navigation 3" = #"Navigation 2"{[Id = "caddo_grades_export", ItemKind = "Table"]}[Data],

    NormalizeText = (x as any) as text =>
        if x = null then "" else Text.Upper(Text.Trim(Text.Clean(Text.From(x)))),

    GetTokens = (x as any) as list =>
        let
            txt = if x = null then "" else Text.Trim(Text.Clean(Text.From(x))),
            tokens = List.Select(Text.Split(txt, " "), each _ <> "")
        in
            tokens,

    FirstTokenNumber = (x as any) as nullable number =>
        let
            tokens = GetTokens(x),
            first = if List.Count(tokens) > 0 then tokens{0} else null
        in
            try Number.From(first) otherwise null,

    LastTokenText = (x as any) as nullable text =>
        let
            tokens = GetTokens(x),
            last = if List.Count(tokens) > 0 then Text.Upper(tokens{List.Count(tokens) - 1}) else null
        in
            last,

    GradeToGPA = (x as any) as nullable number =>
        let
            g = LastTokenText(x)
        in
            if g = null or g = "" then null
            else if Text.StartsWith(g, "A") then 4
            else if Text.StartsWith(g, "B") then 3
            else if Text.StartsWith(g, "C") then 2
            else if Text.StartsWith(g, "D") then 1
            else if Text.StartsWith(g, "F") or Text.StartsWith(g, "E") then 0
            else null,

    CourseBucket = (x as any) as text =>
        let
            course = NormalizeText(x)
        in
            if Text.Contains(course, "LANG ART") then "ELA"
            else if Text.Contains(course, "LANG ARTS") then "ELA"
            else if Text.Contains(course, "CHEMISTRY") then "Science"
            else if Text.Contains(course, "LIFE SCIE") then "Science"
            else if Text.Contains(course, "SOC ST") then "Social Studies"
            else if Text.Contains(course, "ENGLISH") then "ELA"
            else if Text.Contains(course, "READING") then "ELA"
            else if Text.Contains(course, "BIOLOGY") then "Science"
            else if Text.Contains(course, "ALGEBRA") then "Math"
            else if Text.Contains(course, "GEOMETRY") then "Math"
            else if Text.Contains(course, "ELA") then "ELA"
            else if Text.Contains(course, "SCIENCE") then "Science"
            else if Text.Contains(course, "HEALTH") then "Elective"
            else if Text.Contains(course, "MUSIC") then "Elective"
            else if Text.Contains(course, "H&PE;ELEM GRADES") then "Elective"
            else if Text.Contains(course, "MATH") then "Math"
            else if Text.Contains(course, "SCI AL") then "Science"
            else if Text.Contains(course, "CONDUCT") then "Conduct"
            else if Text.Contains(course, "READ AL") then "ELA"
            else if Text.Contains(course, "HIST") then "History"
            else if Text.Contains(course, "PE ") then "Elective"
            else if Text.Contains(course, "ALT HOME") then "Elective"
            else if Text.Contains(course, "SPEECH") then "Elective"
            else if Text.Contains(course, "ADAPTIVE PE") then "Elective"
            else if Text.Contains(course, "BAND") then "Elective"
            else if Text.Contains(course, "LIFE SKILLS") then "Elective"
            else if Text.Contains(course, "STEM") then "Science"
            else if Text.Contains(course, "CHOIR") then "Elective"
            else if Text.Contains(course, "COMP LIT") then "Elective"
            else if Text.Contains(course, "STUDY SKILL") then "Elective"
            else if Text.Contains(course, "PIANO") then "Elective"
            else if Text.Contains(course, "QUEST") then "Elective"
            else if Text.Contains(course, "IBCA") then "Elective"
            else if Text.Contains(course, "ROBOTICS") then "Elective"
            else if Text.Contains(course, "ENG AL") then "ELA"
            else if Text.Contains(course, "ACADEMIC READ") then "Elective"
            else if Text.Contains(course, "GUITAR") then "Elective"
            else if Text.Contains(course, "CMC LAB") then "Elective"
            else if Text.Contains(course, "COMPUTER LIT") then "Elective"
            else if Text.Contains(course, "4 SUCC") then "Elective"
            else if Text.Contains(course, "ACAD READ") then "Elective"
            else if Text.Contains(course, "EUROP HIS") then "History"
            else if Text.Contains(course, "JAG") then "Elective"
            else if Text.Contains(course, "KEYBOARD") then "Elective"
            else if Text.Contains(course, "ENGR/COMP") then "ELA"
            else if Text.Contains(course, "SPANISH") then "Elective"
            else if Text.Contains(course, "WELDING") then "Elective"
            else if course = "FRENCH" then "Elective"
            else if Text.Contains(course, "BUSINESS") then "Elective"
            else if Text.Contains(course, "CYBER") then "Elective"
            else if Text.Contains(course, "FORENSICS") then "Elective"
            else if Text.Contains(course, "ENG ") then "ELA"
            else if Text.Contains(course, "BIO ") then "Science"
            else if Text.Contains(course, "HUM AN") then "Elective"
            else if Text.Contains(course, "PHYSICS") then "Science"
            else if Text.Contains(course, "GEOGRAPHY") then "History"
            else if Text.Contains(course, "LAW") then "Elective"
            else if Text.Contains(course, "ANIME") then "Elective"
            else if Text.Contains(course, "STATS") then "Math"
            else if Text.Contains(course, "CALC ") then "Math"
            else if Text.Contains(course, "PSYCH ") then "Elective"
            else if Text.Contains(course, "FINANCIAL") then "Elective"
            else if Text.Contains(course, "ACT ") then "Elective"
            else if Text.Contains(course, "WORLD GEO") then "History"
            else if Text.Contains(course, "MYTHOLOGY") then "Elective"
            else if Text.Contains(course, "MAC ECON") then "Elective"
            else if Text.Contains(course, "WORKPLACE") then "Elective"
            else if Text.Contains(course, "CC-FASH") then "Elective"
            else if Text.Contains(course, "CREAT WRIT") then "Elective"
            else if Text.Contains(course, "SOCIOLOGY") then "Elective"
            else if Text.Contains(course, "CHEM ") then "Science"
            else if Text.Contains(course, "INTR HLTH") then "Elective"
            else if Text.Contains(course, "DRAMA") then "Elective"
            else if Text.Contains(course, "BARBER") then "Elective"
            else if Text.Contains(course, "FINANC LIT") then "Elective"
            else if Text.Contains(course, "ANIMATION") then "Elective"
            else if Text.Contains(course, "BUS COMP") then "Elective"
            else if Text.Contains(course, "CALCULUS") then "Math"
            else if Text.Contains(course, "NC CDF") then "Elective"
            else if Text.Contains(course, "VIR WP") then "Elective"
            else if Text.Contains(course, "MARKETING") then "Elective"
            else if Text.Contains(course, "FRENCH ") then "Elective"
            else if Text.Contains(course, "TECHNICAL") then "Elective"
            else if Text.Contains(course, "SOCIAL MEDIA") then "Elective"
            else if Text.Contains(course, "FILM") then "Elective"
            else if Text.Contains(course, "SCIEN") then "Science"
            else if Text.Contains(course, "GERMAN") then "Elective"
            else if Text.Contains(course, "COMP SCI") then "Elective"
            else if Text.Contains(course, "CC-") then "Elective"
            else if Text.Contains(course, "JROTC") then "Elective"
            else if Text.Contains(course, "WRLD HIS") then "History"
            else if Text.Contains(course, "PHILOS") then "Elective"
            else if Text.Contains(course, "DIG GRPX") then "Elective"
            else if Text.Contains(course, "TRANS:") then "Elective"
            else if Text.Contains(course, "PRIN OF") then "Elective"
            else if Text.Contains(course, "MULTI") then "Elective"
            else if Text.Contains(course, "ENTREPREN") then "Elective"
            else if Text.Contains(course, "TECH READ") then "Elective"
            else if Text.Contains(course, "VIRTUAL") then "Elective"
            else if Text.Contains(course, "VOCAL") then "Elective"
            else if Text.Contains(course, "ASL") then "Elective"
            else if Text.Contains(course, "HUMAN ANT") then "Elective"
            else if Text.Contains(course, "PERSONAL FINA") then "Elective"
            else if Text.Contains(course, "ENTRE") then "Elective"
            else if Text.Contains(course, "ENGI") then "ELA"
            else if Text.Contains(course, "ALGEBR") then "Math"
            else if Text.Contains(course, "CIVICS") then "Civics"
            else if Text.Contains(course, "USHIS") then "History"
            else if Text.Contains(course, "LATIN") then "Elective"
            else if Text.Contains(course, "ENV SCI") then "Science"
            else if Text.Contains(course, "US HIS") then "History"
            else if Text.Contains(course, "WEST CIVIL") then "History"
            else if Text.Contains(course, "ART") then "Elective"
            else "",

    ExcludedCourses = {
        "1ST ROBOTICS I",
        "21ST CEN IS&AN E",
        "ACADEMIC READINE",
        "ACCOUNTING I E",
        "ACT PREP",
        "ACT PREP E",
        "ACT PREP E S1",
        "ACT PREP H",
        "ACTPREP 4X4S",
        "AD NUTR&FOOD 1CR",
        "AD STRNG ORC4X4S",
        "ADAPTIVE PE",
        "ADVANCED BD II",
        "ADVANCED BD III",
        "AFR AMER STUD",
        "AFR AMER STUD E",
        "ALT HOME LIV SE6",
        "ALT HOME LIV SE7",
        "AM SIGN LNG II E",
        "AP MUSICTHEO4X4S",
        "BASEBALL IV",
        "BASKETBALL I F",
        "BASKETBALL I M",
        "BASKETBALL II M",
        "BASKETBALL III F",
        "BASKETBALL III M",
        "BASKETBALL IV F",
        "BEGIN BAND",
        "BEGIN. ORCHESTRA",
        "BROAD/JOURN 8 1S",
        "BROADCAST I H",
        "BROADCAST II",
        "BROADCAST II H",
        "BROADCASTING I",
        "BT-BARBER-II",
        "BT-COSMETOLOGY-2",
        "CIW INET BUS",
        "CIW INTERNET BUS",
        "CIW NETWORK TECH",
        "CIW WEB DEV 4X4S",
        "CIW WEB DEVELOP",
        "CMC LAB 6",
        "CMC LAB 7",
        "CMC LAB 8",
        "COND STRAT I G",
        "COND STRAT II B",
        "COND STRAT III B",
        "COND STRAT III G",
        "COND STRAT IV B",
        "COND STRATII4X4S",
        "COND STRATIV4X4S",
        "COND.STRATGIESI",
        "COOP MARK ED I",
        "COOP MRK ED I",
        "CUL OCC II (1)",
        "CULINARY ARTS I",
        "CYBER LIT (7TH)",
        "CYBER LIT (8TH)",
        "CYBER SCI 7 HS",
        "CYBER SCI 8 HS",
        "CYBERSECURITY E",
        "CYBERSOCNICERC",
        "CYBERSOCNICERC 8",
        "DANCE I",
        "DANCE IV",
        "DEBATE",
        "DEBATE I",
        "DEBATE II E",
        "DEBATE IV E",
        "DESKTOP PUBLISH",
        "DIG GRPX & ANIM",
        "DIGITAL MEDIA 1",
        "DIGITAL MEDIA II",
        "DIGITAL PHOTO",
        "DISCOVERIES 6"
    },

    #"Inserted conditional column" = Table.AddColumn(#"Navigation 3", "short_name_academics", each CourseBucket([course]), type text),

    #"Filtered rows" = Table.SelectRows(
        #"Inserted conditional column",
        each not List.Contains(ExcludedCourses, NormalizeText([course]))
    ),

    // GPA columns for each 9 weeks
    #"Inserted conditional column 1" = Table.AddColumn(#"Filtered rows", "1st_9_GPA", each GradeToGPA([1st_9_weeks]), Int64.Type),
    #"Inserted conditional column 2" = Table.AddColumn(#"Inserted conditional column 1", "2nd_9_GPA", each GradeToGPA([2nd_9_weeks]), Int64.Type),
    #"Inserted conditional column 3" = Table.AddColumn(#"Inserted conditional column 2", "3rd_9_GPA", each GradeToGPA([3rd_9_weeks]), Int64.Type),
    #"Inserted 4th GPA" = Table.AddColumn(#"Inserted conditional column 3", "4th_9_GPA", each GradeToGPA([4th_9_weeks]), Int64.Type),

    #"Filtered rows 3" = Table.SelectRows(#"Inserted 4th GPA", each [1st_9_GPA] <> null),

    // Numeric grade values for each 9 weeks
    #"Inserted 1st grade value" = Table.AddColumn(#"Filtered rows 3", "1st_9_grade_value", each FirstTokenNumber([1st_9_weeks]), type number),
    #"Inserted 2nd grade value" = Table.AddColumn(#"Inserted 1st grade value", "2nd_9_grade_value", each FirstTokenNumber([2nd_9_weeks]), type number),
    #"Inserted 3rd grade value" = Table.AddColumn(#"Inserted 2nd grade value", "3rd_9_grade_value", each FirstTokenNumber([3rd_9_weeks]), type number),
    #"Inserted 4th grade value" = Table.AddColumn(#"Inserted 3rd grade value", "4th_9_grade_value", each FirstTokenNumber([4th_9_weeks]), type number),

    // Letter grades for each 9 weeks
    #"Inserted 1st letter grade" = Table.AddColumn(#"Inserted 4th grade value", "1st_9_letter_grade", each LastTokenText([1st_9_weeks]), type text),
    #"Inserted 2nd letter grade" = Table.AddColumn(#"Inserted 1st letter grade", "2nd_9_letter_grade", each LastTokenText([2nd_9_weeks]), type text),
    #"Inserted 3rd letter grade" = Table.AddColumn(#"Inserted 2nd letter grade", "3rd_9_letter_grade", each LastTokenText([3rd_9_weeks]), type text),
    #"Inserted 4th letter grade" = Table.AddColumn(#"Inserted 3rd letter grade", "4th_9_letter_grade", each LastTokenText([4th_9_weeks]), type text),

    #"Reordered columns" = Table.ReorderColumns(
        #"Inserted 4th letter grade",
        {
            "student_name",
            "sidno",
            "sasid",
            "grade",
            "gender",
            "dob",
            "school_name",
            "course",
            "1st_9_weeks",
            "1st_9_letter_grade",
            "1st_9_grade_value",
            "1st_9_GPA",
            "2nd_9_weeks",
            "2nd_9_letter_grade",
            "2nd_9_grade_value",
            "2nd_9_GPA",
            "3rd_9_weeks",
            "3rd_9_letter_grade",
            "3rd_9_grade_value",
            "3rd_9_GPA",
            "4th_9_weeks",
            "4th_9_letter_grade",
            "4th_9_grade_value",
            "4th_9_GPA",
            "iep_status",
            "504_status",
            "lep_status",
            "short_name_academics"
        }
    )
in
    #"Reordered columns";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data]{[lakehouseId = "7f6e42de-c8f2-4325-901e-72825da1a68c"]}[Data];
