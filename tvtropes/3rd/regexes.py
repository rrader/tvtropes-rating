import re

# important regex we'll reuse
unique_title_regex_string = """
    (?P<unique_title>
        (?P<tv_show_title>
            "?
                (?P<production_title>
                    [^"]+
                )
            "?
            (
                [ ]
                \(
                    (?P<miniseries>
                        mini
                    )
                \)
            )?
            [ ]
            \(
                (?P<year>
                    [\d?]{4}
                )
                (
                    /
                    (?P<year_number>
                        [IVLX]+
                    )
                )?
            \)
        )
        (
            [ ]
            \(
                (?P<media_type>[^)]+)
            \)
        )?
        (
            [ ]
            \{
                (?P<episode_title>(?!\(\#)[^{]+?)?
                (
                    \ *
                    \(
                        \#
                        (?P<season_number>\d+)
                        \.
                        (?P<episode_number>\d+)
                    \)
                )?
            \}
        )?
        (
            [ ]
            (?P<production_status>
                \{\{SUSPENDED\}\}
            )
        )?
    )
"""
productions_regex_string = unique_title_regex_string + r"\t+([\d?]{4})(-[\d?]{4})?"
rating_regex_string = """
    [ ]{6}
    (?P<rating_distribution>
        [\d\.]{10}
    )
    [ ]+
    (?P<num_ratings>\d+)
    [ ]+
    (?P<rating>\d\.\d)
    [ ]+
    """ + unique_title_regex_string
director_regex_string = """
    ^(?P<unique_name>
        (?P<last_name>
            [^\t,]+
        )
        (
            ,
            [ ]+
            (?P<first_name>
                [^\t\(]+
            )
        )?
        (
            [ ]
            \(
                (?P<number>
                    [IVLX]+
                )
            \)
        )?
    )?
    [\t]+
    """ + unique_title_regex_string + """
    (
        [ ][ ]
        \(
            (?P<description>
                .+
            )
        \)
    )?
"""
genre_regex_string = unique_title_regex_string + r"\t+(?P<genre>[^\t\n]+)$"

unique_title_regex = re.compile(unique_title_regex_string, re.VERBOSE)
productions_regex  = re.compile(productions_regex_string,  re.VERBOSE)
rating_regex       = re.compile(rating_regex_string,       re.VERBOSE)
director_regex     = re.compile(director_regex_string,     re.VERBOSE)
genre_regex        = re.compile(genre_regex_string,        re.VERBOSE)

