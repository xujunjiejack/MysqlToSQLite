creating_stmt = 'CREATE TABLE "data_5_dates" (\n  "familyid" varchar(50) NOT NULL DEFAULT \'\',\n  "inm" varchar(50) DEFAULT NULL,\n  "intw" varchar(50) DEFAULT NULL,\n  "mrisc" varchar(50) DEFAULT NULL,\n  PRIMARY KEY ("familyid"),\n  KEY "familyid" ("familyid")\n)'

unsupported_keyword = ["unsigned", "AUTO_INCREMENT"]

def clean_comma_before_end_parenthesis(creating_stmt_lines):
    for index, line in enumerate(creating_stmt_lines):
        if "(" not in line and ")" in line:
            # try to make sure there is no , at the end of the statement
            creating_stmt_lines[index - 1] = creating_stmt_lines[index - 1].strip(',')
    return creating_stmt_lines


def clean_statements_containing_KEY(creating_stmt_lines):
    # get rid of the statment that contains key:
    # e.g KEY "staffid" ("staffid")

    lines_contain_KEY = [] # remember the lines that contain KEY
    for index, line in enumerate(creating_stmt_lines):
        if "PRIMARY KEY" not in line and "KEY" in line:
            print("FIND IT: %s" % line)
            lines_contain_KEY.append(line)

    for line in lines_contain_KEY:
        creating_stmt_lines.remove(line)

    return creating_stmt_lines


def clean_unsupported_keyword(creating_stmt_lines):
    for index, line in enumerate(creating_stmt_lines):
        # clean the unsigned in the statement
        for keyword in unsupported_keyword:
            if keyword in line:
                creating_stmt_lines[index] = line.replace(keyword, "")
    return creating_stmt_lines

def change_decimal_to_4(creating_stmt_lines):
    """
        Change the double (15, 5) to double(15,4), which has 4 decimal points.
    :param creating_stmt_lines:
    :return:
    """
    for index, line in enumerate(creating_stmt_lines):
        if "double" in line:
            creating_stmt_lines[index] = line.replace("double(15,5)", "double(15,4)")

    return creating_stmt_lines

def change_type_of_twin(creating_stmt_lines):
    # rewrite the line
    for index, line in enumerate(creating_stmt_lines):
        if " \"twin\" " in line.lower():
           # print(line)
            creating_stmt_lines[index] = '  "twin" int(10) NOT NULL DEFAULT \'0\', '
    return creating_stmt_lines


def clean_statment(statement):
    """
        sqlite has a little different statement grammer compared with mysql. Thus, it needs to get cleaned
        Couple more issues need to be done:
            1: the sqlite doesn't recognize unsigned in the creating statement. I probably should get rid of it.
            2: couple tables don't have Primary Key. Thus, when I get rid of the statement that contain KEY, the final statement will contain a comma, thus crashing the sqlite
            3:
    :param statement:
    :return:
    """
    # The order matter for change_type_of_twin and clean_comma_before_end_parenthesis. This order can ensure that
    # no new possible "comma" will be entered after the last comma gets cleaned up.
    clean_action = [clean_statements_containing_KEY, clean_unsupported_keyword,
                    change_type_of_twin, clean_comma_before_end_parenthesis, change_decimal_to_4,
                    ]

    # get rid of the statment that contains key:
    # e.g KEY "staffid" ("staffid")
    lines = statement.split("\n")

    # Can I make it something like sum?
    for clean in clean_action:
        lines = clean(lines)
    new_stmt = "\n".join(lines)
    print(new_stmt)
    return new_stmt

def test():
    stmt = 'CREATE TABLE "calc_4_pi_m" (\
        "familyid" varchar(45) NOT NULL DEFAULT \'\', \
        "twin" int(10) unsigned NOT NULL DEFAULT \'0\', \
        "age" int(10) unsigned NOT NULL DEFAULT \'0\', \
        "gender" int(10) unsigned NOT NULL DEFAULT \'0\', \
        "pdssm" double NOT NULL DEFAULT \'0\',\
        "pdsam" double NOT NULL DEFAULT \'0\',\
        "pdsgm" double NOT NULL DEFAULT \'0\',\
        "apubbm" double NOT NULL DEFAULT \'0\',\
        "apubpm" double NOT NULL DEFAULT \'0\',\
        "apubm" double NOT NULL DEFAULT \'0\',\
        PRIMARY KEY ("familyid","twin")\
    )'

    clean_statment(stmt)
