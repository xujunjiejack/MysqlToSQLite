class AgeAppendingError(Exception):
    pass

class expected_arg_not_provided_err(Exception):
    ''' To be raised if an argument is supposed to be provided'''
    pass
class erroneous_col_err(Exception):
    ''' To be raised if a column that is supposed to be created doesn't get created'''
    pass
class parse_err(Exception):
    ''' To be raised if a table is parsed in such a way that is unusual'''
    pass
class type_is_misc_warning(AgeAppendingError):
    ''' To Be raised if table is parsed & type is misc. 
        Therefore it has no known recent date '''
    pass
class phase_unrecognized_err(AgeAppendingError):
    '''To be raised when a table is parsed and the phase is unrecognized.'''
    pass
class fail_to_create_col_err(AgeAppendingError):
    ''' To Be raised if trying to create a column and fails'''
    pass
class fail_to_fill_col_err(AgeAppendingError):
    ''' To Be called if trying to fill a column and fails.'''
    pass
class fail_to_drp_col_err(AgeAppendingError):
    ''' To be called if trying to drop column and fails.'''
    pass

class fail_to_duplicate_table_err(AgeAppendingError):
    pass

class no_need_warn(Exception):
    ''' To be raised in order to avoid doing extra work.
        such as when you want to add a date column to the table containing that column
        there's just no need!'''
    pass

class empty_list_warn(Exception):
    ''' To be raised if we expect a list full of stuff.
        but an empty list is supplied'''
    pass 

class date_empty_err(Exception):
    ''' To be raised if a date is expected to be passed in but not '''
    pass

class date_wrong_format_err(Exception):
    ''' To be raised if a date string is in the wrong format when being parsed.
        Specifically useful when calculating ages from date strings '''
    pass
