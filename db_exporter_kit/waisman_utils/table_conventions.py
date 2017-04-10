'''
    This package contains many functions designed to do most things with tables in the wtp_data convention

    such as parsing tables, creating new tables, ensuring that new tables conform to the conventions of wtp
    and similar things. 

    this uses the pypyodbc interface to access files and mysql servers. 
    and uses python loggers to print statements. If it doesn't take a logger than it doesn't print anything.

    list of Functions:
    parse_tablename # generates a dictionary with metadata about that table that can be computed from the name
    get_table_type    # tables are of a specific type, this figures out what the type is for a given table  
'''
import logging
from .waisman_general_utils import prettify_str


# this is a list of tables that are exceptionally strange. 
# key should be name of table
# value should be a dict will all the meta-data
    # meta data expected is phase, respondent, instrument abbreviation, type etc. you can include other 
    # stuff if you think it's useful.
table_exceptions =\
{
       
}




# This dict contains the actual rules for each type of table.
# it consists of table types as keys. with subdicts of different requirements they must satisfy. 
filters =\
{
    'data':
    {
        'length':3,
        'disallowed words':
        [
            'dates'
        ]
    },
    'calc':
    {
        'length':3,
        'disallowed words':
        [
            'dates'
        ]
    },
    'tracker':
    {
        'length':3
    },
    'user':
    {
        'length':2
    }
}


# this dict contains the attributes of each type. and where they can be found. 
tablename_maps =\
{
    'parsing maps':
    {
        'data':
        { # data tables should be data_phase_[instr_abrrev,instr_abbrev]_respond
             'tablename':'table_split[0:]',
             'phase':'table_split[1]',
             'instrument abbreviation':'table_split[2 : table_split.__len__()-1]',
             'respondent':'table_split[-1]',
        },
        'calc':
        { # should be tha same as data tables
             'tablename':'table_split[0:table_split.__len__()]',
             'phase':'table_split[1]',
             'instrument abbreviation':'table_split[2: table_split.__len__()-1]',
             'respondent':'table_split[-1]',
        },
        'tracker':
        { # should be data_phase_tr
            'tablename':'table_split[0:table_split.__len__()]',
            'phase':'table_split[1]',
        },
        'user':
        {
            'tablename':'table_split[0:]',
            'phase':'table_split[1]'
        },
        'misc': # misc responds to everything else that we can't say with definite what the parts mean.
        {
             'tablename':'table_split[0:]',
        }
    }
}
# Parse a table name according to the rules specified in the dicts above.
def parse_tablename(tablename, debug = False, logger=logging): # get a dict with the attribute for this tablename
        """ 
            args: tablename
                  
            returns:
                {
                    'type': 'data'/'calc'/'misc'
                    'tablename': tablename,
                    'instrument abbreviation': 
                    'respondent':
                    'phase'
                    etc....
                }

            this function returns a dict of all the meta-data we know about this tablename. 
            this function has 3 steps.
            1. is this tablename found in the exceptions dict?
                yes -> return the meta-data found from the exceptions
            2. compute the type of this table? data/calc/misc
            3. parse the tablename to be the meta-data from the tablename. 
        """
        if debug: logger.info('Parsing table %s'%tablename) 
        attributes_dict = {}
                
        if(tablename in table_exceptions): # table_exceptions are dicts hard coded for specific tablenames that are strange
            if debug: logger.info('tablename found in table exceptions. returning %s'%prettify_str(table_exceptions[tablename]))
            return table_exceptions[tablename]

        table_type = get_table_type(tablename, debug = debug,logger=logger) # determine the type, so we know what rules to use 
        attributes_dict['type'] = table_type # save the type.
        
        table_split = tablename.split('_') # the sections should be split by _

        # attrib is the key for each table type in maps
        for attribute in tablename_maps['parsing maps'][table_type]:
            # select the portion specified in the maps.
            parts = eval(tablename_maps['parsing maps'][table_type][attribute])
            
            # if the portion is multiple parts. then concatenate them
            if(type(parts) is list):            # parts may not be a list if theres a single item.
                parts = '_'.join(parts)
            
            # save the concatenated portion.
            attributes_dict[attribute] = parts
        logger.info('Parsed %s\n-> Returning: %s'%(tablename, prettify_str(attributes_dict)))
        return attributes_dict

# helper for parse_tablename
def get_table_type(tablename, debug=False, logger =logging):
        """ this determines the type of a table based on the generic rules found in parsing_rules
            """
        # split up the tablename for processing    
        table_split = tablename.split('_')
        if debug: logger.info('split %s into %s'%(tablename,str(table_split)))

        # if we don't know how to filter it. then return misc
        if(table_split[0] not in filters):
            logger.info('dont know how to filter %s. returning misc.'%table_split[0])
            return 'misc'
        
        # assuming that table_split[0] i.e. data from [data,w14,tr] is the type. 
        # go to the filters for that type. to make sure it's valid. 
        for aspect in filters[table_split[0]]: 
            # each aspect should be a key in in filtering_functions
            if debug: logger.info('filtering table through filtering function: %s'%aspect)
            if filtering_functions[aspect](table_split, logger=logger, debug=debug) is 'misc':
                return 'misc'
        return table_split[0]







#######################################
### definitions of filtering functions.
def filter_length(table_split,logger=logging,debug=False):
    """ Make sure length is at least this... """
    if(table_split.__len__() < filters[table_split[0]]['length']):
        if debug: 
            logger.info('table is not long enough for a %s we expect a length of at least %s'\
                %(table_split[0],filters[table_split[0]]['length']))
        return 'misc'
    else:
        return table_split[0]
    
def filter_disallowed_words(table_split,logger=logging,debug=False):
    '''make sure it doesn't contain these words'''
    for word in filters[table_split[0]]['disallowed words']:
        if word in table_split:
            if debug:logger.info('%s found in table split. thats not allowed! returning misc.'%word)
            return 'misc'
    return table_split[0]



### End of filtering functions
############################################
############################################


# this dict referances the functions responsible for filtering. 
# the functions should take only the split up tablename and return either misc
# or tablename_split[0]
filtering_functions=\
{
    'length':filter_length,
    'disallowed words':filter_disallowed_words
}