import sys
import json
import logging


def setup_logger(loggername, logfile=None, logfile_mode='w', level=logging.INFO, stdout= True, format='%(name)s/%(level)s:%(message)s'):
    ''' This sets up a logger object and returns it. 
        '''
    l = logging.getLogger(loggername)
    formatter = logging.Formatter(format)
    if logfile:
        filehandler = logging.FileHandler(logfile, mode=logfile_mode)
        filehandler.setFormatter(formatter)
        l.addHandler(filehandler)
    if stdout:
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)
        l.addHandler(streamHandler)
    l.setLevel(level)
    return l

def prettify_str(list_like, indent=2, sort_keys=True):
    """
    Returns a well formatted string with \\n and \\t so that it looks good. 
    Takes a list-like, serializable object
    you can also specify the indent, it defaults to 2

    Throws a TypeError if object is not serializable
    """
    try:
        return json.dumps(list_like, indent=indent, sort_keys = True)
    except:
        print('Cannot Serialize this object in wtp_utils.py prettify_str')
        raise TypeError

def logfile(path,message = None, keep_alive = False, mode='w'):
    """ Writes a string to a file, optionally specify mode. 
            'r' reading
            'w' writing
            'a' append to the end
            'b' binary mode
            ... etc. look at doc for open for more options. 
        """
    f = open(path, mode)
    written = 0
    if(message):
        written = f.write(message)
    if(keep_alive):
        return f
    else:
        f.close()
        return written

def sprint(*messages, files=[sys.stdout], **kwargs):
    '''
        Helper method to print to multiple files
        files replaces the 'file' keyarg in print, 
        but for the remainder of keyword args check out the 
        docs for print.  
    '''
    for f in set(files):
        print(*messages, file=f, **kwargs )
        f.flush()

def iterable_is_all_equal(iterable):
    '''
        returns true if all elements in iterable are equal
        e.g. 
        [1,2,2,3] -> false
        [1,1,1,1] -> true

        testing equality using the defintion of set
        i.e. all indexes must be unique. so we reduce the iterable 
        to a set and it should have length of 1, else they are not all the same.
    '''
    return len(set(iterable)) is 1



