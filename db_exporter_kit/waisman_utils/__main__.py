import os
from datetime import datetime as dt
import table_conventions as tb
import db_utils as db
import logging
import waisman_general_utils as u

print('Doing unit tests of table_conventions')
logname = 'unittests_%s.txt'%dt.now().strftime('%d%m')

logging.basicConfig(filename=logname,level=logging.INFO)
logger = logging.getLogger()

con = db.db_connect(DSN='wtp_data')
tables = db.get_all_tablenames(con)
types = {}
phases = {}
for table in tables:
    ret = tb.parse_tablename(table,logger=logger)
    types[ret['type']] = None
    try:
        phases[ret['phase']] = None
    except:
        pass 
print('types')
print(u.prettify_str(types))
print('phases')
print(u.prettify_str(phases))

