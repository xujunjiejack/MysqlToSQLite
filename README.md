# MysqlToSQLite

## Description:
This project is an essential part for the whole wtp_collab environment. It exports the data from wtp_data to the wtp_collab operated by sqlite in collab drive. During the process, the data will get sanitized to comply to HIPPA to avoid sensitive information to be seen by outsiders. Right now, I have implemented two modes for this script. One is incremental update. The other one is a bulk import. 

The incremental update aims to change the wtp_collab as soon as possible when data in wtp_data has changed. Since it will probably be performed daily, this functionality tries to minimize the operation time required for small increment. For the incremental update, a checksum table that contains checksum for each table in wtp_data is preserved under the folder. Ideally, under the assumption that no one changes data in wtp_collab, it should also reflect the chechsum of each table stored in wtp_collab. When updating, this script will compare this checksum table with the checksums computed for current wtp_data. If any discrepency is discovered for a table, this table in the wtp_collab will then be updated. If no checksum table file is found, then a bulk import wll be performed.  

When performing bulk import mode, this script will export all of those tables in the wtp_data and create a new wtp_collab to replace the old wtp_collab which gets backed up in collab drive. I have intention to make this update happen every month, after the wtp_collab deploies online. Each table, if applicable, will also be appended with the age for that family member for further use. The code is based on the db_exporter, written by Russell before.           

## Component:
* mysqlToSqlite.py: the entrance of the whole migration process. It currently doesn't accept much customization. It only exports every table from wtp_data.
* file_path.json: it defines the directory for wtp_collab, the db name for wtp_collab and directory for the archived data, and the checksum file name
* db_exporter_settings.json: it's used by the project to read different metadata from each table name.
* checksum.json (name can be specfied): it stores the checksum computed for wtp_data after the last run of this script.
* other source code: it's better for you to read the code

## How to use:
Make sure the file_path.json is configured correctly for the file you want to update. Then type in  `<python MysqlToSQLite.py [-u] [-a]>`.
-u will starts incremental update, -a will start bulk import. You can also type in -h for help.

## Log:
Logs are put in the *logs* folder. There will be a lot of log files generated. Normally, three logs:  ageAppender.logs, dataSanitizer.logs and migrator.logs are good places to start for checking whether anything goes wrong.

## Future development:
More customization so that the users can move specific data tables from wtp_data to wtp_collab.
More integrated into the wto_ collab environment.

> *Updated by JJ 09/09/2017*
