# MysqlToSQLite

## Description:
This project is an essential part for the whole wtp_collab environment. It exports the data from wtp_data to the wtp_collab operated by sqlite in collab drive. During the process, each table will get sanitized to comply with HIPPA to avoid sensitive information leak. Each table, if applicable, will also be appended with the age for that all family members. The age can be helpful for collaborators to do analysis. Right now, I have implemented two modes to use this script. One is incremental update. The other one is a bulk import. Other commands are available. 

The incremental update aims to change the wtp_collab as soon as possible when data in wtp_data has changed. Since it will probably be performed daily, this functionality tries to minimize the operation time. The time performed should be association with the tables that need updated. If no tables need change or import in sqlite, the operation time should be almost 0. For the incremental update, a checksum table that contains checksum for each table in wtp_data is preserved under the folder. Ideally, under the assumption that no one changes data in wtp_collab, it should also reflect the chechsum of each table stored in wtp_collab. When updating, this script will compare this checksum table with the checksums computed for current wtp_data. If any discrepency is discovered for a table, this table in the wtp_collab will then be updated. If no checksum table file is found, then a bulk import wll be performed.  

When performing bulk import mode, this script will create a new wtp_collab to replace the old wtp_collab which gets backed up in collab drive. Then, this script will export all of those tables into the wtp_data. I have intention to make this update happen every month, after the wtp_collab deploies online. 

All of the core function is based on the db_exporter, written by Russell.           

## Essential Component:
* mysqlToSqlite.py: the entrance of the whole migration process. It currently doesn't accept much customization. It only exports every table from wtp_data.
* file_path.json: it defines the directory for wtp_collab, the db name for wtp_collab and directory for the archived data, and the checksum file name
* db_exporter_settings.json: it's used by the project to read different metadata from each table name.
* checksum.json (name can be specfied): it stores the checksum computed for wtp_data after the last run of this script.
* other source code: it's better for you to read the code

## How to use:
Make sure the file_path.json is configured correctly for the file you want to update. Then type in  `<python MysqlToSQLite.py [-u] [-a] [-h] [-t] [-changed]>`.You can also type in -h for help.
    -u: starts incremental update
    -a: will start bulk import
    -t:  will import tables specified in source codes.
    -changed: it will display the tables that need updated or imported 

## Log:
Logs are put in the *logs* folder. There will be a lot of log files generated. Normally, three logs:  ageAppender.logs, dataSanitizer.logs and migrator.logs are good places to start for checking whether anything goes wrong.

## Future development:
More customization so that the users can move specific data tables from wtp_data to wtp_collab.
A better way to decide whether a table in sqlite db needs update. Right now, this program only does checksum for wtp_data. There is no way to know whether the data tables in sqlite have been changed.  

> *Updated by JJ 10/14/2017*
