# MysqlToSQLite

## Description:
This project is an essential part for the whole wtp_collab environment. It exports the data from wtp_data to the wtp_collab operated by sqlite in collab drive. During the process, the data will get sanitized to comply to HIPPA to avoid sensitive information to be seen by outsiders. For the most of time, it will export all of those tables in the wtp_data and create a new wtp_collab to replace the old wtp_collab which gets backed up in collab drive. I have intention to make this update happen every month, after the wtp_collab starts to be heavily used. Each table, if applicable, will also be appended with the age for that family member for further use. The code is based on the db_exporter, written by Russell before.           

## Component:
* mysqlToSqlite.py: the entrance of the whole migration process. It currently doesn't accept much customization. It only exports every table from wtp_data.
* file_path.json: it defines the directory for wtp_collab, the db name for wtp_collab and directory for the archived data. 
* db_exporter_settings.json: it's used by the project to read different metadata from each table name.
* other source code: it's better for you to read the code

## Log:
Logs are put in the *logs* folder. There will be a lot of log files generated. Normally, three logs:  ageAppender.logs, dataSanitizer.logs and migrator.logs are good places to start for checking whether anything goes wrong.

## Future development:
More customization so that the users can move specific data tables from wtp_data to wtp_collab.
More integrated into the wto_ collab environment.

> *Updated by JJ 06/06/2017*
