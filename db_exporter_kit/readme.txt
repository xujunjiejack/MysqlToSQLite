This program is designed to export the wtp_data database into the wtp_collab database

It should eventually have the functionality to keep it up to date as well. However for now.... not so much. 

The __main__.py script will be run by running the package i.e. 
```
    bash
    > ls -R 
    ./export_db
        ./export_db/waisman_utils/...
        ./export_db/__init__.py
        ./export_db/__main__.py
        ./export_db/export_db.py
        ./export_db/readme.txt
    > python export_db
    Runs the script.
```

Right now (8/3/15) running the script will attempt to automatically export the db. in the future it should either 
open a gui or take commands to do maintenance things. 