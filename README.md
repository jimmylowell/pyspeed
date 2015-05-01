speedtest_reporting
===================

speedtest_reporting is a collection of scripts to download 
and/or parse TSVs from reporting.speedtest.net

Overview
========
Generate "speedtest_servers" table

    python makeservertable.py
    
Copy config_file-sample.py to config_file.py and fill out

    cp config_file-sample.py config_file.py
    
Run to download and/or parse TSVs

    python pyspeed.py

Requirements
============
Python Requests

PostGIS for PostgreSQL import