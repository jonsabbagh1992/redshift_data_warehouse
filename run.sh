#!/bin/bash 

python set_up_aws_resources.py
python create_tables.py
python etl.py
