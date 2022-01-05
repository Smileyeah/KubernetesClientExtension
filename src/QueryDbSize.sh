#!/bin/bash

echo "select sum((data_length+index_length)/1024/1024)M from information_schema.tables where table_schema='${4}'"|mysql -h$1 -u$2 -p$3
