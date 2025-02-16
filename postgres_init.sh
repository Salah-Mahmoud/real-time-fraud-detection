#!/bin/bash

apt update && apt install nano -y

sed -i 's/^#wal_level = replica/wal_level = logical/' /var/lib/postgresql/data/postgresql.conf

pg_ctl restart -D /var/lib/postgresql/data
