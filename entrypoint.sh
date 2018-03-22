#!/bin/bash
set -e

chown -R redis:redis /var/lib/redis/
chown -R redis:redis /var/log/redis/

/usr/bin/supervisord -n
