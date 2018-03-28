#!/bin/bash
set -e

chown -R redis:redis /var/lib/redis/
chown -R redis:redis /var/log/redis/

if [ ! -f /var/lib/redis/.redis_mode_set ]; then
	/set_redis_mode.sh
fi

/usr/bin/supervisord -n
