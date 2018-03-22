#!/bin/bash

set -x

customize_redis_conf() {
    sed -i 's/daemonize yes/daemonize no/g' $1
    # listen all interfaces
    sed -i 's/bind 127.0.0.1/bind 0.0.0.0/g' $1
    # disable protected-mode
    sed -i 's/protected-mode yes/# protected-mode yes/g' $1
    # no keep-alive
    sed -i 's/tcp-keepalive 300/tcp-keepalive 0/g' $1
    sed -i 's/logfile ""/logfile \/var\/log\/redis\/redis-server.log/g' $1
    # no save
    sed -i 's/save 300 10/#save 300 10/g'  $1
    sed -i 's/save 60 10000/#save 60 10000/g' $1
    # change working dir
    sed -i 's/dir .\//dir \/var\/lib\/redis/g' $1
    # change replication setting
    sed -i 's/# repl-timeout 60/repl-timeout 1200/g' $1
    sed -i 's/# repl-backlog-size 1mb/repl-backlog-size 128mb/g' $1
    sed -i 's/client-output-buffer-limit slave 256mb 64mb 60/client-output-buffer-limit slave 4096mb 2048mb 60/g' $1
}

customize_redis_conf /etc/redis/redis.conf

cat > /run.sh << "EOF"
#!/bin/bash
if [ ! -f /var/lib/redis/.redis_mode_set ]; then
	/set_redis_mode.sh
fi
exec /usr/bin/redis-server /etc/redis/redis.conf
EOF

cat > /etc/supervisor/conf.d/redis.conf << "EOF"
[program:redis]
command=/run.sh
pidfile=/var/log/redis/redis.pid
stdout_logfile=/var/log/supervisor/%(program_name)s.log
stderr_logfile=/var/log/supervisor/%(program_name)s.log
user=redis
EOF


cat > /set_redis_mode.sh << "EOF"
#!/bin/bash

if [ -f /var/lib/redis/.redis_mode_set ]; then
	echo "Redis mode already set!"
	exit 0
fi

if [ -n "$REDIS_MODE" ]; then
	if [ "$REDIS_MODE" == "SLAVE" ]; then
		echo "=> Configuring redis as a slave for $MASTER"
		echo "slaveof $MASTER $MASTER_PORT" >> /etc/redis/redis.conf
		sed -i 's/slave-read-only yes/slave-read-only no/g' /etc/redis/redis.conf
		sed -ri "s/(^save.*)/#\1/g" /etc/redis/redis.conf
	elif [ "$REDIS_MODE" == "CACHE" ]; then
		echo "=> Configuring redis as cache"
		sed -ri "s/(^save.*)/#\1/g" /etc/redis/redis.conf
	else
		echo "=> Unknown $REDIS_MODE mode - ignoring"
	fi
fi

touch /var/lib/redis/.redis_mode_set
EOF

chmod 755 /*.sh
