FROM debian

# RUN sed -i 's/# \(.*multiverse$\)/\1/g' /etc/apt/sources.list && \
RUN  apt-get update && apt-get install -y gcc libc6-dev make
#   rm -rf /var/lib/apt/lists/* && \
RUN  mkdir -p /var/log/redis && mkdir -p /var/lib/redis

ADD . /redis
WORKDIR /redis

ADD sentinel.conf redis.conf /etc/redis/
ADD deploy/redis-server.logrotate /etc/logrotate.d/redis-server

RUN make
RUN /redis/setup.sh

EXPOSE 6379