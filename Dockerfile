FROM viki/base:latest
MAINTAINER platform-dev@viki.com

ENV DEBIAN_FRONTEND noninteractive
RUN sed -i 's/# \(.*multiverse$\)/\1/g' /etc/apt/sources.list && \
  apt-get update && \
  apt-get install -y gcc libc6-dev make && \
  rm -rf /var/lib/apt/lists/* && \
  mkdir -p /var/log/redis && \
  mkdir -p /var/lib/redis

ADD . /redis
WORKDIR /redis

ADD sentinel.conf redis.conf /etc/redis/
ADD deploy/redis-server.logrotate /etc/logrotate.d/redis-server

RUN make -j 4 && make PREFIX=/usr/ install && strip /usr/bin/redis*
RUN /redis/setup.sh

RUN adduser --disabled-password --gecos '' redis

COPY entrypoint.sh /sbin/entrypoint.sh
RUN chmod 755 /sbin/entrypoint.sh

EXPOSE 6379/tcp
ENTRYPOINT ["/sbin/entrypoint.sh"]
