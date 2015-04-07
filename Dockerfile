FROM ubuntu:12.04
MAINTAINER neerav@viki.com
ENV DEBIAN_FRONTEND noninteractive
RUN echo "force-unsafe-io" > /etc/dpkg/dpkg.cfg.d/02apt-speedup && echo "Acquire::http {No-Cache=True;};" > /etc/apt/apt.conf.d/no-http-cache && echo 'DPkg::Post-Invoke {"/bin/rm -f /var/cache/apt/archives/*.deb || true";};' > /etc/apt/apt.conf.d/no-cache
RUN locale-gen en_US en_US.UTF-8
RUN export COUNTRY=$(python -c "import urllib; print urllib.urlopen('http://ipinfo.io/country').read().strip().lower()") && sed -i "s/archive/$COUNTRY.archive/g" /etc/apt/sources.list
RUN apt-get update && apt-get install -y build-essential
ADD . /redis
WORKDIR /redis
RUN make -j 4 && make install
RUN strip /usr/local/bin/redis*
ENTRYPOINT ["/usr/local/bin/redis-server"]
EXPOSE 6379
CMD ["--bind", "0.0.0.0"]
