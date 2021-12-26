#!/bin/bash
apt-get update -y \
    && apt-get install -y wget build-essential libc6-dev \
    && wget http://www.freetds.org/files/stable/freetds-1.1.24.tar.gz \
    && tar -xzf freetds-1.1.24.tar.gz \
    && cd freetds-1.1.24 \
    && ./configure --prefix=/usr/local --with-tdsver=7.3 \
    && make \
    && make install
