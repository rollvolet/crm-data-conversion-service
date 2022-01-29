FROM semtech/mu-ruby-template:feature-ruby-3
LABEL maintainer="erika.pauwels@gmail.com"

RUN apt-get install -y unixodbc unixodbc-dev freetds-dev freetds-bin tdsodbc
