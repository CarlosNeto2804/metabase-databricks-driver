FROM clojure:tools-deps-1.11.1.1413 as build

COPY . /driver

ARG METABASE_VERSION="v0.46.6.4"

RUN apt-get -y update && \
    apt-get -y install curl && \
    curl -Lo - https://github.com/metabase/metabase/archive/refs/tags/${METABASE_VERSION}.tar.gz | tar -xz && mv metabase-* /metabase

WORKDIR /driver

RUN bash /driver/bin/build.sh

#COPY /metabase/databricks-sql.metabase-driver.jar /output/databricks-sql.metabase-driver.jar