FROM alanszp/alpine-scala-sbt-spark

COPY ./ /opt/newspark

WORKDIR /opt/newspark

ENV SBT_OPTS "-Xms1024m -XX:MaxPermSize=1024m -Xmx4G -XX:+UseConcMarkSweepGC -XX:InitialCodeCacheSize=64m -XX:ReservedCodeCacheSize=1024m -XX:+CMSClassUnloadingEnabled"

RUN sbt assembly