FROM alanszp/alpine-scala-sbt-spark

COPY ./ /opt/newspark

WORKDIR /opt/newspark

RUN sbt compile