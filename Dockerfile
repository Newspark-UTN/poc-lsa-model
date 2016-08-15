FROM alanszp/alpine-scala-sbt

COPY ./ /opt/newspark

WORKDIR /opt/newspark

RUN sbt compile