  FROM reynoldsm88/centos-jdk:latest

LABEL maintainer="john.hungerford@twosixlabs.com"

ENV JAVA_OPTS "-Xms512m -Xmx512m -XX:+UseConcMarkSweepGC"
ENV PROGRAM_ARGS "compose"

COPY ./target/java-kafka-consumer-jar-with-dependencies.jar /opt/app/pkg/
COPY ./scripts/run.sh /opt/app/bin/

RUN chmod -R 755 /opt/app

ENTRYPOINT ["/opt/app/bin/run.sh"]