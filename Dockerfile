FROM reynoldsm88/centos-jdk:latest

LABEL maintainer="john.hungerford@twosixlabs.com"

COPY ./target/java-kafka-consumer-jar-with-dependencies.jar /opt/app/pkg
COPY ./scripts/run.sh /opt/app/bin

ENV JAVA_OPTS "-Xms512m -Xmx512m -XX:+UseConcMarkSweepGC"
ENV PROGRAM_ARGS "compose"

RUN chmod -R u+x /opt/app

ENTRYPOINT ["/opt/app/bin/run.sh"]
