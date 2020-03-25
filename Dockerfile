FROM openjdk:8

COPY ./target/java-kafka-consumer-jar-with-dependencies.jar /opt/app/app.jar

ENTRYPOINT ["java", "-jar", "/opt/app/app.jar"]
