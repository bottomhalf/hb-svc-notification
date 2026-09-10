FROM maven:3.8.1-openjdk-17 AS MAVEN

MAINTAINER BOTTOMHALF

COPY pom.xml /build/
COPY src /build/src/

WORKDIR /build/
RUN mvn package -DskipTests

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
EXPOSE 8107

COPY --from=MAVEN /build/target/hiringbell_notificationservice.jar /app/
COPY src/main/resources /app/resources

ENTRYPOINT ["java", "-jar", "hiringbell_notificationservice.jar", "--spring.profiles.active=prod"]