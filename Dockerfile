FROM eclipse-temurin:21-jdk AS build

WORKDIR /workspace

COPY gradlew .
COPY gradle ./gradle
COPY build.gradle .
COPY settings.gradle .

RUN sed -i 's/\r$//' gradlew \
    && chmod +x gradlew \
    && ./gradlew --no-daemon dependencies

COPY src ./src

RUN ./gradlew --no-daemon clean bootJar

FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=build /workspace/build/libs/app.jar ./app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]