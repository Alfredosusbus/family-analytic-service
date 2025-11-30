FROM eclipse-temurin:21-jdk
WORKDIR /app
COPY target/family-analytic-service-1.0.0.jar app.jar
EXPOSE 8585
ENTRYPOINT ["java", "-jar", "app.jar"]