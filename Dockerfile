FROM eclipse-temurin:25-jdk-alpine AS build

WORKDIR /build

COPY gradlew gradlew.bat settings.gradle build.gradle gradle.properties versions.properties ./
COPY gradle/ gradle/

# Pre-download dependencies to leverage Docker caching.
RUN chmod +x gradlew && ./gradlew dependencies --no-daemon --quiet

COPY src/ src/
RUN ./gradlew installDist --no-daemon --quiet -x test


FROM eclipse-temurin:25-jre-alpine AS runtime

# Run as a non-root user
RUN adduser -D -H -s /sbin/nologin capella \
    && mkdir /config && chown capella:capella /config

RUN apk add --no-cache libstdc++

USER capella

WORKDIR /app

COPY --from=build --chown=capella:capella /build/build/install/capella/ .

# Bake domain config into the image at build time.
# Provide a config/ directory in the build context containing:
#   - application.yaml
#   - any transform JSON files referenced in application.yaml
#
#  Pass CAPELLA_ADDITIONAL_KAFKA_PROPERTIES at runtime:
#   docker run -e CAPELLA_ADDITIONAL_KAFKA_PROPERTIES='{"cluster-a":{"username":"...","password":"..."}}' ...
COPY --chown=capella:capella config/ /config/

# Sensible defaults for JVM options
ENV JAVA_OPTS="-XX:InitialRAMPercentage=50 -XX:MaxRAMPercentage=50 -XX:+UseZGC -XX:+ExitOnOutOfMemoryError"

ENV CAPELLA_CONFIG_FILE=/config/application.yaml

# State store — /data must be mounted to a persistent volume at runtime.
# Override CAPELLA_STATE_STORE_PATH if you mount to a different path.
ENV CAPELLA_STATE_STORE_PATH=/data/rocksdb
VOLUME /data

ENTRYPOINT ["bin/capella"]
