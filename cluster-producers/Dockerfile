FROM alpine:latest

# Set environment variables for Java and Maven versions
ENV JAVA_VERSION=11.0.23
ENV MAVEN_VERSION=3.8.8

# Install required packages
RUN apk update && \
    apk add --no-cache \
    curl \
    tar \
    bash \
    openjdk11-jdk

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install Maven
RUN mkdir -p /usr/share/maven /usr/share/maven/ref && \
    curl -fsSL https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz \
    | tar -xzC /usr/share/maven --strip-components=1 && \
    ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

# Set environment variables for Maven
ENV MAVEN_HOME=/usr/share/maven
ENV MAVEN_CONFIG="$USER_HOME_DIR/.m2"

WORKDIR /app

COPY kafka-producers /app
