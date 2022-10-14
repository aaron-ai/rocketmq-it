FROM ubuntu:18.04

LABEL MAINTAINER="aiyangkun"

# Install OpenJDK-8
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk;

COPY target/rocketmq-proxy-it.jar /root/rocketmq-proxy-it.jar

CMD ["java", "-jar", "/root/rocketmq-proxy-it.jar"]