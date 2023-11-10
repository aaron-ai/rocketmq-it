FROM ubuntu:18.04

LABEL MAINTAINER="aiyangkun"

# Install OpenJDK-8
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk;

COPY target/rocketmq-all-in-one-it.jar /root/rocketmq-all-in-one-it.jar

CMD ["java", "-jar", "/root/rocketmq-all-in-one-it.jar"]