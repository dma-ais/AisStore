FROM java:8

RUN mkdir -p /data
RUN wget --progress=bar "https://dma.ci.cloudbees.com/job/AisStore/lastSuccessfulBuild/artifact/ais-store-rest/target/ais-store-rest-1.2.3.RELEASE.jar"
EXPOSE 8080
ADD ./start.sh /start.sh
RUN chmod u+x /start.sh

ENV DIRECTORY /data

CMD ["/bin/bash", "/start.sh"]

