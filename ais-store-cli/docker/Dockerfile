FROM java:8

RUN mkdir -p /data
RUN wget --progress=bar "https://dma.ci.cloudbees.com/job/AisStore/lastSuccessfulBuild/artifact/ais-store-cli/target/*zip*/target.zip" -O /archive.zip
RUN unzip /archive.zip -d /
ADD ./start.sh /start.sh
ENV ARGUMENTS -help
CMD ["/bin/bash", "/start.sh"]