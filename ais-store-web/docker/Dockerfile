FROM java:8

RUN wget --progress=bar "https://dma.ci.cloudbees.com/job/AisDownloader/lastSuccessfulBuild/artifact/*zip*/archive.zip" -O /archive.zip
RUN unzip /archive.zip -d /
ADD ./start.sh /start.sh
EXPOSE 8080
CMD ["/bin/bash", "/start.sh"]
