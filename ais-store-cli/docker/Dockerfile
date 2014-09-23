FROM kaspernielsen/javabox

RUN apt-get install maven -y
RUN mkdir -p /data
RUN git clone https://github.com/dma-ais/AisStore /AisStore
RUN (cd /AisStore; mvn clean install)

ADD ./start.sh /start.sh

ENV SOURCES "sourceName=1.2.3.4"
ENV BACKUP "/data"

CMD ["/bin/bash", "/start.sh"]
