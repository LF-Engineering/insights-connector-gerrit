FROM alpine:3.14
WORKDIR /app
ENV GERRIT_URL='<GERRIT-URL>'
ENV GERRIT_PROJECT='<GERRIT-PROJECT>'
ENV ES_URL='<GERRIT-ES-URL>'
ENV STAGE='<STAGE>'
ENV ELASTIC_LOG_URL='<ELASTIC-LOG-URL>'
ENV ELASTIC_LOG_USER='<ELASTIC-LOG-USER>'
ENV ELASTIC_LOG_PASSWORD='<ELASTIC-LOG-PASSWORD>'
# RUN apk update && apk add git
RUN apk update && apk add --no-cache bash
RUN ls -ltra
COPY gerrit ./
CMD ./gerrit --gerrit-url=${GERRIT_URL} --gerrit-project=${GERRIT_PROJECT} --gerrit-es-url=${ES_URL}
