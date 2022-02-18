FROM alpine:3.14

RUN apk add --update openssh

WORKDIR /app
ENV GERRIT_URL='<GERRIT-URL>'
ENV GERRIT_PROJECT='<GERRIT-PROJECT>'
ENV GERRIT_PROJECT_FILTER='<GERRIT-PROJECT-FILTER>'
ENV GERRIT_USERNAME='<GERRIT-USERNAME>'
ENV GERRIT_SSH_KEY='<GERRIT-SSH-KEY>'
ENV ES_URL='<GERRIT-ES-URL>'
ENV STAGE='<STAGE>'
ENV ELASTIC_LOG_URL='<ELASTIC-LOG-URL>'
ENV ELASTIC_LOG_USER='<ELASTIC-LOG-USER>'
ENV ELASTIC_LOG_PASSWORD='<ELASTIC-LOG-PASSWORD>'
# RUN apk update && apk add git
RUN apk update && apk add --no-cache bash
RUN ls -ltra
COPY gerrit ./
CMD ./gerrit --gerrit-disable-host-key-check=true --gerrit-url=${GERRIT_URL} --gerrit-project=${GERRIT_PROJECT} --gerrit-project-filter=${GERRIT_PROJECT_FILTER} --gerrit-es-url=${ES_URL} --gerrit-user=${GERRIT_USERNAME} --gerrit-ssh-key=${GERRIT_SSH_KEY}
