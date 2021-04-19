FROM harbor.sktai.io/aidp/noct/base:latest

ENV NOCT_HOME /noct-combiner

USER root
# Required
# GOOGLE_APPLICATION_CREDENTIALS
# VAULT_ADDR
# VAULT_TOKEN

ADD resources/noct-combiner ${NOCT_HOME}
ADD scripts/mount.sh ${NOCT_HOME}/mount.sh
ADD scripts/entrypoint.sh ${NOCT_HOME}/entrypoint.sh

RUN chown -R root:root ${NOCT_HOME}
WORKDIR ${NOCT_HOME}
ENTRYPOINT exec ${NOCT_HOME}/docker-entrypoint.sh
