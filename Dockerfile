FROM python:3.7
MAINTAINER Evgeny Medvedev <evge.medvedev@gmail.com>
ENV PROJECT_DIR=ethereum-etl

RUN mkdir /$PROJECT_DIR
WORKDIR /$PROJECT_DIR
COPY . .
RUN pip install --upgrade pip && pip install -e /$PROJECT_DIR/[streaming]

# Add Tini
ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

ENTRYPOINT ethereumetl stream --provider-uri ${GETH_URL} --start-block ${START_BLOCK} --output kafka/${KAFKA_URL} --entity-types ${ENTITY_TYPES} --batch-size ${BATCH_SIZE} --block-batch-size ${BLOCK_BATCH_SIZE}
