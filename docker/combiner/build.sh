#!/bin/bash

CURR_DIR=$(cd $(dirname ${0}) && pwd)

VERSION=1

CONTAINER_NAME=noct_combiner
IMAGE_NAME=harbor.sktai.io/aidp/noct/combiner
TMP_TAG=${IMAGE_NAME}:0
NEW_TAG=${IMAGE_NAME}:${VERSION}
LATEST_TAG=${IMAGE_NAME}:latest

docker build --force-rm=true --no-cache -f ./Dockerfile -t ${TMP_TAG} .
docker image ls ${TMP_TAG}

function cleanup_remaining_container() {
    CANDIDATES=$(docker container ls -a -q -f name=${CONTAINER_NAME})
    if [ ! -z ${CANDIDATES} ]; then
        echo "* Stop and remove container"
        docker container kill ${CONTAINER_NAME}
        docker container rm -f ${CONTAINER_NAME}
    fi
}

cleanup_remaining_container

echo "* Run a new docker container"
docker run -i -d --name=${CONTAINER_NAME} --privileged ${TMP_TAG} /sbin/init

if [ $? != 0 ]; then
    echo "Unexpected error happened at initialization after instantiation. Resolve the problem above and retry again."
    exit 1
fi

docker commit -m "${COMMENT_ON_IMAGE}" ${CONTAINER_NAME} ${NEW_TAG}
docker tag ${NEW_TAG} ${LATEST_TAG}

echo "* list up relevant docker images"
docker image ls ${IMAGE_NAME}

echo "* inspect comment in docker image"
docker inspect ${LATEST_TAG} | grep Comment

echo "* upload docker images"
docker push ${NEW_TAG}
docker push ${LATEST_TAG}

cleanup_remaining_container

echo "* Bye :)"

exit 0
