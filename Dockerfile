ARG PARENT_IMAGE_NAME=openjdk
ARG PARENT_IMAGE_TAG=14-alpine

FROM ${PARENT_IMAGE_NAME}:${PARENT_IMAGE_TAG}

LABEL maintainer="info@redmic.es"

COPY /target/*.jar ./

CMD ["java", "-cp", "./vesselrestrictionchecker.jar:./dependency/*", \
	"es.redmic.kafkastreams.vesselrestrictionchecker.VesselRestrictionCheckerApplication"]
