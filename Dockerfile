ARG PARENT_IMAGE_NAME=adoptopenjdk/openjdk8
ARG PARENT_IMAGE_TAG=jdk8u222-b10-slim

FROM ${PARENT_IMAGE_NAME}:${PARENT_IMAGE_TAG}

LABEL maintainer="info@redmic.es"

COPY /target/*.jar ./

COPY /target/dependency/*.jar ./dependency/

CMD ["java", "-cp", "./vessel-restriction-checker.jar:./dependency/*", \
	"es.redmic.vesselrestrictionchecker.VesselRestrictionCheckerApplication"]
