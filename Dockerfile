ARG PARENT_IMAGE_NAME=openjdk
ARG PARENT_IMAGE_TAG=14-alpine

FROM ${PARENT_IMAGE_NAME}:${PARENT_IMAGE_TAG}

LABEL maintainer="info@redmic.es"

RUN apk add --no-cache \
	libstdc++=8.3.0-r0 \
	libc6-compat=1.1.22-r3

COPY /target/*.jar ./

COPY /target/dependency/*.jar ./dependency/

CMD ["java", "-cp", "./vessel-restriction-checker.jar:./dependency/*", \
	"es.redmic.vesselrestrictionchecker.VesselRestrictionCheckerApplication"]
