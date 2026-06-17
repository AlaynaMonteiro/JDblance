# JDblance: one image for the load balancer, the demo origins, the load generator
# and the alerting sidecar.
#
# Built with the same toolchain the test suite runs on (Ubuntu 24.04, g++ 13);
# the runtime stage is the base image plus four stripped binaries and curl
# (health check, and the alert bot's HTTPS delivery).  No language runtime,
# nothing to pip install, nothing to audit beyond glibc.
FROM ubuntu:24.04 AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends g++ make \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Makefile ./
COPY src/ src/
COPY cmd/ cmd/
RUN make -j"$(nproc)" build/jdblance build/jdblance-origin build/jdblance-loadgen build/jdblance-alertbot \
 && strip build/jdblance build/jdblance-origin build/jdblance-loadgen build/jdblance-alertbot

FROM ubuntu:24.04
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 jdblance
COPY --from=build /src/build/jdblance /src/build/jdblance-origin /src/build/jdblance-loadgen \
                  /src/build/jdblance-alertbot /usr/local/bin/
COPY lb.json /etc/jdblance/lb.json
ENV JDBLANCE_CONFIG=/etc/jdblance/lb.json

# Data plane, control plane.
EXPOSE 8000 8001
USER jdblance

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -sf http://127.0.0.1:8001/healthz || exit 1

ENTRYPOINT ["jdblance"]
