#!/bin/bash
curl --location --output /tmp/jvm.tar.gz https://github.com/SAP/SapMachine/releases/download/sapmachine-11.0.2/sapmachine-jre-11.0.2_linux-x64_bin.tar.gz && cd /tmp && tar --strip-components=1 -xzf jvm.tar.gz
if [ ! -f wss-unified-agent.jar ]; then
    curl -LJO --output /tmp/wss-unified-agent.jar https://github.com/whitesource/unified-agent-distribution/raw/master/standAlone/wss-unified-agent.jar
fi