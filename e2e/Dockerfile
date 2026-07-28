FROM e2e-extension as ext
FROM ghcr.io/linuxsuren/api-testing:master

WORKDIR /workspace
COPY . .
COPY --from=ext /usr/local/bin/atest-store-mqtt /usr/local/bin/atest-store-mqtt

CMD [ "/workspace/entrypoint.sh" ]
