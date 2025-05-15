FROM mcr.microsoft.com/devcontainers/python:1-3.12-bullseye

ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN sudo chown -R vscode:vscode $VIRTUAL_ENV

RUN pip install loguru maturin[patchelf]
