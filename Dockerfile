FROM bitnami/spark:3.1.2
USER root
WORKDIR /workspace
ENV PYTHONIOENCODING utf8
RUN export PYTHONIOENCODING
COPY requirements.txt /workspace
RUN pip install -r requirements.txt
