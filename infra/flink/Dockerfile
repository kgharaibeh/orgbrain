FROM flink:1.19.3-scala_2.12-java11

# Install Python 3 and build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev gcc g++ \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf python3 /usr/bin/python

# Install PyFlink and anonymizer dependencies
# Note: apache-flink PyPI package must match the Flink runtime version
RUN pip3 install --no-cache-dir \
        apache-flink==1.19.0 \
        kafka-python==2.0.2 \
        requests==2.32.3 \
        hvac==2.1.0 \
        ff3==1.0.1 \
        cryptography==42.0.8 \
        pyyaml==6.0.1 \
        presidio-analyzer==2.2.354 \
        presidio-anonymizer==2.2.354

# Download the spaCy English model required by presidio-analyzer
RUN python3 -m spacy download en_core_web_sm

# Copy the anonymizer job and PII engine into the image
COPY governance/anonymizer/flink_anonymizer_job.py /opt/flink/jobs/flink_anonymizer_job.py
COPY governance/anonymizer/pii_engine.py            /opt/flink/jobs/pii_engine.py

WORKDIR /opt/flink/jobs
