FROM iomete.azurecr.io/iomete/spark-py:3.5.3-v7

WORKDIR "/app"

# Reset to root to run installation tasks
USER 0

# add source code to the /app directory
COPY setup.py setup.cfg driver.py ./
COPY data_compaction_job ./data_compaction_job

RUN pip install .  --break-system-packages

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}
