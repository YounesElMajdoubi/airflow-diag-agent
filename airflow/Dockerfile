# Image Airflow officielle + script d'initialisation du projet.
# psycopg2 et requests sont déjà fournis par l'image (fournisseur postgres) ; on les
# installe quand même avec les contraintes officielles pour ne pas dépendre de ce détail.
FROM apache/airflow:3.3.1
ARG AIRFLOW_VERSION=3.3.1
RUN PY=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")') && \
    pip install --no-cache-dir psycopg2-binary requests \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY}.txt"
COPY --chmod=755 init.sh /opt/airflow/init.sh
