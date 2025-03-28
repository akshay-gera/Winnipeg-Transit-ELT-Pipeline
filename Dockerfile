FROM quay.io/astronomer/astro-runtime:12.7.1
# Install git and other dependencies
USER root
RUN apt-get update && apt-get install -y git
  # Switch back to the original user
RUN pip install dbt-bigquery
# Install git
# Copy the profiles.yml file into the container
COPY profiles/profiles.yml /home/astro/.dbt/profiles.yml