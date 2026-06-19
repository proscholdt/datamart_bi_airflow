# Imagem base do Astronomer (Astro Runtime).
# Runtime 3.2 = Airflow 3.2 (igual ao Deployment datamart-bi).
# Para Airflow 3 usa-se só major.minor (pega o último patch).
#   https://www.astronomer.io/docs/astro/runtime-release-notes
FROM quay.io/astronomer/astro-runtime:3.2
