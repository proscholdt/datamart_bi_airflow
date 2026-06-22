import subprocess
import sys
import os

orchestrators = [
    os.path.abspath(os.path.join("datamart", "facebook", "1_bronze", "transformers", "1_facebook_campaign_bronze_to_silver_parquet.py")),
    os.path.abspath(os.path.join("datamart", "facebook", "1_bronze", "transformers", "2_facebook_campaign_moveTOcarregados.py")),
    os.path.abspath(os.path.join("datamart", "facebook", "1_bronze", "transformers", "3_facebook_adset_bronze_to_silver_parquet.py")),
    os.path.abspath(os.path.join("datamart", "facebook", "1_bronze", "transformers", "4_facebook_adset_moveTOcarregados.py")),
    os.path.abspath(os.path.join("datamart", "facebook", "1_bronze", "transformers", "5_facebook_ad_bronze_to_silver_parquet.py")),
    os.path.abspath(os.path.join("datamart", "facebook", "1_bronze", "transformers", "6_facebook_ad_moveTOcarregados.py"))
]


print("🚀 Iniciando execução sequencial dos orquestradores...\n")

for idx, orchestrator in enumerate(orchestrators, 1):
    print(f"▶️ [{idx}/{len(orchestrators)}] Executando: {orchestrator}")

    try:
        subprocess.run(
            [sys.executable, orchestrator],
            check=True
        )
        print(f"✅ [{idx}] Concluído com sucesso.\n")

    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar {orchestrator}")
        print("\nExecução interrompida devido a erro.")
        sys.exit(1)

print("🎉 Todos os orquestradores foram executados com sucesso!")
