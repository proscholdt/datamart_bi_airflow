import subprocess
import sys
import os

# Lista de orquestradores (em ordem)
orchestrators = [
    os.path.abspath(os.path.join("datamart", "facebook", "2_silver", "1_facebook_campaing_silver_TO_gold.py")),
    os.path.abspath(os.path.join("datamart", "facebook", "2_silver", "2_facebook_campaing_moveTOcarregados.py")),
    os.path.abspath(os.path.join("datamart", "facebook", "2_silver", "3_facebook_adset_silver_to_gold.py")),
    os.path.abspath(os.path.join("datamart", "facebook", "2_silver", "4_facebook_adset_moveTOcarregados.py")),
    os.path.abspath(os.path.join("datamart", "facebook", "2_silver", "5_facebook_ad_silver_to_gold.py")),
    os.path.abspath(os.path.join("datamart", "facebook", "2_silver", "6_facebook_ad_moveTOcarregados.py"))
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
