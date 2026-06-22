import subprocess
import sys
import os

# Lista de orquestradores (em ordem)
orchestrators = [
    os.path.abspath(os.path.join("datamart","facebook", "1_bronze","1_Orchestrator", "1_orchestrator_facebook_bronze.py")),
    os.path.abspath(os.path.join("datamart","facebook", "1_bronze","1_Orchestrator", "2_orchestrator_facebook_bronze.py")),
    os.path.abspath(os.path.join("datamart","facebook", "2_silver","1_Orchestrator", "3_orchestrator_facebook_silver.py")),
    os.path.abspath(os.path.join("datamart","facebook", "3_gold","1_Orchestrator", "4_orchestrator_facebook_gold.py"))
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
