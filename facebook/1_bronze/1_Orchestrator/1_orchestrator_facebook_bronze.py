import subprocess
import sys
import os

# Lista de orquestradores (em ordem)
orchestrators = [
    os.path.abspath(os.path.join("datamart", "facebook", "1_bronze", "cargaDiaria", "1_diaAdia_face_camp.py")),
    os.path.abspath(os.path.join("datamart", "facebook", "1_bronze", "cargaDiaria", "2_diaAdia_face_adset.py")),   
    os.path.abspath(os.path.join("datamart", "facebook", "1_bronze", "cargaDiaria", "3_diaAdia_face_ad.py"))
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
