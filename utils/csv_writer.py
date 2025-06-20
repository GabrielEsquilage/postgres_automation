import csv
import os

def salvar_csv(resultados, caminho_arquivo, colunas_cabecalho):
    os.makedirs(os.path.dirname(caminho_arquivo), exist_ok=True)

    with open(caminho_arquivo, mode="w", newline='') as arquivo:
        writer = csv.writer(arquivo)
        writer.writerow(colunas_cabecalho)

        for linha in resultados:
            linha_para_escrever = [linha[col] for col in [c.lower() for c in colunas_cabecalho]]
            writer.writerow(linha_para_escrever)

    print(f"\n📄 CSV salvo em: {caminho_arquivo}")