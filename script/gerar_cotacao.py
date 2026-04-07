import requests
import csv

DATA_INICIO = "09-01-2016"
DATA_FIM    = "10-31-2018"

url = (
    "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
    f"CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
    f"?@dataInicial='{DATA_INICIO}'&@dataFinalCotacao='{DATA_FIM}'"
    f"&$select=dataHoraCotacao,cotacaoCompra&$format=json"
)

resposta = requests.get(url, timeout=60)
dados    = resposta.json().get("value", [])

print(f"Registros retornados: {len(dados)}")

with open("cotacao_dolar.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["dataHoraCotacao", "cotacaoCompra"])
    writer.writeheader()
    writer.writerows(dados)

print("Arquivo cotacao_dolar.csv gerado com sucesso.")