# Atividade de Engenharia de Dados — Visagio Rocket Lab 2026

## Descrição

Pipeline de dados completo para um e-commerce, desenvolvido como atividade prática do processo seletivo Visagio Rocket Lab 2026. O projeto implementa uma arquitetura Medallion (Bronze, Silver, Gold) no Databricks, utilizando o dataset fornecido pela Visagio baseado no dataset público da Olist como fonte de dados.

## Arquitetura

```
Fontes de Dados
  ├── Dataset Olist (fornecido pela Visagio via Google Drive) — 9 arquivos CSV
  └── API Banco Central (PTAX) — cotação do dólar (coletada localmente)

Camada Bronze — Ingestão bruta
  └── 10 tabelas Delta com timestamp de ingestão

Camada Silver — Limpeza e padronização
  └── 10 tabelas Delta com regras de negócio aplicadas

Camada Gold — Data Marts analíticos
  └── 2 tabelas agregadas com KPIs de negócio
```

## Estrutura do Repositório

```
atividade_rocketlab_2026/
  ├── notebooks/
  │   ├── Atividade_land_to_bronze.ipynb
  │   ├── Atividade_bronze_to_silver.ipynb
  │   └── Atividade_silver_to_gold.ipynb
  ├── workflow/
  │   └── pipeline_olist_rocketlab.yml
  ├── docs/
  │   └── job_execucao_sucesso.png
  └── README.md
```

## Tecnologias Utilizadas

- **Databricks** (Trial — Serverless) — plataforma de Data Lakehouse
- **Apache Spark / PySpark** — processamento distribuído
- **Delta Lake** — formato de armazenamento com suporte a ACID transactions
- **Python** — linguagem principal dos notebooks
- **Databricks Workflows** — orquestração do pipeline

## Dataset

Dataset fornecido pela Visagio via Google Drive, baseado no [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Contém dados reais de um e-commerce brasileiro com pedidos, clientes, produtos, vendedores, pagamentos e avaliações entre 2016 e 2018. A versão fornecida pela Visagio inclui colunas adicionais em relação ao dataset público original, como `customer_name`, `customer_gender`, `customer_birth_date`, `customer_age`, `seller_name` e `seller_registration_date`.

## Camada Bronze

Responsável pela ingestão dos dados brutos sem nenhuma transformação. Cada tabela recebe uma coluna `timestamp_ingestion` com o momento exato da carga, permitindo rastreabilidade e deduplicação nas camadas superiores.

| Tabela | Origem | Registros |
|--------|--------|-----------|
| bronze.tb_customers | olist_customers_dataset.csv | 99.441 |
| bronze.tb_geolocalizacao | olist_geolocation_dataset.csv | 1.000.163 |
| bronze.tb_order_items | olist_order_items_dataset.csv | 112.650 |
| bronze.tb_order_payments | olist_order_payments_dataset.csv | 103.886 |
| bronze.tb_order_reviews | olist_order_reviews_dataset.csv | 104.162 |
| bronze.tb_orders | olist_orders_dataset.csv | 99.441 |
| bronze.tb_products | olist_products_dataset.csv | 32.951 |
| bronze.tb_sellers | olist_sellers_dataset.csv | 3.095 |
| bronze.tb_product_category_name_translation | product_category_name_translation.csv | 71 |
| bronze.tb_cotacao_dolar | cotacao_dolar.csv (coletado via API localmente) | 542 |

### Decisão: Ingestão da cotação do dólar via CSV local

A atividade solicitava a extração da cotação do dólar diretamente via API do Banco Central (PTAX) dentro do notebook Bronze. No entanto, o ambiente Databricks Trial possui restrição de acesso à rede externa, impedindo requisições HTTP para domínios fora da plataforma — incluindo `olinda.bcb.gov.br`.

Como solução, foi desenvolvido um script Python executado localmente que consome a mesma API com os mesmos parâmetros solicitados no enunciado (período de 09/2016 a 10/2018, endpoint PTAX, formato JSON) e gera um arquivo `cotacao_dolar.csv`. Esse arquivo foi carregado no Volume `landing_zone` do Databricks, seguindo o mesmo fluxo de ingestão dos demais arquivos brutos. O script de coleta está documentado e comentado no notebook `Atividade_land_to_bronze.ipynb`.

## Camada Silver

Responsável pela limpeza, padronização e aplicação das regras de negócio. Todas as colunas estão renomeadas para português com tipagem correta. As tabelas Bronze nunca são alteradas.

### Decisões técnicas aplicadas

**Deduplicação via Window Function**
Em `dim_consumidores`, `dim_produtos` e `dim_vendedores`, a deduplicação é feita particionando pelo ID da entidade e ordenando de forma decrescente pelo `timestamp_ingestion`. Isso garante que, em reprocessamentos futuros, apenas o registro mais recente seja mantido — padrão conhecido como "deduplicação sênior".

**Tradução de status e tipos de pagamento**
Os campos `order_status` em `fat_pedidos` e `payment_type` em `fat_pagamentos_pedidos` foram traduzidos de inglês para português via `when/otherwise`, preservando os valores originais na Bronze e aplicando a tradução apenas na Silver.

**Colunas derivadas de entrega**
Em `fat_pedidos`, foram criadas quatro colunas derivadas: `tempo_entrega_dias`, `tempo_entrega_estimado_dias`, `diferenca_entrega_dias` e `entrega_no_prazo`. A coluna `entrega_no_prazo` considera o status do pedido — pedidos não entregues recebem "Nao Entregue" independentemente das datas.

**Tolerância a falhas em datas**
Em `fat_avaliacoes_pedidos`, as colunas de data são convertidas com `try_to_timestamp` em vez de `to_timestamp`. Essa função retorna `null` para valores inválidos em vez de lançar uma exceção, garantindo que registros com datas mal formatadas não interrompam o processamento.

**Preenchimento de nulos em avaliações**
Títulos e comentários vazios ou nulos são preenchidos explicitamente com "Sem titulo" e "Sem comentario" usando `coalesce` combinado com `when/trim`, evitando que nulos se propaguem para a camada Gold.

**Calendário contínuo de cotação do dólar**
A API do Banco Central não fornece cotação para finais de semana e feriados. Em `dim_cotacao_dolar`, foi criado um calendário contínuo cobrindo todo o período do dataset e os dias sem cotação recebem o valor de fechamento do último dia útil anterior, usando `last(ignorenulls=True)` sobre uma Window Function ordenada por data. Isso garante que todos os pedidos tenham um valor de conversão USD disponível.

**Otimização física com OPTIMIZE e ZORDER**
No final do notebook Silver, as tabelas fato são otimizadas com `OPTIMIZE ZORDER BY (id_pedido, data_pedido)`. O OPTIMIZE compacta os pequenos arquivos gerados pelo Delta Lake em arquivos maiores, e o ZORDER reorganiza os dados fisicamente no disco pelas colunas de filtragem mais frequentes, acelerando as consultas analíticas da camada Gold.

| Tabela | Registros |
|--------|-----------|
| silver.dim_consumidores | 99.441 |
| silver.fat_pedidos | 99.441 |
| silver.fat_itens_pedidos | 112.650 |
| silver.fat_pagamentos_pedidos | 103.886 |
| silver.fat_avaliacoes_pedidos | 95.307 |
| silver.dim_produtos | 32.951 |
| silver.dim_vendedores | 3.095 |
| silver.dim_categoria_produtos_traducao | 71 |
| silver.dim_cotacao_dolar | 791 |
| silver.fat_pedido_total | 99.441 |

## Camada Gold

Responsável pelos Data Marts analíticos prontos para consumo pela área de negócio. As gravações são feitas em modo `overwrite` com `overwriteSchema = true` para garantir idempotência em reprocessamentos.

### Projeto 1 — Visão Comercial

**gold.fat_vendas_comercial** agrega as vendas por ano, mês e categoria de produto, calculando `total_pedidos` (contagem distinta de pedidos), `qtd_itens_vendidos` (contagem absoluta de itens), `receita_total_brl`, `receita_total_usd` e `ticket_medio_brl`. Os valores financeiros são arredondados para exatamente 2 casas decimais.

Rankings exibidos via `display()`:
- Top 5 produtos mais vendidos (com nome do produto e categoria)
- Top 5 produtos menos vendidos

### Projeto 2 — Satisfação de Clientes

**gold.fat_avaliacoes_clientes** agrega avaliações por categoria de produto, nome do vendedor e estado, calculando `total_avaliacoes`, `avaliacao_media`, `total_avaliacoes_positivas` (notas >= 4), `total_avaliacoes_negativas` (notas <= 2) e `percentual_satisfacao`.

Rankings exibidos via `display()` com critério de ordenação composto (nota + volume de avaliações como desempate):
- Produto mais bem avaliado
- Produto menos bem avaliado
- Vendedor mais bem avaliado
- Vendedor menos bem avaliado

## Orquestração

O pipeline é orquestrado via **Databricks Workflows** com 3 tasks sequenciais:

```
Atividade_land_to_bronze → Atividade_bronze_to_silver → Atividade_silver_to_gold
```

Agendamento: diariamente às 13h00 (UTC-03:00 — America/Fortaleza)

A dependência entre tasks garante que a Silver só executa após o sucesso da Bronze, e a Gold só executa após o sucesso da Silver. Em caso de falha em qualquer etapa, as tasks subsequentes são automaticamente canceladas.
