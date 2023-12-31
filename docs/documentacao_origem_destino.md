# Documentação Técnica - API INFORMAÇÕES AFL

## Introdução

Este projeto visa reunir dados de diversas fontes sobre estabelecimentos no setor de Alimentação Fora do Lar (AFL), como Restaurantes, Bares, Padarias, Casas noturnas, Eventos e outros. O objetivo é qualificar, validar, padronizar e fornecer esses dados através de uma Web API com tecnologia REST.

As fontes de dados incluem os dados abertos da [Receita Federal do Brasil](https://www.gov.br/receitafederal/pt-br) para CNPJ. Além disso, utilizamos nossa base interna do Sistema de Gestão Abrasel (SIGA) e dados de terceiros, como a rede credenciada de:

- [Sodexo](https://www.sodexobeneficios.com.br/sodexo-club/rede-credenciada/)
- [Alelo](https://www.alelo.com.br/onde-aceita)
- [Ben Visa Vale](https://bensite.conductor.com.br/usuario/#rede-credenciada)
- [VR](https://portal.vr.com.br/portal/app/rede-credenciada/)
- [Ticket](https://www.ticket.com.br/portal-usuario/rede-credenciada)
- Estabelecimentos credenciados no [Cadastur](https://cadastur.turismo.gov.br/hotsite/#!/public/capa/entrar)

## Autenticação e Autorização

Acesso aos dados qualificados é fornecido através da Sensedia Platform, que atua como um gateway de acesso, gerenciando autenticação, autorização e operações de API.

## Formato das Solicitações e Respostas

As solicitações são feitas via protocolo HTTP, usando os métodos POST para autenticação e GET para obtenção de dados. Os dados são retornados nos formatos JSON, CSV e XLSX.

## Endpoints e Rotas

- [**GET**] `/api/leads/v1/estabelecimentos`
  
  Este endpoint retorna uma lista de estabelecimentos. Vários parâmetros de consulta estão disponíveis para filtragem. Por exemplo, é possível filtrar por origem, UF, cidade e outros campos.

  ```json
    [
        {
            "CNPJ": "",
            "RAZAO_SOCIAL_RFB": "",
            "NOME_FANTASIA": "",
            "CEP": "",
            "ENDERECO": "",
            "BAIRRO": "",
            "CIDADE": "",
            "UF": "",
            "TELEFONE": "",
            "TELEFONE_RFB": "",
            "EMAIL": "",
            "ORIGEM": [
                "CADASTUR",
                "VR",
                "TICKET",
                "ALELO",
                "SODEXO",
                "BENVISAVALE"
            ],
            "BASE_SIGA": "" ,
            "ASSOCIADO": "",
            "SOU_ABRASEL": "",
            "LATITUDES": "",
            "LONGITUDES": ""
        }
    ]
    ```

- [**GET**] `/api/leads/v1/estabelecimentos/counts`
  
  Este endpoint retorna contagens de ocorrências com base nos filtros fornecidos. Os parâmetros de `groupby` e `orderby` permitem a ordenação dos resultados.

  ```json
  [
      {
          "TOTAL": 17062
      }
  ]
  ```

## Tipos de Dados

- **CNPJ**: string - Identificador único da empresa na Receita Federal.
- **RAZAO_SOCIAL_RFB**: string - Nome da empresa na Receita Federal.
- **NOME_FANTASIA**: string - Nome público do estabelecimento.
- **CEP**: string - Código de Endereçamento Postal, criado e utilizado pelos Correios para facilitar o encaminhamento e a entrega das correspondências aos destinatários.
- **ENDERECO**: string - Endereço físico do estabelecimento na base da receita federal.
- **BAIRRO**: string - Distrito ou bairro de endereço físico do estabelecimento.
- **CIDADE**: string - Município de localização física do estabelecimento.
- **UF**: string - Sigla da Unidade Federativa/Estado de localização física do estabelecimento.
- **TELEFONE**: string - Telefone do estabelecimento na base ordenada pelo parâmetro `origem` se não informado origem, pega da base do cadastur.
- **TELEFONE_RFB**: string - Telefone do estabelecimento na base da receita federal
- **EMAIL**: string - Endereço eletrônico do estabelecimento na fonte determinada pelo parâmetro `origem` se não informado, da base do cadastur ou receita federal.
- **ORIGEM**: array - Lista contendo as fontes de onde esse estabelecimento pode vir, qualificador de enriquecimento e curadoria dos dados.
- **ASSOCIADO**: string - Informa se o estabelecimento se encontra na base do `SIGA`, `0` se não existir, `ATIVO` se existir e for associado, `INATIVO` se existir e for ex-associado.
- **SOU_ABRASEL**: string - Informa se o estabelecimento se encontra na base do `SIGA`, `0` se não existir, `ATIVO` se existir e for associado com sou abrasel, `INATIVO` se existir e for ex-associado sem sou abrasel, `CANCELADO` se tiver o cadastro no sou abrasel cancelado por algum motivo que não seja não ser associado.

## Fontes de Dados

- **Dados Abertos da Receita Federal do Brasil**: Atualizados mensalmente, contêm informações sobre empresas ativas e inativas.
- **Rede Credenciada**: Dados das redes Sodexo, Alelo, Ben Visa Vale, VR e Ticket, contendo informações de localização e aceitação de bandeiras.
- **Cadastur**: Dados atualizados em tempo real pelo Ministério do Turismo, mostrando empresas associadas ao turismo.
- **API do SIGA**: Validar a presença de estabelecimentos em nossa base interna.

## Exemplos de Uso

Esses dados qualificados são úteis para identificar leads, aumentar a base de associados da Abrasel e mapear a presença de estabelecimentos do setor de AFL em todo o Brasil. Também são úteis para usuários finais que desejam encontrar estabelecimentos em uma determinada região.

## Códigos de Status e Erros

- **200**: Sucesso.
- **206**: Sucesso com retorno parcial dos dados.
- **204**: Sucesso, mas sem dados.
- **400**: Erro de solicitação.
- **404**: Página não encontrada.
- **405**: Método não permitido.
- **500**: Erro interno do servidor.