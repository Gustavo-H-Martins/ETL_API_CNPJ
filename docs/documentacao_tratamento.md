# Documentação de Tratamento de Dados

## Fonte: [Dados Abertos da Receita Federal do Brasil (RFB)](http://200.152.38.155/CNPJ/)

### **Dados Obtidos:**
1. **Estabelecimentos:** 
    [Estabelecimentos0.zip](http://200.152.38.155/CNPJ/Estabelecimentos0.zip) /
    [Estabelecimentos1.zip](http://200.152.38.155/CNPJ/Estabelecimentos1.zip) /
    [Estabelecimentos2.zip](http://200.152.38.155/CNPJ/Estabelecimentos2.zip) /
    [Estabelecimentos3.zip](http://200.152.38.155/CNPJ/Estabelecimentos3.zip) /
    [Estabelecimentos4.zip](http://200.152.38.155/CNPJ/Estabelecimentos4.zip) /
    [Estabelecimentos5.zip](http://200.152.38.155/CNPJ/Estabelecimentos5.zip) /
    [Estabelecimentos6.zip](http://200.152.38.155/CNPJ/Estabelecimentos6.zip) /
    [Estabelecimentos7.zip](http://200.152.38.155/CNPJ/Estabelecimentos7.zip) /
    [Estabelecimentos8.zip](http://200.152.38.155/CNPJ/Estabelecimentos8.zip) /
    [Estabelecimentos9.zip](http://200.152.38.155/CNPJ/Estabelecimentos9.zip)

2. **Empresas:**
    [Empresas0.zip](http://200.152.38.155/CNPJ/Empresas0.zip) / 
    [Empresas1.zip](http://200.152.38.155/CNPJ/Empresas1.zip) / 
    [Empresas2.zip](http://200.152.38.155/CNPJ/Empresas2.zip) / 
    [Empresas3.zip](http://200.152.38.155/CNPJ/Empresas3.zip) / 
    [Empresas4.zip](http://200.152.38.155/CNPJ/Empresas4.zip) / 
    [Empresas5.zip](http://200.152.38.155/CNPJ/Empresas5.zip) / 
    [Empresas6.zip](http://200.152.38.155/CNPJ/Empresas6.zip) / 
    [Empresas7.zip](http://200.152.38.155/CNPJ/Empresas7.zip) / 
    [Empresas8.zip](http://200.152.38.155/CNPJ/Empresas8.zip) / 
    [Empresas9.zip](http://200.152.38.155/CNPJ/Empresas9.zip)
3. **Municipios:**
    [Municipios.zip](http://200.152.38.155/CNPJ/Municipios.zip)
4. **Cnaes:**
    [Cnaes.zip](http://200.152.38.155/CNPJ/Cnaes.zip)

### **Manipulação de dados:**
1. Utilizado **Apache Spark** como stack para processamento distribuído e manipulação dos dados brutos
    ```Python
        # Define ou busca uma sessão do Spark
            spark = (
                SparkSession.builder.master("local[2]")
                .appName("OnlineReader")
                .config("spark.driver.memory", "3g")
                .config("spark.driver.maxResultSize", "3g")
                .getOrCreate()
            )
            spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    ```
2. Transforma cada conjunto de dados baixados em _`dataframes`_ spark e posteriormente em visualizações temporárias do **`Spark.SQL`**
    ```Python
        ESTABELECIMENTOS.createOrReplaceTempView("ESTABELECIMENTOS")
        EMPRESAS.createOrReplaceTempView("EMPRESAS")
        MUNICIPIOS.createOrReplaceTempView("MUNICIPIOS")
        CNAES.createOrReplaceTempView("CNAES")
    ```

3. Concatena todos os dados das views em um _`dataframes`_ spark e convertido em _`dataframes`_ **`pandas`** e salvo como arquivo `csv` único com as empresas inclusas nos _cnaes_ representados pelo setor de `AFL`
    ```Python
        CNAES_FILTROS = {
        5620104:'Fornecimento de alimentos preparados preponderantemente para consumo domiciliar',
        5611201:'Restaurantes e similares',
        5611203:'Lanchonetes casas de chá de sucos e similares',
        5611204:'Bares e outros estabelecimentos especializados em servir bebidas sem entretenimento',
        5611205:'Bares e outros estabelecimentos especializados em servir bebidas com entretenimento',
        4721102:'Padaria e confeitaria com predominância de revenda',
        5612100:'Serviços ambulantes de alimentação'
        }
    ```
### **Dados Tratados:**
1. **CNPJ:** Utilizado como identificador único da empresa, removidos os duplicados na etapa de transformação, permanencia dos ativos.
2. **RAZAO_SOCIAL:** Nome da empresa utilizado para validação e enriquecimento, e verificação de existência de rastro digitais na api do google search
3. **Outros dados:** Validados para garantir integridade e precisão.
   1. **NOME_FANTASIA:**
   2. **ENDERECO:**
   3. **BAIRRO:**
   4. **CIDADE:**
   5. **UF:**
   6. **CEP:**
   7. **TELEFONE:**
   8.  **EMAIL:**
   9.  **CNAE_PRINCIPAL:**
   10. **CNAE_DESCRICAO:**
   11. **SITUACAO_CADASTRAL:**
   12. **DATA_SITUACAO_CADASTRAL:**
   13. **DATA_INICIO_ATIVIDADE:**

### **Tratamentos Realizados:**
1. **Validação de CNPJ:** Verificação da estrutura e autenticidade do CNPJ.
   1. Fit de amostragem 20k de cnpj usados para mapeamendo com crawler de busca na receita federal.
2. **Padronização de Nomes:** Padronização dos nomes de empresas.
3. **Remoção de Duplicatas:** Identificação e remoção de entradas duplicadas.
4. **Enriquecimento de Dados:** Uso de outras fontes para enriquecer as informações, como endereço e contato com api de cep e geonames.

## Fonte: Cadastur
### **Dados Obtidos:**
1. Os dados são obtidos via requisição http com a api pública do [**cadastur**](https://cadastur.turismo.gov.br/cadastur-backend/rest/portal/obterDadosPrestadores)
    ```json
        {
            "currentPage": 1,
            "pageSize": 10000,
            "totalResults": 22,
            "sortFields": "nomePrestador",
            "sortDirections": "ASC",
            "filtros": {
                "noPrestador": "",
                "localidade": 9668,
                "nuAtividadeTuristica": "Restaurante, Cafeteria, Bar e Similares",
                "souPrestador": false,
                "souTurista": true,
                "localidadesUfs": "São Paulo, SP",
                "localidadeNuUf": 25,
                "flPossuiVeiculo": "",
                "bairro": "centro"
            },
            "list": [
                {
                "id": 00000000,
                "tipoPessoa": "PJ",
                "numeroCadastro": "xxxxxxxxxx",
                "dtInicioVigencia": 1668865026057,
                "dtFimVigencia": 1732023426057,
                "noWebSite": null,
                "nuTelefone": "xxxxxxxxxx",
                "noLogradouro": "xxxxxxxxxx",
                "complemento": "xxxxxxxxxx",
                "nuCep": "000000000",
                "sguf": "SP",
                "noBairro": "Centro",
                "nomePrestador": "xxxxxxxxxx",
                "registroRf": "xxxxxxxxxx",
                "nuAtividadeTuristica": 00,
                "atividade": "Restaurante, Cafeteria, Bar e Similares",
                "nuSituacaoCadastral": 4,
                "situacao": "Regular",
                "nuUf": 00,
                "localidadeNuUf": null,
                "localidade": "São Paulo",
                "noLocalidade": "São Paulo",
                "nuLocalidade": 9668,
                "nuPessoa": 90958,
                "natJuridEspecial": false,
                "municipio": "São Paulo",
                "nuMunicipio": 9668,
                "flPossuiVeiculo": null,
                "nuSitCadTramite": 9,
                "atividadeRedeSociais": null
                }
            ]
        }
    ```
### **Manipulação de dados:**
1. Há uma processo de normalização e padronização dos dados feito direto na requisição http
    ```Python
        # Formata o json de retorno
        data = response.json()["list"]
        base = []
        for d in data:
            base.append({
                "CNPJ": str(d.get("numeroCadastro", "")),
                "CNPJ_FORMATADO": formata_cnpj(d.get("numeroCadastro", "")),
                "NOME_FANTASIA" : str(d.get("nomePrestador", "")),
                "RAZAO_SOCIAL" : str(d.get("registroRf", "")),
                "INICIO_VIGENCIA": formata_data(d.get("dtInicioVigencia", 0)),
                "FIM_VIGENCIA": formata_data(d.get("dtFimVigencia", 0)),
                "SITE": d.get("noWebSite", ""),
                "TELEFONE": formata_telefone(d.get("nuTelefone", "")),
                "CEP": d.get("nuCep", ""),
                "ENDERECO": str(d.get("noLogradouro", "")).capitalize() + ", " + str(d.get("complemento","")).capitalize(),
                "BAIRRO": formata_bairro_e_cidade(d.get("noBairro","")),
                "CIDADE" : formata_bairro_e_cidade(d.get("municipio", "")),
                "UF": d.get("sguf", ""),
                "ATIVIDADE": str(d.get("atividade","")).capitalize(),
                "COD_SITUACAO_CADASTRAL": d.get("nuSituacaoCadastral", ""),
                "SITUACAO_CADASTRAL": d.get("situacao",""),
                "ID_PRESTADOR": d.get("id",""),
                "URL_DETALHES_PRESTADOR" : formata_url_prestador(d.get("id",""))
            })
    ```
2. Após isso os dados são lidos em um `dataframe` pandas, removido as duplicatas e salvos em uma tabela sql
### **Dados Tratados:**
1. **RAZAO_SOCIAL:** Nome da empresa ou estabelecimento.
2. **ENDERECO:** Endereço físico do estabelecimento.
3. **URL_DETALHES_PRESTADOR:** Endpoint referente ao detalhamento do prestador cadastrado
4. **CONTATO:** Informações de contato, como telefone e e-mail.

### **Tratamentos Realizados:**

1. **Padronização de Endereços:** Padronização do formato de endereços.
2. **Validação de Contato:** Verificação da validade de números de telefone e e-mails.
3. **Enriquecimento de Dados:** Complemento de informações com base em outras fontes.

## Fontes: Alelo, Ben Visa Vale, Sodexo, Ticket, VR
### **Dados Obtidos:**
Todos os dados são coletados via webcrawler usando selenium para interagir com os sites dinâmicos
```Python
    # pega a resolução da tela
    user32 = ctypes.windll.user32
    resolucao = user32.GetSystemMetrics(0), user32.GetSystemMetrics(1)
    # Omite o Navegador na Execução
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_experimental_option(
        "prefs", {"profile.default_content_setting_values.geolocation": 1})
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation","enable-logging"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument(f'--window-size={resolucao[0]},{resolucao[1]}')
    chrome_options.add_argument("start-maximized")
    prefs = {
            'profile.default_content_setting_values':
                {
                    'notifications': 1,
                    'geolocation': 1
                },
            'devtools.preferences': {
                'emulation.geolocationOverride': "\"11.111698@-122.222954:\"",
            },
            'profile.content_settings.exceptions.geolocation':{
                'BaseUrls.Root.AbsoluteUri': {
                    'last_modified': '13160237885099795',
                    'setting': '1'
                }
            },
            'profile.geolocation.default_content_setting': 1

        }

    chrome_options.add_experimental_option('prefs', prefs)
```
1. [Sodexo](https://www.sodexobeneficios.com.br/sodexo-club/rede-credenciada/)
2. [Alelo](https://www.alelo.com.br/onde-aceita)
3. [Ben Visa Vale](https://bensite.conductor.com.br/usuario/#rede-credenciada)
4. [VR](https://portal.vr.com.br/portal/app/rede-credenciada/)
5. [Ticket](https://www.ticket.com.br/portal-usuario/rede-credenciada)

### **Manipulação de dados:**
* Os dados são coletados, adicionados a um dicionário de dados, convertidos em um `dataframe` pandas e salvos de forma incremental em um `csv`
  
### **Dados Tratados:**
1. **NOME_ESTABELECIMENTO:** Nome do estabelecimento que aceita as bandeiras.
2. **ENDERECO:** Endereço do estabelecimento.
3. **CONTATO:** Informações de contato, como telefone e email.
4. **COORDENADAS** Informações de coordenadas e localização latitude e longitude

### **Tratamentos Realizados:**

1. **Validação de Endereços:** Verificação e padronização dos endereços.
2. **Enriquecimento de Dados:** Complemento de informações faltantes, como bairro e cidade.

## Fonte: SIGA (Sistema de Gestão Abrasel)
### **Dados Obtidos:**
1. Os dados são obtidos via requisição http com a api pública do [**siga**](https://siga.abrasel.com.br/tools/wsv/associados.jwsv)
    ```json
        [
            {
        "Data": null,
        "S/R": "Minas Gerais",
        "Nome Fantasia": "xxxxxxxxxx",
        "Razão Social": "xxxxxxxxxx",
        "CNPJ": "xxxxxxxxxx",
        "Logradouro": "xxxxxxxxxx",
        "Numero": "00",
        "Comp.": null,
        "Bairro": "Lourdes",
        "CEP": "000000-000",
        "Cidade": "Belo Horizonte",
        "UF": "MG",
        "Contato": null,
        "E-mail pessoal": null,
        "Telefone": null,
        "Celular": null,
        "Criação E-mail": null,
        "E-mail Sou Abrasel": null,
        "Status": "ativo",
        "Status_SouAbrasel": "inativo"
            }
        ]
    ```
### **Manipulação de dados:**
1. Há uma processo de normalização e padronização dos dados feito direto na requisição http
    ```Python
        # Formata o json de retorno
        response = requests.get(URL_SIGA,headers=params, verify=False)
        data = response.json()
        base = []
        for d in data:
            base.append({
            "SEC_REG" : d["S/R"].upper() if d["S/R"] else "",
            "NOME_FANTASIA" : d["Nome Fantasia"].upper() if d["Nome Fantasia"] else "",
            "RAZAO_SOCIAL" : d["Razão Social"].upper() if d["Razão Social"] else "",
            "CNPJ" : d["CNPJ"] if d["CNPJ"] else "",
            "ENDERECO" : d["Logradouro"].upper() if d["Logradouro"] else "" + ", " + d["Numero"].upper() if d["Numero"] else "" + "" + d["Comp."].upper() if d["Comp."] else "",
            "BAIRRO" : d["Bairro"].upper() if d["Bairro"] else "",
            "CEP" : d["CEP"] if d["CEP"] else "" ,
            "CIDADE" : d["Cidade"].upper() if d["Cidade"] else "",
            "UF" : d["UF"].upper() if d["UF"] else "",
            "ASSOCIADO": d["Status"].upper(),
            "SOU_ABRASEL": d["Status_SouAbrasel"].upper()
            })
    ```
2. Após isso os dados são lidos em um `dataframe` pandas, removido as duplicatas e salvos em uma tabela sql
### **Dados Tratados:**
1. **CNPJ:** CNPJ do estabelecimento.
2. **NOME_FANTASIA:** Dados internos para validação.
3. **ASSOCIADO:** Padronização do retorno para automação do fluxo de atualização
4. **SOU_ABRASE:L** Padronização do retorno para automação do fluxo de atualização

### **Tratamentos Realizados:**
1. **Verificação de Associação:** Validação se o CNPJ pertence a um associado.
2. **Validação de Dados Internos:** Verificação de informações internas para garantir coesão.
