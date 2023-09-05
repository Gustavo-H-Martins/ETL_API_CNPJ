//app/routes/leads.js
/**
 * Libs
 */
const db = require('../config/db');
const logToDatabase = require('../config/log')
const router = require('express').Router();
const { salvaCache, lerCache } = require('./cache');
const moment = require('moment');

/**
 * Funções de apoio
 */
// função para contar o número total de registros
function countRecords(query, params) {
  return new Promise((resolve, reject) => {
    db.get(`SELECT COUNT(*) as total FROM (${query})`, params, (err, row) => {
      if (err) reject(err);
      else resolve(row.total);
    });
  });
}

// Função para formatar o valor do CNPJ
const formatCnpj = (cnpj) => {
  // Remover caracteres não numéricos
  cnpj = cnpj.replace(/\D/g, '');

  // Adicionar os separadores
  cnpj = cnpj.replace(/(\d{2})(\d)/, '$1.$2');
  cnpj = cnpj.replace(/(\d{3})(\d)/, '$1.$2');
  cnpj = cnpj.replace(/(\d{3})(\d)/, '$1/$2');
  cnpj = cnpj.replace(/(\d{4})(\d)/, '$1-$2');

  return cnpj;
};


// INCLUSÃO DA FUNÇÃO QUE RECEBE O ARRAY E OS PARÂMETROS DO GROUPBY E CONCATENA
// Usando reduce para criar um objeto que soma os valores por grupo
function agrupar(array, groupby) {
  // Criando um objeto intermediário que acumula os valores por grupo
  let acumulador = array.reduce((objeto, atual) => {
    // Verificando se o groupby contém a propriedade CNAE
    if (groupby.includes("CNAE_SECUNDARIO")) {
      // Convertendo a string CNAE em um array de strings
      let origens = atual.CNAE_SECUNDARIO.split(',').map(element => element.trim());

      // Iterando sobre cada elemento do array de cnae
      origens.forEach(cnae => {
        // Criando uma chave composta pelas propriedades do groupby, substituindo a propriedade CNAE pelo valor da cnae atual
        let chave = groupby.map(prop => prop === "CNAE_SECUNDARIO" ? cnae : atual[prop]).join("|");
        // Se a chave não existir no objeto, iniciando com um objeto com as mesmas propriedades do atual, mas com TOTAL zero
        if (!objeto[chave]) {
          objeto[chave] = { ...atual, TOTAL: 0 };
        }
        // Somando o valor ao objeto
        objeto[chave].TOTAL += atual.TOTAL;
      });
    } //se groupby não tiver a propriedade origens 
    else {
      // Criando uma chave composta pelas propriedades do groupby
      let chave = groupby.map(prop => atual[prop]).join("|");
      // Se a chave não existir no objeto, iniciando com um objeto com as mesmas propriedades do atual, mas com TOTAL zero
      if (!objeto[chave]) {
        objeto[chave] = { ...atual, TOTAL: 0 };
      }
      // Somando o valor ao objeto
      objeto[chave].TOTAL += atual.TOTAL;
    }
    // Retornando o objeto para a próxima iteração
    return objeto;
  }, {}); // O objeto inicial é vazio

  // Convertendo o objeto intermediário em um array de pares [chave, valor]
  let pares = Object.entries(acumulador);

  // Transformando cada par em um objeto com as propriedades desejadas
  let resultado = pares.map(par => {
    // Separando a chave em um array de valores
    let valores = par[0].split("|");
    // Criando um objeto com as propriedades do groupby e seus respectivos valores
    let grupo = groupby.reduce((obj, prop, i) => {
      obj[prop] = valores[i];
      return obj;
    }, {});
    // Adicionando a propriedade TOTAL ao objeto
    grupo.TOTAL = par[1].TOTAL;
    // Retornando o objeto para o array final
    return grupo;
  });

  // Retornando o array final
  return resultado;
}

/**
 * Router - estrutura
 */
// GetAll-Detalhes
router.route("/estabelecimentos")
  .get(async function(req, res, next) {
    const clientIp = req.ip;
    const situacaos = req.query.situacao ? req.query.situacao.split(",") : null;
    const situacaoTuple = situacaos ? `(${situacaos.map((valor) => `"${valor.toUpperCase().replace(/-/g, ' ')}"`).join(",")})` : null;
    const dataInicialAbertura = req.query.dataInicialAbertura ? [moment(req.query.dataInicialAbertura, 'DD-MM-YYYY').format('YYYY-MM-DD')] : null;
    const dataFinalAbertura = req.query.dataFinalAbertura ? [moment(req.query.dataFinalAbertura, 'DD-MM-YYYY').format('YYYY-MM-DD')] : null;
    const dataInicialSituacao = req.query.dataInicialSituacao ? [moment(req.query.dataInicialSituacao, 'DD-MM-YYYY').format('YYYY-MM-DD')] : null;
    const dataFinalSituacao = req.query.dataFinalSituacao ? [moment(req.query.dataFinalSituacao, 'DD-MM-YYYY').format('YYYY-MM-DD')] : null;
    const cnpj = req.query.cnpj ? [req.query.cnpj] : null;
    const cod_cnaes = req.query.cod_cnae ? req.query.cod_cnae.split(",") : (req.query.CNAE ? req.query.CNAE.split(",") : null);
    const cnaeTuple = cod_cnaes ? `(${cod_cnaes.map((valor) => `"${valor.toUpperCase().replace(/-/g, ' ')}"`).join(",")})` : null;
    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const bairros = req.query.bairro ? req.query.bairro.split(",") : null;
    const bairroTuple = bairros ? `(${bairros.map((valor) => `"${valor.toUpperCase().replace(/-/g, ' ')}"`).join(",")})` : null;
    const page = parseInt(req.query.page) || 1;
    const pageSize = parseInt(req.query.pageSize) || 100;
    const offset = (page - 1) * pageSize;
    let query = `
        SELECT DISTINCT * FROM RECEITA
        --
        /**/
        `;
    let conditions = [];
    if (uf) conditions.push(`UF = "${uf}"`);
    if (cidade) conditions.push(`CIDADE = "${cidade}"`);
    if (bairros) conditions.push(`BAIRRO IN ${bairroTuple}`);
    if (cnpj === null) conditions.push(`CNPJ IS NOT NULL`);
    if (cod_cnaes) conditions.push(`CNAE_PRINCIPAL IN ${cnaeTuple}`);
    if (situacaos) conditions.push(`SITUACAO IN ${situacaoTuple}`);
    if (dataInicialAbertura !== null && dataFinalAbertura !== null) conditions.push(`DATA_INICIO_ATIVIDADE BETWEEN ('${dataInicialAbertura}') AND ('${dataFinalAbertura}')`);
    if (dataInicialSituacao && dataFinalSituacao) conditions.push(`DATA_SITUACAO_CADASTRAL BETWEEN ('${dataInicialSituacao}') AND ('${dataFinalSituacao}')`);
    if (conditions.length > 0) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);

    // Assume paginacao como um campo adicional
    const chave_paginacao = true;

    // combina paginação com req.query para criar o cacheParameters
    const cacheParameters = { ...req.query, chave_paginacao };

    // omite page e pagasize do objeto cacheParameters
    delete cacheParameters.page;
    delete cacheParameters.pageSize;

    // Converte os parâmetros para JSON e usa como cacheKey na tabela
    const cacheKey = JSON.stringify(cacheParameters);
    try {
      // Verifica se a informação de paginação está em cache
      const cachedPagination = await lerCache(cacheKey);
      if (cachedPagination) {
        console.log("Retornando dados do cache.");
        // Se as informações de paginação estão em cache.
        // Busca as os dados no banco.
        db.all(
          query + `LIMIT ${pageSize} OFFSET ${offset}`,
          [],
          async (err, rows) => {
            if (err) {
              res.status(500).json({ error: err.message });
              return;
            }

            rows = rows.map((row) => {
              row.CNAE_SECUNDARIO = row.CNAE_SECUNDARIO.split(",").map((element) =>
                element.trim()
              );
              row.CNPJ = formatCnpj(row.CNPJ)              
              return row;
            });
            const totalPages = Math.ceil(cachedPagination.totalCount / pageSize);

            // incluindo page e pageSize no pagination
            cachedPagination.totalPages = totalPages;
            cachedPagination.currentPage = page;
            cachedPagination.pageSize = pageSize;

            // Incluindo as informações no Header
            res.setHeader("X-Total-Count", cachedPagination.totalCount);
            res.setHeader("X-Total-Pages", cachedPagination.totalPages);
            res.setHeader("Content-Length", JSON.stringify(rows).length); 

            // Criando o objeto de resposta com informações de paginação e linhas de dados
            const responseObj = {
              info: cachedPagination,
              retultados: rows,
            };

            // Retorna os dados
            res.status(200).json(responseObj);
          }
        );
      } else {
        // Se as informações de paginação não estão em cache.
        //Calculando as informações de cache
        const totalRecords = await countRecords(query, []);
        const totalPages = Math.ceil(totalRecords / pageSize);

        // Criando objeto com detalhes da paginação
        const pagination = {
          totalCount: totalRecords,
        };

        // Salva os dados de paginação no cache
        await salvaCache(cacheKey, pagination);
        
        // Executando a consulta principal para buscar os dados paginados
        db.all(
          query + `LIMIT ${pageSize} OFFSET ${offset}`,
          [],
          async (err, rows) => {
            if (err) {
              res.status(500).json({ error: err.message });
              return;
            }

            rows = rows.map((row) => {
              row.CNAE_SECUNDARIO = row.CNAE_SECUNDARIO.split(",").map((element) =>
                element.trim()
              );
              row.CNPJ = formatCnpj(row.CNPJ)
              return row;
            });

            // incluindo page e pageSize no pagination
            pagination.totalPages = totalPages;
            pagination.currentPage = page;
            pagination.pageSize = pageSize;

            // Incluindo as informações no Header
            res.setHeader("X-Total-Count", pagination.totalCount);
            res.setHeader("X-Total-Pages", pagination.totalPages);
            res.setHeader("Content-Length", JSON.stringify(rows).length);
            
            // Criando o objeto de resposta com informações de paginação e linhas de dados
            const responseObj = {
              pagination: pagination,
              data: rows,
            };

              // If the format is not specified or invalid, send the data as JSON in the response
              res.status(200).json(responseObj);
          }
        );
      }
    } catch (err) {
      res.status(500).json({ error: "Erro ao ler o cache." });
    }
  })

// GetAll-Contagem
router.route("/estabelecimentos/counts")
  .get(function(req, res, next) {
    const reqParams = JSON.stringify(req.query);
    lerCache(reqParams)
      .then(rows => {
        if (rows !== null) {
          // Se o cache existir e os parâmetros forem os mesmos, retorna os dados em cache
          return res.json(rows);
        } else {
          const clientIp = req.ip;
          const situacaos = req.query.situacao ? req.query.situacao.split(",") : null;
          const situacaoTuple = situacaos ? `(${situacaos.map((valor) => `"${valor.toUpperCase().replace(/-/g, ' ')}"`).join(",")})` : null;
          const dataInicialAbertura = req.query.dataInicialAbertura ? [moment(req.query.dataInicialAbertura, 'DD-MM-YYYY').format('YYYY-MM-DD')] : null;
          const dataFinalAbertura = req.query.dataFinalAbertura ? [moment(req.query.dataFinalAbertura, 'DD-MM-YYYY').format('YYYY-MM-DD')] : null;
          const dataInicialSituacao = req.query.dataInicialSituacao ? [moment(req.query.dataInicialSituacao, 'DD-MM-YYYY').format('YYYY-MM-DD')] : null;
          const dataFinalSituacao = req.query.dataFinalSituacao ? [moment(req.query.dataFinalSituacao, 'DD-MM-YYYY').format('YYYY-MM-DD')] : null;
          const cnpj = req.query.cnpj ? [req.query.cnpj.toUpperCase()] : null;
          const cod_cnaes = req.query.cod_cnae ? req.query.cod_cnae.split(",") : (req.query.CNAE ? req.query.CNAE.split(",") : null);
          const cnaeTuple = cod_cnaes ? `(${cod_cnaes.map((valor) => `"${valor.toUpperCase().replace(/-/g, ' ')}"`).join(",")})` : null;
          const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
          const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
          const bairros = req.query.bairro ? req.query.bairro.split(",") : null;
          const bairroTuple = bairros ? `(${bairros.map((valor) => `"${valor.toUpperCase().replace(/-/g, ' ')}"`).join(",")})` : null;
          const groupby = req.query.groupby ? req.query.groupby.toLocaleUpperCase().split(",") : null;
          const orderby = req.query.orderby ? [req.query.orderby.toLocaleUpperCase()] : "DESC";
          let query = `
            SELECT COUNT(CNPJ) AS TOTAL FROM RECEITA
            --
            /**/
            ;
          `;
          let conditions = [];
          if (uf) conditions.push(`UF = "${uf}"`);
          if (cidade) conditions.push(`CIDADE = "${cidade}"`);
          if (bairros) conditions.push(`BAIRRO IN ${bairroTuple}`);
          if (cnpj === null) conditions.push(`CNPJ IS NOT NULL`);
          if (cod_cnaes) conditions.push(`CNAE_PRINCIPAL IN ${cnaeTuple}`);
          if (situacaos) conditions.push(`SITUACAO IN ${situacaoTuple}`);
          if (dataInicialAbertura !== null && dataFinalAbertura !== null) conditions.push(`DATA_INICIO_ATIVIDADE BETWEEN ('${dataInicialAbertura}') AND ('${dataFinalAbertura}')`);
          if (dataInicialSituacao && dataFinalSituacao) conditions.push(`DATA_SITUACAO_CADASTRAL BETWEEN ('${dataInicialSituacao}') AND ('${dataFinalSituacao}')`);
          if (conditions.length > 0) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);
          if (groupby) query = query.replace(/\/\*\*\//g, ` GROUP BY ${groupby}`);
          if (groupby) query = query.replace("COUNT(CNPJ)", `${groupby}, COUNT(CNPJ)`);
          if (orderby) query = query.replace(";", `ORDER BY TOTAL ${orderby} ;`);
          //console.log(query)

          db.all(query, (err, rows) => {
            if (err) {
              res.status(500).json({ error: err.message });
              return;
            }
            // Testando a função com diferentes parâmetros
            if (groupby) rows = agrupar(rows, groupby);

            // Salve os dados no cache
            salvaCache(reqParams, rows);

            //console.log(rows)
            res.status(200).json(rows);
          });
        }
      }
    ).catch(error => {
        console.error('Erro ao ler cache:', error);
        res.status(500).json({ error: 'Erro ao ler cache.' });
  });
});
module.exports = router;