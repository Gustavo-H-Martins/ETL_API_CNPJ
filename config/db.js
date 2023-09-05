// app/config/db.js (Camada Config)
/**
 * Libs
 */
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const dbFile = `${__dirname}`.replace('config', 'files/database.db')
// Configuração do servidor
// const dbFile = require('./variaveisAmbiente')
// Conectar ao banco de dados SQLite3
const db = new sqlite3.Database(dbFile, (err) => {
  if (err) {
    console.error(err.message);
  }
  console.log('conectado no banco de dados.');
  db.exec(`
    /* CRIANDO ÍNDICES PARA AS TABELAS QUE VÃO CRIAR AS VIEWS */
    CREATE INDEX IF NOT EXISTS idx_tb_rfb_cnpj ON tb_rfb (CNPJ);

    /* CRIANDO VIEW PARA DADOS DA RECEITA */
    CREATE VIEW IF NOT EXISTS  RECEITA AS
        SELECT 
            RFB.CNPJ AS CNPJ,
            RFB.RAZAO_SOCIAL AS RAZAO_SOCIAL_RFB,
            RFB.NOME_FANTASIA AS NOME_FANTASIA,
            RFB.CEP AS CEP,
            RFB.ENDERECO AS ENDERECO,
            RFB.BAIRRO AS BAIRRO,
            RFB.CIDADE AS CIDADE,
            RFB.UF AS UF,
            RFB.TELEFONE AS TELEFONE,
            RFB.EMAIL AS "E-MAIL",
            RFB.CNAE_PRINCIPAL AS CNAE_PRINCIPAL,
            RFB.CNAE_DESCRICAO AS CNAE_DESCRICAO,
            RFB.CNAE_SECUNDARIO AS CNAE_SECUNDARIO,
            RFB.SITUACAO_CADASTRAL AS SITUACAO_CADASTRAL,
            CASE 
              WHEN RFB.SITUACAO_CADASTRAL = "01" THEN "NULA"
              WHEN RFB.SITUACAO_CADASTRAL = '02' THEN 'ATIVA'
              WHEN RFB.SITUACAO_CADASTRAL = '03' THEN 'SUSPENSA'
              WHEN RFB.SITUACAO_CADASTRAL = '04' THEN 'INAPTA'
              WHEN RFB.SITUACAO_CADASTRAL = '08' THEN 'BAIXADA'
            END AS SITUACAO,
            strftime(
                '%Y-%m-%d', date(
                    RFB.DATA_INICIO_ATIVIDADE
                )
            ) AS DATA_INICIO_ATIVIDADE,
            strftime(
                '%Y-%m-%d',date(
                    RFB.DATA_SITUACAO_CADASTRAL
                )
            ) AS DATA_SITUACAO_CADASTRAL
        FROM tb_rfb RFB;
    `, function(err) {
    if (err) {
      console.error(err.message)
    } else {
      console.log("View criada com sucesso!")
    }
  });
});

module.exports = db;