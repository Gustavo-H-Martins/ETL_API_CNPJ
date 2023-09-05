// app/app.js ( Index)
/**
 * libs
 */
const express = require('express');
const http = require('http');
const https = require('https');
const fs = require('fs');
const cors = require('cors');
const bodyParser = require('body-parser');
const swaggerUi = require('swagger-ui-express');

const { EventEmitter } = require('events');
const bus = new EventEmitter();

// Aumentar o limite de ouvintes para o evento "Bus"
bus.setMaxListeners(20);

/*
// Acessa e ler os arquivos de certificado e key ssl
const httpsOptions = {
      key: fs.readdirSync('key.pem'),
      cert : fs.readdirSync('cert.pem'),
      passphrase: 'abrasel'
};
*/

// Pega o ip da máquina
var IP = require("ip");

// Inicia o App
const app = express();

// Define as portas
/*
const httpPort = 8080;
const httpsPort = 8443;
*/

const httpsPort = 3000

/**
 * Routes.
 */
const rotas = require('./routes/rotas.js');

/**
 * Middlewares
 */
// adicionando favicon
const path = require('path');
app.get('/favicon.ico', (req, res) => {
  res.sendFile(path.join(__dirname, 'files//static/favicon.png'))
})

// coleta o ip do cliente
app.use((err, req, res, next) => {
    const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
    const date = new Date()
    const formattedDate = `${date.getDate()}/${date.getMonth() + 1}/${date.getFullYear()}`; // formata a data como DD/MM/AAAA
    const formattedTime = `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`; // formata a hora como HH:MM:SS
    dataChamada = `Data: ${formattedDate} - Hora: ${formattedTime}`
    console.log(`IP do cliente: ${ip} Em ${dataChamada}`);
    if (err.code === 'ECONNREFUSED'){
      res.status(502).json({error: 'Bad Gateway meu amigo, tá barrado na entrada!'});
    } else {
      next(err);
    }
    next();
  });


// analisa solicitações recebidas com cargas JSON 
app.use(express.json());
app.use(bodyParser.urlencoded({ extended: false }));

// definindo o coors para a api 
app.use(cors())

// analisa as solicitações recebidas com cargas úteis codificadas com urlen 
// extended: true - analisando os dados codificados com URL com a biblioteca querystring 
app.use(express.urlencoded({extended: true}));

// analisa as solicitações recebida e passa para o routes estabelecimentos com
app.use('/api/cnpj-abrasel/v1', rotas);

// direciona para a página com a documentação da api
const swaggerFile = require('./swagger/swagger.json');
app.use('/api/cnpj-abrasel/v1/docs', swaggerUi.serve, swaggerUi.setup(swaggerFile));

// starta a api service
function onStart(){
    console.log(`O servidor está escutando na porta ${httpsPort} url: http://${IP.address()}:${httpsPort}/`);
}

//app.listen(PORT, onStart);
http.createServer(app).listen(httpsPort, onStart);
//https.createServer(httpsOptions,app).listen(httpsPort, onStart);
module.exports = app;