var express = require('express');
var app = express();
var Client = require('node-rest-client').Client;
var client = new Client();
var bodyParser = require('body-parser');
var os = require('os');
var fs = require('fs');
var http = require('http');
var https = require('https');

var privateKey  = fs.readFileSync('server.key', 'utf8');
var certificate = fs.readFileSync('server.crt', 'utf8');
var credentials = {key: privateKey, cert: certificate};

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.static(__dirname));

app.options('/identity', function (req, res) {

    res.set({
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST',
        'Access-Control-Allow-Headers': 'X-Auth-Token, Accept, content-type, crossdomain',
        'Access-Control-Max-Age': 1728000
    })
    res.sendStatus(200);

});

app.post('/identity', function (req, res) {

    var args = {
        data: req.body,
        headers:{"Content-Type": "application/json"}
    };

    client.post("https://identity.api.rackspacecloud.com/v2.0/tokens", args, function(data,response) {
        res.set({
            'Access-Control-Allow-Origin': '*',
            'content-type': 'application/json'
        })
        res.send(data);
    });

});

app.options('/v2.0/:tenantId/events/getEvents', function (req, res) {

    res.set({
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET',
        'Access-Control-Allow-Headers': 'X-Auth-Token, Accept, content-type, crossdomain',
        'Access-Control-Max-Age': 1728000
    })
    res.sendStatus(200);

});

app.get('/v2.0/:tenantId/events/getEvents', function (req, res) {

    var args = {
        data: req.body,
        headers:{"Content-Type": "application/json", "X-Auth-Token": req.headers.X-Auth-Token}
    };

    var reqURL = "http://iad.metrics.api.rackspacecloud.com/v2.0/"+req.params.tenantId+"events/getEvents?from="+
        req.query.from+"&until="+req.query.until;

    if (req.query.tags){
       reqURL+="&tags="+req.query.tags
    }

    client.get(reqURL, args, function(data,response) {
        res.set({
            'Access-Control-Allow-Origin': '*',
            'content-type': 'application/json'
        })
        res.send(data);
    });

});

client.on('error',function(err){
    console.error('Something went wrong on the client', err);
});


app.get('/', function (req, res) {

    var interfaces = os.networkInterfaces();
    var addresses = [];
    for (var k in interfaces) {
        for (var k2 in interfaces[k]) {
            var address = interfaces[k][k2];
            if (address.family === 'IPv4' && !address.internal) {
                addresses.push(address.address);
            }
        }
    }

    res.set({
        'Access-Control-Allow-Origin': '*',
        'content-type': 'text/html'
    })
    res.send('<!DOCTYPE html><html>' +
        '<head>' +
        '<style>' +
        '.background-image {' +
        'position: fixed;' +
        'left: 0;' +
        'right: 0;' +
        'z-index: 1;' +

        'display: block;' +
        'background-image: url(grafana_bck.png);' +
        'width: 100%;' +
        'height: 800px;' +

        '-webkit-filter: blur(5px);' +
        '-moz-filter: blur(10px);' +
        '-o-filter: blur(5px);' +
        '-ms-filter: blur(5px);' +
        'filter: blur(5px);' +
        '}' +

        '.content {' +
        'position: fixed;' +
        'margin-left: auto;' +
        'margin-right: auto;' +
        'z-index: 9999;' +
        'left: 45%;' +
        'top: 50%;' +
        '}' +

        'a {' +
        'display: block;' +
        'width: 125px;' +
        'height: 40px;' +
        'text-decoration: none;' +
        'background: #F5F500 ;' +
        'padding: 10px;' +
        'text-align: center;' +
        'border-radius: 5px;' +
        'color: #202020;' +
        'font-weight: bold;' +
        'font-family: Georgia' +
        '}' +
        '</style>' +
        '</head>' +
        '<body>' +
        '<div class="background-image"></div>' +
        '<div class="content">'+
        '<a href=http://'+addresses[0]+'>Click here to start Grafana</a>' +
        '</div>' +
        '</body>' +
        '</html>');
});

var httpServer = http.createServer(app);
var httpsServer = https.createServer(credentials, app);

httpServer.listen(3000);
httpsServer.listen(443);

console.log("HTTP server running on 3000");
console.log("HTTPS server running on 443")