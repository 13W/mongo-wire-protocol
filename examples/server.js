/**
 * Created by Vladimir on 27.10.16.
 */
'use strict';

const net = require('net'),
    server = net.createServer(),
    MongoWireProtocol = require('../');

server.on('connection', (socket) => {
    console.log('New connection received');
    var request = new MongoWireProtocol();
    socket.on('data', chunk => {
        if (request.finished) {
            request = new MongoWireProtocol();
        }
        request.parse(chunk);
        if (!request.finished) {
            return;
        }

        console.log(`
Request body length: ${request.length}
Request Id: ${request.requestId}
Response Id: ${request.responseTo}
OP Code: ${request.opCode}

Raw: ${JSON.stringify(request)}
        `);

        if (request.query) {
            var data = {
                opCode: request.opCodes.OP_REPLY,
                responseTo: request.requestId,
                numberReturned: 1,
                cursorID: new Buffer(8).fill(0),
                startingFrom: 0,
                documents: {
                    ok: 0,
                    errmsg: `no such cmd: ${Object.keys(request.query)[0]}`,
                    code: 59,
                    'bad cmd': request.query
                }
            };

            if (request.query.whatsmyuri) {
                data.documents = {you: `${socket.remoteAddress}:${socket.remotePort}`, ok: 1};
            }
            if (request.query.getLog) {
                data.documents = {"totalLinesWritten":0,"log":[],"ok":1};
            }
            if (request.query.replSetGetStatus) {
                data.documents = {"ok":0,"errmsg":"not running with --replSet"};
            }
            if (request.query.isMaster) {
                data.documents = {
                    "ismaster": true,
                    "maxBsonObjectSize": 16777216,
                    "maxMessageSizeBytes": 48000000,
                    "maxWriteBatchSize": 1000,
                    "localTime": new Date().toISOString(),
                    "maxWireVersion": 2,
                    "minWireVersion": 0,
                    "ok": 1
                };
            }

            if (request.query.listDatabases) {
                data.documents = {
                    databases: [
                        {
                            name: 'test',
                            sizeOnDisk: 2097152000,
                            empty: false
                        },
                        {
                            name: 'admin',
                            sizeOnDisk: 1,
                            empty: true
                        }
                    ], "totalSize": 2097152000, "ok": 1
                };
            }

            if (request.query.listCollections) {
                data.documents = [
                    {name: 'test.system.indexes'},
                    {name: 'test.system.namespaces'}
                ];
                data.numberReturned = data.documents.length;
            }

            if (request.fullCollectionName === 'test.system.namespaces') {
                data.documents = [
                    {name: 'test.system.indexes'}
                ];
                data.numberReturned = data.documents.length;
            }

            var response = new MongoWireProtocol(data);
            socket.write(response.buffer);
        }
    });

    socket.on('error', error => {
        console.error(error);
    });
    socket.on('close', () => {
        console.log('Connection closed');
    });
});

server.on('listening', () => {
    console.log('Server started');
});

server.listen({port: 27117, host: '127.0.0.1'});
