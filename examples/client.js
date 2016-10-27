/**
 * Created by Vladimir <zero@13w.me> on 27.10.16.
 */

'use strict';
const net = require('net'),
    mongo = net.createConnection({host: '127.0.0.1', port: 27017}),
    MongoWireProtocol = require('../'),
    mwp = new MongoWireProtocol();

mongo.on('connect', () => {
    console.log('Connected to mongo!');
    fire();
});

mongo.on('data', chunk => {
    if (!mongo.communicator || mongo.communicator.finished) {
        mongo.communicator = new MongoWireProtocol();
    }

    var response = mongo.communicator.parse(chunk);
    if (!response.finished) {
        return;
    }

    console.log(`Mongo response: ${JSON.stringify(response)};`);
    fire(response);
});

mongo.on('close', () => {
    console.log('Mongo connection closed');
});

mongo.on('error', error => {
    console.error(error);
});

function removeAllData(collection) {
    console.log(`Truncate collection ${collection}...`);
    mongo.write(new MongoWireProtocol({
        opCode: mwp.opCodes.OP_DELETE,
        ZERO: 0,
        fullCollectionName: collection,
        selector: {}
    }).buffer);
}

function query(collection, selector, fieldsSelector) {
    console.log(`Querying mongo ${JSON.stringify(selector)}, fields: ${JSON.stringify(fieldsSelector)}...`);
    mongo.write(new MongoWireProtocol({
        opCode: mwp.opCodes.OP_QUERY,
        flags: 0,
        fullCollectionName: collection,
        numberToSkip: 0,
        numberToReturn: 100,
        query: selector,
        returnFieldsSelector: fieldsSelector
    }).buffer);
}

function killCursor(cursorIDs) {
    console.log(`Killing cursor ${cursorIDs}`);
    mongo.write(new MongoWireProtocol({
        opCode: mwp.opCodes.OP_KILL_CURSORS,
        ZERO: 0,
        numberOfCursorIDs: cursorIDs.length,
        cursorIDs: cursorIDs
    }).buffer);
}

function insert(collection, documents) {
    console.log('Inserting test data');
    mongo.write(new MongoWireProtocol({
        opCode: mwp.opCodes.OP_INSERT,
        OPTS_CONTINUE_ON_ERROR: true,
        fullCollectionName: collection,
        documents: documents
    }).buffer);
}

function getLastError() {
    console.log('Get last error...');
    mongo.write(new MongoWireProtocol({
        opCode: mwp.opCodes.OP_QUERY,
        fullCollectionName: 'admin.$cmd',
        numberToSkip: 0,
        numberToReturn: -1,
        query: {getLastError: 1}
    }).buffer);
}

var funcs = [
    function () {
        removeAllData('test.test');
        getLastError();
    },
    function () {
        insert('test.test', [
            {name: 'document1', _id: 1, description: 'test document 1 description'},
            {name: 'document2', _id: 2, description: 'test document 2 description'},
            {name: 'document3', _id: 3, description: 'test document 3 description'},
            {name: 'document4', _id: 4, description: 'test document 4 description'},
        ]);
        getLastError();
    },
    function () {
        query('test.test', {_id: {$gte: 2}}, {description: 1});
    },
    function (lastResponse) {
        killCursor(lastResponse.cursorID);
        getLastError();
    }
];

function fire(lastResponse) {
    var func = funcs.shift();
    if (func) {
        func(lastResponse);
    } else {
        mongo.destroy();
    }
}
