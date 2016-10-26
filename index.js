/**
 * Created by Vladimir on 27.10.16.
 */

'use strict';

const BSON = new (require('bson').BSONNative)();

const int32 = 'int32',
    int64 = 'int64',
    cstring = 'cstring',
    document = 'BSON';


function MongoWireProtocol(data) {
    if (!(this instanceof MongoWireProtocol)) {
        return new MongoWireProtocol(data);
    }

    Object.defineProperties(this, {
        buffer: {
            enumerable: false,
            writable: true,
            value: null
        },
        index: {
            enumerable: false,
            writable: true,
            value: 0
        },
        offset: {
            enumerable: false,
            writable: true,
            value: 0
        },
        completedSteps: {
            enumerable: false,
            writable: true,
            value: {}
        },
        finished: {
            enumerable: false,
            writable: true,
            value: false
        },
        chunks: {
            enumerable: false,
            writable: true,
            value: 0
        }
    });

    if (Buffer.isBuffer(data)) {
        this.buffer = data;
        this.parse();
    } else if (data) {
        Object.assign(this, data);
        this.pack();
    }
}

MongoWireProtocol.prototype.opCodes = {
    OP_REPLY: 1,
    OP_MSG: 1000,
    OP_UPDATE: 2001,
    OP_INSERT: 2002,
    RESERVED: 2003,
    OP_QUERY: 2004,
    OP_GET_MORE: 2005,
    OP_DELETE: 2006,
    OP_KILL_CURSORS: 2007,
    OP_COMMAND: 2010,
    OP_COMMANDREPLY: 2011
};

MongoWireProtocol.prototype.flagCodes = {
    [MongoWireProtocol.prototype.opCodes.OP_REPLY]: {
        CURSOR_NOT_FOUND: 0,
        QUERY_FAILURE: 2,
        SHARD_CONFIG_STALE: 4,
        AWAIT_CAPABLE: 8
    },
    [MongoWireProtocol.prototype.opCodes.OP_QUERY]: {
        OPTS_NONE: 0,
        OPTS_TAILABLE_CURSOR: 2,
        OPTS_SLAVE: 4,
        OPTS_OPLOG_REPLAY: 8,
        OPTS_NO_CURSOR_TIMEOUT: 16,
        OPTS_AWAIT_DATA: 32,
        OPTS_EXHAUST: 64,
        OPTS_PARTIAL: 128
    },
    [MongoWireProtocol.prototype.opCodes.OP_UPDATE]: {
        OPTS_UPSERT: 0,
        OPTS_MULTI_UPDATE: 2
    },
    [MongoWireProtocol.prototype.opCodes.OP_INSERT]: {
        OPTS_CONTINUE_ON_ERROR: 0
    },
    [MongoWireProtocol.prototype.opCodes.OP_DELETE]: {
        OPTS_SINGLE_REMOVE: 0
    }
};

MongoWireProtocol.prototype.specs = {
    [MongoWireProtocol.prototype.opCodes.OP_REPLY]: {
        flags: int32,
        cursorID: int64,
        startingFrom: int32,
        numberReturned: int32,
        documents: [document]
    },
    [MongoWireProtocol.prototype.opCodes.OP_MSG]: {
        message: cstring
    },
    [MongoWireProtocol.prototype.opCodes.OP_UPDATE]: {
        ZERO: int32,
        fullCollectionName: cstring,
        flags: int32,
        selector: document,
        update: document
    },
    [MongoWireProtocol.prototype.opCodes.OP_INSERT]: {
        flags: int32,
        fullCollectionName: cstring,
        documents: [document]
    },
    [MongoWireProtocol.prototype.opCodes.RESERVED]: {

    },
    [MongoWireProtocol.prototype.opCodes.OP_QUERY]: {
        flags: int32,
        fullCollectionName: cstring,
        numberToSkip: int32,
        numberToReturn: int32,
        query: document,
        returnFieldsSelector: [document, 0, true]
    },
    [MongoWireProtocol.prototype.opCodes.OP_GET_MORE]: {
        ZERO: int32,
        fullCollectionName: cstring,
        numberToReturn: int32,
        cursorID: int64
    },
    [MongoWireProtocol.prototype.opCodes.OP_DELETE]: {
        ZERO: int32,
        fullCollectionName: cstring,
        flags: int32,
        selector: document
    },
    [MongoWireProtocol.prototype.opCodes.OP_KILL_CURSORS]: {
        ZERO: int32,
        numberOfCursorIDs: int32,
        cursorIDs: [int64]
    },
    [MongoWireProtocol.prototype.opCodes.OP_COMMAND]: {
        database: cstring,
        commandName: cstring,
        metadata: document,
        commandArgs: document,
        inputDocs: document
    },
    [MongoWireProtocol.prototype.opCodes.OP_COMMANDREPLY]: {
        metadata: document,
        commandReply: document,
        outputDocs: document
    },
};

MongoWireProtocol.prototype.parse = function parse(chunk) {
    if (this.finished) {
        return this;
    }

    this.chunks += 1;

    var data = this.buffer;
    if (chunk) {
        data = this.buffer ? Buffer.concat([this.buffer, chunk]) : chunk;
    }

    if (this.index === 0 && this.offset === 0) {
        this.length = data.readInt32LE(0);
        this.requestId = data.readInt32LE(4);
        this.responseTo = data.readInt32LE(8);
        this.opCode = data.readInt32LE(12);
        this.index = 16;
    }

    var spec = this.specs[this.opCode],
        chunkIncomplete = false,
        key;

    break_spec:
        for (key in spec) {
            if (this.index + this.offset === this.length || chunkIncomplete) {
                break;
            }
            if (!spec.hasOwnProperty(key) || this.completedSteps[key]) {
                continue;
            }
            var type = spec[key],
                isArray = false,
                optional = false;

            if (Array.isArray(type)) {
                isArray = true;
                optional = type[2];
                type = type[0];
            }

            var ptr = isArray && Array.isArray(this[key]) ? this[key] : [];

            switch(type) {
                case int32:
                    if (this.length - this.index + this.offset < 4) {
                        chunkIncomplete = true;
                        break break_spec;
                    }

                    this[key] = data.readInt32LE(this.index);
                    this.index += 4;
                    if (key === 'flags') {
                        var flags = this.flagCodes[this.opCode];
                        for (var opt in flags) {
                            if (!flags.hasOwnProperty(opt)) {
                                continue;
                            }
                            if (this[key] & flags[opt] !== 0) {
                                this[opt] = true;
                            }
                        }
                    }
                    break;
                case int64:
                    if (this.length - this.index + this.offset < 8) {
                        chunkIncomplete = true;
                        break break_spec;
                    }

                    do {
                        ptr.push(data.slice(this.index, this.index + 8));
                        this.index += 8;
                    } while (isArray && this.index + this.offset < this.length);
                    this[key] = isArray ? ptr : ptr[0];
                    break;
                case cstring:
                    var index = this.index;
                    do {
                        if (data[index] === 0x00) {
                            this[key] = data.slice(this.index, index).toString('utf8');
                            this.index = index + 1;
                            break;
                        }
                        index += 1;
                    } while (this.offset + index < this.length);
                    break;
                case document:
                    do {
                        var size = data.readInt32LE(this.index);
                        if (this.index + size > data.byteLength) {
                            chunkIncomplete = true;
                            break break_spec;
                        }
                        ptr.push(BSON.deserialize(data.slice(this.index, this.index + size)));
                        this.index += size;
                    } while (isArray && this.index + this.offset < this.length);
                    this[key] = isArray ? ptr : ptr[0];
                    break;
                default:
                    throw new Error('Failed to parse Request');
            }

            this.completedSteps[key] = !chunkIncomplete;
        }

    this.buffer = data.slice(this.index);
    this.offset += this.index;
    this.index = 0;
    if (!chunkIncomplete) {
        this.finished = true;
    }

    return this;
};

MongoWireProtocol.prototype.pack = function pack() {
    var header = new Buffer(16),
        buffers = [header],
        spec = this.specs[this.opCode];

    header.writeInt32LE(this.requestId || 0, 4);
    header.writeInt32LE(this.responseTo || 0, 8);
    header.writeInt32LE(this.opCode, 12);

    for (var key in spec) {
        if (!spec.hasOwnProperty(key)) {
            continue;
        }

        var type = spec[key],
            value = this[key],
            isArray = false,
            optional = false,
            buffer;

        if (Array.isArray(type)) {
            isArray = true;
            optional = type[2];
            type = type[0];
        }

        if (optional && value === undefined) {
            continue;
        }
        if (value === undefined && key !== 'flags') {
            throw new Error(`Failed to format data. Key '${key}' is missed.`);
        }

        switch (type) {
            case int32:
                buffer = new Buffer(4);
                if (key === 'flags') {
                    var flags = this.flagCodes[this.opCode],
                        flag;
                    value = 0;

                    for (flag in flags) {
                        if (this.hasOwnProperty(flag) && this[flag]) {
                            value |= flags[flag];
                        }
                    }
                }
                buffer.writeInt32LE(value, 0);
                break;
            case int64:
                buffer = value;
                break;
            case cstring:
                buffer = new Buffer(value.length + 1);
                buffer.write(value, 0);
                buffer.writeInt8(0x00, value.length);
                break;
            case document:
                var documents = Array.isArray(value) ? value : [value],
                    documentsLength = isArray ? documents.length : 1,
                    documentsArray = [];

                for (var i = 0; i < documentsLength; i += 1) {
                    documentsArray.push(BSON.serialize(documents[i], false, true, false));
                }

                buffer = Buffer.concat(documentsArray);
                break;
            default:
                throw new Error('Failed to format object.');
        }

        buffers.push(buffer);
    }

    this.buffer = Buffer.concat(buffers);
    this.buffer.writeInt32LE(this.buffer.byteLength, 0);

    return this;
};

module.exports = MongoWireProtocol;
