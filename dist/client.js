"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.SpiceClient = void 0;
const path_1 = __importDefault(require("path"));
const https = __importStar(require("https"));
const grpc = __importStar(require("@grpc/grpc-js"));
const protoLoader = __importStar(require("@grpc/proto-loader"));
const node_fetch_1 = __importStar(require("node-fetch"));
const apache_arrow_1 = require("apache-arrow");
const flight_1 = require("./flight");
const retry = __importStar(require("./retry"));
const user_agent_1 = require("./user-agent");
const httpsAgent = new https.Agent({ keepAlive: true });
const PROTO_PATH = './proto/Flight.proto';
// If we're running in a Next.js environment, we need to adjust the path to the proto file
const PACKAGE_PATH = __dirname.includes('.next')
    ? path_1.default.join(__dirname.substring(0, __dirname.indexOf('.next')), './node_modules/@spiceai/spice/dist')
    : __dirname;
const fullProtoPath = path_1.default.join(PACKAGE_PATH, PROTO_PATH);
const packageDefinition = protoLoader.loadSync(fullProtoPath, {
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});
const arrow = grpc.loadPackageDefinition(packageDefinition).arrow;
const flightProto = arrow.flight.protocol;
class SpiceClient {
    constructor(params = {}) {
        this._flightTlsEnabled = true;
        this._maxRetries = retry.FLIGHT_QUERY_MAX_RETRIES;
        // support legacy constructor with api_key as first agument
        if (typeof params === 'string') {
            this._apiKey = params;
            this._httpUrl = 'https://data.spiceai.io';
            this._flightUrl = 'flight.spiceai.io:443';
            this._userAgent = (0, user_agent_1.getUserAgent)();
        }
        else {
            const { apiKey, httpUrl, flightUrl, flightTlsEnabled, userAgent } = params;
            this._apiKey = apiKey;
            this._httpUrl = httpUrl || 'http://127.0.0.1:8090';
            this._flightUrl = flightUrl || '127.0.0.1:50051';
            this._flightTlsEnabled =
                flightTlsEnabled !== undefined
                    ? flightTlsEnabled
                    : this._flightUrl.includes('127.0.0.1')
                        ? false
                        : true;
            // Prepend the user-supplied user agent (if any) with the default user agent
            this._userAgent = userAgent
                ? `${userAgent} ${(0, user_agent_1.getUserAgent)()}`
                : (0, user_agent_1.getUserAgent)();
        }
    }
    createClient(meta) {
        if (!this._flightTlsEnabled) {
            return new flightProto.FlightService(this._flightUrl, grpc.credentials.createInsecure());
        }
        const creds = grpc.credentials.createSsl();
        const metaCallback = (_params, callback) => {
            callback(null, meta);
        };
        const callCreds = grpc.credentials.createFromMetadataGenerator(metaCallback);
        const combCreds = grpc.credentials.combineChannelCredentials(creds, callCreds);
        return new flightProto.FlightService(this._flightUrl, combCreds);
    }
    async getResultStream(queryText, getFlightClient = undefined) {
        const meta = new grpc.Metadata();
        const client = this.createClient(meta);
        meta.set('authorization', 'Bearer ' + this._apiKey);
        meta.set('User-Agent', this._userAgent);
        let queryBuff = Buffer.from(queryText, 'utf8');
        let flightTicket = await new Promise((resolve, reject) => {
            // GetFlightInfo returns FlightInfo that have endpoints with ticket to call DoGet with
            client.GetFlightInfo({ type: flight_1.DescriptorType.CMD, cmd: queryBuff }, (err, result) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve(result.endpoint[0].ticket);
            });
        });
        if (getFlightClient) {
            getFlightClient(client);
        }
        // DoGet return a stream of FlightData
        return client.DoGet(flightTicket);
    }
    async query(queryText, onData = undefined) {
        return retry.retryWithExponentialBackoff(async () => {
            return this.doQueryRequest(queryText, onData);
        }, this._maxRetries);
    }
    async doQueryRequest(queryText, onData = undefined) {
        let client;
        const resultStream = await this.getResultStream(queryText, (c) => {
            client = c;
        });
        // indicates that data has been partially or fully sent
        let isDataAlreadySent = false;
        let schema;
        let chunks = [];
        resultStream.on('data', (response) => {
            let ipcMessage = (0, flight_1.getIpcMessage)(response);
            chunks.push(ipcMessage);
            if (!schema) {
                schema = ipcMessage;
            }
            else if (onData) {
                isDataAlreadySent = true;
                onData((0, apache_arrow_1.tableFromIPC)([schema, ipcMessage]));
            }
        });
        return new Promise((resolve, reject) => {
            resultStream.on('status', (response) => {
                const table = (0, apache_arrow_1.tableFromIPC)(chunks);
                client.close();
                resolve(table);
            });
            resultStream.on('error', (err) => {
                client.close();
                if (isDataAlreadySent)
                    retry.dontRetry(err);
                reject(err);
            });
        });
    }
    /*
     * Sets the maximum number of times to retry Query calls. The default is 3
     * @param maxRetries Num of max retries. Setting to 0 will disable retries
     */
    setMaxRetries(maxRetries) {
        if (maxRetries < 0) {
            throw new Error('maxRetries must be greater than or equal to 0');
        }
        this._maxRetries = maxRetries;
    }
    async refreshDataset(dataset, refresh_overrides) {
        if (!refresh_overrides) {
            refresh_overrides = {
                refresh_sql: null,
                refresh_mode: null,
                refresh_jitter_max: null,
            };
        }
        refresh_overrides.refresh_sql = refresh_overrides.refresh_sql || null;
        refresh_overrides.refresh_mode = refresh_overrides.refresh_mode || null;
        refresh_overrides.refresh_jitter_max =
            refresh_overrides.refresh_jitter_max || null;
        const body = JSON.stringify(refresh_overrides);
        const response = await this.fetchInternal('POST', `/v1/datasets/${dataset}/acceleration/refresh`, undefined, body);
        if (response.status !== 201) {
            const responseText = await response.text();
            throw new Error(`Failed to refresh dataset ${dataset}. Status code: ${response.status}, Response: ${responseText}`);
        }
    }
    fetchInternal(method, path, params, body) {
        let url;
        if (params && Object.keys(params).length) {
            url = `${this._httpUrl}${path}?${new URLSearchParams(params)}`;
        }
        else {
            url = `${this._httpUrl}${path}`;
        }
        const headers = [
            ['Content-Type', 'application/json'],
            ['Accept-Encoding', 'br, gzip, deflate'],
            ['User-Agent', this._userAgent],
        ];
        if (this._apiKey) {
            headers.push(['X-API-Key', this._apiKey || '']);
        }
        if (this._httpUrl.startsWith('https://')) {
            return (0, node_fetch_1.default)(url, {
                headers: new node_fetch_1.Headers(headers),
                agent: httpsAgent,
                method,
                body,
            });
        }
        else {
            return (0, node_fetch_1.default)(url, {
                headers: new node_fetch_1.Headers(headers),
                method,
                body,
            });
        }
    }
}
exports.SpiceClient = SpiceClient;
//# sourceMappingURL=client.js.map