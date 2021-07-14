"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const BoltS3OpsClient_1 = require("./BoltS3OpsClient");
const perf = require("execution-time")();
/**
 * <summary>
 * lambdaHandler is the handler function that is invoked by AWS Lambda to process an incoming event
 * for Bolt/S3 Performance testing.
 * lambdaHandler accepts the following input parameters as part of the event:
 * 1) requestType - type of request / operation to be performed.The following requests are supported:
 *    a) list_objects_v2 - list objects
 *    b) get_object - get object
 *    c) get_object_ttfb - get object (first byte)
 *    d) get_object_passthrough - get object (via passthrough) of unmonitored bucket
 *    e) get_object_passthrough_ttfb - get object (first byte via passthrough) of unmonitored bucket
 *    f) put_object - upload object
 *    g) delete_object - delete object
 *    h) all - put, get, delete, list objects(default request if none specified)
 * 2) bucket - buck
 * Following are examples of events, for various requests, that can be used to invoke the handler function.
 * a) Measure List objects performance of Bolt/S3.
 *    {"requestType": "list_objects_v2", "bucket": "<bucket>", "maxKeys": "<maxKeys>"}
 * b) Measure Get object performance of Bolt / S3.
 *    {"requestType": "get_object", "bucket": "<bucket>"}
 * c) Measure Get object (first byte) performance of Bolt / S3.
 *    {"requestType": "get_object_ttfb", "bucket": "<bucket>"}
 * d) Measure Get object passthrough performance of Bolt.
 *    {"requestType": "get_object_passthrough", "bucket": "<unmonitored-bucket>"}
 * e) Measure Get object passthrough (first byte) performance of Bolt.
 *    {"requestType": "get_object_passthrough_ttfb", "bucket": "<unmonitored-bucket>"}
 * f) Measure Put object performance of Bolt / S3.
 *    {"requestType": "put_object", "bucket": "<bucket>"}
 * g) Measure Delete object performance of Bolt / S3.
 *    {"requestType": "delete_object", "bucket": "<bucket>"}
 * h) Measure Put, Delete, Get, List objects performance of Bolt / S3.
 *    {"requestType": "all", "bucket": "<bucket>"}
 * </summary>
 * <param name="input">incoming event data</param>
 * <param name="context">lambda context</param>
 * <re>response from BoltS3Perf</r
 *  */
exports.lambdaHandler = (event, context, callback) => __awaiter(void 0, void 0, void 0, function* () {
    const getPerfStats = (requestType) => __awaiter(void 0, void 0, void 0, function* () {
        const maxKeys = event.maxKeys
            ? event.maxKeys <= 1000
                ? event.maxKeys
                : 1000
            : 1000;
        const generateRandomValue = () => new Array(event.maxObjLength ? event.maxObjLength : 100)
            .fill(0)
            .map((x, i) => String.fromCharCode(Math.floor(Math.random() * (122 - 48)) + 48))
            .join("");
        const opsClient = new BoltS3OpsClient_1.BoltS3OpsClient();
        const keys = requestType === BoltS3OpsClient_1.RequestType.ListObjectsV2
            ? new Array(10).fill(0).map((x, i) => "dummy key") // For ListObjectsV2, fetching objects process is only repeated for 10 times
            : [BoltS3OpsClient_1.RequestType.PutObject, BoltS3OpsClient_1.RequestType.DeleteObject].includes(requestType)
                ? new Array(maxKeys).fill(0).map((x, i) => `bolt-s3-perf-${i}`) // Auto generating keys for PUT or DELETE related performace tests
                : ((yield opsClient.processEvent(Object.assign(Object.assign({}, event), { requestType: BoltS3OpsClient_1.RequestType.ListObjectsV2, sdkType: BoltS3OpsClient_1.SdkTypes.S3 })))["objects"] || []).slice(0, maxKeys); // Fetch keys from buckets (S3/Bolt) for GET related performace tests
        // Run performance stats for given sdkType either S3 or Bolt
        const runFor = (sdkType) => __awaiter(void 0, void 0, void 0, function* () {
            const times = [], throughputs = [], objectSizes = [];
            let compressedObjectsCount = 0, unCompressedObjectsCount = 0;
            for (let key of keys) {
                perf.start();
                const response = yield opsClient.processEvent(Object.assign(Object.assign({}, event), { requestType, isForStats: true, sdkType: sdkType, key, value: generateRandomValue() }));
                const perfTime = perf.stop().time;
                times.push(perfTime);
                if (requestType === BoltS3OpsClient_1.RequestType.ListObjectsV2) {
                    throughputs.push(response.objects.length / perfTime);
                }
                else if ([
                    BoltS3OpsClient_1.RequestType.GetObject,
                    BoltS3OpsClient_1.RequestType.GetObjectTTFB,
                    BoltS3OpsClient_1.RequestType.GetObjectPassthrough,
                    BoltS3OpsClient_1.RequestType.GetObjectPassthroughTTFB,
                ].includes(requestType)) {
                    if (response.isObjectCompressed) {
                        compressedObjectsCount++;
                    }
                    else {
                        unCompressedObjectsCount++;
                    }
                    objectSizes.push(response.contentLength);
                }
            }
            return Object.assign(Object.assign({}, computePerfStats(times, throughputs, objectSizes)), (compressedObjectsCount || unCompressedObjectsCount
                ? {
                    compressedObjectsCount,
                    unCompressedObjectsCount,
                }
                : {}));
        });
        const s3PerfStats = yield runFor(BoltS3OpsClient_1.SdkTypes.S3);
        const boltPerfStats = yield runFor(BoltS3OpsClient_1.SdkTypes.Bolt);
        console.log(`Performance statistics of ${requestType} just got completed.`);
        return {
            // requestType,
            s3PerfStats,
            boltPerfStats,
        };
    });
    Object.keys(event).forEach((prop) => {
        if (["sdkType", "requestType"].includes(prop)) {
            event[prop] = event[prop].toUpperCase();
        }
    });
    console.log({ event });
    const perfStats = event.requestType !== BoltS3OpsClient_1.RequestType.All
        ? yield getPerfStats(event.requestType)
        : {
            [BoltS3OpsClient_1.RequestType.PutObject]: yield getPerfStats(BoltS3OpsClient_1.RequestType.PutObject),
            [BoltS3OpsClient_1.RequestType.DeleteObject]: yield getPerfStats(BoltS3OpsClient_1.RequestType.DeleteObject),
            [BoltS3OpsClient_1.RequestType.ListObjectsV2]: yield getPerfStats(BoltS3OpsClient_1.RequestType.ListObjectsV2),
            [BoltS3OpsClient_1.RequestType.GetObject]: yield getPerfStats(BoltS3OpsClient_1.RequestType.GetObject),
        };
    return new Promise((res, rej) => {
        callback(undefined, perfStats);
        res("success");
    });
});
/**
 * @param opTimes array of latencies
 * @param tpTimes array of throughputs
 * @param objSizes array of object sizes
 * @returns performance statistics (latency, throughput, object size)
 */
function computePerfStats(opTimes, tpTimes = [], objSizes = []) {
    const sort = (arr) => arr.sort((a, b) => a - b);
    const average = (arr) => arr.reduce((a, b) => a + b) / arr.length;
    const sum = (arr) => arr.reduce((incr, x) => incr + x, 0);
    const stats = (_times, _fixedPositions, _measurement) => {
        if (_times.length === 0) {
            return {};
        }
        _times = sort(_times);
        const stats = {
            average: `${average(_times).toFixed(_fixedPositions)} ${_measurement}`,
            p50: `${_times[Math.floor(_times.length / 2)].toFixed(_fixedPositions)} ${_measurement}`,
            p90: `${_times[Math.floor((_times.length - 1) * 0.9)].toFixed(_fixedPositions)} ${_measurement}`,
        };
        return stats;
    };
    return Object.assign({ latency: stats(opTimes, 2, "ms"), throughput: tpTimes.length > 0 || opTimes.length === 0
            ? stats(tpTimes, 5, "objects/ms")
            : `${(opTimes.length / sum(opTimes)).toFixed(5)} objects/ms` }, (objSizes.length > 0 ? { objectSize: stats(objSizes, 2, "bytes") } : {}));
}
// process.env.BOLT_URL =
//   "	https://bolt.us-east-1.solaw2.bolt.projectn.co";
// process.env.AWS_REGION = "us-east-1";
// exports.lambdaHandler(
//   {
//     "requestType": "get_object",
//     "bucket": "solaw-demo-east-1",
//     "key": "config",
//     "maxKeys": 100
//   },
//   {},
//   console.log
// );
//# sourceMappingURL=BoltS3PerfHandler.js.map