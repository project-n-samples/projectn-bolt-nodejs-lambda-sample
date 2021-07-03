import { BoltS3OpsClient, SdkTypes, RequestTypes } from "./BoltS3OpsClient";
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
 *    {"requestType": "list_objects_v2", "bucket": "<bucket>"}
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

exports.lambdaHandler = async (event, context, callback) => {
  Object.keys(event).forEach((prop) => {
    if (["sdkType", "requestType"].includes(prop)) {
      event[prop] = event[prop].toUpperCase();
    }
  });
  const numberOfObjects = event.numKeysStr
    ? parseInt(event.numKeysStr) <= 1000
      ? parseInt(event.numKeysStr)
      : 1000
    : 1000;
  const generateRandomValue = () =>
    new Array(event.objLengthStr ? parseInt(event.objLengthStr) : 100)
      .fill(0)
      .map((x, i) =>
        String.fromCharCode(Math.floor(Math.random() * (122 - 48)) + 48)
      )
      .join("");
  const computePerfStats = (
    opTimes: Array<number>,
    tpTimes: Array<number> = [],
    objTimes: Array<number> = []
  ) => {
    const sort = (arr: Array<number>) => arr.sort((a, b) => a - b);
    const average = (arr: Array<number>) =>
      arr.reduce((a, b) => a + b) / arr.length;
    const stats = (_times, _measurement) => {
      if (_times.length === 0) {
        return {};
      }
      _times = sort(_times);
      const stats = {
        average: `${average(_times).toFixed(2)} ${_measurement}`,
        p50: `${_times[Math.floor(_times.length / 2)].toFixed(
          2
        )} ${_measurement}`,
        p90: `${_times[Math.floor((_times.length - 1) * 0.9)].toFixed(
          2
        )} ${_measurement}`,
      };
      return stats;
    };
    return {
      latency: stats(opTimes, "ms"),
      throughput:
        tpTimes.length > 0
          ? stats(tpTimes, "objects/ms")
          : `${
              opTimes.length / opTimes.reduce((incr, x) => incr + x, 0)
            } objects/ms`,
      objectSize: stats(objTimes, "bytes"),
    };
  };

  await (async () => {
    const opsClient = new BoltS3OpsClient();
    const keys =
      event.requestType === RequestTypes.ListObjectsV2
        ? new Array(10).fill(0).map((x, i) => "just to iterate")
        : [
            RequestTypes.PutObject,
            RequestTypes.DeleteObject,
            RequestTypes.All,
          ].includes(event.requestType)
        ? new Array(numberOfObjects).fill(0).map((x, i) => `bolt-s3-perf-${i}`)
        : (
            (
              await opsClient.processEvent({
                ...event,
                requestType: RequestTypes.ListObjectsV2,
                sdkType: SdkTypes.S3,
              })
            )["objects"] || []
          ).slice(0, numberOfObjects);

    const s3Times = [];
    const boltTimes = [];
    const s3Throughputs = [];
    const boltThroughputs = [];
    const s3ObjectSizes = [];
    const boltObjectSizes = [];

    for (let key of keys) {
      const runFor = async (sdkType, _times, _throughputs, _objectSizes) => {
        perf.start();
        const response = await opsClient.processEvent({
          ...event,
          sdkType: sdkType,
          key,
          value: generateRandomValue(),
        }); // TODO: Clean-up event before process though not a problem
        const perfTime = perf.stop().time;
        _times.push(perfTime);
        if (event.requestType === RequestTypes.ListObjectsV2) {
          _throughputs.push(response["objects"].length / perfTime);
        } else if (event.requestType === RequestTypes.GetObject) {
          _objectSizes.push(response["contentLength"]);
        }
      };
      await runFor(SdkTypes.S3, s3Times, s3Throughputs, s3ObjectSizes);
      await runFor(SdkTypes.Bolt, boltTimes, boltThroughputs, boltObjectSizes);
    }
    return new Promise((res, rej) => {
      callback(undefined, {
        s3PerfStats: computePerfStats(s3Times, s3Throughputs, s3ObjectSizes),
        boltPerfStats: computePerfStats(
          boltTimes,
          boltThroughputs,
          boltObjectSizes
        ),
      });
      res("success");
    });
  })();
};

// process.env.BOLT_URL =
//     "https://bolt.us-east-2.projectn.us-east-2.bolt.projectn.co";
// process.env.AWS_REGION = "us-east-2";

// exports.lambdaHandler(
//   {
//     bucket: "mp-test-bucket-7",
//     key: "package.json"
//   },
//   {},
//   console.log
// );
