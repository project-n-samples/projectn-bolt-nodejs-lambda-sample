import { BoltS3OpsClient, SdkTypes, RequestTypes } from "./BoltS3OpsClient";

/**
 * <summary>
 * lambdaHandler is the handler function that is invoked by AWS Lambda to process an incoming event for
 * performing data validation tests.
 * lambdaHandler accepts the following input parameters as part of the event:
 * 1) bucket - bucket name
 * 2) key - key name
 * lambdaHandler retrieves the object from Bolt and S3 (if BucketClean is OFF), computes and returns their
 * corresponding MD5 hash. If the object is gzip encoded, object is decompressed before computing its MD5.
 * </summary>
 * <param name="event">incoming event</param>
 * <param name="context">lambda context</param>
 * <returns>md5s of object retrieved from Bolt and S3.</returns>
 */
exports.lambdaHandler = async (event, context, callback) => {
  await (async () => {
    const opsClient = new BoltS3OpsClient();
    const boltGetObjectResponse = await opsClient.processEvent({
      ...event,
      requestType: RequestTypes.GetObject,
      sdkType: SdkTypes.Bolt,
    });
    const s3GetObjectResponse = await opsClient.processEvent({
      ...event,
      requestType: RequestTypes.GetObject,
      sdkType: SdkTypes.S3,
    });
    return new Promise((res, rej) => {
      callback(undefined, {
        "s3-md5": s3GetObjectResponse["md5"],
        "bolt-md5": boltGetObjectResponse["md5"],
      });
      res("success");
    });
  })();
};
