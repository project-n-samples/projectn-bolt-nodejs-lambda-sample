import { BoltS3OpsClient, SdkTypes, RequestType, LambdaEvent } from "./BoltS3OpsClient";
const perf = require("execution-time")();

/**
 * <summary>
 * lambdaHandler is the handler function that is invoked by AWS Lambda to process an incoming event for
 * performing auto-heal tests.
 * lambdaHandler accepts the following input parameters as part of the event:
 * 1) bucket - bucket name
 * 2) key - key name
 * </summary>
 * <param name="input">incoming event</param>
 * <param name="context">lambda context</param>
 * <returns>time taken to auto-heal</returns>
 */
exports.lambdaHandler = async (event: LambdaEvent, context, callback) => {
  const WAIT_TIME_BETWEEN_RETRIES = 2000; //ms
  const wait = (ms) => {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  };
  const opsClient = new BoltS3OpsClient();
  let isObjectHealed = false;
  perf.start();
  while (!isObjectHealed) {
    try {
      await opsClient.processEvent({
        ...event,
        requestType: RequestType.GetObject,
        sdkType: SdkTypes.Bolt,
      });

      isObjectHealed = true;
    } catch (ex) {
      console.log("Waiting...");
      await wait(WAIT_TIME_BETWEEN_RETRIES);
      console.log("Re-trying Get Object...");
    }
  }
  const results = perf.stop();
  return new Promise((res, rej) => {
    callback(undefined, {
      auto_heal_time: `${(results.time > WAIT_TIME_BETWEEN_RETRIES
        ? results.time - WAIT_TIME_BETWEEN_RETRIES
        : results.time
      ).toFixed(2)} ms`,
    });
    res("success");
  });
};