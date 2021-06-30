import { BoltS3OpsClient, SdkTypes, RequestTypes } from "./BoltS3OpsClient";

exports.lambdaHandler = async (event, context, callback) => {
  /**
   *lambda_handler is the handler function that is invoked by AWS Lambda to process an incoming event.

    lambda_handler accepts the following input parameters as part of the event:
    1) sdkType - Endpoint to which request is sent. The following values are supported:
       S3 - The Request is sent to S3.
       Bolt - The Request is sent to Bolt, whose endpoint is configured via 'BOLT_URL' environment variable

    2) requestType - type of request / operation to be performed. The following requests are supported:
       a) list_objects_v2 - list objects
       b) list_buckets - list buckets
       c) head_object - head object
       d) head_bucket - head bucket
       e) get_object - get object (md5 hash)
       f) put_object - upload object
       g) delete_object - delete object

    3) bucket - bucket name

    4) key - key name

    Following are examples of events, for various requests, that can be used to invoke the handler function.
    a) Listing first 1000 objects from Bolt bucket:
        {"requestType": "list_objects_v2", "sdkType": "BOLT", "bucket": "<bucket>"}

    b) Listing buckets from S3:
        {"requestType": "list_buckets", "sdkType": "S3"}

    c) Get Bolt object metadata (HeadObject):
        {"requestType": "head_object", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>"}

    d) Check if S3 bucket exists (HeadBucket):
        {"requestType": "head_bucket","sdkType": "S3", "bucket": "<bucket>"}

    e) Retrieve object (its MD5 Hash) from Bolt:
        {"requestType": "get_object", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>"}

    f) Upload object to Bolt:
        {"requestType": "put_object", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>", "value": "<value>"}

    g) Delete object from Bolt:
        {"requestType": "delete_object", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>"}
   */
  await (async () => {
    const opsClient = new BoltS3OpsClient();
    const response = await opsClient.processEvent(event);
  
    return new Promise((res, rej) => {
        callback(undefined,{response});
        res('success');
    });
  })();
};

// process.env.BOLT_URL = "https://bolt.us-east-2.projectn.us-east-2.bolt.projectn.co"
// process.env.AWS_REGION = "us-east-2"
// exports.lambdaHandler({
//     "requestType": "head_bucket",
//     "sdkType": "s3",
//     "bucket": "mp-test-bucket-2"
//   }, {}, console.log)