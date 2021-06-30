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
exports.BoltS3OpsClient = exports.RequestTypes = exports.SdkTypes = void 0;
const projectn_bolt_aws_typescript_sdk_1 = require("projectn-bolt-aws-typescript-sdk");
const client_s3_1 = require("@aws-sdk/client-s3");
const { createHmac, createHash } = require("crypto");
const client_s3_2 = require("@aws-sdk/client-s3");
process.env.BOLT_URL =
    "https://bolt.us-east-2.projectn.us-east-2.bolt.projectn.co";
process.env.AWS_REGION = "us-east-1";
var SdkTypes;
(function (SdkTypes) {
    SdkTypes["Bolt"] = "BOLT";
    SdkTypes["S3"] = "S3";
})(SdkTypes = exports.SdkTypes || (exports.SdkTypes = {}));
var RequestTypes;
(function (RequestTypes) {
    RequestTypes["ListObjectsV2"] = "LIST_OBJECTS_V2";
    RequestTypes["GetObject"] = "GET_OBJECT";
    RequestTypes["HeadObject"] = "HEAD_OBJECT";
    RequestTypes["ListBuckets"] = "LIST_BUCKETS";
    RequestTypes["HeadBucket"] = "HEAD_BUCKET";
    RequestTypes["PutObject"] = "PUT_OBJECT";
    RequestTypes["DeleteObject"] = "DELETE_OBJECT";
})(RequestTypes = exports.RequestTypes || (exports.RequestTypes = {}));
/**
 * processEvent extracts the parameters (sdkType, requestType, bucket/key) from the event,
 * uses those parameters to send an Object/Bucket CRUD request to Bolt/S3 and returns back an appropriate response.
 */
class BoltS3OpsClient {
    constructor() { }
    processEvent(event) {
        return __awaiter(this, void 0, void 0, function* () {
            Object.keys(event).forEach((x) => {
                event[x] = event[x].toUpperCase();
            });
            /**
             * request is sent to S3 if 'sdkType' is not passed as a parameter in the event.
             * create an Bolt/S3 Client depending on the 'sdkType'
             */
            const client = event.sdkType === SdkTypes.Bolt ? new projectn_bolt_aws_typescript_sdk_1.BoltS3Client({}) : new client_s3_1.S3Client({});
            try {
                //Performs an S3 / Bolt operation based on the input 'requestType'
                if (event.requestType === RequestTypes.ListObjectsV2) {
                    return this.listObjectsV2(client, event.bucket);
                }
                else if (event.requestType === RequestTypes.GetObject) {
                    return this.getObject(client, event.bucket, event.key);
                }
                else if (event.requestType === RequestTypes.HeadObject) {
                    return this.headObject(client, event.bucket, event.key);
                }
                else if (event.requestType === RequestTypes.ListBuckets) {
                    return this.listBuckets(client);
                }
                else if (event.requestType === RequestTypes.HeadBucket) {
                    return this.headBucket(client, event.bucket);
                }
                else if (event.requestType === RequestTypes.PutObject) {
                    return this.putObject(client, event.bucket, event.key, event.value);
                }
                else if (event.requestType === RequestTypes.DeleteObject) {
                    return this.deleteObject(client, event.bucket, event.key);
                }
            }
            catch (ex) {
                console.error(ex);
                return new Error(ex);
            }
        });
    }
    /**
     * Returns a list of 1000 objects from the given bucket in Bolt/S3
     * @param client
     * @param bucket
     * @returns list of first 1000 objects
     */
    listObjectsV2(client, bucket) {
        return __awaiter(this, void 0, void 0, function* () {
            const command = new client_s3_2.ListObjectsV2Command({ Bucket: bucket });
            const response = yield client.send(command);
            const objects = (response["Contents"] || []).map((x) => x.Key);
            return { objects: objects };
        });
    }
    /**
     * Gets the object from Bolt/S3, computes and returns the object's MD5 hash
       If the object is gzip encoded, object is decompressed before computing its MD5.
     * @param client
     * @param bucket
     * @param key
     * @returns md5 hash of the object
     */
    getObject(client, bucket, key) {
        return __awaiter(this, void 0, void 0, function* () {
            const command = new client_s3_2.GetObjectCommand({ Bucket: bucket, Key: key });
            const response = yield client.send(command);
            // If Object is gzip encoded, compute MD5 on the decompressed object.
            let md5 = "";
            // TODO: (MP)
            // if (response["ContentEncoding"] == "gzip" || key.endsWith(".gz")) {
            //   md5 = hashlib
            //     .md5(gzip.decompress(response["Body"].read()))
            //     .hexdigest()
            //     .upper();
            // } else {
            //   md5 = hashlib.md5(resp["Body"].read()).hexdigest().upper();
            // }
            return { md5: md5 };
        });
    }
    /**
     *
     * Retrieves the object's metadata from Bolt / S3.
     * @param client
     * @param bucket
     * @param key
     * @returns object metadata
     */
    headObject(client, bucket, key) {
        return __awaiter(this, void 0, void 0, function* () {
            const command = new client_s3_2.HeadObjectCommand({ Bucket: bucket, Key: key });
            const response = yield client.send(command);
            return {
                Expiration: response["Expiration"],
                lastModified: response["LastModified"].toISOString(),
                ContentLength: response["ContentLength"],
                ContentEncoding: response["ContentEncoding"],
                ETag: response["ETag"],
                VersionId: response["VersionId"],
                StorageClass: response["StorageClass"],
            };
        });
    }
    /**
     * Returns list of buckets owned by the sender of the request
     * @param client
     * @returns list of buckets
     */
    listBuckets(client) {
        return __awaiter(this, void 0, void 0, function* () {
            const command = new client_s3_2.ListBucketsCommand({});
            const response = yield client.send(command);
            const buckets = (response["Buckets"] || []).map((x) => x.Name);
            return { buckets: buckets };
        });
    }
    /**
     * Checks if the bucket exists in Bolt/S3.
     * @param client
     * @param bucket
     * @returns status code and Region if the bucket exists
     */
    headBucket(client, bucket) {
        return __awaiter(this, void 0, void 0, function* () {
            const command = new client_s3_2.HeadBucketCommand({ Bucket: bucket });
            const response = yield client.send(command);
            const headers = response["ResponseMetadata"]["HTTPHeaders"];
            const statusCode = response["ResponseMetadata"]["HTTPStatusCode"];
            return {
                statusCode: statusCode,
                region: headers["x-amz-bucket-region"],
            };
        });
    }
    /**
     * Uploads an object to Bolt/S3
     * @param client
     * @param bucket
     * @param key
     * @param value
     * @returns object metadata
     */
    putObject(client, bucket, key, value) {
        return __awaiter(this, void 0, void 0, function* () {
            const command = new client_s3_2.PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: value,
            });
            const response = yield client.send(command);
            return {
                ETag: response["ETag"],
                Expiration: response["Expiration"],
                VersionId: response["VersionId"],
            };
        });
    }
    /**
     * Delete an object from Bolt/S3
     * @param client
     * @param bucket
     * @param key
     * @returns status code
     */
    deleteObject(client, bucket, key) {
        return __awaiter(this, void 0, void 0, function* () {
            const command = new client_s3_2.DeleteObjectCommand({ Bucket: bucket, Key: key });
            const response = yield client.send(command);
            const statusCode = response["ResponseMetadata"]["HTTPStatusCode"];
            return {
                statusCode: statusCode,
            };
        });
    }
}
exports.BoltS3OpsClient = BoltS3OpsClient;
//# sourceMappingURL=BoltS3OpsClient.js.map