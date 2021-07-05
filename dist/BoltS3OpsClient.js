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
const zlib = require("zlib");
const client_s3_2 = require("@aws-sdk/client-s3");
var SdkTypes;
(function (SdkTypes) {
    SdkTypes["Bolt"] = "BOLT";
    SdkTypes["S3"] = "S3";
})(SdkTypes = exports.SdkTypes || (exports.SdkTypes = {}));
var RequestTypes;
(function (RequestTypes) {
    RequestTypes["ListObjectsV2"] = "LIST_OBJECTS_V2";
    RequestTypes["GetObject"] = "GET_OBJECT";
    RequestTypes["GetObjectTTFB"] = "GET_OBJECT_TTFB";
    RequestTypes["HeadObject"] = "HEAD_OBJECT";
    RequestTypes["ListBuckets"] = "LIST_BUCKETS";
    RequestTypes["HeadBucket"] = "HEAD_BUCKET";
    RequestTypes["PutObject"] = "PUT_OBJECT";
    RequestTypes["DeleteObject"] = "DELETE_OBJECT";
    RequestTypes["GetObjectPassthrough"] = "GET_OBJECT_PASSTHROUGH";
    RequestTypes["GetObjectPassthroughTTFB"] = "GET_OBJECT_PASSTHROUGH_TTFB";
    RequestTypes["All"] = "ALL";
})(RequestTypes = exports.RequestTypes || (exports.RequestTypes = {}));
/**
 * processEvent extracts the parameters (sdkType, requestType, bucket/key) from the event,
 * uses those parameters to send an Object/Bucket CRUD request to Bolt/S3 and returns back an appropriate response.
 */
class BoltS3OpsClient {
    constructor() { }
    processEvent(event) {
        return __awaiter(this, void 0, void 0, function* () {
            Object.keys(event).forEach((prop) => {
                if (["sdkType", "requestType"].includes(prop)) {
                    event[prop] = event[prop].toUpperCase();
                }
            });
            // console.log({ event }); // TODO: (MP) Delete for later
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
                else if ([
                    RequestTypes.GetObject,
                    RequestTypes.GetObjectTTFB,
                    RequestTypes.GetObjectPassthrough,
                    RequestTypes.GetObjectPassthroughTTFB,
                ].includes(event.requestType)) {
                    return this.getObject(client, event.bucket, event.key, event.isForStats, [
                        RequestTypes.GetObjectTTFB,
                        RequestTypes.GetObjectPassthroughTTFB,
                    ].includes(event.requestType));
                }
                else if (event.requestType === RequestTypes.HeadObject) {
                    return this.headObject(client, event.bucket, event.key);
                }
                else if (event.requestType === RequestTypes.ListBuckets) {
                    return this.listBuckets(client);
                }
                else if (event.requestType === RequestTypes.HeadBucket) {
                    return this.headBucketAlongWithRegion(client, event.bucket);
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
            const keys = (response["Contents"] || []).map((x) => x.Key);
            return { objects: keys };
        });
    }
    streamToBuffer(stream, timeToFirstByte = false) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => {
                if (timeToFirstByte) {
                    resolve(stream.read(1));
                    stream.on("error", reject);
                }
                else {
                    const chunks = [];
                    stream.on("data", (chunk) => chunks.push(chunk));
                    stream.on("error", reject);
                    stream.on("end", () => resolve(Buffer.concat(chunks)));
                }
            });
        });
    }
    streamToString(stream, timeToFirstByte = false) {
        return __awaiter(this, void 0, void 0, function* () {
            const buffer = yield this.streamToBuffer(stream, timeToFirstByte);
            return new Promise((resolve, reject) => {
                resolve(buffer.toString("utf8"));
            });
        });
    }
    dezipped(stream, timeToFirstByte = false) {
        return __awaiter(this, void 0, void 0, function* () {
            const buffer = yield this.streamToBuffer(stream, timeToFirstByte);
            return new Promise((resolve, reject) => {
                if (!timeToFirstByte) {
                    zlib.gunzip(buffer, function (err, buffer) {
                        resolve(buffer.toString("utf8"));
                    });
                }
                else {
                    resolve(buffer.toString("utf8"));
                }
            });
        });
    }
    /**
     * Gets the object from Bolt/S3, computes and returns the object's MD5 hash
       If the object is gzip encoded, object is decompressed before computing its MD5.
     * @param client
     * @param bucket
     * @param key
     * @param timeToFirstByte
     * @returns md5 hash of the object
     */
    getObject(client, bucket, key, isForStats = false, timeToFirstByte = false) {
        return __awaiter(this, void 0, void 0, function* () {
            const command = new client_s3_2.GetObjectCommand({ Bucket: bucket, Key: key });
            const response = yield client.send(command);
            const body = response["Body"];
            // If Object is gzip encoded, compute MD5 on the decompressed object.
            const isObjectCompressed = response["ContentEncoding"] == "gzip" || key.endsWith(".gz");
            const data = isObjectCompressed
                ? yield this.dezipped(body, timeToFirstByte)
                : yield this.streamToString(body, timeToFirstByte);
            const md5 = createHash("md5").update(data).digest("hex").toUpperCase();
            const additional = isForStats
                ? { contentLength: response["ContentLength"], isObjectCompressed }
                : {};
            return Object.assign({ md5 }, additional);
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
     * @returns status code and region if the bucket exists
     */
    headBucketAlongWithRegion(client, bucket) {
        return __awaiter(this, void 0, void 0, function* () {
            const command = new client_s3_2.GetBucketLocationCommand({ Bucket: bucket });
            const response = yield client.send(command);
            const statusCode = response.$metadata && response.$metadata.httpStatusCode;
            return {
                statusCode: statusCode,
                region: response.LocationConstraint,
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
            const statusCode = response.$metadata && response.$metadata.httpStatusCode;
            return {
                statusCode: statusCode,
            };
        });
    }
}
exports.BoltS3OpsClient = BoltS3OpsClient;
//# sourceMappingURL=BoltS3OpsClient.js.map