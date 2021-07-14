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
exports.BoltS3OpsClient = exports.RequestType = exports.SdkTypes = void 0;
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
var RequestType;
(function (RequestType) {
    RequestType["ListObjectsV2"] = "LIST_OBJECTS_V2";
    RequestType["GetObject"] = "GET_OBJECT";
    RequestType["GetObjectTTFB"] = "GET_OBJECT_TTFB";
    RequestType["HeadObject"] = "HEAD_OBJECT";
    RequestType["ListBuckets"] = "LIST_BUCKETS";
    RequestType["HeadBucket"] = "HEAD_BUCKET";
    RequestType["PutObject"] = "PUT_OBJECT";
    RequestType["DeleteObject"] = "DELETE_OBJECT";
    RequestType["GetObjectPassthrough"] = "GET_OBJECT_PASSTHROUGH";
    RequestType["GetObjectPassthroughTTFB"] = "GET_OBJECT_PASSTHROUGH_TTFB";
    RequestType["All"] = "ALL";
})(RequestType = exports.RequestType || (exports.RequestType = {}));
/**
 * processEvent extracts the parameters (sdkType, requestType, bucket/key) from the event,
 * uses those parameters to send an Object/Bucket CRUD request to Bolt/S3 and returns back an appropriate response.
 */
class BoltS3OpsClient {
    constructor() { }
    processEvent(event) {
        return __awaiter(this, void 0, void 0, function* () {
            console.log({ event });
            Object.keys(event).forEach((prop) => {
                if (["sdkType", "requestType"].includes(prop)) {
                    event[prop] = event[prop].toUpperCase();
                }
            });
            /**
             * request is sent to S3 if 'sdkType' is not passed as a parameter in the event.
             * create an Bolt/S3 Client depending on the 'sdkType'
             */
            const client = event.sdkType === SdkTypes.Bolt ? new projectn_bolt_aws_typescript_sdk_1.BoltS3Client({}) : new client_s3_1.S3Client({});
            try {
                //Performs an S3 / Bolt operation based on the input 'requestType'
                switch (event.requestType) {
                    case RequestType.ListObjectsV2:
                        return this.listObjectsV2(client, event.bucket, event.maxKeys);
                    case RequestType.GetObject:
                    case RequestType.GetObjectTTFB:
                    case RequestType.GetObjectPassthrough:
                    case RequestType.GetObjectPassthroughTTFB:
                        return this.getObject(client, event.bucket, event.key, event.isForStats, [
                            RequestType.GetObjectTTFB,
                            RequestType.GetObjectPassthroughTTFB,
                        ].includes(event.requestType));
                    case RequestType.ListBuckets:
                        return this.listBuckets(client);
                    case RequestType.HeadBucket:
                        return this.headBucketAlongWithRegion(client, event.bucket);
                    case RequestType.HeadObject:
                        return this.headObject(client, event.bucket, event.key);
                    case RequestType.PutObject:
                        return this.putObject(client, event.bucket, event.key, event.value);
                    case RequestType.DeleteObject:
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
    listObjectsV2(client, bucket, maxKeys = 1000) {
        return __awaiter(this, void 0, void 0, function* () {
            const command = new client_s3_2.ListObjectsV2Command({
                Bucket: bucket,
                MaxKeys: maxKeys,
            });
            const response = yield client.send(command);
            const keys = (response["Contents"] || []).map((x) => x.Key);
            return { objects: keys };
        });
    }
    streamToBuffer(stream, timeToFirstByte = false) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => {
                if (timeToFirstByte) {
                    // resolve(stream.read(1)); //TODO: (MP): .read() not working for S3 - Revisit later
                    const chunks = [];
                    stream.on("data", (chunk) => {
                        chunks.push(chunk);
                        resolve(Buffer.concat(chunks));
                    });
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
                ? { contentLength: response.ContentLength, isObjectCompressed }
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
                expiration: response.Expiration,
                lastModified: response.LastModified.toISOString(),
                contentLength: response.ContentLength,
                contentEncoding: response.ContentEncoding,
                eTag: response.ETag,
                versionId: response.VersionId,
                storageClass: response.StorageClass,
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
                eTag: response.ETag,
                expiration: response.Expiration,
                versionId: response.VersionId,
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