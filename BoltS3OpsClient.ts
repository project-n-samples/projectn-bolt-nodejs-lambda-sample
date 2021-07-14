import { BoltS3Client } from "projectn-bolt-aws-typescript-sdk";
import { S3Client } from "@aws-sdk/client-s3";
const { createHmac, createHash } = require("crypto");
const zlib = require("zlib");
import { Readable } from "stream";

import {
  ListObjectsV2Command,
  GetObjectCommand,
  HeadObjectCommand,
  ListBucketsCommand,
  GetBucketLocationCommand,
  PutObjectCommand,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";

export type LambdaEvent = {
  sdkType: string;
  requestType: RequestType;
  bucket?: string;
  key?: string;
  value?: string;

  maxKeys?: number; // Max number of keys (objects) to fetch
  maxObjLength?: number; // Max length of alphanumeric random value to create
  isForStats?: boolean;
  TTFB?: boolean; // Time to first byte
};

export enum SdkTypes {
  Bolt = "BOLT",
  S3 = "S3",
}

export enum RequestType {
  ListObjectsV2 = "LIST_OBJECTS_V2",
  GetObject = "GET_OBJECT",
  GetObjectTTFB = "GET_OBJECT_TTFB", // This is only for Perf
  HeadObject = "HEAD_OBJECT",
  ListBuckets = "LIST_BUCKETS",
  HeadBucket = "HEAD_BUCKET",
  PutObject = "PUT_OBJECT",
  DeleteObject = "DELETE_OBJECT",

  GetObjectPassthrough = "GET_OBJECT_PASSTHROUGH", // This is only for Perf
  GetObjectPassthroughTTFB = "GET_OBJECT_PASSTHROUGH_TTFB", // This is only for Perf
  All = "ALL", // This is only for Perf
}

interface IBoltS3OpsClient {
  processEvent: any;
}

export type ListObjectsV2Response = {
  objects: string[];
};

export type GetObjectResponse = {
  md5: string;
  contentLength?: number;
  isObjectCompressed?: boolean;
};

export type HeadObjectResponse = {
  expiration: string;
  lastModified: string;
  contentLength: number;
  contentEncoding: string;
  eTag: string;
  versionId: string;
  storageClass: string;
};

export type ListBucketsResponse = { buckets: string[] };

export type HeadBucketAlongWithRegionResponse = {
  statusCode: number;
  region: string;
};

export type PutObjectResponse = {
  eTag?: string;
  expiration?: string;
  versionId?: string;
};

export type DeleteObjectResponse = { statusCode?: number };
/**
 * processEvent extracts the parameters (sdkType, requestType, bucket/key) from the event,
 * uses those parameters to send an Object/Bucket CRUD request to Bolt/S3 and returns back an appropriate response.
 */
export class BoltS3OpsClient implements IBoltS3OpsClient {
  constructor() {}

  async processEvent(
    event: LambdaEvent
  ): Promise<
    | ListObjectsV2Response
    | GetObjectResponse
    | HeadObjectResponse
    | ListBucketsResponse
    | HeadBucketAlongWithRegionResponse
    | PutObjectResponse
    | DeleteObjectResponse
    | Error
  > {
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
    const client =
      event.sdkType === SdkTypes.Bolt ? new BoltS3Client({}) : new S3Client({});

    try {
      //Performs an S3 / Bolt operation based on the input 'requestType'

      switch (event.requestType) {
        case RequestType.ListObjectsV2:
          return this.listObjectsV2(client, event.bucket, event.maxKeys);
        case RequestType.GetObject:
        case RequestType.GetObjectTTFB:
        case RequestType.GetObjectPassthrough:
        case RequestType.GetObjectPassthroughTTFB:
          return this.getObject(
            client,
            event.bucket,
            event.key,
            event.isForStats,
            [
              RequestType.GetObjectTTFB,
              RequestType.GetObjectPassthroughTTFB,
            ].includes(event.requestType as RequestType)
          );
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
    } catch (ex) {
      console.error(ex);
      return new Error(ex);
    }
  }

  /**
   * Returns a list of 1000 objects from the given bucket in Bolt/S3
   * @param client
   * @param bucket
   * @returns list of first 1000 objects
   */
  async listObjectsV2(
    client: S3Client,
    bucket: string,
    maxKeys: number = 1000
  ): Promise<ListObjectsV2Response> {
    const command = new ListObjectsV2Command({
      Bucket: bucket,
      MaxKeys: maxKeys,
    });
    const response = await client.send(command);
    const keys = (response["Contents"] || []).map((x) => x.Key);
    return { objects: keys };
  }

  async streamToBuffer(
    stream: Readable,
    timeToFirstByte = false
  ): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      if (timeToFirstByte) {
        // resolve(stream.read(1)); //TODO: (MP): .read() not working for S3 - Revisit later
        const chunks = [];
        stream.on("data", (chunk) => {
          chunks.push(chunk);
          resolve(Buffer.concat(chunks));
        });
        stream.on("error", reject);
      } else {
        const chunks = [];
        stream.on("data", (chunk) => chunks.push(chunk));
        stream.on("error", reject);
        stream.on("end", () => resolve(Buffer.concat(chunks)));
      }
    });
  }

  async streamToString(stream: Readable, timeToFirstByte = false) {
    const buffer = await this.streamToBuffer(stream, timeToFirstByte);
    return new Promise((resolve, reject) => {
      resolve(buffer.toString("utf8"));
    });
  }

  async dezipped(stream, timeToFirstByte = false) {
    const buffer = await this.streamToBuffer(stream, timeToFirstByte);
    return new Promise((resolve, reject) => {
      if (!timeToFirstByte) {
        zlib.gunzip(buffer, function (err, buffer) {
          resolve(buffer.toString("utf8"));
        });
      } else {
        resolve(buffer.toString("utf8"));
      }
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
  async getObject(
    client: S3Client,
    bucket: string,
    key: string,
    isForStats: boolean = false,
    timeToFirstByte: boolean = false
  ): Promise<GetObjectResponse> {
    const command = new GetObjectCommand({ Bucket: bucket, Key: key });
    const response = await client.send(command);
    const body = response["Body"];
    // If Object is gzip encoded, compute MD5 on the decompressed object.
    const isObjectCompressed =
      response["ContentEncoding"] == "gzip" || key.endsWith(".gz");
    const data = isObjectCompressed
      ? await this.dezipped(body, timeToFirstByte)
      : await this.streamToString(body as Readable, timeToFirstByte);
    const md5 = createHash("md5").update(data).digest("hex").toUpperCase();
    const additional = isForStats
      ? { contentLength: response.ContentLength, isObjectCompressed }
      : {};
    return { md5, ...additional };
  }

  /**
   *
   * Retrieves the object's metadata from Bolt / S3.
   * @param client
   * @param bucket
   * @param key
   * @returns object metadata
   */
  async headObject(
    client: S3Client,
    bucket: string,
    key: string
  ): Promise<HeadObjectResponse> {
    const command = new HeadObjectCommand({ Bucket: bucket, Key: key });
    const response = await client.send(command);
    return {
      expiration: response.Expiration,
      lastModified: response.LastModified.toISOString(),
      contentLength: response.ContentLength,
      contentEncoding: response.ContentEncoding,
      eTag: response.ETag,
      versionId: response.VersionId,
      storageClass: response.StorageClass,
    };
  }

  /**
   * Returns list of buckets owned by the sender of the request
   * @param client
   * @returns list of buckets
   */
  async listBuckets(client: S3Client): Promise<ListBucketsResponse> {
    const command = new ListBucketsCommand({});
    const response = await client.send(command);
    const buckets = (response["Buckets"] || []).map((x) => x.Name);
    return { buckets: buckets };
  }

  /**
   * Checks if the bucket exists in Bolt/S3.
   * @param client
   * @param bucket
   * @returns status code and region if the bucket exists
   */
  async headBucketAlongWithRegion(
    client: S3Client,
    bucket: string
  ): Promise<HeadBucketAlongWithRegionResponse> {
    const command = new GetBucketLocationCommand({ Bucket: bucket });
    const response = await client.send(command);
    const statusCode = response.$metadata && response.$metadata.httpStatusCode;
    return {
      statusCode: statusCode,
      region: response.LocationConstraint,
    };
  }

  /**
   * Uploads an object to Bolt/S3
   * @param client
   * @param bucket
   * @param key
   * @param value
   * @returns object metadata
   */
  async putObject(
    client: S3Client,
    bucket: string,
    key: string,
    value: string
  ): Promise<PutObjectResponse> {
    const command = new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: value,
    });
    const response = await client.send(command);
    return {
      eTag: response.ETag,
      expiration: response.Expiration,
      versionId: response.VersionId,
    };
  }

  /**
   * Delete an object from Bolt/S3
   * @param client
   * @param bucket
   * @param key
   * @returns status code
   */
  async deleteObject(
    client: S3Client,
    bucket: string,
    key: string
  ): Promise<DeleteObjectResponse> {
    const command = new DeleteObjectCommand({ Bucket: bucket, Key: key });
    const response = await client.send(command);
    const statusCode = response.$metadata && response.$metadata.httpStatusCode;
    return {
      statusCode: statusCode,
    };
  }
}
