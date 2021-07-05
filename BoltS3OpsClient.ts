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

type LambdaEventType = {
  sdkType: string;
  requestType: string;
  bucket?: string;
  key?: string;
  value?: string;

  isForStats?: boolean;
  TTFB?: boolean; // Time to first byte
};

export enum SdkTypes {
  Bolt = "BOLT",
  S3 = "S3",
}

export enum RequestTypes {
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

/**
 * processEvent extracts the parameters (sdkType, requestType, bucket/key) from the event,
 * uses those parameters to send an Object/Bucket CRUD request to Bolt/S3 and returns back an appropriate response.
 */
export class BoltS3OpsClient implements IBoltS3OpsClient {
  constructor() {}

  async processEvent(event: LambdaEventType) {
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
    const client =
      event.sdkType === SdkTypes.Bolt ? new BoltS3Client({}) : new S3Client({});

    try {
      //Performs an S3 / Bolt operation based on the input 'requestType'
      if (event.requestType === RequestTypes.ListObjectsV2) {
        return this.listObjectsV2(client, event.bucket);
      } else if (
        [
          RequestTypes.GetObject,
          RequestTypes.GetObjectTTFB,
          RequestTypes.GetObjectPassthrough,
          RequestTypes.GetObjectPassthroughTTFB,
        ].includes(event.requestType as RequestTypes)
      ) {
        return this.getObject(
          client,
          event.bucket,
          event.key,
          event.isForStats,
          [
            RequestTypes.GetObjectTTFB,
            RequestTypes.GetObjectPassthroughTTFB,
          ].includes(event.requestType as RequestTypes)
        );
      } else if (event.requestType === RequestTypes.HeadObject) {
        return this.headObject(client, event.bucket, event.key);
      } else if (event.requestType === RequestTypes.ListBuckets) {
        return this.listBuckets(client);
      } else if (event.requestType === RequestTypes.HeadBucket) {
        return this.headBucketAlongWithRegion(client, event.bucket);
      } else if (event.requestType === RequestTypes.PutObject) {
        return this.putObject(client, event.bucket, event.key, event.value);
      } else if (event.requestType === RequestTypes.DeleteObject) {
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
  async listObjectsV2(client: S3Client, bucket: string) {
    const command = new ListObjectsV2Command({ Bucket: bucket });
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
        resolve(stream.read(1));
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
  ) {
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
      ? { contentLength: response["ContentLength"], isObjectCompressed }
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
  async headObject(client: S3Client, bucket: string, key: string) {
    const command = new HeadObjectCommand({ Bucket: bucket, Key: key });
    const response = await client.send(command);
    return {
      Expiration: response["Expiration"],
      lastModified: response["LastModified"].toISOString(),
      ContentLength: response["ContentLength"],
      ContentEncoding: response["ContentEncoding"],
      ETag: response["ETag"],
      VersionId: response["VersionId"],
      StorageClass: response["StorageClass"],
    };
  }

  /**
   * Returns list of buckets owned by the sender of the request
   * @param client
   * @returns list of buckets
   */
  async listBuckets(client: S3Client) {
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
  async headBucketAlongWithRegion(client: S3Client, bucket: string) {
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
  ) {
    const command = new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: value,
    });
    const response = await client.send(command);
    return {
      ETag: response["ETag"],
      Expiration: response["Expiration"],
      VersionId: response["VersionId"],
    };
  }

  /**
   * Delete an object from Bolt/S3
   * @param client
   * @param bucket
   * @param key
   * @returns status code
   */
  async deleteObject(client: S3Client, bucket: string, key: string) {
    const command = new DeleteObjectCommand({ Bucket: bucket, Key: key });
    const response = await client.send(command);
    const statusCode = response.$metadata && response.$metadata.httpStatusCode;
    return {
      statusCode: statusCode,
    };
  }
}
