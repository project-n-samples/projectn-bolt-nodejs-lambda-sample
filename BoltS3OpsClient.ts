import { BoltS3Client } from "projectn-bolt-aws-typescript-sdk";
import { S3Client } from "@aws-sdk/client-s3";
const { createHmac, createHash } = require("crypto");

import {
  ListObjectsV2Command,
  GetObjectCommand,
  HeadObjectCommand,
  ListBucketsCommand,
  HeadBucketCommand,
  PutObjectCommand,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";


type LambdaEventType = {
  sdkType: string;
  requestType: string;
  bucket?: string;
  key?: string;
  value?: string;
};

export enum SdkTypes {
  Bolt = "BOLT",
  S3 = "S3",
}

export enum RequestTypes {
  ListObjectsV2 = "LIST_OBJECTS_V2",
  GetObject = "GET_OBJECT",
  HeadObject = "HEAD_OBJECT",
  ListBuckets = "LIST_BUCKETS",
  HeadBucket = "HEAD_BUCKET",
  PutObject = "PUT_OBJECT",
  DeleteObject = "DELETE_OBJECT",
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
      if(['sdkType', 'requestType'].includes(prop)) {
        event[prop] = event[prop].toUpperCase();
      }
    });
    console.log({event}); // TODO: (MP) Delete for later
    /**
     * request is sent to S3 if 'sdkType' is not passed as a parameter in the event.
     * create an Bolt/S3 Client depending on the 'sdkType'
     */
    const client =
      event.sdkType === SdkTypes.Bolt ? new BoltS3Client({forcePathStyle: true}) : new S3Client({});

    try {
      //Performs an S3 / Bolt operation based on the input 'requestType'
      if (event.requestType === RequestTypes.ListObjectsV2) {
        return this.listObjectsV2(client, event.bucket);
      } else if (event.requestType === RequestTypes.GetObject) {
        return this.getObject(client, event.bucket, event.key);
      } else if (event.requestType === RequestTypes.HeadObject) {
        return this.headObject(client, event.bucket, event.key);
      } else if (event.requestType === RequestTypes.ListBuckets) {
        return this.listBuckets(client);
      } else if (event.requestType === RequestTypes.HeadBucket) {
        return this.headBucket(client, event.bucket);
      } else if (event.requestType === RequestTypes.PutObject) {
        return this.putObject(
          client,
          event.bucket,
          event.key,
          event.value
        );
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
    const objects = (response["Contents"] || []).map((x) => x.Key);
    return { objects: objects };
  }

  /**
   * Gets the object from Bolt/S3, computes and returns the object's MD5 hash
     If the object is gzip encoded, object is decompressed before computing its MD5.
   * @param client
   * @param bucket 
   * @param key 
   * @returns md5 hash of the object
   */
  async getObject(client: S3Client, bucket: string, key: string) {
    const command = new GetObjectCommand({ Bucket: bucket, Key: key });
    const response = await client.send(command);
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
   * @returns status code if the bucket exists
   */
  async headBucket(client: S3Client, bucket: string) {
    const command = new HeadBucketCommand({ Bucket: bucket });
    const response = await client.send(command);
    const statusCode = response.$metadata && response.$metadata.httpStatusCode;
    return {
      statusCode: statusCode,
      // region: headers["x-amz-bucket-region"],
      /** Note:
       * As of now HeadBucketCommandOutput metadata prop not fetching region information
       * For more info, check these below links
       * https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-s3/classes/headbucketcommand.html
       * https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-s3/interfaces/headbucketcommandoutput.html
       * */
    };
    return response;
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
    const statusCode = response["ResponseMetadata"]["HTTPStatusCode"];
    return {
      statusCode: statusCode,
    };
  }
}
