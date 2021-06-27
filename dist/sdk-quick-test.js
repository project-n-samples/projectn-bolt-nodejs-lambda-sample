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
const projectn_bolt_aws_typescript_sdk_1 = require("projectn-bolt-aws-typescript-sdk");
const { S3Client } = require("@aws-sdk/client-s3");
const client_s3_1 = require("@aws-sdk/client-s3");
process.env.BOLT_URL = "https://bolt.us-east-2.projectn.us-east-2.bolt.projectn.co";
process.env.AWS_REGION = "us-east-1";
const boltS3Client = new projectn_bolt_aws_typescript_sdk_1.BoltS3Client();
const s3Client = new S3Client();
const command = new client_s3_1.ListBucketsCommand({});
(function () {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            const boltS3Response = yield boltS3Client.send(command);
            const s3Response = yield s3Client.send(command);
            console.log(`BoltS3Client - Buckets count: ${boltS3Response.Buckets.length}`);
            console.log(`S3Client - Buckets count: ${s3Response.Buckets.length}`);
        }
        catch (err) {
            console.error(err);
        }
    });
})();
//# sourceMappingURL=sdk-quick-test.js.map