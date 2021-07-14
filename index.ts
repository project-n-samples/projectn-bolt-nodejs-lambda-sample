import { lambdaHandler as boltS3OpsClientHandler } from "./BoltS3OpsClientHandler";
import { lambdaHandler as boltS3ValidateObjHandler } from "./BoltS3ValidateObjHandler";
import { lambdaHandler as boltAutoHealHandler } from "./BoltAutoHealHandler";
import { lambdaHandler as boltS3PerfHandler } from "./BoltS3PerfHandler";

export const BoltS3OpsClientHandler = boltS3OpsClientHandler;
export const BoltS3ValidateObjHandler = boltS3ValidateObjHandler;
export const BoltAutoHealHandler = boltAutoHealHandler;
export const BoltS3PerfHandler = boltS3PerfHandler;

exports.BoltS3OpsClientHandler = boltS3OpsClientHandler;
exports.BoltS3ValidateObjHandler = boltS3ValidateObjHandler;
exports.BoltAutoHealHandler = boltAutoHealHandler;
exports.BoltS3PerfHandler = boltS3PerfHandler;
