"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getUserAgent = getUserAgent;
const os_1 = __importDefault(require("os"));
const version_1 = require("./version");
function getUserAgent() {
    const osType = os_1.default.type();
    const osRelease = os_1.default.release();
    const osArch = os_1.default.machine();
    return `spice.js/${version_1.VERSION} (${osType}/${osRelease} ${osArch})`;
}
//# sourceMappingURL=user-agent.js.map