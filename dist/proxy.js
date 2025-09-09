import { createRequire } from "node:module";
import { exec } from "node:child_process";
import { createReadStream, createWriteStream, existsSync, mkdirSync } from "node:fs";
import { createServer } from "node:http";
import path from "node:path";
import { promisify } from "node:util";

//#region rolldown:runtime
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __commonJS = (cb, mod) => function() {
	return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
};
var __copyProps = (to, from, except, desc) => {
	if (from && typeof from === "object" || typeof from === "function") for (var keys = __getOwnPropNames(from), i = 0, n = keys.length, key; i < n; i++) {
		key = keys[i];
		if (!__hasOwnProp.call(to, key) && key !== except) __defProp(to, key, {
			get: ((k) => from[k]).bind(null, key),
			enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable
		});
	}
	return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", {
	value: mod,
	enumerable: true
}) : target, mod));
var __require = /* @__PURE__ */ createRequire(import.meta.url);

//#endregion
//#region node_modules/undici/lib/core/symbols.js
var require_symbols = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/core/symbols.js": ((exports, module) => {
	module.exports = {
		kClose: Symbol("close"),
		kDestroy: Symbol("destroy"),
		kDispatch: Symbol("dispatch"),
		kUrl: Symbol("url"),
		kWriting: Symbol("writing"),
		kResuming: Symbol("resuming"),
		kQueue: Symbol("queue"),
		kConnect: Symbol("connect"),
		kConnecting: Symbol("connecting"),
		kKeepAliveDefaultTimeout: Symbol("default keep alive timeout"),
		kKeepAliveMaxTimeout: Symbol("max keep alive timeout"),
		kKeepAliveTimeoutThreshold: Symbol("keep alive timeout threshold"),
		kKeepAliveTimeoutValue: Symbol("keep alive timeout"),
		kKeepAlive: Symbol("keep alive"),
		kHeadersTimeout: Symbol("headers timeout"),
		kBodyTimeout: Symbol("body timeout"),
		kServerName: Symbol("server name"),
		kLocalAddress: Symbol("local address"),
		kHost: Symbol("host"),
		kNoRef: Symbol("no ref"),
		kBodyUsed: Symbol("used"),
		kBody: Symbol("abstracted request body"),
		kRunning: Symbol("running"),
		kBlocking: Symbol("blocking"),
		kPending: Symbol("pending"),
		kSize: Symbol("size"),
		kBusy: Symbol("busy"),
		kQueued: Symbol("queued"),
		kFree: Symbol("free"),
		kConnected: Symbol("connected"),
		kClosed: Symbol("closed"),
		kNeedDrain: Symbol("need drain"),
		kReset: Symbol("reset"),
		kDestroyed: Symbol.for("nodejs.stream.destroyed"),
		kResume: Symbol("resume"),
		kOnError: Symbol("on error"),
		kMaxHeadersSize: Symbol("max headers size"),
		kRunningIdx: Symbol("running index"),
		kPendingIdx: Symbol("pending index"),
		kError: Symbol("error"),
		kClients: Symbol("clients"),
		kClient: Symbol("client"),
		kParser: Symbol("parser"),
		kOnDestroyed: Symbol("destroy callbacks"),
		kPipelining: Symbol("pipelining"),
		kSocket: Symbol("socket"),
		kHostHeader: Symbol("host header"),
		kConnector: Symbol("connector"),
		kStrictContentLength: Symbol("strict content length"),
		kMaxRedirections: Symbol("maxRedirections"),
		kMaxRequests: Symbol("maxRequestsPerClient"),
		kProxy: Symbol("proxy agent options"),
		kCounter: Symbol("socket request counter"),
		kMaxResponseSize: Symbol("max response size"),
		kHTTP2Session: Symbol("http2Session"),
		kHTTP2SessionState: Symbol("http2Session state"),
		kRetryHandlerDefaultRetry: Symbol("retry agent default retry"),
		kConstruct: Symbol("constructable"),
		kListeners: Symbol("listeners"),
		kHTTPContext: Symbol("http context"),
		kMaxConcurrentStreams: Symbol("max concurrent streams"),
		kNoProxyAgent: Symbol("no proxy agent"),
		kHttpProxyAgent: Symbol("http proxy agent"),
		kHttpsProxyAgent: Symbol("https proxy agent")
	};
}) });

//#endregion
//#region node_modules/undici/lib/util/timers.js
var require_timers = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/util/timers.js": ((exports, module) => {
	/**
	* This module offers an optimized timer implementation designed for scenarios
	* where high precision is not critical.
	*
	* The timer achieves faster performance by using a low-resolution approach,
	* with an accuracy target of within 500ms. This makes it particularly useful
	* for timers with delays of 1 second or more, where exact timing is less
	* crucial.
	*
	* It's important to note that Node.js timers are inherently imprecise, as
	* delays can occur due to the event loop being blocked by other operations.
	* Consequently, timers may trigger later than their scheduled time.
	*/
	/**
	* The fastNow variable contains the internal fast timer clock value.
	*
	* @type {number}
	*/
	let fastNow = 0;
	/**
	* RESOLUTION_MS represents the target resolution time in milliseconds.
	*
	* @type {number}
	* @default 1000
	*/
	const RESOLUTION_MS = 1e3;
	/**
	* TICK_MS defines the desired interval in milliseconds between each tick.
	* The target value is set to half the resolution time, minus 1 ms, to account
	* for potential event loop overhead.
	*
	* @type {number}
	* @default 499
	*/
	const TICK_MS = (RESOLUTION_MS >> 1) - 1;
	/**
	* fastNowTimeout is a Node.js timer used to manage and process
	* the FastTimers stored in the `fastTimers` array.
	*
	* @type {NodeJS.Timeout}
	*/
	let fastNowTimeout;
	/**
	* The kFastTimer symbol is used to identify FastTimer instances.
	*
	* @type {Symbol}
	*/
	const kFastTimer = Symbol("kFastTimer");
	/**
	* The fastTimers array contains all active FastTimers.
	*
	* @type {FastTimer[]}
	*/
	const fastTimers = [];
	/**
	* These constants represent the various states of a FastTimer.
	*/
	/**
	* The `NOT_IN_LIST` constant indicates that the FastTimer is not included
	* in the `fastTimers` array. Timers with this status will not be processed
	* during the next tick by the `onTick` function.
	*
	* A FastTimer can be re-added to the `fastTimers` array by invoking the
	* `refresh` method on the FastTimer instance.
	*
	* @type {-2}
	*/
	const NOT_IN_LIST = -2;
	/**
	* The `TO_BE_CLEARED` constant indicates that the FastTimer is scheduled
	* for removal from the `fastTimers` array. A FastTimer in this state will
	* be removed in the next tick by the `onTick` function and will no longer
	* be processed.
	*
	* This status is also set when the `clear` method is called on the FastTimer instance.
	*
	* @type {-1}
	*/
	const TO_BE_CLEARED = -1;
	/**
	* The `PENDING` constant signifies that the FastTimer is awaiting processing
	* in the next tick by the `onTick` function. Timers with this status will have
	* their `_idleStart` value set and their status updated to `ACTIVE` in the next tick.
	*
	* @type {0}
	*/
	const PENDING = 0;
	/**
	* The `ACTIVE` constant indicates that the FastTimer is active and waiting
	* for its timer to expire. During the next tick, the `onTick` function will
	* check if the timer has expired, and if so, it will execute the associated callback.
	*
	* @type {1}
	*/
	const ACTIVE = 1;
	/**
	* The onTick function processes the fastTimers array.
	*
	* @returns {void}
	*/
	function onTick() {
		/**
		* Increment the fastNow value by the TICK_MS value, despite the actual time
		* that has passed since the last tick. This approach ensures independence
		* from the system clock and delays caused by a blocked event loop.
		*
		* @type {number}
		*/
		fastNow += TICK_MS;
		/**
		* The `idx` variable is used to iterate over the `fastTimers` array.
		* Expired timers are removed by replacing them with the last element in the array.
		* Consequently, `idx` is only incremented when the current element is not removed.
		*
		* @type {number}
		*/
		let idx = 0;
		/**
		* The len variable will contain the length of the fastTimers array
		* and will be decremented when a FastTimer should be removed from the
		* fastTimers array.
		*
		* @type {number}
		*/
		let len = fastTimers.length;
		while (idx < len) {
			/**
			* @type {FastTimer}
			*/
			const timer = fastTimers[idx];
			if (timer._state === PENDING) {
				timer._idleStart = fastNow - TICK_MS;
				timer._state = ACTIVE;
			} else if (timer._state === ACTIVE && fastNow >= timer._idleStart + timer._idleTimeout) {
				timer._state = TO_BE_CLEARED;
				timer._idleStart = -1;
				timer._onTimeout(timer._timerArg);
			}
			if (timer._state === TO_BE_CLEARED) {
				timer._state = NOT_IN_LIST;
				if (--len !== 0) fastTimers[idx] = fastTimers[len];
			} else ++idx;
		}
		fastTimers.length = len;
		if (fastTimers.length !== 0) refreshTimeout();
	}
	function refreshTimeout() {
		if (fastNowTimeout?.refresh) fastNowTimeout.refresh();
		else {
			clearTimeout(fastNowTimeout);
			fastNowTimeout = setTimeout(onTick, TICK_MS);
			fastNowTimeout?.unref();
		}
	}
	/**
	* The `FastTimer` class is a data structure designed to store and manage
	* timer information.
	*/
	var FastTimer = class {
		[kFastTimer] = true;
		/**
		* The state of the timer, which can be one of the following:
		* - NOT_IN_LIST (-2)
		* - TO_BE_CLEARED (-1)
		* - PENDING (0)
		* - ACTIVE (1)
		*
		* @type {-2|-1|0|1}
		* @private
		*/
		_state = NOT_IN_LIST;
		/**
		* The number of milliseconds to wait before calling the callback.
		*
		* @type {number}
		* @private
		*/
		_idleTimeout = -1;
		/**
		* The time in milliseconds when the timer was started. This value is used to
		* calculate when the timer should expire.
		*
		* @type {number}
		* @default -1
		* @private
		*/
		_idleStart = -1;
		/**
		* The function to be executed when the timer expires.
		* @type {Function}
		* @private
		*/
		_onTimeout;
		/**
		* The argument to be passed to the callback when the timer expires.
		*
		* @type {*}
		* @private
		*/
		_timerArg;
		/**
		* @constructor
		* @param {Function} callback A function to be executed after the timer
		* expires.
		* @param {number} delay The time, in milliseconds that the timer should wait
		* before the specified function or code is executed.
		* @param {*} arg
		*/
		constructor(callback, delay$2, arg) {
			this._onTimeout = callback;
			this._idleTimeout = delay$2;
			this._timerArg = arg;
			this.refresh();
		}
		/**
		* Sets the timer's start time to the current time, and reschedules the timer
		* to call its callback at the previously specified duration adjusted to the
		* current time.
		* Using this on a timer that has already called its callback will reactivate
		* the timer.
		*
		* @returns {void}
		*/
		refresh() {
			if (this._state === NOT_IN_LIST) fastTimers.push(this);
			if (!fastNowTimeout || fastTimers.length === 1) refreshTimeout();
			this._state = PENDING;
		}
		/**
		* The `clear` method cancels the timer, preventing it from executing.
		*
		* @returns {void}
		* @private
		*/
		clear() {
			this._state = TO_BE_CLEARED;
			this._idleStart = -1;
		}
	};
	/**
	* This module exports a setTimeout and clearTimeout function that can be
	* used as a drop-in replacement for the native functions.
	*/
	module.exports = {
		setTimeout(callback, delay$2, arg) {
			return delay$2 <= RESOLUTION_MS ? setTimeout(callback, delay$2, arg) : new FastTimer(callback, delay$2, arg);
		},
		clearTimeout(timeout) {
			if (timeout[kFastTimer])
 /**
			* @type {FastTimer}
			*/
			timeout.clear();
			else clearTimeout(timeout);
		},
		setFastTimeout(callback, delay$2, arg) {
			return new FastTimer(callback, delay$2, arg);
		},
		clearFastTimeout(timeout) {
			timeout.clear();
		},
		now() {
			return fastNow;
		},
		tick(delay$2 = 0) {
			fastNow += delay$2 - RESOLUTION_MS + 1;
			onTick();
			onTick();
		},
		reset() {
			fastNow = 0;
			fastTimers.length = 0;
			clearTimeout(fastNowTimeout);
			fastNowTimeout = null;
		},
		kFastTimer
	};
}) });

//#endregion
//#region node_modules/undici/lib/core/errors.js
var require_errors = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/core/errors.js": ((exports, module) => {
	var UndiciError$4 = class extends Error {
		constructor(message, options) {
			super(message, options);
			this.name = "UndiciError";
			this.code = "UND_ERR";
		}
	};
	var ConnectTimeoutError$1 = class extends UndiciError$4 {
		constructor(message) {
			super(message);
			this.name = "ConnectTimeoutError";
			this.message = message || "Connect Timeout Error";
			this.code = "UND_ERR_CONNECT_TIMEOUT";
		}
	};
	var HeadersTimeoutError$1 = class extends UndiciError$4 {
		constructor(message) {
			super(message);
			this.name = "HeadersTimeoutError";
			this.message = message || "Headers Timeout Error";
			this.code = "UND_ERR_HEADERS_TIMEOUT";
		}
	};
	var HeadersOverflowError$1 = class extends UndiciError$4 {
		constructor(message) {
			super(message);
			this.name = "HeadersOverflowError";
			this.message = message || "Headers Overflow Error";
			this.code = "UND_ERR_HEADERS_OVERFLOW";
		}
	};
	var BodyTimeoutError$1 = class extends UndiciError$4 {
		constructor(message) {
			super(message);
			this.name = "BodyTimeoutError";
			this.message = message || "Body Timeout Error";
			this.code = "UND_ERR_BODY_TIMEOUT";
		}
	};
	var ResponseStatusCodeError = class extends UndiciError$4 {
		constructor(message, statusCode, headers, body) {
			super(message);
			this.name = "ResponseStatusCodeError";
			this.message = message || "Response Status Code Error";
			this.code = "UND_ERR_RESPONSE_STATUS_CODE";
			this.body = body;
			this.status = statusCode;
			this.statusCode = statusCode;
			this.headers = headers;
		}
	};
	var InvalidArgumentError$32 = class extends UndiciError$4 {
		constructor(message) {
			super(message);
			this.name = "InvalidArgumentError";
			this.message = message || "Invalid Argument Error";
			this.code = "UND_ERR_INVALID_ARG";
		}
	};
	var InvalidReturnValueError$2 = class extends UndiciError$4 {
		constructor(message) {
			super(message);
			this.name = "InvalidReturnValueError";
			this.message = message || "Invalid Return Value Error";
			this.code = "UND_ERR_INVALID_RETURN_VALUE";
		}
	};
	var AbortError$2 = class extends UndiciError$4 {
		constructor(message) {
			super(message);
			this.name = "AbortError";
			this.message = message || "The operation was aborted";
		}
	};
	var RequestAbortedError$8 = class extends AbortError$2 {
		constructor(message) {
			super(message);
			this.name = "AbortError";
			this.message = message || "Request aborted";
			this.code = "UND_ERR_ABORTED";
		}
	};
	var InformationalError$4 = class extends UndiciError$4 {
		constructor(message) {
			super(message);
			this.name = "InformationalError";
			this.message = message || "Request information";
			this.code = "UND_ERR_INFO";
		}
	};
	var RequestContentLengthMismatchError$2 = class extends UndiciError$4 {
		constructor(message) {
			super(message);
			this.name = "RequestContentLengthMismatchError";
			this.message = message || "Request body length does not match content-length header";
			this.code = "UND_ERR_REQ_CONTENT_LENGTH_MISMATCH";
		}
	};
	var ResponseContentLengthMismatchError$1 = class extends UndiciError$4 {
		constructor(message) {
			super(message);
			this.name = "ResponseContentLengthMismatchError";
			this.message = message || "Response body length does not match content-length header";
			this.code = "UND_ERR_RES_CONTENT_LENGTH_MISMATCH";
		}
	};
	var ClientDestroyedError$2 = class extends UndiciError$4 {
		constructor(message) {
			super(message);
			this.name = "ClientDestroyedError";
			this.message = message || "The client is destroyed";
			this.code = "UND_ERR_DESTROYED";
		}
	};
	var ClientClosedError$1 = class extends UndiciError$4 {
		constructor(message) {
			super(message);
			this.name = "ClientClosedError";
			this.message = message || "The client is closed";
			this.code = "UND_ERR_CLOSED";
		}
	};
	var SocketError$4 = class extends UndiciError$4 {
		constructor(message, socket) {
			super(message);
			this.name = "SocketError";
			this.message = message || "Socket error";
			this.code = "UND_ERR_SOCKET";
			this.socket = socket;
		}
	};
	var NotSupportedError$2 = class extends UndiciError$4 {
		constructor(message) {
			super(message);
			this.name = "NotSupportedError";
			this.message = message || "Not supported error";
			this.code = "UND_ERR_NOT_SUPPORTED";
		}
	};
	var BalancedPoolMissingUpstreamError$1 = class extends UndiciError$4 {
		constructor(message) {
			super(message);
			this.name = "MissingUpstreamError";
			this.message = message || "No upstream has been added to the BalancedPool";
			this.code = "UND_ERR_BPL_MISSING_UPSTREAM";
		}
	};
	var HTTPParserError$1 = class extends Error {
		constructor(message, code, data) {
			super(message);
			this.name = "HTTPParserError";
			this.code = code ? `HPE_${code}` : void 0;
			this.data = data ? data.toString() : void 0;
		}
	};
	var ResponseExceededMaxSizeError$1 = class extends UndiciError$4 {
		constructor(message) {
			super(message);
			this.name = "ResponseExceededMaxSizeError";
			this.message = message || "Response content exceeded max size";
			this.code = "UND_ERR_RES_EXCEEDED_MAX_SIZE";
		}
	};
	var RequestRetryError$1 = class extends UndiciError$4 {
		constructor(message, code, { headers, data }) {
			super(message);
			this.name = "RequestRetryError";
			this.message = message || "Request retry error";
			this.code = "UND_ERR_REQ_RETRY";
			this.statusCode = code;
			this.data = data;
			this.headers = headers;
		}
	};
	var ResponseError$1 = class extends UndiciError$4 {
		constructor(message, code, { headers, body }) {
			super(message);
			this.name = "ResponseError";
			this.message = message || "Response error";
			this.code = "UND_ERR_RESPONSE";
			this.statusCode = code;
			this.body = body;
			this.headers = headers;
		}
	};
	var SecureProxyConnectionError$1 = class extends UndiciError$4 {
		constructor(cause, message, options = {}) {
			super(message, {
				cause,
				...options
			});
			this.name = "SecureProxyConnectionError";
			this.message = message || "Secure Proxy Connection failed";
			this.code = "UND_ERR_PRX_TLS";
			this.cause = cause;
		}
	};
	module.exports = {
		AbortError: AbortError$2,
		HTTPParserError: HTTPParserError$1,
		UndiciError: UndiciError$4,
		HeadersTimeoutError: HeadersTimeoutError$1,
		HeadersOverflowError: HeadersOverflowError$1,
		BodyTimeoutError: BodyTimeoutError$1,
		RequestContentLengthMismatchError: RequestContentLengthMismatchError$2,
		ConnectTimeoutError: ConnectTimeoutError$1,
		ResponseStatusCodeError,
		InvalidArgumentError: InvalidArgumentError$32,
		InvalidReturnValueError: InvalidReturnValueError$2,
		RequestAbortedError: RequestAbortedError$8,
		ClientDestroyedError: ClientDestroyedError$2,
		ClientClosedError: ClientClosedError$1,
		InformationalError: InformationalError$4,
		SocketError: SocketError$4,
		NotSupportedError: NotSupportedError$2,
		ResponseContentLengthMismatchError: ResponseContentLengthMismatchError$1,
		BalancedPoolMissingUpstreamError: BalancedPoolMissingUpstreamError$1,
		ResponseExceededMaxSizeError: ResponseExceededMaxSizeError$1,
		RequestRetryError: RequestRetryError$1,
		ResponseError: ResponseError$1,
		SecureProxyConnectionError: SecureProxyConnectionError$1
	};
}) });

//#endregion
//#region node_modules/undici/lib/core/constants.js
var require_constants$4 = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/core/constants.js": ((exports, module) => {
	/**
	* @see https://developer.mozilla.org/docs/Web/HTTP/Headers
	*/
	const wellknownHeaderNames$1 = [
		"Accept",
		"Accept-Encoding",
		"Accept-Language",
		"Accept-Ranges",
		"Access-Control-Allow-Credentials",
		"Access-Control-Allow-Headers",
		"Access-Control-Allow-Methods",
		"Access-Control-Allow-Origin",
		"Access-Control-Expose-Headers",
		"Access-Control-Max-Age",
		"Access-Control-Request-Headers",
		"Access-Control-Request-Method",
		"Age",
		"Allow",
		"Alt-Svc",
		"Alt-Used",
		"Authorization",
		"Cache-Control",
		"Clear-Site-Data",
		"Connection",
		"Content-Disposition",
		"Content-Encoding",
		"Content-Language",
		"Content-Length",
		"Content-Location",
		"Content-Range",
		"Content-Security-Policy",
		"Content-Security-Policy-Report-Only",
		"Content-Type",
		"Cookie",
		"Cross-Origin-Embedder-Policy",
		"Cross-Origin-Opener-Policy",
		"Cross-Origin-Resource-Policy",
		"Date",
		"Device-Memory",
		"Downlink",
		"ECT",
		"ETag",
		"Expect",
		"Expect-CT",
		"Expires",
		"Forwarded",
		"From",
		"Host",
		"If-Match",
		"If-Modified-Since",
		"If-None-Match",
		"If-Range",
		"If-Unmodified-Since",
		"Keep-Alive",
		"Last-Modified",
		"Link",
		"Location",
		"Max-Forwards",
		"Origin",
		"Permissions-Policy",
		"Pragma",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"RTT",
		"Range",
		"Referer",
		"Referrer-Policy",
		"Refresh",
		"Retry-After",
		"Sec-WebSocket-Accept",
		"Sec-WebSocket-Extensions",
		"Sec-WebSocket-Key",
		"Sec-WebSocket-Protocol",
		"Sec-WebSocket-Version",
		"Server",
		"Server-Timing",
		"Service-Worker-Allowed",
		"Service-Worker-Navigation-Preload",
		"Set-Cookie",
		"SourceMap",
		"Strict-Transport-Security",
		"Supports-Loading-Mode",
		"TE",
		"Timing-Allow-Origin",
		"Trailer",
		"Transfer-Encoding",
		"Upgrade",
		"Upgrade-Insecure-Requests",
		"User-Agent",
		"Vary",
		"Via",
		"WWW-Authenticate",
		"X-Content-Type-Options",
		"X-DNS-Prefetch-Control",
		"X-Frame-Options",
		"X-Permitted-Cross-Domain-Policies",
		"X-Powered-By",
		"X-Requested-With",
		"X-XSS-Protection"
	];
	/** @type {Record<typeof wellknownHeaderNames[number]|Lowercase<typeof wellknownHeaderNames[number]>, string>} */
	const headerNameLowerCasedRecord$3 = {};
	Object.setPrototypeOf(headerNameLowerCasedRecord$3, null);
	/**
	* @type {Record<Lowercase<typeof wellknownHeaderNames[number]>, Buffer>}
	*/
	const wellknownHeaderNameBuffers = {};
	Object.setPrototypeOf(wellknownHeaderNameBuffers, null);
	/**
	* @param {string} header Lowercased header
	* @returns {Buffer}
	*/
	function getHeaderNameAsBuffer(header) {
		let buffer$1 = wellknownHeaderNameBuffers[header];
		if (buffer$1 === void 0) buffer$1 = Buffer.from(header);
		return buffer$1;
	}
	for (let i = 0; i < wellknownHeaderNames$1.length; ++i) {
		const key = wellknownHeaderNames$1[i];
		const lowerCasedKey = key.toLowerCase();
		headerNameLowerCasedRecord$3[key] = headerNameLowerCasedRecord$3[lowerCasedKey] = lowerCasedKey;
	}
	module.exports = {
		wellknownHeaderNames: wellknownHeaderNames$1,
		headerNameLowerCasedRecord: headerNameLowerCasedRecord$3,
		getHeaderNameAsBuffer
	};
}) });

//#endregion
//#region node_modules/undici/lib/core/tree.js
var require_tree = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/core/tree.js": ((exports, module) => {
	const { wellknownHeaderNames, headerNameLowerCasedRecord: headerNameLowerCasedRecord$2 } = require_constants$4();
	var TstNode = class TstNode {
		/** @type {any} */
		value = null;
		/** @type {null | TstNode} */
		left = null;
		/** @type {null | TstNode} */
		middle = null;
		/** @type {null | TstNode} */
		right = null;
		/** @type {number} */
		code;
		/**
		* @param {string} key
		* @param {any} value
		* @param {number} index
		*/
		constructor(key, value, index) {
			if (index === void 0 || index >= key.length) throw new TypeError("Unreachable");
			if ((this.code = key.charCodeAt(index)) > 127) throw new TypeError("key must be ascii string");
			if (key.length !== ++index) this.middle = new TstNode(key, value, index);
			else this.value = value;
		}
		/**
		* @param {string} key
		* @param {any} value
		* @returns {void}
		*/
		add(key, value) {
			const length = key.length;
			if (length === 0) throw new TypeError("Unreachable");
			let index = 0;
			/**
			* @type {TstNode}
			*/
			let node = this;
			while (true) {
				const code = key.charCodeAt(index);
				if (code > 127) throw new TypeError("key must be ascii string");
				if (node.code === code) if (length === ++index) {
					node.value = value;
					break;
				} else if (node.middle !== null) node = node.middle;
				else {
					node.middle = new TstNode(key, value, index);
					break;
				}
				else if (node.code < code) if (node.left !== null) node = node.left;
				else {
					node.left = new TstNode(key, value, index);
					break;
				}
				else if (node.right !== null) node = node.right;
				else {
					node.right = new TstNode(key, value, index);
					break;
				}
			}
		}
		/**
		* @param {Uint8Array} key
		* @returns {TstNode | null}
		*/
		search(key) {
			const keylength = key.length;
			let index = 0;
			/**
			* @type {TstNode|null}
			*/
			let node = this;
			while (node !== null && index < keylength) {
				let code = key[index];
				if (code <= 90 && code >= 65) code |= 32;
				while (node !== null) {
					if (code === node.code) {
						if (keylength === ++index) return node;
						node = node.middle;
						break;
					}
					node = node.code < code ? node.left : node.right;
				}
			}
			return null;
		}
	};
	var TernarySearchTree = class {
		/** @type {TstNode | null} */
		node = null;
		/**
		* @param {string} key
		* @param {any} value
		* @returns {void}
		* */
		insert(key, value) {
			if (this.node === null) this.node = new TstNode(key, value, 0);
			else this.node.add(key, value);
		}
		/**
		* @param {Uint8Array} key
		* @returns {any}
		*/
		lookup(key) {
			return this.node?.search(key)?.value ?? null;
		}
	};
	const tree$1 = new TernarySearchTree();
	for (let i = 0; i < wellknownHeaderNames.length; ++i) {
		const key = headerNameLowerCasedRecord$2[wellknownHeaderNames[i]];
		tree$1.insert(key, key);
	}
	module.exports = {
		TernarySearchTree,
		tree: tree$1
	};
}) });

//#endregion
//#region node_modules/undici/lib/core/util.js
var require_util$5 = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/core/util.js": ((exports, module) => {
	const assert$30 = __require("node:assert");
	const { kDestroyed: kDestroyed$2, kBodyUsed: kBodyUsed$1, kListeners, kBody: kBody$2 } = require_symbols();
	const { IncomingMessage } = __require("node:http");
	const stream$1 = __require("node:stream");
	const net$2 = __require("node:net");
	const { stringify: stringify$2 } = __require("node:querystring");
	const { EventEmitter: EE$2 } = __require("node:events");
	const timers$1 = require_timers();
	const { InvalidArgumentError: InvalidArgumentError$31, ConnectTimeoutError } = require_errors();
	const { headerNameLowerCasedRecord: headerNameLowerCasedRecord$1 } = require_constants$4();
	const { tree } = require_tree();
	const [nodeMajor, nodeMinor] = process.versions.node.split(".", 2).map((v) => Number(v));
	var BodyAsyncIterable$1 = class {
		constructor(body) {
			this[kBody$2] = body;
			this[kBodyUsed$1] = false;
		}
		async *[Symbol.asyncIterator]() {
			assert$30(!this[kBodyUsed$1], "disturbed");
			this[kBodyUsed$1] = true;
			yield* this[kBody$2];
		}
	};
	function noop$9() {}
	/**
	* @param {*} body
	* @returns {*}
	*/
	function wrapRequestBody$1(body) {
		if (isStream$1(body)) {
			if (bodyLength(body) === 0) body.on("data", function() {
				assert$30(false);
			});
			if (typeof body.readableDidRead !== "boolean") {
				body[kBodyUsed$1] = false;
				EE$2.prototype.on.call(body, "data", function() {
					this[kBodyUsed$1] = true;
				});
			}
			return body;
		} else if (body && typeof body.pipeTo === "function") return new BodyAsyncIterable$1(body);
		else if (body && typeof body !== "string" && !ArrayBuffer.isView(body) && isIterable$1(body)) return new BodyAsyncIterable$1(body);
		else return body;
	}
	/**
	* @param {*} obj
	* @returns {obj is import('node:stream').Stream}
	*/
	function isStream$1(obj) {
		return obj && typeof obj === "object" && typeof obj.pipe === "function" && typeof obj.on === "function";
	}
	/**
	* @param {*} object
	* @returns {object is Blob}
	* based on https://github.com/node-fetch/fetch-blob/blob/8ab587d34080de94140b54f07168451e7d0b655e/index.js#L229-L241 (MIT License)
	*/
	function isBlobLike$1(object) {
		if (object === null) return false;
		else if (object instanceof Blob) return true;
		else if (typeof object !== "object") return false;
		else {
			const sTag = object[Symbol.toStringTag];
			return (sTag === "Blob" || sTag === "File") && ("stream" in object && typeof object.stream === "function" || "arrayBuffer" in object && typeof object.arrayBuffer === "function");
		}
	}
	/**
	* @param {string} url The path to check for query strings or fragments.
	* @returns {boolean} Returns true if the path contains a query string or fragment.
	*/
	function pathHasQueryOrFragment$1(url) {
		return url.includes("?") || url.includes("#");
	}
	/**
	* @param {string} url The URL to add the query params to
	* @param {import('node:querystring').ParsedUrlQueryInput} queryParams The object to serialize into a URL query string
	* @returns {string} The URL with the query params added
	*/
	function serializePathWithQuery$4(url, queryParams) {
		if (pathHasQueryOrFragment$1(url)) throw new Error("Query params cannot be passed when url already contains \"?\" or \"#\".");
		const stringified = stringify$2(queryParams);
		if (stringified) url += "?" + stringified;
		return url;
	}
	/**
	* @param {number|string|undefined} port
	* @returns {boolean}
	*/
	function isValidPort(port$1) {
		const value = parseInt(port$1, 10);
		return value === Number(port$1) && value >= 0 && value <= 65535;
	}
	/**
	* Check if the value is a valid http or https prefixed string.
	*
	* @param {string} value
	* @returns {boolean}
	*/
	function isHttpOrHttpsPrefixed(value) {
		return value != null && value[0] === "h" && value[1] === "t" && value[2] === "t" && value[3] === "p" && (value[4] === ":" || value[4] === "s" && value[5] === ":");
	}
	/**
	* @param {string|URL|Record<string,string>} url
	* @returns {URL}
	*/
	function parseURL(url) {
		if (typeof url === "string") {
			/**
			* @type {URL}
			*/
			url = new URL(url);
			if (!isHttpOrHttpsPrefixed(url.origin || url.protocol)) throw new InvalidArgumentError$31("Invalid URL protocol: the URL must start with `http:` or `https:`.");
			return url;
		}
		if (!url || typeof url !== "object") throw new InvalidArgumentError$31("Invalid URL: The URL argument must be a non-null object.");
		if (!(url instanceof URL)) {
			if (url.port != null && url.port !== "" && isValidPort(url.port) === false) throw new InvalidArgumentError$31("Invalid URL: port must be a valid integer or a string representation of an integer.");
			if (url.path != null && typeof url.path !== "string") throw new InvalidArgumentError$31("Invalid URL path: the path must be a string or null/undefined.");
			if (url.pathname != null && typeof url.pathname !== "string") throw new InvalidArgumentError$31("Invalid URL pathname: the pathname must be a string or null/undefined.");
			if (url.hostname != null && typeof url.hostname !== "string") throw new InvalidArgumentError$31("Invalid URL hostname: the hostname must be a string or null/undefined.");
			if (url.origin != null && typeof url.origin !== "string") throw new InvalidArgumentError$31("Invalid URL origin: the origin must be a string or null/undefined.");
			if (!isHttpOrHttpsPrefixed(url.origin || url.protocol)) throw new InvalidArgumentError$31("Invalid URL protocol: the URL must start with `http:` or `https:`.");
			const port$1 = url.port != null ? url.port : url.protocol === "https:" ? 443 : 80;
			let origin = url.origin != null ? url.origin : `${url.protocol || ""}//${url.hostname || ""}:${port$1}`;
			let path$1 = url.path != null ? url.path : `${url.pathname || ""}${url.search || ""}`;
			if (origin[origin.length - 1] === "/") origin = origin.slice(0, origin.length - 1);
			if (path$1 && path$1[0] !== "/") path$1 = `/${path$1}`;
			return new URL(`${origin}${path$1}`);
		}
		if (!isHttpOrHttpsPrefixed(url.origin || url.protocol)) throw new InvalidArgumentError$31("Invalid URL protocol: the URL must start with `http:` or `https:`.");
		return url;
	}
	/**
	* @param {string|URL|Record<string, string>} url
	* @returns {URL}
	*/
	function parseOrigin$1(url) {
		url = parseURL(url);
		if (url.pathname !== "/" || url.search || url.hash) throw new InvalidArgumentError$31("invalid url");
		return url;
	}
	/**
	* @param {string} host
	* @returns {string}
	*/
	function getHostname(host) {
		if (host[0] === "[") {
			const idx$1 = host.indexOf("]");
			assert$30(idx$1 !== -1);
			return host.substring(1, idx$1);
		}
		const idx = host.indexOf(":");
		if (idx === -1) return host;
		return host.substring(0, idx);
	}
	/**
	* IP addresses are not valid server names per RFC6066
	* Currently, the only server names supported are DNS hostnames
	* @param {string|null} host
	* @returns {string|null}
	*/
	function getServerName$1(host) {
		if (!host) return null;
		assert$30(typeof host === "string");
		const servername = getHostname(host);
		if (net$2.isIP(servername)) return "";
		return servername;
	}
	/**
	* @function
	* @template T
	* @param {T} obj
	* @returns {T}
	*/
	function deepClone(obj) {
		return JSON.parse(JSON.stringify(obj));
	}
	/**
	* @param {*} obj
	* @returns {obj is AsyncIterable}
	*/
	function isAsyncIterable(obj) {
		return !!(obj != null && typeof obj[Symbol.asyncIterator] === "function");
	}
	/**
	* @param {*} obj
	* @returns {obj is Iterable}
	*/
	function isIterable$1(obj) {
		return !!(obj != null && (typeof obj[Symbol.iterator] === "function" || typeof obj[Symbol.asyncIterator] === "function"));
	}
	/**
	* @param {Blob|Buffer|import ('stream').Stream} body
	* @returns {number|null}
	*/
	function bodyLength(body) {
		if (body == null) return 0;
		else if (isStream$1(body)) {
			const state = body._readableState;
			return state && state.objectMode === false && state.ended === true && Number.isFinite(state.length) ? state.length : null;
		} else if (isBlobLike$1(body)) return body.size != null ? body.size : null;
		else if (isBuffer$1(body)) return body.byteLength;
		return null;
	}
	/**
	* @param {import ('stream').Stream} body
	* @returns {boolean}
	*/
	function isDestroyed(body) {
		return body && !!(body.destroyed || body[kDestroyed$2] || stream$1.isDestroyed?.(body));
	}
	/**
	* @param {import ('stream').Stream} stream
	* @param {Error} [err]
	* @returns {void}
	*/
	function destroy$1(stream$2, err) {
		if (stream$2 == null || !isStream$1(stream$2) || isDestroyed(stream$2)) return;
		if (typeof stream$2.destroy === "function") {
			if (Object.getPrototypeOf(stream$2).constructor === IncomingMessage) stream$2.socket = null;
			stream$2.destroy(err);
		} else if (err) queueMicrotask(() => {
			stream$2.emit("error", err);
		});
		if (stream$2.destroyed !== true) stream$2[kDestroyed$2] = true;
	}
	const KEEPALIVE_TIMEOUT_EXPR = /timeout=(\d+)/;
	/**
	* @param {string} val
	* @returns {number | null}
	*/
	function parseKeepAliveTimeout(val) {
		const m = val.match(KEEPALIVE_TIMEOUT_EXPR);
		return m ? parseInt(m[1], 10) * 1e3 : null;
	}
	/**
	* Retrieves a header name and returns its lowercase value.
	* @param {string | Buffer} value Header name
	* @returns {string}
	*/
	function headerNameToString(value) {
		return typeof value === "string" ? headerNameLowerCasedRecord$1[value] ?? value.toLowerCase() : tree.lookup(value) ?? value.toString("latin1").toLowerCase();
	}
	/**
	* Receive the buffer as a string and return its lowercase value.
	* @param {Buffer} value Header name
	* @returns {string}
	*/
	function bufferToLowerCasedHeaderName$2(value) {
		return tree.lookup(value) ?? value.toString("latin1").toLowerCase();
	}
	/**
	* @param {(Buffer | string)[]} headers
	* @param {Record<string, string | string[]>} [obj]
	* @returns {Record<string, string | string[]>}
	*/
	function parseHeaders$1(headers, obj) {
		if (obj === void 0) obj = {};
		for (let i = 0; i < headers.length; i += 2) {
			const key = headerNameToString(headers[i]);
			let val = obj[key];
			if (val) {
				if (typeof val === "string") {
					val = [val];
					obj[key] = val;
				}
				val.push(headers[i + 1].toString("utf8"));
			} else {
				const headersValue = headers[i + 1];
				if (typeof headersValue === "string") obj[key] = headersValue;
				else obj[key] = Array.isArray(headersValue) ? headersValue.map((x) => x.toString("utf8")) : headersValue.toString("utf8");
			}
		}
		if ("content-length" in obj && "content-disposition" in obj) obj["content-disposition"] = Buffer.from(obj["content-disposition"]).toString("latin1");
		return obj;
	}
	/**
	* @param {Buffer[]} headers
	* @returns {string[]}
	*/
	function parseRawHeaders(headers) {
		const headersLength = headers.length;
		/**
		* @type {string[]}
		*/
		const ret = new Array(headersLength);
		let hasContentLength = false;
		let contentDispositionIdx = -1;
		let key;
		let val;
		let kLen = 0;
		for (let n = 0; n < headersLength; n += 2) {
			key = headers[n];
			val = headers[n + 1];
			typeof key !== "string" && (key = key.toString());
			typeof val !== "string" && (val = val.toString("utf8"));
			kLen = key.length;
			if (kLen === 14 && key[7] === "-" && (key === "content-length" || key.toLowerCase() === "content-length")) hasContentLength = true;
			else if (kLen === 19 && key[7] === "-" && (key === "content-disposition" || key.toLowerCase() === "content-disposition")) contentDispositionIdx = n + 1;
			ret[n] = key;
			ret[n + 1] = val;
		}
		if (hasContentLength && contentDispositionIdx !== -1) ret[contentDispositionIdx] = Buffer.from(ret[contentDispositionIdx]).toString("latin1");
		return ret;
	}
	/**
	* @param {string[]} headers
	* @param {Buffer[]} headers
	*/
	function encodeRawHeaders(headers) {
		if (!Array.isArray(headers)) throw new TypeError("expected headers to be an array");
		return headers.map((x) => Buffer.from(x));
	}
	/**
	* @param {*} buffer
	* @returns {buffer is Buffer}
	*/
	function isBuffer$1(buffer$1) {
		return buffer$1 instanceof Uint8Array || Buffer.isBuffer(buffer$1);
	}
	/**
	* Asserts that the handler object is a request handler.
	*
	* @param {object} handler
	* @param {string} method
	* @param {string} [upgrade]
	* @returns {asserts handler is import('../api/api-request').RequestHandler}
	*/
	function assertRequestHandler$1(handler, method, upgrade$1) {
		if (!handler || typeof handler !== "object") throw new InvalidArgumentError$31("handler must be an object");
		if (typeof handler.onRequestStart === "function") return;
		if (typeof handler.onConnect !== "function") throw new InvalidArgumentError$31("invalid onConnect method");
		if (typeof handler.onError !== "function") throw new InvalidArgumentError$31("invalid onError method");
		if (typeof handler.onBodySent !== "function" && handler.onBodySent !== void 0) throw new InvalidArgumentError$31("invalid onBodySent method");
		if (upgrade$1 || method === "CONNECT") {
			if (typeof handler.onUpgrade !== "function") throw new InvalidArgumentError$31("invalid onUpgrade method");
		} else {
			if (typeof handler.onHeaders !== "function") throw new InvalidArgumentError$31("invalid onHeaders method");
			if (typeof handler.onData !== "function") throw new InvalidArgumentError$31("invalid onData method");
			if (typeof handler.onComplete !== "function") throw new InvalidArgumentError$31("invalid onComplete method");
		}
	}
	/**
	* A body is disturbed if it has been read from and it cannot be re-used without
	* losing state or data.
	* @param {import('node:stream').Readable} body
	* @returns {boolean}
	*/
	function isDisturbed$3(body) {
		return !!(body && (stream$1.isDisturbed(body) || body[kBodyUsed$1]));
	}
	/**
	* @typedef {object} SocketInfo
	* @property {string} [localAddress]
	* @property {number} [localPort]
	* @property {string} [remoteAddress]
	* @property {number} [remotePort]
	* @property {string} [remoteFamily]
	* @property {number} [timeout]
	* @property {number} bytesWritten
	* @property {number} bytesRead
	*/
	/**
	* @param {import('net').Socket} socket
	* @returns {SocketInfo}
	*/
	function getSocketInfo(socket) {
		return {
			localAddress: socket.localAddress,
			localPort: socket.localPort,
			remoteAddress: socket.remoteAddress,
			remotePort: socket.remotePort,
			remoteFamily: socket.remoteFamily,
			timeout: socket.timeout,
			bytesWritten: socket.bytesWritten,
			bytesRead: socket.bytesRead
		};
	}
	/**
	* @param {Iterable} iterable
	* @returns {ReadableStream}
	*/
	function ReadableStreamFrom$3(iterable) {
		let iterator;
		return new ReadableStream({
			async start() {
				iterator = iterable[Symbol.asyncIterator]();
			},
			pull(controller) {
				async function pull() {
					const { done, value } = await iterator.next();
					if (done) queueMicrotask(() => {
						controller.close();
						controller.byobRequest?.respond(0);
					});
					else {
						const buf = Buffer.isBuffer(value) ? value : Buffer.from(value);
						if (buf.byteLength) controller.enqueue(new Uint8Array(buf));
						else return await pull();
					}
				}
				return pull();
			},
			async cancel() {
				await iterator.return();
			},
			type: "bytes"
		});
	}
	/**
	* The object should be a FormData instance and contains all the required
	* methods.
	* @param {*} object
	* @returns {object is FormData}
	*/
	function isFormDataLike$1(object) {
		return object && typeof object === "object" && typeof object.append === "function" && typeof object.delete === "function" && typeof object.get === "function" && typeof object.getAll === "function" && typeof object.has === "function" && typeof object.set === "function" && object[Symbol.toStringTag] === "FormData";
	}
	function addAbortListener$2(signal, listener) {
		if ("addEventListener" in signal) {
			signal.addEventListener("abort", listener, { once: true });
			return () => signal.removeEventListener("abort", listener);
		}
		signal.once("abort", listener);
		return () => signal.removeListener("abort", listener);
	}
	/**
	* @see https://tools.ietf.org/html/rfc7230#section-3.2.6
	* @param {number} c
	* @returns {boolean}
	*/
	function isTokenCharCode(c) {
		switch (c) {
			case 34:
			case 40:
			case 41:
			case 44:
			case 47:
			case 58:
			case 59:
			case 60:
			case 61:
			case 62:
			case 63:
			case 64:
			case 91:
			case 92:
			case 93:
			case 123:
			case 125: return false;
			default: return c >= 33 && c <= 126;
		}
	}
	/**
	* @param {string} characters
	* @returns {boolean}
	*/
	function isValidHTTPToken$3(characters) {
		if (characters.length === 0) return false;
		for (let i = 0; i < characters.length; ++i) if (!isTokenCharCode(characters.charCodeAt(i))) return false;
		return true;
	}
	/**
	* Matches if val contains an invalid field-vchar
	*  field-value    = *( field-content / obs-fold )
	*  field-content  = field-vchar [ 1*( SP / HTAB ) field-vchar ]
	*  field-vchar    = VCHAR / obs-text
	*/
	const headerCharRegex = /[^\t\x20-\x7e\x80-\xff]/;
	/**
	* @param {string} characters
	* @returns {boolean}
	*/
	function isValidHeaderValue$3(characters) {
		return !headerCharRegex.test(characters);
	}
	const rangeHeaderRegex = /^bytes (\d+)-(\d+)\/(\d+)?$/;
	/**
	* @typedef {object} RangeHeader
	* @property {number} start
	* @property {number | null} end
	* @property {number | null} size
	*/
	/**
	* Parse accordingly to RFC 9110
	* @see https://www.rfc-editor.org/rfc/rfc9110#field.content-range
	* @param {string} [range]
	* @returns {RangeHeader|null}
	*/
	function parseRangeHeader$1(range) {
		if (range == null || range === "") return {
			start: 0,
			end: null,
			size: null
		};
		const m = range ? range.match(rangeHeaderRegex) : null;
		return m ? {
			start: parseInt(m[1]),
			end: m[2] ? parseInt(m[2]) : null,
			size: m[3] ? parseInt(m[3]) : null
		} : null;
	}
	/**
	* @template {import("events").EventEmitter} T
	* @param {T} obj
	* @param {string} name
	* @param {(...args: any[]) => void} listener
	* @returns {T}
	*/
	function addListener(obj, name, listener) {
		(obj[kListeners] ??= []).push([name, listener]);
		obj.on(name, listener);
		return obj;
	}
	/**
	* @template {import("events").EventEmitter} T
	* @param {T} obj
	* @returns {T}
	*/
	function removeAllListeners$1(obj) {
		if (obj[kListeners] != null) {
			for (const [name, listener] of obj[kListeners]) obj.removeListener(name, listener);
			obj[kListeners] = null;
		}
		return obj;
	}
	/**
	* @param {import ('../dispatcher/client')} client
	* @param {import ('../core/request')} request
	* @param {Error} err
	*/
	function errorRequest(client, request$2, err) {
		try {
			request$2.onError(err);
			assert$30(request$2.aborted);
		} catch (err$1) {
			client.emit("error", err$1);
		}
	}
	/**
	* @param {WeakRef<net.Socket>} socketWeakRef
	* @param {object} opts
	* @param {number} opts.timeout
	* @param {string} opts.hostname
	* @param {number} opts.port
	* @returns {() => void}
	*/
	const setupConnectTimeout = process.platform === "win32" ? (socketWeakRef, opts) => {
		if (!opts.timeout) return noop$9;
		let s1 = null;
		let s2 = null;
		const fastTimer = timers$1.setFastTimeout(() => {
			s1 = setImmediate(() => {
				s2 = setImmediate(() => onConnectTimeout(socketWeakRef.deref(), opts));
			});
		}, opts.timeout);
		return () => {
			timers$1.clearFastTimeout(fastTimer);
			clearImmediate(s1);
			clearImmediate(s2);
		};
	} : (socketWeakRef, opts) => {
		if (!opts.timeout) return noop$9;
		let s1 = null;
		const fastTimer = timers$1.setFastTimeout(() => {
			s1 = setImmediate(() => {
				onConnectTimeout(socketWeakRef.deref(), opts);
			});
		}, opts.timeout);
		return () => {
			timers$1.clearFastTimeout(fastTimer);
			clearImmediate(s1);
		};
	};
	/**
	* @param {net.Socket} socket
	* @param {object} opts
	* @param {number} opts.timeout
	* @param {string} opts.hostname
	* @param {number} opts.port
	*/
	function onConnectTimeout(socket, opts) {
		if (socket == null) return;
		let message = "Connect Timeout Error";
		if (Array.isArray(socket.autoSelectFamilyAttemptedAddresses)) message += ` (attempted addresses: ${socket.autoSelectFamilyAttemptedAddresses.join(", ")},`;
		else message += ` (attempted address: ${opts.hostname}:${opts.port},`;
		message += ` timeout: ${opts.timeout}ms)`;
		destroy$1(socket, new ConnectTimeoutError(message));
	}
	const kEnumerableProperty$11 = Object.create(null);
	kEnumerableProperty$11.enumerable = true;
	const normalizedMethodRecordsBase$2 = {
		delete: "DELETE",
		DELETE: "DELETE",
		get: "GET",
		GET: "GET",
		head: "HEAD",
		HEAD: "HEAD",
		options: "OPTIONS",
		OPTIONS: "OPTIONS",
		post: "POST",
		POST: "POST",
		put: "PUT",
		PUT: "PUT"
	};
	const normalizedMethodRecords$2 = {
		...normalizedMethodRecordsBase$2,
		patch: "patch",
		PATCH: "PATCH"
	};
	Object.setPrototypeOf(normalizedMethodRecordsBase$2, null);
	Object.setPrototypeOf(normalizedMethodRecords$2, null);
	module.exports = {
		kEnumerableProperty: kEnumerableProperty$11,
		isDisturbed: isDisturbed$3,
		isBlobLike: isBlobLike$1,
		parseOrigin: parseOrigin$1,
		parseURL,
		getServerName: getServerName$1,
		isStream: isStream$1,
		isIterable: isIterable$1,
		isAsyncIterable,
		isDestroyed,
		headerNameToString,
		bufferToLowerCasedHeaderName: bufferToLowerCasedHeaderName$2,
		addListener,
		removeAllListeners: removeAllListeners$1,
		errorRequest,
		parseRawHeaders,
		encodeRawHeaders,
		parseHeaders: parseHeaders$1,
		parseKeepAliveTimeout,
		destroy: destroy$1,
		bodyLength,
		deepClone,
		ReadableStreamFrom: ReadableStreamFrom$3,
		isBuffer: isBuffer$1,
		assertRequestHandler: assertRequestHandler$1,
		getSocketInfo,
		isFormDataLike: isFormDataLike$1,
		pathHasQueryOrFragment: pathHasQueryOrFragment$1,
		serializePathWithQuery: serializePathWithQuery$4,
		addAbortListener: addAbortListener$2,
		isValidHTTPToken: isValidHTTPToken$3,
		isValidHeaderValue: isValidHeaderValue$3,
		isTokenCharCode,
		parseRangeHeader: parseRangeHeader$1,
		normalizedMethodRecordsBase: normalizedMethodRecordsBase$2,
		normalizedMethodRecords: normalizedMethodRecords$2,
		isValidPort,
		isHttpOrHttpsPrefixed,
		nodeMajor,
		nodeMinor,
		safeHTTPMethods: Object.freeze([
			"GET",
			"HEAD",
			"OPTIONS",
			"TRACE"
		]),
		wrapRequestBody: wrapRequestBody$1,
		setupConnectTimeout
	};
}) });

//#endregion
//#region node_modules/undici/lib/util/stats.js
var require_stats = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/util/stats.js": ((exports, module) => {
	const { kConnected: kConnected$4, kPending: kPending$4, kRunning: kRunning$5, kSize: kSize$5, kFree: kFree$1, kQueued: kQueued$1 } = require_symbols();
	var ClientStats$1 = class {
		constructor(client) {
			this.connected = client[kConnected$4];
			this.pending = client[kPending$4];
			this.running = client[kRunning$5];
			this.size = client[kSize$5];
		}
	};
	var PoolStats$1 = class {
		constructor(pool) {
			this.connected = pool[kConnected$4];
			this.free = pool[kFree$1];
			this.pending = pool[kPending$4];
			this.queued = pool[kQueued$1];
			this.running = pool[kRunning$5];
			this.size = pool[kSize$5];
		}
	};
	module.exports = {
		ClientStats: ClientStats$1,
		PoolStats: PoolStats$1
	};
}) });

//#endregion
//#region node_modules/undici/lib/core/diagnostics.js
var require_diagnostics = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/core/diagnostics.js": ((exports, module) => {
	const diagnosticsChannel = __require("node:diagnostics_channel");
	const util$21 = __require("node:util");
	const undiciDebugLog = util$21.debuglog("undici");
	const fetchDebuglog = util$21.debuglog("fetch");
	const websocketDebuglog = util$21.debuglog("websocket");
	const channels$6 = {
		beforeConnect: diagnosticsChannel.channel("undici:client:beforeConnect"),
		connected: diagnosticsChannel.channel("undici:client:connected"),
		connectError: diagnosticsChannel.channel("undici:client:connectError"),
		sendHeaders: diagnosticsChannel.channel("undici:client:sendHeaders"),
		create: diagnosticsChannel.channel("undici:request:create"),
		bodySent: diagnosticsChannel.channel("undici:request:bodySent"),
		bodyChunkSent: diagnosticsChannel.channel("undici:request:bodyChunkSent"),
		bodyChunkReceived: diagnosticsChannel.channel("undici:request:bodyChunkReceived"),
		headers: diagnosticsChannel.channel("undici:request:headers"),
		trailers: diagnosticsChannel.channel("undici:request:trailers"),
		error: diagnosticsChannel.channel("undici:request:error"),
		open: diagnosticsChannel.channel("undici:websocket:open"),
		close: diagnosticsChannel.channel("undici:websocket:close"),
		socketError: diagnosticsChannel.channel("undici:websocket:socket_error"),
		ping: diagnosticsChannel.channel("undici:websocket:ping"),
		pong: diagnosticsChannel.channel("undici:websocket:pong")
	};
	let isTrackingClientEvents = false;
	function trackClientEvents(debugLog = undiciDebugLog) {
		if (isTrackingClientEvents) return;
		isTrackingClientEvents = true;
		diagnosticsChannel.subscribe("undici:client:beforeConnect", (evt) => {
			const { connectParams: { version, protocol, port: port$1, host } } = evt;
			debugLog("connecting to %s%s using %s%s", host, port$1 ? `:${port$1}` : "", protocol, version);
		});
		diagnosticsChannel.subscribe("undici:client:connected", (evt) => {
			const { connectParams: { version, protocol, port: port$1, host } } = evt;
			debugLog("connected to %s%s using %s%s", host, port$1 ? `:${port$1}` : "", protocol, version);
		});
		diagnosticsChannel.subscribe("undici:client:connectError", (evt) => {
			const { connectParams: { version, protocol, port: port$1, host }, error } = evt;
			debugLog("connection to %s%s using %s%s errored - %s", host, port$1 ? `:${port$1}` : "", protocol, version, error.message);
		});
		diagnosticsChannel.subscribe("undici:client:sendHeaders", (evt) => {
			const { request: { method, path: path$1, origin } } = evt;
			debugLog("sending request to %s %s%s", method, origin, path$1);
		});
	}
	let isTrackingRequestEvents = false;
	function trackRequestEvents(debugLog = undiciDebugLog) {
		if (isTrackingRequestEvents) return;
		isTrackingRequestEvents = true;
		diagnosticsChannel.subscribe("undici:request:headers", (evt) => {
			const { request: { method, path: path$1, origin }, response: { statusCode } } = evt;
			debugLog("received response to %s %s%s - HTTP %d", method, origin, path$1, statusCode);
		});
		diagnosticsChannel.subscribe("undici:request:trailers", (evt) => {
			const { request: { method, path: path$1, origin } } = evt;
			debugLog("trailers received from %s %s%s", method, origin, path$1);
		});
		diagnosticsChannel.subscribe("undici:request:error", (evt) => {
			const { request: { method, path: path$1, origin }, error } = evt;
			debugLog("request to %s %s%s errored - %s", method, origin, path$1, error.message);
		});
	}
	let isTrackingWebSocketEvents = false;
	function trackWebSocketEvents(debugLog = websocketDebuglog) {
		if (isTrackingWebSocketEvents) return;
		isTrackingWebSocketEvents = true;
		diagnosticsChannel.subscribe("undici:websocket:open", (evt) => {
			const { address: { address, port: port$1 } } = evt;
			debugLog("connection opened %s%s", address, port$1 ? `:${port$1}` : "");
		});
		diagnosticsChannel.subscribe("undici:websocket:close", (evt) => {
			const { websocket, code, reason } = evt;
			debugLog("closed connection to %s - %s %s", websocket.url, code, reason);
		});
		diagnosticsChannel.subscribe("undici:websocket:socket_error", (err) => {
			debugLog("connection errored - %s", err.message);
		});
		diagnosticsChannel.subscribe("undici:websocket:ping", (evt) => {
			debugLog("ping received");
		});
		diagnosticsChannel.subscribe("undici:websocket:pong", (evt) => {
			debugLog("pong received");
		});
	}
	if (undiciDebugLog.enabled || fetchDebuglog.enabled) {
		trackClientEvents(fetchDebuglog.enabled ? fetchDebuglog : undiciDebugLog);
		trackRequestEvents(fetchDebuglog.enabled ? fetchDebuglog : undiciDebugLog);
	}
	if (websocketDebuglog.enabled) {
		trackClientEvents(undiciDebugLog.enabled ? undiciDebugLog : websocketDebuglog);
		trackWebSocketEvents(websocketDebuglog);
	}
	module.exports = { channels: channels$6 };
}) });

//#endregion
//#region node_modules/undici/lib/core/request.js
var require_request$1 = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/core/request.js": ((exports, module) => {
	const { InvalidArgumentError: InvalidArgumentError$30, NotSupportedError: NotSupportedError$1 } = require_errors();
	const assert$29 = __require("node:assert");
	const { isValidHTTPToken: isValidHTTPToken$2, isValidHeaderValue: isValidHeaderValue$2, isStream, destroy, isBuffer, isFormDataLike, isIterable, isBlobLike, serializePathWithQuery: serializePathWithQuery$3, assertRequestHandler, getServerName, normalizedMethodRecords: normalizedMethodRecords$1 } = require_util$5();
	const { channels: channels$5 } = require_diagnostics();
	const { headerNameLowerCasedRecord } = require_constants$4();
	const invalidPathRegex = /[^\u0021-\u00ff]/;
	const kHandler = Symbol("handler");
	var Request$4 = class {
		constructor(origin, { path: path$1, method, body, headers, query, idempotent, blocking, upgrade: upgrade$1, headersTimeout, bodyTimeout, reset, expectContinue, servername, throwOnError, maxRedirections }, handler) {
			if (typeof path$1 !== "string") throw new InvalidArgumentError$30("path must be a string");
			else if (path$1[0] !== "/" && !(path$1.startsWith("http://") || path$1.startsWith("https://")) && method !== "CONNECT") throw new InvalidArgumentError$30("path must be an absolute URL or start with a slash");
			else if (invalidPathRegex.test(path$1)) throw new InvalidArgumentError$30("invalid request path");
			if (typeof method !== "string") throw new InvalidArgumentError$30("method must be a string");
			else if (normalizedMethodRecords$1[method] === void 0 && !isValidHTTPToken$2(method)) throw new InvalidArgumentError$30("invalid request method");
			if (upgrade$1 && typeof upgrade$1 !== "string") throw new InvalidArgumentError$30("upgrade must be a string");
			if (headersTimeout != null && (!Number.isFinite(headersTimeout) || headersTimeout < 0)) throw new InvalidArgumentError$30("invalid headersTimeout");
			if (bodyTimeout != null && (!Number.isFinite(bodyTimeout) || bodyTimeout < 0)) throw new InvalidArgumentError$30("invalid bodyTimeout");
			if (reset != null && typeof reset !== "boolean") throw new InvalidArgumentError$30("invalid reset");
			if (expectContinue != null && typeof expectContinue !== "boolean") throw new InvalidArgumentError$30("invalid expectContinue");
			if (throwOnError != null) throw new InvalidArgumentError$30("invalid throwOnError");
			if (maxRedirections != null && maxRedirections !== 0) throw new InvalidArgumentError$30("maxRedirections is not supported, use the redirect interceptor");
			this.headersTimeout = headersTimeout;
			this.bodyTimeout = bodyTimeout;
			this.method = method;
			this.abort = null;
			if (body == null) this.body = null;
			else if (isStream(body)) {
				this.body = body;
				const rState = this.body._readableState;
				if (!rState || !rState.autoDestroy) {
					this.endHandler = function autoDestroy() {
						destroy(this);
					};
					this.body.on("end", this.endHandler);
				}
				this.errorHandler = (err) => {
					if (this.abort) this.abort(err);
					else this.error = err;
				};
				this.body.on("error", this.errorHandler);
			} else if (isBuffer(body)) this.body = body.byteLength ? body : null;
			else if (ArrayBuffer.isView(body)) this.body = body.buffer.byteLength ? Buffer.from(body.buffer, body.byteOffset, body.byteLength) : null;
			else if (body instanceof ArrayBuffer) this.body = body.byteLength ? Buffer.from(body) : null;
			else if (typeof body === "string") this.body = body.length ? Buffer.from(body) : null;
			else if (isFormDataLike(body) || isIterable(body) || isBlobLike(body)) this.body = body;
			else throw new InvalidArgumentError$30("body must be a string, a Buffer, a Readable stream, an iterable, or an async iterable");
			this.completed = false;
			this.aborted = false;
			this.upgrade = upgrade$1 || null;
			this.path = query ? serializePathWithQuery$3(path$1, query) : path$1;
			this.origin = origin;
			this.idempotent = idempotent == null ? method === "HEAD" || method === "GET" : idempotent;
			this.blocking = blocking ?? this.method !== "HEAD";
			this.reset = reset == null ? null : reset;
			this.host = null;
			this.contentLength = null;
			this.contentType = null;
			this.headers = [];
			this.expectContinue = expectContinue != null ? expectContinue : false;
			if (Array.isArray(headers)) {
				if (headers.length % 2 !== 0) throw new InvalidArgumentError$30("headers array must be even");
				for (let i = 0; i < headers.length; i += 2) processHeader(this, headers[i], headers[i + 1]);
			} else if (headers && typeof headers === "object") if (headers[Symbol.iterator]) for (const header of headers) {
				if (!Array.isArray(header) || header.length !== 2) throw new InvalidArgumentError$30("headers must be in key-value pair format");
				processHeader(this, header[0], header[1]);
			}
			else {
				const keys = Object.keys(headers);
				for (let i = 0; i < keys.length; ++i) processHeader(this, keys[i], headers[keys[i]]);
			}
			else if (headers != null) throw new InvalidArgumentError$30("headers must be an object or an array");
			assertRequestHandler(handler, method, upgrade$1);
			this.servername = servername || getServerName(this.host) || null;
			this[kHandler] = handler;
			if (channels$5.create.hasSubscribers) channels$5.create.publish({ request: this });
		}
		onBodySent(chunk) {
			if (channels$5.bodyChunkSent.hasSubscribers) channels$5.bodyChunkSent.publish({
				request: this,
				chunk
			});
			if (this[kHandler].onBodySent) try {
				return this[kHandler].onBodySent(chunk);
			} catch (err) {
				this.abort(err);
			}
		}
		onRequestSent() {
			if (channels$5.bodySent.hasSubscribers) channels$5.bodySent.publish({ request: this });
			if (this[kHandler].onRequestSent) try {
				return this[kHandler].onRequestSent();
			} catch (err) {
				this.abort(err);
			}
		}
		onConnect(abort$1) {
			assert$29(!this.aborted);
			assert$29(!this.completed);
			if (this.error) abort$1(this.error);
			else {
				this.abort = abort$1;
				return this[kHandler].onConnect(abort$1);
			}
		}
		onResponseStarted() {
			return this[kHandler].onResponseStarted?.();
		}
		onHeaders(statusCode, headers, resume$1, statusText) {
			assert$29(!this.aborted);
			assert$29(!this.completed);
			if (channels$5.headers.hasSubscribers) channels$5.headers.publish({
				request: this,
				response: {
					statusCode,
					headers,
					statusText
				}
			});
			try {
				return this[kHandler].onHeaders(statusCode, headers, resume$1, statusText);
			} catch (err) {
				this.abort(err);
			}
		}
		onData(chunk) {
			assert$29(!this.aborted);
			assert$29(!this.completed);
			if (channels$5.bodyChunkReceived.hasSubscribers) channels$5.bodyChunkReceived.publish({
				request: this,
				chunk
			});
			try {
				return this[kHandler].onData(chunk);
			} catch (err) {
				this.abort(err);
				return false;
			}
		}
		onUpgrade(statusCode, headers, socket) {
			assert$29(!this.aborted);
			assert$29(!this.completed);
			return this[kHandler].onUpgrade(statusCode, headers, socket);
		}
		onComplete(trailers) {
			this.onFinally();
			assert$29(!this.aborted);
			assert$29(!this.completed);
			this.completed = true;
			if (channels$5.trailers.hasSubscribers) channels$5.trailers.publish({
				request: this,
				trailers
			});
			try {
				return this[kHandler].onComplete(trailers);
			} catch (err) {
				this.onError(err);
			}
		}
		onError(error) {
			this.onFinally();
			if (channels$5.error.hasSubscribers) channels$5.error.publish({
				request: this,
				error
			});
			if (this.aborted) return;
			this.aborted = true;
			return this[kHandler].onError(error);
		}
		onFinally() {
			if (this.errorHandler) {
				this.body.off("error", this.errorHandler);
				this.errorHandler = null;
			}
			if (this.endHandler) {
				this.body.off("end", this.endHandler);
				this.endHandler = null;
			}
		}
		addHeader(key, value) {
			processHeader(this, key, value);
			return this;
		}
	};
	function processHeader(request$2, key, val) {
		if (val && typeof val === "object" && !Array.isArray(val)) throw new InvalidArgumentError$30(`invalid ${key} header`);
		else if (val === void 0) return;
		let headerName = headerNameLowerCasedRecord[key];
		if (headerName === void 0) {
			headerName = key.toLowerCase();
			if (headerNameLowerCasedRecord[headerName] === void 0 && !isValidHTTPToken$2(headerName)) throw new InvalidArgumentError$30("invalid header key");
		}
		if (Array.isArray(val)) {
			const arr = [];
			for (let i = 0; i < val.length; i++) if (typeof val[i] === "string") {
				if (!isValidHeaderValue$2(val[i])) throw new InvalidArgumentError$30(`invalid ${key} header`);
				arr.push(val[i]);
			} else if (val[i] === null) arr.push("");
			else if (typeof val[i] === "object") throw new InvalidArgumentError$30(`invalid ${key} header`);
			else arr.push(`${val[i]}`);
			val = arr;
		} else if (typeof val === "string") {
			if (!isValidHeaderValue$2(val)) throw new InvalidArgumentError$30(`invalid ${key} header`);
		} else if (val === null) val = "";
		else val = `${val}`;
		if (request$2.host === null && headerName === "host") {
			if (typeof val !== "string") throw new InvalidArgumentError$30("invalid host header");
			request$2.host = val;
		} else if (request$2.contentLength === null && headerName === "content-length") {
			request$2.contentLength = parseInt(val, 10);
			if (!Number.isFinite(request$2.contentLength)) throw new InvalidArgumentError$30("invalid content-length header");
		} else if (request$2.contentType === null && headerName === "content-type") {
			request$2.contentType = val;
			request$2.headers.push(key, val);
		} else if (headerName === "transfer-encoding" || headerName === "keep-alive" || headerName === "upgrade") throw new InvalidArgumentError$30(`invalid ${headerName} header`);
		else if (headerName === "connection") {
			const value = typeof val === "string" ? val.toLowerCase() : null;
			if (value !== "close" && value !== "keep-alive") throw new InvalidArgumentError$30("invalid connection header");
			if (value === "close") request$2.reset = true;
		} else if (headerName === "expect") throw new NotSupportedError$1("expect header not supported");
		else request$2.headers.push(key, val);
	}
	module.exports = Request$4;
}) });

//#endregion
//#region node_modules/undici/lib/handler/wrap-handler.js
var require_wrap_handler = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/handler/wrap-handler.js": ((exports, module) => {
	const { InvalidArgumentError: InvalidArgumentError$29 } = require_errors();
	module.exports = class WrapHandler$4 {
		#handler;
		constructor(handler) {
			this.#handler = handler;
		}
		static wrap(handler) {
			return handler.onRequestStart ? handler : new WrapHandler$4(handler);
		}
		onConnect(abort$1, context) {
			return this.#handler.onConnect?.(abort$1, context);
		}
		onHeaders(statusCode, rawHeaders, resume$1, statusMessage) {
			return this.#handler.onHeaders?.(statusCode, rawHeaders, resume$1, statusMessage);
		}
		onUpgrade(statusCode, rawHeaders, socket) {
			return this.#handler.onUpgrade?.(statusCode, rawHeaders, socket);
		}
		onData(data) {
			return this.#handler.onData?.(data);
		}
		onComplete(trailers) {
			return this.#handler.onComplete?.(trailers);
		}
		onError(err) {
			if (!this.#handler.onError) throw err;
			return this.#handler.onError?.(err);
		}
		onRequestStart(controller, context) {
			this.#handler.onConnect?.((reason) => controller.abort(reason), context);
		}
		onRequestUpgrade(controller, statusCode, headers, socket) {
			const rawHeaders = [];
			for (const [key, val] of Object.entries(headers)) rawHeaders.push(Buffer.from(key), Array.isArray(val) ? val.map((v) => Buffer.from(v)) : Buffer.from(val));
			this.#handler.onUpgrade?.(statusCode, rawHeaders, socket);
		}
		onResponseStart(controller, statusCode, headers, statusMessage) {
			const rawHeaders = [];
			for (const [key, val] of Object.entries(headers)) rawHeaders.push(Buffer.from(key), Array.isArray(val) ? val.map((v) => Buffer.from(v)) : Buffer.from(val));
			if (this.#handler.onHeaders?.(statusCode, rawHeaders, () => controller.resume(), statusMessage) === false) controller.pause();
		}
		onResponseData(controller, data) {
			if (this.#handler.onData?.(data) === false) controller.pause();
		}
		onResponseEnd(controller, trailers) {
			const rawTrailers = [];
			for (const [key, val] of Object.entries(trailers)) rawTrailers.push(Buffer.from(key), Array.isArray(val) ? val.map((v) => Buffer.from(v)) : Buffer.from(val));
			this.#handler.onComplete?.(rawTrailers);
		}
		onResponseError(controller, err) {
			if (!this.#handler.onError) throw new InvalidArgumentError$29("invalid onError method");
			this.#handler.onError?.(err);
		}
	};
}) });

//#endregion
//#region node_modules/undici/lib/dispatcher/dispatcher.js
var require_dispatcher = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/dispatcher/dispatcher.js": ((exports, module) => {
	const EventEmitter$1 = __require("node:events");
	const WrapHandler$3 = require_wrap_handler();
	const wrapInterceptor = (dispatch) => (opts, handler) => dispatch(opts, WrapHandler$3.wrap(handler));
	var Dispatcher$4 = class extends EventEmitter$1 {
		dispatch() {
			throw new Error("not implemented");
		}
		close() {
			throw new Error("not implemented");
		}
		destroy() {
			throw new Error("not implemented");
		}
		compose(...args) {
			const interceptors = Array.isArray(args[0]) ? args[0] : args;
			let dispatch = this.dispatch.bind(this);
			for (const interceptor of interceptors) {
				if (interceptor == null) continue;
				if (typeof interceptor !== "function") throw new TypeError(`invalid interceptor, expected function received ${typeof interceptor}`);
				dispatch = interceptor(dispatch);
				dispatch = wrapInterceptor(dispatch);
				if (dispatch == null || typeof dispatch !== "function" || dispatch.length !== 2) throw new TypeError("invalid interceptor");
			}
			return new Proxy(this, { get: (target, key) => key === "dispatch" ? dispatch : target[key] });
		}
	};
	module.exports = Dispatcher$4;
}) });

//#endregion
//#region node_modules/undici/lib/handler/unwrap-handler.js
var require_unwrap_handler = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/handler/unwrap-handler.js": ((exports, module) => {
	const { parseHeaders } = require_util$5();
	const { InvalidArgumentError: InvalidArgumentError$28 } = require_errors();
	const kResume$4 = Symbol("resume");
	var UnwrapController = class {
		#paused = false;
		#reason = null;
		#aborted = false;
		#abort;
		[kResume$4] = null;
		constructor(abort$1) {
			this.#abort = abort$1;
		}
		pause() {
			this.#paused = true;
		}
		resume() {
			if (this.#paused) {
				this.#paused = false;
				this[kResume$4]?.();
			}
		}
		abort(reason) {
			if (!this.#aborted) {
				this.#aborted = true;
				this.#reason = reason;
				this.#abort(reason);
			}
		}
		get aborted() {
			return this.#aborted;
		}
		get reason() {
			return this.#reason;
		}
		get paused() {
			return this.#paused;
		}
	};
	module.exports = class UnwrapHandler$1 {
		#handler;
		#controller;
		constructor(handler) {
			this.#handler = handler;
		}
		static unwrap(handler) {
			return !handler.onRequestStart ? handler : new UnwrapHandler$1(handler);
		}
		onConnect(abort$1, context) {
			this.#controller = new UnwrapController(abort$1);
			this.#handler.onRequestStart?.(this.#controller, context);
		}
		onUpgrade(statusCode, rawHeaders, socket) {
			this.#handler.onRequestUpgrade?.(this.#controller, statusCode, parseHeaders(rawHeaders), socket);
		}
		onHeaders(statusCode, rawHeaders, resume$1, statusMessage) {
			this.#controller[kResume$4] = resume$1;
			this.#handler.onResponseStart?.(this.#controller, statusCode, parseHeaders(rawHeaders), statusMessage);
			return !this.#controller.paused;
		}
		onData(data) {
			this.#handler.onResponseData?.(this.#controller, data);
			return !this.#controller.paused;
		}
		onComplete(rawTrailers) {
			this.#handler.onResponseEnd?.(this.#controller, parseHeaders(rawTrailers));
		}
		onError(err) {
			if (!this.#handler.onResponseError) throw new InvalidArgumentError$28("invalid onError method");
			this.#handler.onResponseError?.(this.#controller, err);
		}
	};
}) });

//#endregion
//#region node_modules/undici/lib/dispatcher/dispatcher-base.js
var require_dispatcher_base = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/dispatcher/dispatcher-base.js": ((exports, module) => {
	const Dispatcher$3 = require_dispatcher();
	const UnwrapHandler = require_unwrap_handler();
	const { ClientDestroyedError: ClientDestroyedError$1, ClientClosedError, InvalidArgumentError: InvalidArgumentError$27 } = require_errors();
	const { kDestroy: kDestroy$6, kClose: kClose$8, kClosed: kClosed$3, kDestroyed: kDestroyed$1, kDispatch: kDispatch$5 } = require_symbols();
	const kOnDestroyed = Symbol("onDestroyed");
	const kOnClosed = Symbol("onClosed");
	var DispatcherBase$6 = class extends Dispatcher$3 {
		constructor() {
			super();
			this[kDestroyed$1] = false;
			this[kOnDestroyed] = null;
			this[kClosed$3] = false;
			this[kOnClosed] = [];
		}
		get destroyed() {
			return this[kDestroyed$1];
		}
		get closed() {
			return this[kClosed$3];
		}
		close(callback) {
			if (callback === void 0) return new Promise((resolve$1, reject) => {
				this.close((err, data) => {
					return err ? reject(err) : resolve$1(data);
				});
			});
			if (typeof callback !== "function") throw new InvalidArgumentError$27("invalid callback");
			if (this[kDestroyed$1]) {
				queueMicrotask(() => callback(new ClientDestroyedError$1(), null));
				return;
			}
			if (this[kClosed$3]) {
				if (this[kOnClosed]) this[kOnClosed].push(callback);
				else queueMicrotask(() => callback(null, null));
				return;
			}
			this[kClosed$3] = true;
			this[kOnClosed].push(callback);
			const onClosed = () => {
				const callbacks = this[kOnClosed];
				this[kOnClosed] = null;
				for (let i = 0; i < callbacks.length; i++) callbacks[i](null, null);
			};
			this[kClose$8]().then(() => this.destroy()).then(() => {
				queueMicrotask(onClosed);
			});
		}
		destroy(err, callback) {
			if (typeof err === "function") {
				callback = err;
				err = null;
			}
			if (callback === void 0) return new Promise((resolve$1, reject) => {
				this.destroy(err, (err$1, data) => {
					return err$1 ? reject(err$1) : resolve$1(data);
				});
			});
			if (typeof callback !== "function") throw new InvalidArgumentError$27("invalid callback");
			if (this[kDestroyed$1]) {
				if (this[kOnDestroyed]) this[kOnDestroyed].push(callback);
				else queueMicrotask(() => callback(null, null));
				return;
			}
			if (!err) err = new ClientDestroyedError$1();
			this[kDestroyed$1] = true;
			this[kOnDestroyed] = this[kOnDestroyed] || [];
			this[kOnDestroyed].push(callback);
			const onDestroyed = () => {
				const callbacks = this[kOnDestroyed];
				this[kOnDestroyed] = null;
				for (let i = 0; i < callbacks.length; i++) callbacks[i](null, null);
			};
			this[kDestroy$6](err).then(() => {
				queueMicrotask(onDestroyed);
			});
		}
		dispatch(opts, handler) {
			if (!handler || typeof handler !== "object") throw new InvalidArgumentError$27("handler must be an object");
			handler = UnwrapHandler.unwrap(handler);
			try {
				if (!opts || typeof opts !== "object") throw new InvalidArgumentError$27("opts must be an object.");
				if (this[kDestroyed$1] || this[kOnDestroyed]) throw new ClientDestroyedError$1();
				if (this[kClosed$3]) throw new ClientClosedError();
				return this[kDispatch$5](opts, handler);
			} catch (err) {
				if (typeof handler.onError !== "function") throw err;
				handler.onError(err);
				return false;
			}
		}
	};
	module.exports = DispatcherBase$6;
}) });

//#endregion
//#region node_modules/undici/lib/core/connect.js
var require_connect = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/core/connect.js": ((exports, module) => {
	const net$1 = __require("node:net");
	const assert$28 = __require("node:assert");
	const util$20 = require_util$5();
	const { InvalidArgumentError: InvalidArgumentError$26 } = require_errors();
	let tls;
	const SessionCache = class WeakSessionCache {
		constructor(maxCachedSessions) {
			this._maxCachedSessions = maxCachedSessions;
			this._sessionCache = /* @__PURE__ */ new Map();
			this._sessionRegistry = new FinalizationRegistry((key) => {
				if (this._sessionCache.size < this._maxCachedSessions) return;
				const ref = this._sessionCache.get(key);
				if (ref !== void 0 && ref.deref() === void 0) this._sessionCache.delete(key);
			});
		}
		get(sessionKey) {
			const ref = this._sessionCache.get(sessionKey);
			return ref ? ref.deref() : null;
		}
		set(sessionKey, session) {
			if (this._maxCachedSessions === 0) return;
			this._sessionCache.set(sessionKey, new WeakRef(session));
			this._sessionRegistry.register(session, sessionKey);
		}
	};
	function buildConnector$4({ allowH2, maxCachedSessions, socketPath, timeout, session: customSession,...opts }) {
		if (maxCachedSessions != null && (!Number.isInteger(maxCachedSessions) || maxCachedSessions < 0)) throw new InvalidArgumentError$26("maxCachedSessions must be a positive integer or zero");
		const options = {
			path: socketPath,
			...opts
		};
		const sessionCache = new SessionCache(maxCachedSessions == null ? 100 : maxCachedSessions);
		timeout = timeout == null ? 1e4 : timeout;
		allowH2 = allowH2 != null ? allowH2 : false;
		return function connect$3({ hostname: hostname$1, host, protocol, port: port$1, servername, localAddress, httpSocket }, callback) {
			let socket;
			if (protocol === "https:") {
				if (!tls) tls = __require("node:tls");
				servername = servername || options.servername || util$20.getServerName(host) || null;
				const sessionKey = servername || hostname$1;
				assert$28(sessionKey);
				const session = customSession || sessionCache.get(sessionKey) || null;
				port$1 = port$1 || 443;
				socket = tls.connect({
					highWaterMark: 16384,
					...options,
					servername,
					session,
					localAddress,
					ALPNProtocols: allowH2 ? ["http/1.1", "h2"] : ["http/1.1"],
					socket: httpSocket,
					port: port$1,
					host: hostname$1
				});
				socket.on("session", function(session$1) {
					sessionCache.set(sessionKey, session$1);
				});
			} else {
				assert$28(!httpSocket, "httpSocket can only be sent on TLS update");
				port$1 = port$1 || 80;
				socket = net$1.connect({
					highWaterMark: 64 * 1024,
					...options,
					localAddress,
					port: port$1,
					host: hostname$1
				});
			}
			if (options.keepAlive == null || options.keepAlive) {
				const keepAliveInitialDelay = options.keepAliveInitialDelay === void 0 ? 6e4 : options.keepAliveInitialDelay;
				socket.setKeepAlive(true, keepAliveInitialDelay);
			}
			const clearConnectTimeout = util$20.setupConnectTimeout(new WeakRef(socket), {
				timeout,
				hostname: hostname$1,
				port: port$1
			});
			socket.setNoDelay(true).once(protocol === "https:" ? "secureConnect" : "connect", function() {
				queueMicrotask(clearConnectTimeout);
				if (callback) {
					const cb = callback;
					callback = null;
					cb(null, this);
				}
			}).on("error", function(err) {
				queueMicrotask(clearConnectTimeout);
				if (callback) {
					const cb = callback;
					callback = null;
					cb(err);
				}
			});
			return socket;
		};
	}
	module.exports = buildConnector$4;
}) });

//#endregion
//#region node_modules/undici/lib/llhttp/utils.js
var require_utils = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/llhttp/utils.js": ((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.enumToMap = enumToMap;
	function enumToMap(obj, filter = [], exceptions = []) {
		const emptyFilter = (filter?.length ?? 0) === 0;
		const emptyExceptions = (exceptions?.length ?? 0) === 0;
		return Object.fromEntries(Object.entries(obj).filter(([, value]) => {
			return typeof value === "number" && (emptyFilter || filter.includes(value)) && (emptyExceptions || !exceptions.includes(value));
		}));
	}
}) });

//#endregion
//#region node_modules/undici/lib/llhttp/constants.js
var require_constants$3 = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/llhttp/constants.js": ((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.SPECIAL_HEADERS = exports.MINOR = exports.MAJOR = exports.HTAB_SP_VCHAR_OBS_TEXT = exports.QUOTED_STRING = exports.CONNECTION_TOKEN_CHARS = exports.HEADER_CHARS = exports.TOKEN = exports.HEX = exports.URL_CHAR = exports.USERINFO_CHARS = exports.MARK = exports.ALPHANUM = exports.NUM = exports.HEX_MAP = exports.NUM_MAP = exports.ALPHA = exports.STATUSES_HTTP = exports.H_METHOD_MAP = exports.METHOD_MAP = exports.METHODS_RTSP = exports.METHODS_ICE = exports.METHODS_HTTP = exports.HEADER_STATE = exports.FINISH = exports.STATUSES = exports.METHODS = exports.LENIENT_FLAGS = exports.FLAGS = exports.TYPE = exports.ERROR = void 0;
	const utils_1 = require_utils();
	exports.ERROR = {
		OK: 0,
		INTERNAL: 1,
		STRICT: 2,
		CR_EXPECTED: 25,
		LF_EXPECTED: 3,
		UNEXPECTED_CONTENT_LENGTH: 4,
		UNEXPECTED_SPACE: 30,
		CLOSED_CONNECTION: 5,
		INVALID_METHOD: 6,
		INVALID_URL: 7,
		INVALID_CONSTANT: 8,
		INVALID_VERSION: 9,
		INVALID_HEADER_TOKEN: 10,
		INVALID_CONTENT_LENGTH: 11,
		INVALID_CHUNK_SIZE: 12,
		INVALID_STATUS: 13,
		INVALID_EOF_STATE: 14,
		INVALID_TRANSFER_ENCODING: 15,
		CB_MESSAGE_BEGIN: 16,
		CB_HEADERS_COMPLETE: 17,
		CB_MESSAGE_COMPLETE: 18,
		CB_CHUNK_HEADER: 19,
		CB_CHUNK_COMPLETE: 20,
		PAUSED: 21,
		PAUSED_UPGRADE: 22,
		PAUSED_H2_UPGRADE: 23,
		USER: 24,
		CB_URL_COMPLETE: 26,
		CB_STATUS_COMPLETE: 27,
		CB_METHOD_COMPLETE: 32,
		CB_VERSION_COMPLETE: 33,
		CB_HEADER_FIELD_COMPLETE: 28,
		CB_HEADER_VALUE_COMPLETE: 29,
		CB_CHUNK_EXTENSION_NAME_COMPLETE: 34,
		CB_CHUNK_EXTENSION_VALUE_COMPLETE: 35,
		CB_RESET: 31,
		CB_PROTOCOL_COMPLETE: 38
	};
	exports.TYPE = {
		BOTH: 0,
		REQUEST: 1,
		RESPONSE: 2
	};
	exports.FLAGS = {
		CONNECTION_KEEP_ALIVE: 1,
		CONNECTION_CLOSE: 2,
		CONNECTION_UPGRADE: 4,
		CHUNKED: 8,
		UPGRADE: 16,
		CONTENT_LENGTH: 32,
		SKIPBODY: 64,
		TRAILING: 128,
		TRANSFER_ENCODING: 512
	};
	exports.LENIENT_FLAGS = {
		HEADERS: 1,
		CHUNKED_LENGTH: 2,
		KEEP_ALIVE: 4,
		TRANSFER_ENCODING: 8,
		VERSION: 16,
		DATA_AFTER_CLOSE: 32,
		OPTIONAL_LF_AFTER_CR: 64,
		OPTIONAL_CRLF_AFTER_CHUNK: 128,
		OPTIONAL_CR_BEFORE_LF: 256,
		SPACES_AFTER_CHUNK_SIZE: 512
	};
	exports.METHODS = {
		"DELETE": 0,
		"GET": 1,
		"HEAD": 2,
		"POST": 3,
		"PUT": 4,
		"CONNECT": 5,
		"OPTIONS": 6,
		"TRACE": 7,
		"COPY": 8,
		"LOCK": 9,
		"MKCOL": 10,
		"MOVE": 11,
		"PROPFIND": 12,
		"PROPPATCH": 13,
		"SEARCH": 14,
		"UNLOCK": 15,
		"BIND": 16,
		"REBIND": 17,
		"UNBIND": 18,
		"ACL": 19,
		"REPORT": 20,
		"MKACTIVITY": 21,
		"CHECKOUT": 22,
		"MERGE": 23,
		"M-SEARCH": 24,
		"NOTIFY": 25,
		"SUBSCRIBE": 26,
		"UNSUBSCRIBE": 27,
		"PATCH": 28,
		"PURGE": 29,
		"MKCALENDAR": 30,
		"LINK": 31,
		"UNLINK": 32,
		"SOURCE": 33,
		"PRI": 34,
		"DESCRIBE": 35,
		"ANNOUNCE": 36,
		"SETUP": 37,
		"PLAY": 38,
		"PAUSE": 39,
		"TEARDOWN": 40,
		"GET_PARAMETER": 41,
		"SET_PARAMETER": 42,
		"REDIRECT": 43,
		"RECORD": 44,
		"FLUSH": 45,
		"QUERY": 46
	};
	exports.STATUSES = {
		CONTINUE: 100,
		SWITCHING_PROTOCOLS: 101,
		PROCESSING: 102,
		EARLY_HINTS: 103,
		RESPONSE_IS_STALE: 110,
		REVALIDATION_FAILED: 111,
		DISCONNECTED_OPERATION: 112,
		HEURISTIC_EXPIRATION: 113,
		MISCELLANEOUS_WARNING: 199,
		OK: 200,
		CREATED: 201,
		ACCEPTED: 202,
		NON_AUTHORITATIVE_INFORMATION: 203,
		NO_CONTENT: 204,
		RESET_CONTENT: 205,
		PARTIAL_CONTENT: 206,
		MULTI_STATUS: 207,
		ALREADY_REPORTED: 208,
		TRANSFORMATION_APPLIED: 214,
		IM_USED: 226,
		MISCELLANEOUS_PERSISTENT_WARNING: 299,
		MULTIPLE_CHOICES: 300,
		MOVED_PERMANENTLY: 301,
		FOUND: 302,
		SEE_OTHER: 303,
		NOT_MODIFIED: 304,
		USE_PROXY: 305,
		SWITCH_PROXY: 306,
		TEMPORARY_REDIRECT: 307,
		PERMANENT_REDIRECT: 308,
		BAD_REQUEST: 400,
		UNAUTHORIZED: 401,
		PAYMENT_REQUIRED: 402,
		FORBIDDEN: 403,
		NOT_FOUND: 404,
		METHOD_NOT_ALLOWED: 405,
		NOT_ACCEPTABLE: 406,
		PROXY_AUTHENTICATION_REQUIRED: 407,
		REQUEST_TIMEOUT: 408,
		CONFLICT: 409,
		GONE: 410,
		LENGTH_REQUIRED: 411,
		PRECONDITION_FAILED: 412,
		PAYLOAD_TOO_LARGE: 413,
		URI_TOO_LONG: 414,
		UNSUPPORTED_MEDIA_TYPE: 415,
		RANGE_NOT_SATISFIABLE: 416,
		EXPECTATION_FAILED: 417,
		IM_A_TEAPOT: 418,
		PAGE_EXPIRED: 419,
		ENHANCE_YOUR_CALM: 420,
		MISDIRECTED_REQUEST: 421,
		UNPROCESSABLE_ENTITY: 422,
		LOCKED: 423,
		FAILED_DEPENDENCY: 424,
		TOO_EARLY: 425,
		UPGRADE_REQUIRED: 426,
		PRECONDITION_REQUIRED: 428,
		TOO_MANY_REQUESTS: 429,
		REQUEST_HEADER_FIELDS_TOO_LARGE_UNOFFICIAL: 430,
		REQUEST_HEADER_FIELDS_TOO_LARGE: 431,
		LOGIN_TIMEOUT: 440,
		NO_RESPONSE: 444,
		RETRY_WITH: 449,
		BLOCKED_BY_PARENTAL_CONTROL: 450,
		UNAVAILABLE_FOR_LEGAL_REASONS: 451,
		CLIENT_CLOSED_LOAD_BALANCED_REQUEST: 460,
		INVALID_X_FORWARDED_FOR: 463,
		REQUEST_HEADER_TOO_LARGE: 494,
		SSL_CERTIFICATE_ERROR: 495,
		SSL_CERTIFICATE_REQUIRED: 496,
		HTTP_REQUEST_SENT_TO_HTTPS_PORT: 497,
		INVALID_TOKEN: 498,
		CLIENT_CLOSED_REQUEST: 499,
		INTERNAL_SERVER_ERROR: 500,
		NOT_IMPLEMENTED: 501,
		BAD_GATEWAY: 502,
		SERVICE_UNAVAILABLE: 503,
		GATEWAY_TIMEOUT: 504,
		HTTP_VERSION_NOT_SUPPORTED: 505,
		VARIANT_ALSO_NEGOTIATES: 506,
		INSUFFICIENT_STORAGE: 507,
		LOOP_DETECTED: 508,
		BANDWIDTH_LIMIT_EXCEEDED: 509,
		NOT_EXTENDED: 510,
		NETWORK_AUTHENTICATION_REQUIRED: 511,
		WEB_SERVER_UNKNOWN_ERROR: 520,
		WEB_SERVER_IS_DOWN: 521,
		CONNECTION_TIMEOUT: 522,
		ORIGIN_IS_UNREACHABLE: 523,
		TIMEOUT_OCCURED: 524,
		SSL_HANDSHAKE_FAILED: 525,
		INVALID_SSL_CERTIFICATE: 526,
		RAILGUN_ERROR: 527,
		SITE_IS_OVERLOADED: 529,
		SITE_IS_FROZEN: 530,
		IDENTITY_PROVIDER_AUTHENTICATION_ERROR: 561,
		NETWORK_READ_TIMEOUT: 598,
		NETWORK_CONNECT_TIMEOUT: 599
	};
	exports.FINISH = {
		SAFE: 0,
		SAFE_WITH_CB: 1,
		UNSAFE: 2
	};
	exports.HEADER_STATE = {
		GENERAL: 0,
		CONNECTION: 1,
		CONTENT_LENGTH: 2,
		TRANSFER_ENCODING: 3,
		UPGRADE: 4,
		CONNECTION_KEEP_ALIVE: 5,
		CONNECTION_CLOSE: 6,
		CONNECTION_UPGRADE: 7,
		TRANSFER_ENCODING_CHUNKED: 8
	};
	exports.METHODS_HTTP = [
		exports.METHODS.DELETE,
		exports.METHODS.GET,
		exports.METHODS.HEAD,
		exports.METHODS.POST,
		exports.METHODS.PUT,
		exports.METHODS.CONNECT,
		exports.METHODS.OPTIONS,
		exports.METHODS.TRACE,
		exports.METHODS.COPY,
		exports.METHODS.LOCK,
		exports.METHODS.MKCOL,
		exports.METHODS.MOVE,
		exports.METHODS.PROPFIND,
		exports.METHODS.PROPPATCH,
		exports.METHODS.SEARCH,
		exports.METHODS.UNLOCK,
		exports.METHODS.BIND,
		exports.METHODS.REBIND,
		exports.METHODS.UNBIND,
		exports.METHODS.ACL,
		exports.METHODS.REPORT,
		exports.METHODS.MKACTIVITY,
		exports.METHODS.CHECKOUT,
		exports.METHODS.MERGE,
		exports.METHODS["M-SEARCH"],
		exports.METHODS.NOTIFY,
		exports.METHODS.SUBSCRIBE,
		exports.METHODS.UNSUBSCRIBE,
		exports.METHODS.PATCH,
		exports.METHODS.PURGE,
		exports.METHODS.MKCALENDAR,
		exports.METHODS.LINK,
		exports.METHODS.UNLINK,
		exports.METHODS.PRI,
		exports.METHODS.SOURCE,
		exports.METHODS.QUERY
	];
	exports.METHODS_ICE = [exports.METHODS.SOURCE];
	exports.METHODS_RTSP = [
		exports.METHODS.OPTIONS,
		exports.METHODS.DESCRIBE,
		exports.METHODS.ANNOUNCE,
		exports.METHODS.SETUP,
		exports.METHODS.PLAY,
		exports.METHODS.PAUSE,
		exports.METHODS.TEARDOWN,
		exports.METHODS.GET_PARAMETER,
		exports.METHODS.SET_PARAMETER,
		exports.METHODS.REDIRECT,
		exports.METHODS.RECORD,
		exports.METHODS.FLUSH,
		exports.METHODS.GET,
		exports.METHODS.POST
	];
	exports.METHOD_MAP = (0, utils_1.enumToMap)(exports.METHODS);
	exports.H_METHOD_MAP = Object.fromEntries(Object.entries(exports.METHODS).filter(([k]) => k.startsWith("H")));
	exports.STATUSES_HTTP = [
		exports.STATUSES.CONTINUE,
		exports.STATUSES.SWITCHING_PROTOCOLS,
		exports.STATUSES.PROCESSING,
		exports.STATUSES.EARLY_HINTS,
		exports.STATUSES.RESPONSE_IS_STALE,
		exports.STATUSES.REVALIDATION_FAILED,
		exports.STATUSES.DISCONNECTED_OPERATION,
		exports.STATUSES.HEURISTIC_EXPIRATION,
		exports.STATUSES.MISCELLANEOUS_WARNING,
		exports.STATUSES.OK,
		exports.STATUSES.CREATED,
		exports.STATUSES.ACCEPTED,
		exports.STATUSES.NON_AUTHORITATIVE_INFORMATION,
		exports.STATUSES.NO_CONTENT,
		exports.STATUSES.RESET_CONTENT,
		exports.STATUSES.PARTIAL_CONTENT,
		exports.STATUSES.MULTI_STATUS,
		exports.STATUSES.ALREADY_REPORTED,
		exports.STATUSES.TRANSFORMATION_APPLIED,
		exports.STATUSES.IM_USED,
		exports.STATUSES.MISCELLANEOUS_PERSISTENT_WARNING,
		exports.STATUSES.MULTIPLE_CHOICES,
		exports.STATUSES.MOVED_PERMANENTLY,
		exports.STATUSES.FOUND,
		exports.STATUSES.SEE_OTHER,
		exports.STATUSES.NOT_MODIFIED,
		exports.STATUSES.USE_PROXY,
		exports.STATUSES.SWITCH_PROXY,
		exports.STATUSES.TEMPORARY_REDIRECT,
		exports.STATUSES.PERMANENT_REDIRECT,
		exports.STATUSES.BAD_REQUEST,
		exports.STATUSES.UNAUTHORIZED,
		exports.STATUSES.PAYMENT_REQUIRED,
		exports.STATUSES.FORBIDDEN,
		exports.STATUSES.NOT_FOUND,
		exports.STATUSES.METHOD_NOT_ALLOWED,
		exports.STATUSES.NOT_ACCEPTABLE,
		exports.STATUSES.PROXY_AUTHENTICATION_REQUIRED,
		exports.STATUSES.REQUEST_TIMEOUT,
		exports.STATUSES.CONFLICT,
		exports.STATUSES.GONE,
		exports.STATUSES.LENGTH_REQUIRED,
		exports.STATUSES.PRECONDITION_FAILED,
		exports.STATUSES.PAYLOAD_TOO_LARGE,
		exports.STATUSES.URI_TOO_LONG,
		exports.STATUSES.UNSUPPORTED_MEDIA_TYPE,
		exports.STATUSES.RANGE_NOT_SATISFIABLE,
		exports.STATUSES.EXPECTATION_FAILED,
		exports.STATUSES.IM_A_TEAPOT,
		exports.STATUSES.PAGE_EXPIRED,
		exports.STATUSES.ENHANCE_YOUR_CALM,
		exports.STATUSES.MISDIRECTED_REQUEST,
		exports.STATUSES.UNPROCESSABLE_ENTITY,
		exports.STATUSES.LOCKED,
		exports.STATUSES.FAILED_DEPENDENCY,
		exports.STATUSES.TOO_EARLY,
		exports.STATUSES.UPGRADE_REQUIRED,
		exports.STATUSES.PRECONDITION_REQUIRED,
		exports.STATUSES.TOO_MANY_REQUESTS,
		exports.STATUSES.REQUEST_HEADER_FIELDS_TOO_LARGE_UNOFFICIAL,
		exports.STATUSES.REQUEST_HEADER_FIELDS_TOO_LARGE,
		exports.STATUSES.LOGIN_TIMEOUT,
		exports.STATUSES.NO_RESPONSE,
		exports.STATUSES.RETRY_WITH,
		exports.STATUSES.BLOCKED_BY_PARENTAL_CONTROL,
		exports.STATUSES.UNAVAILABLE_FOR_LEGAL_REASONS,
		exports.STATUSES.CLIENT_CLOSED_LOAD_BALANCED_REQUEST,
		exports.STATUSES.INVALID_X_FORWARDED_FOR,
		exports.STATUSES.REQUEST_HEADER_TOO_LARGE,
		exports.STATUSES.SSL_CERTIFICATE_ERROR,
		exports.STATUSES.SSL_CERTIFICATE_REQUIRED,
		exports.STATUSES.HTTP_REQUEST_SENT_TO_HTTPS_PORT,
		exports.STATUSES.INVALID_TOKEN,
		exports.STATUSES.CLIENT_CLOSED_REQUEST,
		exports.STATUSES.INTERNAL_SERVER_ERROR,
		exports.STATUSES.NOT_IMPLEMENTED,
		exports.STATUSES.BAD_GATEWAY,
		exports.STATUSES.SERVICE_UNAVAILABLE,
		exports.STATUSES.GATEWAY_TIMEOUT,
		exports.STATUSES.HTTP_VERSION_NOT_SUPPORTED,
		exports.STATUSES.VARIANT_ALSO_NEGOTIATES,
		exports.STATUSES.INSUFFICIENT_STORAGE,
		exports.STATUSES.LOOP_DETECTED,
		exports.STATUSES.BANDWIDTH_LIMIT_EXCEEDED,
		exports.STATUSES.NOT_EXTENDED,
		exports.STATUSES.NETWORK_AUTHENTICATION_REQUIRED,
		exports.STATUSES.WEB_SERVER_UNKNOWN_ERROR,
		exports.STATUSES.WEB_SERVER_IS_DOWN,
		exports.STATUSES.CONNECTION_TIMEOUT,
		exports.STATUSES.ORIGIN_IS_UNREACHABLE,
		exports.STATUSES.TIMEOUT_OCCURED,
		exports.STATUSES.SSL_HANDSHAKE_FAILED,
		exports.STATUSES.INVALID_SSL_CERTIFICATE,
		exports.STATUSES.RAILGUN_ERROR,
		exports.STATUSES.SITE_IS_OVERLOADED,
		exports.STATUSES.SITE_IS_FROZEN,
		exports.STATUSES.IDENTITY_PROVIDER_AUTHENTICATION_ERROR,
		exports.STATUSES.NETWORK_READ_TIMEOUT,
		exports.STATUSES.NETWORK_CONNECT_TIMEOUT
	];
	exports.ALPHA = [];
	for (let i = "A".charCodeAt(0); i <= "Z".charCodeAt(0); i++) {
		exports.ALPHA.push(String.fromCharCode(i));
		exports.ALPHA.push(String.fromCharCode(i + 32));
	}
	exports.NUM_MAP = {
		0: 0,
		1: 1,
		2: 2,
		3: 3,
		4: 4,
		5: 5,
		6: 6,
		7: 7,
		8: 8,
		9: 9
	};
	exports.HEX_MAP = {
		0: 0,
		1: 1,
		2: 2,
		3: 3,
		4: 4,
		5: 5,
		6: 6,
		7: 7,
		8: 8,
		9: 9,
		A: 10,
		B: 11,
		C: 12,
		D: 13,
		E: 14,
		F: 15,
		a: 10,
		b: 11,
		c: 12,
		d: 13,
		e: 14,
		f: 15
	};
	exports.NUM = [
		"0",
		"1",
		"2",
		"3",
		"4",
		"5",
		"6",
		"7",
		"8",
		"9"
	];
	exports.ALPHANUM = exports.ALPHA.concat(exports.NUM);
	exports.MARK = [
		"-",
		"_",
		".",
		"!",
		"~",
		"*",
		"'",
		"(",
		")"
	];
	exports.USERINFO_CHARS = exports.ALPHANUM.concat(exports.MARK).concat([
		"%",
		";",
		":",
		"&",
		"=",
		"+",
		"$",
		","
	]);
	exports.URL_CHAR = [
		"!",
		"\"",
		"$",
		"%",
		"&",
		"'",
		"(",
		")",
		"*",
		"+",
		",",
		"-",
		".",
		"/",
		":",
		";",
		"<",
		"=",
		">",
		"@",
		"[",
		"\\",
		"]",
		"^",
		"_",
		"`",
		"{",
		"|",
		"}",
		"~"
	].concat(exports.ALPHANUM);
	exports.HEX = exports.NUM.concat([
		"a",
		"b",
		"c",
		"d",
		"e",
		"f",
		"A",
		"B",
		"C",
		"D",
		"E",
		"F"
	]);
	exports.TOKEN = [
		"!",
		"#",
		"$",
		"%",
		"&",
		"'",
		"*",
		"+",
		"-",
		".",
		"^",
		"_",
		"`",
		"|",
		"~"
	].concat(exports.ALPHANUM);
	exports.HEADER_CHARS = ["	"];
	for (let i = 32; i <= 255; i++) if (i !== 127) exports.HEADER_CHARS.push(i);
	exports.CONNECTION_TOKEN_CHARS = exports.HEADER_CHARS.filter((c) => c !== 44);
	exports.QUOTED_STRING = ["	", " "];
	for (let i = 33; i <= 255; i++) if (i !== 34 && i !== 92) exports.QUOTED_STRING.push(i);
	exports.HTAB_SP_VCHAR_OBS_TEXT = ["	", " "];
	for (let i = 33; i <= 126; i++) exports.HTAB_SP_VCHAR_OBS_TEXT.push(i);
	for (let i = 128; i <= 255; i++) exports.HTAB_SP_VCHAR_OBS_TEXT.push(i);
	exports.MAJOR = exports.NUM_MAP;
	exports.MINOR = exports.MAJOR;
	exports.SPECIAL_HEADERS = {
		"connection": exports.HEADER_STATE.CONNECTION,
		"content-length": exports.HEADER_STATE.CONTENT_LENGTH,
		"proxy-connection": exports.HEADER_STATE.CONNECTION,
		"transfer-encoding": exports.HEADER_STATE.TRANSFER_ENCODING,
		"upgrade": exports.HEADER_STATE.UPGRADE
	};
	exports.default = {
		ERROR: exports.ERROR,
		TYPE: exports.TYPE,
		FLAGS: exports.FLAGS,
		LENIENT_FLAGS: exports.LENIENT_FLAGS,
		METHODS: exports.METHODS,
		STATUSES: exports.STATUSES,
		FINISH: exports.FINISH,
		HEADER_STATE: exports.HEADER_STATE,
		ALPHA: exports.ALPHA,
		NUM_MAP: exports.NUM_MAP,
		HEX_MAP: exports.HEX_MAP,
		NUM: exports.NUM,
		ALPHANUM: exports.ALPHANUM,
		MARK: exports.MARK,
		USERINFO_CHARS: exports.USERINFO_CHARS,
		URL_CHAR: exports.URL_CHAR,
		HEX: exports.HEX,
		TOKEN: exports.TOKEN,
		HEADER_CHARS: exports.HEADER_CHARS,
		CONNECTION_TOKEN_CHARS: exports.CONNECTION_TOKEN_CHARS,
		QUOTED_STRING: exports.QUOTED_STRING,
		HTAB_SP_VCHAR_OBS_TEXT: exports.HTAB_SP_VCHAR_OBS_TEXT,
		MAJOR: exports.MAJOR,
		MINOR: exports.MINOR,
		SPECIAL_HEADERS: exports.SPECIAL_HEADERS,
		METHODS_HTTP: exports.METHODS_HTTP,
		METHODS_ICE: exports.METHODS_ICE,
		METHODS_RTSP: exports.METHODS_RTSP,
		METHOD_MAP: exports.METHOD_MAP,
		H_METHOD_MAP: exports.H_METHOD_MAP,
		STATUSES_HTTP: exports.STATUSES_HTTP
	};
}) });

//#endregion
//#region node_modules/undici/lib/llhttp/llhttp-wasm.js
var require_llhttp_wasm = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/llhttp/llhttp-wasm.js": ((exports, module) => {
	const { Buffer: Buffer$2 } = __require("node:buffer");
	const wasmBase64$1 = "AGFzbQEAAAABJwdgAX8Bf2ADf39/AX9gAn9/AGABfwBgBH9/f38Bf2AAAGADf39/AALLAQgDZW52GHdhc21fb25faGVhZGVyc19jb21wbGV0ZQAEA2VudhV3YXNtX29uX21lc3NhZ2VfYmVnaW4AAANlbnYLd2FzbV9vbl91cmwAAQNlbnYOd2FzbV9vbl9zdGF0dXMAAQNlbnYUd2FzbV9vbl9oZWFkZXJfZmllbGQAAQNlbnYUd2FzbV9vbl9oZWFkZXJfdmFsdWUAAQNlbnYMd2FzbV9vbl9ib2R5AAEDZW52GHdhc21fb25fbWVzc2FnZV9jb21wbGV0ZQAAAzU0BQYAAAMAAAAAAAADAQMAAwMDAAACAAAAAAICAgICAgICAgIBAQEBAQEBAQEBAwAAAwAAAAQFAXABExMFAwEAAgYIAX8BQcDZBAsHxQcoBm1lbW9yeQIAC19pbml0aWFsaXplAAgZX19pbmRpcmVjdF9mdW5jdGlvbl90YWJsZQEAC2xsaHR0cF9pbml0AAkYbGxodHRwX3Nob3VsZF9rZWVwX2FsaXZlADcMbGxodHRwX2FsbG9jAAsGbWFsbG9jADkLbGxodHRwX2ZyZWUADARmcmVlAAwPbGxodHRwX2dldF90eXBlAA0VbGxodHRwX2dldF9odHRwX21ham9yAA4VbGxodHRwX2dldF9odHRwX21pbm9yAA8RbGxodHRwX2dldF9tZXRob2QAEBZsbGh0dHBfZ2V0X3N0YXR1c19jb2RlABESbGxodHRwX2dldF91cGdyYWRlABIMbGxodHRwX3Jlc2V0ABMObGxodHRwX2V4ZWN1dGUAFBRsbGh0dHBfc2V0dGluZ3NfaW5pdAAVDWxsaHR0cF9maW5pc2gAFgxsbGh0dHBfcGF1c2UAFw1sbGh0dHBfcmVzdW1lABgbbGxodHRwX3Jlc3VtZV9hZnRlcl91cGdyYWRlABkQbGxodHRwX2dldF9lcnJubwAaF2xsaHR0cF9nZXRfZXJyb3JfcmVhc29uABsXbGxodHRwX3NldF9lcnJvcl9yZWFzb24AHBRsbGh0dHBfZ2V0X2Vycm9yX3BvcwAdEWxsaHR0cF9lcnJub19uYW1lAB4SbGxodHRwX21ldGhvZF9uYW1lAB8SbGxodHRwX3N0YXR1c19uYW1lACAabGxodHRw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AXRrQT5qIQILIAEgAjYCHCABQgA3AhAgAkECdEH01wBqIQACQEHI1QAoAgAiA0EBIAJ0IgdxRQRAIAAgATYCAEHI1QAgAyAHcjYCACABIAA2AhggASABNgIIIAEgATYCDAwBCyAEQRkgAkEBdmtBACACQR9HG3QhAiAAKAIAIQACQANAIAAiAygCBEF4cSAERg0BIAJBHXYhACACQQF0IQIgAyAAQQRxakEQaiIHKAIAIgANAAsgByABNgIAIAEgAzYCGCABIAE2AgwgASABNgIIDAELIAMoAggiACABNgIMIAMgATYCCCABQQA2AhggASADNgIMIAEgADYCCAtB5NUAQeTVACgCAEEBayIAQX8gABs2AgALCwcAIAAtACgLBwAgAC0AKgsHACAALQArCwcAIAAtACkLBwAgAC8BNAsHACAALQAwC0ABBH8gACgCGCEBIAAvAS4hAiAALQAoIQMgACgCOCEEIAAQOCAAIAQ2AjggACADOgAoIAAgAjsBLiAAIAE2AhgL5YUCAgd/A34gASACaiEEAkAgACIDKAIMIgANACADKAIEBEAgAyABNgIECyMAQRBrIgkkAAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAn8CQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkAC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C0HjAQwHC0HXAQwGC0HYAQwFC0HZAQwEC0HaAQwDC0HbAQwCC0HdAQwBC0HcAQshAgNAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQCADAn8CQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAn8CQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAn8CQAJAAkACQAJAAkACQAJ/AkACQAJAAn8CQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAIAMCfwJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACfwJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkAg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ogKiAqICogKiAqICogKiAqICogKiAqICogIAogILIAQgAUEBaiIBRw0AC0HJACECDOwCCyABQQFqIQEgA0Evai0AAEEBcQ2lAiADQQA2AhwgAyABNgIUIANBlxA2AhAgA0EKNgIMQQAhAgzrAgsgASAERwRAA0AgAS0AAEEgRw0VIAQgAUEBaiIBRw0AC0H4ACECDOsCC0H4ACECDOoCCyADQQI6ACgMOAtBACECIANBADYCHCADQb8LNgIQIANBAjYCDCADIAFBAWo2AhQM6AILQQAhAgzOAgtBDSECDM0CC0ETIQIMzAILQRUhAgzLAgtBFiECDMoCC0EYIQIMyQILQRkhAgzIAgtBGiECDMcCC0EbIQIMxgILQRwhAgzFAgtBHSECDMQCC0EeIQIMwwILQR8hAgzCAgtBICECDMECC0EiIQIMwAILQSMhAgy/AgtBJSECDL4CC0HlACECDL0CCyADQT02AhwgAyABNgIUIAMgADYCDEEAIQIM1QILIANBGzYCHCADIAE2AhQgA0GkHDYCECADQRU2AgxBACECDNQCCyADQSA2AhwgAyABNgIUIANBmBo2AhAgA0EVNgIMQQAhAgzTAgsgA0ETNgIcIAMgATYCFCADQZgaNgIQIANBFTYCDEEAIQIM0gILIANBCzYCHCADIAE2AhQgA0GYGjYCECADQRU2AgxBACECDNECCyADQRA2AhwgAyABNgIUIANBmBo2AhAgA0EVNgIMQQAhAgzQAgsgA0EgNgIcIAMgATYCFCADQaQcNgIQIANBFTYCDEEAIQIMzwILIANBCzYCHCADIAE2AhQgA0GkHDYCECADQRU2AgxBACECDM4CCyADQQw2AhwgAyABNgIUIANBpBw2AhAgA0EVNgIMQQAhAgzNAgtBACECIANBADYCHCADIAE2AhQgA0HdDjYCECADQRI2AgwMzAILAkADQAJAIAEtAABBCmsOBAACAgACCyAEIAFBAWoiAUcNAAtB/QEhAgzMAgsCQAJAIAMtADZBAUcNAEEAIQACQCADKAI4IgJFDQAgAigCYCICRQ0AIAMgAhEAACEACyAARQ0AIABBFUcNASADQfwBNgIcIAMgATYCFCADQdwZNgIQIANBFTYCDEEAIQIMzQILQdwBIQIMswILIANBADYCHCADIAE2AhQgA0H5CzYCECADQR82AgxBACECDMsCCwJAAkAgAy0AKEEBaw4CBAEAC0HbASECDLICC0HUASECDLECCyADQQI6ADFBACEAAkAgAygCOCICRQ0AIAIoAgAiAkUNACADIAIRAAAhAAsgAEUEQEHdASECDLECCyAAQRVHBEAgA0EANgIcIAMgATYCFCADQbQMNgIQIANBEDYCDEEAIQIMygILIANB+wE2AhwgAyABNgIUIANBgRo2AhAgA0EVNgIMQQAhAgzJAgsgASAERgRAQfoBIQIMyQILIAEtAABByABGDQEgA0EBOgAoC0HAASECDK4CC0HaASECDK0CCyABIARHBEAgA0EMNgIIIAMgATYCBEHZASECDK0CC0H5ASECDMUCCyABIARGBEBB+AEhAgzFAgsgAS0AAEHIAEcNBCABQQFqIQFB2AEhAgyrAgsgASAERgRAQfcBIQIMxAILAkACQCABLQAAQcUAaw4QAAUFBQUFBQUFBQUFBQUFAQULIAFBAWohAUHWASECDKsCCyABQQFqIQFB1wEhAgyqAgtB9gEhAiABIARGDcICIAMoAgAiACAEIAFraiEFIAEgAGtBAmohBgJAA0AgAS0AACAAQbrVAGotAABHDQMgAEECRg0BIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADMMCCyADKAIEIQAgA0IANwMAIAMgACAGQQFqIgEQLiIARQRAQeMBIQIMqgILIANB9QE2AhwgAyABNgIUIAMgADYCDEEAIQIMwgILQfQBIQIgASAERg3BAiADKAIAIgAgBCABa2ohBSABIABrQQFqIQYCQANAIAEtAAAgAEG41QBqLQAARw0CIABBAUYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAzCAgsgA0GBBDsBKCADKAIEIQAgA0IANwMAIAMgACAGQQFqIgEQLiIADQMMAgsgA0EANgIAC0EAIQIgA0EANgIcIAMgATYCFCADQeUfNgIQIANBCDYCDAy/AgtB1QEhAgylAgsgA0HzATYCHCADIAE2AhQgAyAANgIMQQAhAgy9AgtBACEAAkAgAygCOCICRQ0AIAIoAkAiAkUNACADIAIRAAAhAAsgAEUNbiAAQRVHBEAgA0EANgIcIAMgATYCFCADQYIPNgIQIANBIDYCDEEAIQIMvQILIANBjwE2AhwgAyABNgIUIANB7Bs2AhAgA0EVNgIMQQAhAgy8AgsgASAERwRAIANBDTYCCCADIAE2AgRB0wEhAgyjAgtB8gEhAgy7AgsgASAERgRAQfEBIQIMuwILAkACQAJAIAEtAABByABrDgsAAQgICAgICAgIAggLIAFBAWohAUHQASECDKMCCyABQQFqIQFB0QEhAgyiAgsgAUEBaiEBQdIBIQIMoQILQfABIQIgASAERg25AiADKAIAIgAgBCABa2ohBiABIABrQQJqIQUDQCABLQAAIABBtdUAai0AAEcNBCAAQQJGDQMgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAY2AgAMuQILQe8BIQIgASAERg24AiADKAIAIgAgBCABa2ohBiABIABrQQFqIQUDQCABLQAAIABBs9UAai0AAEcNAyAAQQFGDQIgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAY2AgAMuAILQe4BIQIgASAERg23AiADKAIAIgAgBCABa2ohBiABIABrQQJqIQUDQCABLQAAIABBsNUAai0AAEcNAiAAQQJGDQEgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAY2AgAMtwILIAMoAgQhACADQgA3AwAgAyAAIAVBAWoiARArIgBFDQIgA0HsATYCHCADIAE2AhQgAyAANgIMQQAhAgy2AgsgA0EANgIACyADKAIEIQAgA0EANgIEIAMgACABECsiAEUNnAIgA0HtATYCHCADIAE2AhQgAyAANgIMQQAhAgy0AgtBzwEhAgyaAgtBACEAAkAgAygCOCICRQ0AIAIoAjQiAkUNACADIAIRAAAhAAsCQCAABEAgAEEVRg0BIANBADYCHCADIAE2AhQgA0HqDTYCECADQSY2AgxBACECDLQCC0HOASECDJoCCyADQesBNgIcIAMgATYCFCADQYAbNgIQIANBFTYCDEEAIQIMsgILIAEgBEYEQEHrASECDLICCyABLQAAQS9GBEAgAUEBaiEBDAELIANBADYCHCADIAE2AhQgA0GyODYCECADQQg2AgxBACECDLECC0HNASECDJcCCyABIARHBEAgA0EONgIIIAMgATYCBEHMASECDJcCC0HqASECDK8CCyABIARGBEBB6QEhAgyvAgsgAS0AAEEwayIAQf8BcUEKSQRAIAMgADoAKiABQQFqIQFBywEhAgyWAgsgAygCBCEAIANBADYCBCADIAAgARAvIgBFDZcCIANB6AE2AhwgAyABNgIUIAMgADYCDEEAIQIMrgILIAEgBEYEQEHnASECDK4CCwJAIAEtAABBLkYEQCABQQFqIQEMAQsgAygCBCEAIANBADYCBCADIAAgARAvIgBFDZgCIANB5gE2AhwgAyABNgIUIAMgADYCDEEAIQIMrgILQcoBIQIMlAILIAEgBEYEQEHlASECDK0CC0EAIQBBASEFQQEhB0EAIQICQAJAAkACQAJAAn8CQAJAAkACQAJAAkACQCABLQAAQTBrDgoKCQABAgMEBQYICwtBAgwGC0EDDAULQQQMBAtBBQwDC0EGDAILQQcMAQtBCAshAkEAIQVBACEHDAILQQkhAkEBIQBBACEFQQAhBwwBC0EAIQVBASECCyADIAI6ACsgAUEBaiEBAkACQCADLQAuQRBxDQACQAJAAkAgAy0AKg4DAQACBAsgB0UNAwwCCyAADQEMAgsgBUUNAQsgAygCBCEAIANBADYCBCADIAAgARAvIgBFDQIgA0HiATYCHCADIAE2AhQgAyAANgIMQQAhAgyvAgsgAygCBCEAIANBADYCBCADIAAgARAvIgBFDZoCIANB4wE2AhwgAyABNgIUIAMgADYCDEEAIQIMrgILIAMoAgQhACADQQA2AgQgAyAAIAEQLyIARQ2YAiADQeQBNgIcIAMgATYCFCADIAA2AgwMrQILQckBIQIMkwILQQAhAAJAIAMoAjgiAkUNACACKAJEIgJFDQAgAyACEQAAIQALAkAgAARAIABBFUYNASADQQA2AhwgAyABNgIUIANBpA02AhAgA0EhNgIMQQAhAgytAgtByAEhAgyTAgsgA0HhATYCHCADIAE2AhQgA0HQGjYCECADQRU2AgxBACECDKsCCyABIARGBEBB4QEhAgyrAgsCQCABLQAAQSBGBEAgA0EAOwE0IAFBAWohAQwBCyADQQA2AhwgAyABNgIUIANBmRE2AhAgA0EJNgIMQQAhAgyrAgtBxwEhAgyRAgsgASAERgRAQeABIQIMqgILAkAgAS0AAEEwa0H/AXEiAkEKSQRAIAFBAWohAQJAIAMvATQiAEGZM0sNACADIABBCmwiADsBNCAAQf7/A3EgAkH//wNzSw0AIAMgACACajsBNAwCC0EAIQIgA0EANgIcIAMgATYCFCADQZUeNgIQIANBDTYCDAyrAgsgA0EANgIcIAMgATYCFCADQZUeNgIQIANBDTYCDEEAIQIMqgILQcYBIQIMkAILIAEgBEYEQEHfASECDKkCCwJAIAEtAABBMGtB/wFxIgJBCkkEQCABQQFqIQECQCADLwE0IgBBmTNLDQAgAyAAQQpsIgA7ATQgAEH+/wNxIAJB//8Dc0sNACADIAAgAmo7ATQMAgtBACECIANBADYCHCADIAE2AhQgA0GVHjYCECADQQ02AgwMqgILIANBADYCHCADIAE2AhQgA0GVHjYCECADQQ02AgxBACECDKkCC0HFASECDI8CCyABIARGBEBB3gEhAgyoAgsCQCABLQAAQTBrQf8BcSICQQpJBEAgAUEBaiEBAkAgAy8BNCIAQZkzSw0AIAMgAEEKbCIAOwE0IABB/v8DcSACQf//A3NLDQAgAyAAIAJqOwE0DAILQQAhAiADQQA2AhwgAyABNgIUIANBlR42AhAgA0ENNgIMDKkCCyADQQA2AhwgAyABNgIUIANBlR42AhAgA0ENNgIMQQAhAgyoAgtBxAEhAgyOAgsgASAERgRAQd0BIQIMpwILAkACQAJAAkAgAS0AAEEKaw4XAgMDAAMDAwMDAwMDAwMDAwMDAwMDAwEDCyABQQFqDAULIAFBAWohAUHDASECDI8CCyABQQFqIQEgA0Evai0AAEEBcQ0IIANBADYCHCADIAE2AhQgA0GNCzYCECADQQ02AgxBACECDKcCCyADQQA2AhwgAyABNgIUIANBjQs2AhAgA0ENNgIMQQAhAgymAgsgASAERwRAIANBDzYCCCADIAE2AgRBASECDI0CC0HcASECDKUCCwJAAkADQAJAIAEtAABBCmsOBAIAAAMACyAEIAFBAWoiAUcNAAtB2wEhAgymAgsgAygCBCEAIANBADYCBCADIAAgARAtIgBFBEAgAUEBaiEBDAQLIANB2gE2AhwgAyAANgIMIAMgAUEBajYCFEEAIQIMpQILIAMoAgQhACADQQA2AgQgAyAAIAEQLSIADQEgAUEBagshAUHBASECDIoCCyADQdkBNgIcIAMgADYCDCADIAFBAWo2AhRBACECDKICC0HCASECDIgCCyADQS9qLQAAQQFxDQEgA0EANgIcIAMgATYCFCADQeQcNgIQIANBGTYCDEEAIQIMoAILIAEgBEYEQEHZASEC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IAFBAWohAUGeASECDOQBCyABQQFqIQFBnwEhAgzjAQtBtwEhAiABIARGDfsBIAMoAgAiACAEIAFraiEFIAEgAGtBA2ohBgJAA0AgAS0AACAAQezUAGotAABHDSggAEEDRg0BIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADPwBCyADQQA2AgAgBkEBaiEBQQ0MKAtBtgEhAiABIARGDfoBIAMoAgAiACAEIAFraiEFIAEgAGtBAmohBgJAA0AgAS0AACAAQaHVAGotAABHDScgAEECRg0BIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADPsBCyADQQA2AgAgBkEBaiEBQQwMJwtBtQEhAiABIARGDfkBIAMoAgAiACAEIAFraiEFIAEgAGtBAWohBgJAA0AgAS0AACAAQerUAGotAABHDSYgAEEBRg0BIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADPoBCyADQQA2AgAgBkEBaiEBQQMMJgtBtAEhAiABIARGDfgBIAMoAgAiACAEIAFraiEFIAEgAGtBAWohBgJAA0AgAS0AACAAQejUAGotAABHDSUgAEEBRg0BIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADPkBCyADQQA2AgAgBkEBaiEBQSYMJQsgASAERgRAQbMBIQIM+AELAkACQCABLQAAQdQAaw4CAAEnCyABQQFqIQFBmQEhAgzfAQsgAUEBaiEBQZoBIQIM3gELQbIBIQIgASAERg32ASADKAIAIgAgBCABa2ohBSABIABrQQFqIQYCQANAIAEtAAAgAEHm1ABqLQAARw0jIABBAUYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAz3AQsgA0EANgIAIAZBAWohAUEnDCMLQbEBIQIgASAERg31ASADKAIAIgAgBCABa2ohBSABIABrQQFqIQYCQANAIAEtAAAgAEHk1ABqLQAARw0iIABBAUYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAz2AQsgA0EANgIAIAZBAWohAUEcDCILQbABIQIgASAERg30ASADKAIAIgAgBCABa2ohBSABIABrQQVqIQYCQANAIAEtAAAgAEHe1ABqLQAARw0hIABBBUYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAz1AQsgA0EANgIAIAZBAWohAUEGDCELQa8BIQIgASAERg3zASADKAIAIgAgBCABa2ohBSABIABrQQRqIQYCQANAIAEtAAAgAEHZ1ABqLQAARw0gIABBBEYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAz0AQsgA0EANgIAIAZBAWohAUEZDCALIAEgBEYEQEGuASECDPMBCwJAAkACQAJAIAEtAABBLWsOIwAkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJAEkJCQkJAIkJCQDJAsgAUEBaiEBQY4BIQIM3AELIAFBAWohAUGPASECDNsBCyABQQFqIQFBlAEhAgzaAQsgAUEBaiEBQZUBIQIM2QELQa0BIQIgASAERg3xASADKAIAIgAgBCABa2ohBSABIABrQQFqIQYCQANAIAEtAAAgAEHX1ABqLQAARw0eIABBAUYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAzyAQsgA0EANgIAIAZBAWohAUELDB4LIAEgBEYEQEGsASECDPEBCwJAAkAgAS0AAEHBAGsOAwAgASALIAFBAWohAUGQASECDNgBCyABQQFqIQFBkwEhAgzXAQsgASAERgRAQasBIQIM8AELAkACQCABLQAAQcEAaw4PAB8fHx8fHx8fHx8fHx8BHwsgAUEBaiEBQZEBIQIM1wELIAFBAWohAUGSASECDNYBCyABIARGBEBBqgEhAgzvAQsgAS0AAEHMAEcNHCABQQFqIQFBCgwbC0GpASECIAEgBEYN7QEgAygCACIAIAQgAWtqIQUgASAAa0EFaiEGAkADQCABLQAAIABB0dQAai0AAEcNGiAAQQVGDQEgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAU2AgAM7gELIANBADYCACAGQQFqIQFBHgwaC0GoASECIAEgBEYN7AEgAygCACIAIAQgAWtqIQUgASAAa0EGaiEGAkADQCABLQAAIABBytQAai0AAEcNGSAAQQZGDQEgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAU2AgAM7QELIANBADYCACAGQQFqIQFBFQwZC0GnASECIAEgBEYN6wEgAygCACIAIAQgAWtqIQUgASAAa0ECaiEGAkADQCABLQAAIABBx9QAai0AAEcNGCAAQQJGDQEgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAU2AgAM7AELIANBADYCACAGQQFqIQFBFwwYC0GmASECIAEgBEYN6gEgAygCACIAIAQgAWtqIQUgASAAa0EFaiEGAkADQCABLQAAIABBwdQAai0AAEcNFyAAQQVGDQEgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAU2AgAM6wELIANBADYCACAGQQFqIQFBGAwXCyABIARGBEBBpQEhAgzqAQsCQAJAIAEtAABByQBrDgcAGRkZGRkBGQsgAUEBaiEBQYsBIQIM0QELIAFBAWohAUGMASECDNABC0GkASECIAEgBEYN6AEgAygCACIAIAQgAWtqIQUgASAAa0EBaiEGAkADQCABLQAAIABBptUAai0AAEcNFSAAQQFGDQEgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAU2AgAM6QELIANBADYCACAGQQFqIQFBCQwVC0GjASECIAEgBEYN5wEgAygCACIAIAQgAWtqIQUgASAAa0EBaiEGAkADQCABLQAAIABBpNUAai0AAEcNFCAAQQFGDQEgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAU2AgAM6AELIANBADYCACAGQQFqIQFBHwwUC0GiASECIAEgBEYN5gEgAygCACIAIAQgAWtqIQUgASAAa0ECaiEGAkADQCABLQAAIABBvtQAai0AAEcNEyAAQQJGDQEgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAU2AgAM5wELIANBADYCACAGQQFqIQFBAgwTC0GhASECIAEgBEYN5QEgAygCACIAIAQgAWtqIQUgASAAa0EBaiEGA0AgAS0AACAAQbzUAGotAABHDREgAEEBRg0CIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADOUBCyABIARGBEBBoAEhAgzlAQtBASABLQAAQd8ARw0RGiABQQFqIQFBhwEhAgzLAQsgA0EANgIAIAZBAWohAUGIASECDMoBC0GfASECIAEgBEYN4gEgAygCACIAIAQgAWtqIQUgASAAa0EIaiEGAkADQCABLQAAIABBhNUAai0AAEcNDyAAQQhGDQEgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAU2AgAM4wELIANBADYCACAGQQFqIQFBKQwPC0GeASECIAEgBEYN4QEgAygCACIAIAQgAWtqIQUgASAAa0EDaiEGAkADQCABLQAAIABBuNQAai0AAEcNDiAAQQNGDQEgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAU2AgAM4gELIANBADYCACAGQQFqIQFBLQwOCyABIARGBEBBnQEhAgzhAQsgAS0AAEHFAEcNDiABQQFqIQFBhAEhAgzHAQsgASAERgRAQZwBIQIM4AELAkACQCABLQAAQcwAaw4IAA8PDw8PDwEPCyABQQFqIQFBggEhAgzHAQsgAUEBaiEBQYMBIQIMxgELQZsBIQIgASAERg3eASADKAIAIgAgBCABa2ohBSABIABrQQRqIQYCQANAIAEtAAAgAEGz1ABqLQAARw0LIABBBEYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAzfAQsgA0EANgIAIAZBAWohAUEjDAsLQZoBIQIgASAERg3dASADKAIAIgAgBCABa2ohBSABIABrQQJqIQYCQANAIAEtAAAgAEGw1ABqLQAARw0KIABBAkYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAzeAQsgA0EANgIAIAZBAWohAUEADAoLIAEgBEYEQEGZASECDN0BCwJAAkAgAS0AAEHIAGsOCAAMDAwMDAwBDAsgAUEBaiEBQf0AIQIMxAELIAFBAWohAUGAASECDMMBCyABIARGBEBBmAEhAgzcAQsCQAJAIAEtAABBzgBrDgMACwELCyABQQFqIQFB/gAhAgzDAQsgAUEBaiEBQf8AIQIMwgELIAEgBEYEQEGXASECDNsBCyABLQAAQdkARw0IIAFBAWohAUEIDAcLQZYBIQIgASAERg3ZASADKAIAIgAgBCABa2ohBSABIABrQQNqIQYCQANAIAEtAAAgAEGs1ABqLQAARw0GIABBA0YNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAzaAQsgA0EANgIAIAZBAWohAUEFDAYLQZUBIQIgASAERg3YASADKAIAIgAgBCABa2ohBSABIABrQQVqIQYCQANAIAEtAAAgAEGm1ABqLQAARw0FIABBBUYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAzZAQsgA0EANgIAIAZBAWohAUEWDAULQZQBIQIgASAERg3XASADKAIAIgAgBCABa2ohBSABIABrQQJqIQYCQANAIAEtAAAgAEGh1QBqLQAARw0EIABBAkYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAzYAQsgA0EANgIAIAZBAWohAUEQDAQLIAEgBEYEQEGTASECDNcBCwJAAkAgAS0AAEHDAGsODAAGBgYGBgYGBgYGAQYLIAFBAWohAUH5ACECDL4BCyABQQFqIQFB+gAhAgy9AQtBkgEhAiABIARGDdUBIAMoAgAiACAEIAFraiEFIAEgAGtBBWohBgJAA0AgAS0AACAAQaDUAGotAABHDQIgAEEFRg0BIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADNYBCyADQQA2AgAgBkEBaiEBQSQMAgsgA0EANgIADAILIAEgBEYEQEGRASECDNQBCyABLQAAQcwARw0BIAFBAWohAUETCzoAKSADKAIEIQAgA0EANgIEIAMgACABEC4iAA0CDAELQQAhAiADQQA2AhwgAyABNgIUIANB/h82AhAgA0EGNgIMDNEBC0H4ACECDLcBCyADQZABNgIcIAMgATYCFCADIAA2AgxBACECDM8BC0EAIQACQCADKAI4IgJFDQAgAigCQCICRQ0AIAMgAhEAACEACyAARQ0AIABBFUYNASADQQA2AhwgAyABNgIUIANBgg82AhAgA0EgNgIMQQAhAgzOAQtB9wAhAgy0AQsgA0GPATYCHCADIAE2AhQgA0HsGzYCECADQRU2AgxBACECDMwBCyABIARGBEBBjwEhAgzMAQsCQCABLQAAQSBGBEAgAUEBaiEBDAELIANBADYCHCADIAE2AhQgA0GbHzYCECADQQY2AgxBACECDMwBC0ECIQIMsgELA0AgAS0AAEEgRw0CIAQgAUEBaiIBRw0AC0GOASECDMoBCyABIARGBEBBjQEhAgzKAQsCQCABLQAAQQlrDgRKAABKAAtB9QAhAgywAQsgAy0AKUEFRgRAQfYAIQIMsAELQfQAIQIMrwELIAEgBEYEQEGMASECDMgBCyADQRA2AgggAyABNgIEDAoLIAEgBEYEQEGLASECDMcBCwJAIAEtAABBCWsOBEcAAEcAC0HzACECDK0BCyABIARHBEAgA0EQNgIIIAMgATYCBEHxACECDK0BC0GKASECDMUBCwJAIAEgBEcEQANAIAEtAABBoNAAai0AACIAQQNHBEACQCAAQQFrDgJJAAQLQfAAIQIMrwELIAQgAUEBaiIBRw0AC0GIASEC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BCABQQFqIgFHDQALQfsAIQIMpwELIAMoAgQhACADQQA2AgQgAyAAIAEQLCIARQ0DIANB2wA2AhwgAyABNgIUIAMgADYCDEEAIQIMpgELIAMoAgQhACADQQA2AgQgAyAAIAEQLCIARQ0DIANB3QA2AhwgAyABNgIUIAMgADYCDEEAIQIMpQELIAMoAgQhACADQQA2AgQgAyAAIAEQLCIARQ0DIANB+gA2AhwgAyABNgIUIAMgADYCDEEAIQIMpAELIANBADYCHCADIAE2AhQgA0G8CjYCECADQQc2AgxBACECDKMBC0HPACECDIkBC0HRACECDIgBC0HnACECDIcBCyABIARGBEBB+gAhAgygAQsCQCABLQAAQQlrDgQgAAAgAAsgAUEBaiEBQeYAIQIMhgELIAEgBEYEQEH5ACECDJ8BCwJAIAEtAABBCWsOBB8AAB8AC0EAIQACQCADKAI4IgJFDQAgAigCOCICRQ0AIAMgAhEAACEACyAARQRAQeIBIQIMhgELIABBFUcEQCADQQA2AhwgAyABNgIUIANByQ02AhAgA0EaNgIMQQAhAgyfAQsgA0H4ADYCHCADIAE2AhQgA0HqGjYCECADQRU2AgxBACECDJ4BCyABIARHBEAgA0ENNgIIIAMgATYCBEHkACECDIUBC0H3ACECDJ0BCyABIARGBEBB9gAhAgydAQsCQAJAAkAgAS0AAEHIAGsOCwABCwsLCwsLCwsCCwsgAUEBaiEBQd0AIQIMhQELIAFBAWohAUHgACECDIQBCyABQQFqIQFB4wAhAgyDAQtB9QAhAiABIARGDZsBIAMoAgAiACAEIAFraiEFIAEgAGtBAmohBgJAA0AgAS0AACAAQbXVAGotAABHDQggAEECRg0BIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADJwBCyADKAIEIQAgA0IANwMAIAMgACAGQQFqIgEQKyIABEAgA0H0ADYCHCADIAE2AhQgAyAANgIMQQAhAgycAQtB4gAhAgyCAQtBACEAAkAgAygCOCICRQ0AIAIoAjQiAkUNACADIAIRAAAhAAsCQCAABEAgAEEVRg0BIANBADYCHCADIAE2AhQgA0HqDTYCECADQSY2AgxBACECDJwBC0HhACECDIIBCyADQfMANgIcIAMgATYCFCADQYAbNgIQIANBFTYCDEEAIQIMmgELIAMtACkiAEEja0ELSQ0JAkAgAEEGSw0AQQEgAHRBygBxRQ0ADAoLQQAhAiADQQA2AhwgAyABNgIUIANB7Qk2AhAgA0EINgIMDJkBC0HyACECIAEgBEYNmAEgAygCACIAIAQgAWtqIQUgASAAa0EBaiEGAkADQCABLQAAIABBs9UAai0AAEcNBSAAQQFGDQEgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAU2AgAMmQELIAMoAgQhACADQgA3AwAgAyAAIAZBAWoiARArIgAEQCADQfEANgIcIAMgATYCFCADIAA2AgxBACECDJkBC0HfACECDH8LQQAhAAJAIAMoAjgiAkUNACACKAI0IgJFDQAgAyACEQAAIQALAkAgAARAIABBFUYNASADQQA2AhwgAyABNgIUIANB6g02AhAgA0EmNgIMQQAhAgyZAQtB3gAhAgx/CyADQfAANgIcIAMgATYCFCADQYAbNgIQIANBFTYCDEEAIQIMlwELIAMtAClBIUYNBiADQQA2AhwgAyABNgIUIANBkQo2AhAgA0EINgIMQQAhAgyWAQtB7wAhAiABIARGDZUBIAMoAgAiACAEIAFraiEFIAEgAGtBAmohBgJAA0AgAS0AACAAQbDVAGotAABHDQIgAEECRg0BIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADJYBCyADKAIEIQAgA0IANwMAIAMgACAGQQFqIgEQKyIARQ0CIANB7QA2AhwgAyABNgIUIAMgADYCDEEAIQIMlQELIANBADYCAAsgAygCBCEAIANBADYCBCADIAAgARArIgBFDYABIANB7gA2AhwgAyABNgIUIAMgADYCDEEAIQIMkwELQdwAIQIMeQtBACEAAkAgAygCOCICRQ0AIAIoAjQiAkUNACADIAIRAAAhAAsCQCAABEAgAEEVRg0BIANBADYCHCADIAE2AhQgA0HqDTYCECADQSY2AgxBACECDJMBC0HbACECDHkLIANB7AA2AhwgAyABNgIUIANBgBs2AhAgA0EVNgIMQQAhAgyRAQsgAy0AKSIAQSNJDQAgAEEuRg0AIANBADYCHCADIAE2AhQgA0HJCTYCECADQQg2AgxBACECDJABC0HaACECDHYLIAEgBEYEQEHrACECDI8BCwJAIAEtAABBL0YEQCABQQFqIQEMAQsgA0EANgIcIAMgATYCFCADQbI4NgIQIANBCDYCDEEAIQIMjwELQdkAIQIMdQsgASAERwRAIANBDjYCCCADIAE2AgRB2AAhAgx1C0HqACECDI0BCyABIARGBEBB6QAhAgyNAQsgAS0AAEEwayIAQf8BcUEKSQRAIAMgADoAKiABQQFqIQFB1wAhAgx0CyADKAIEIQAgA0EANgIEIAMgACABEC8iAEUNeiADQegANgIcIAMgATYCFCADIAA2AgxBACECDIwBCyABIARGBEBB5wAhAgyMAQsCQCABLQAAQS5GBEAgAUEBaiEBDAELIAMoAgQhACADQQA2AgQgAyAAIAEQLyIARQ17IANB5gA2AhwgAyABNgIUIAMgADYCDEEAIQIMjAELQdYAIQIMcgsgASAERgRAQeUAIQIMiwELQQAhAEEBIQVBASEHQQAhAgJAAkACQAJAAkACfwJAAkACQAJAAkACQAJAIAEtAABBMGsOCgoJAAECAwQFBggLC0ECDAYLQQMMBQtBBAwEC0EFDAMLQQYMAgtBBwwBC0EICyECQQAhBUEAIQcMAgtBCSECQQEhAEEAIQVBACEHDAELQQAhBUEBIQILIAMgAjoAKyABQQFqIQECQAJAIAMtAC5BEHENAAJAAkACQCADLQAqDgMBAAIECyAHRQ0DDAILIAANAQwCCyAFRQ0BCyADKAIEIQAgA0EANgIEIAMgACABEC8iAEUNAiADQeIANgIcIAMgATYCFCADIAA2AgxBACECDI0BCyADKAIEIQAgA0EANgIEIAMgACABEC8iAEUNfSADQeMANgIcIAMgATYCFCADIAA2AgxBACECDIwBCyADKAIEIQAgA0EANgIEIAMgACABEC8iAEUNeyADQeQANgIcIAMgATYCFCADIAA2AgwMiwELQdQAIQIMcQsgAy0AKUEiRg2GAUHTACECDHALQQAhAAJAIAMoAjgiAkUNACACKAJEIgJFDQAgAyACEQAAIQALIABFBEBB1QAhAgxwCyAAQRVHBEAgA0EANgIcIAMgATYCFCADQaQNNgIQIANBITYCDEEAIQIMiQELIANB4QA2AhwgAyABNgIUIANB0Bo2AhAgA0EVNgIMQQAhAgyIAQsgASAERgRAQeAAIQIMiAELAkACQAJAAkACQCABLQAAQQprDgQBBAQABAsgAUEBaiEBDAELIAFBAWohASADQS9qLQAAQQFxRQ0BC0HSACECDHALIANBADYCHCADIAE2AhQgA0G2ETYCECADQQk2AgxBACECDIgBCyADQQA2AhwgAyABNgIUIANBthE2AhAgA0EJNgIMQQAhAgyHAQsgASAERgRAQd8AIQIMhwELIAEtAABBCkYEQCABQQFqIQEMCQsgAy0ALkHAAHENCCADQQA2AhwgAyABNgIUIANBthE2AhAgA0ECNgIMQQAhAgyGAQsgASAERgRAQd0AIQIMhgELIAEtAAAiAkENRgRAIAFBAWohAUHQACECDG0LIAEhACACQQlrDgQFAQEFAQsgBCABIgBGBEBB3AAhAgyFAQsgAC0AAEEKRw0AIABBAWoMAgtBACECIANBADYCHCADIAA2AhQgA0HKLTYCECADQQc2AgwMgwELIAEgBEYEQEHbACECDIMBCwJAIAEtAABBCWsOBAMAAAMACyABQQFqCyEBQc4AIQIMaAsgASAERgRAQdoAIQIMgQELIAEtAABBCWsOBAABAQABC0EAIQIgA0EANgIcIANBmhI2AhAgA0EHNgIMIAMgAUEBajYCFAx/CyADQYASOwEqQQAhAAJAIAMoAjgiAkUNACACKAI4IgJFDQAgAyACEQAAIQALIABFDQAgAEEVRw0BIANB2QA2AhwgAyABNgIUIANB6ho2AhAgA0EVNgIMQQAhAgx+C0HNACECDGQLIANBADYCHCADIAE2AhQgA0HJDTYCECADQRo2AgxBACECDHwLIAEgBEYEQEHZACECDHwLIAEtAABBIEcNPSABQQFqIQEgAy0ALkEBcQ09IANBADYCHCADIAE2AhQgA0HCHDYCECADQR42AgxBACECDHsLIAEgBEYEQEHYACECDHsLAkACQAJAAkACQCABLQAAIgBBCmsOBAIDAwABCyABQQFqIQFBLCECDGULIABBOkcNASADQQA2AhwgAyABNgIUIANB5xE2AhAgA0EKNgIMQQAhAgx9CyABQQFqIQEgA0Evai0AAEEBcUUNcyADLQAyQYABcUUEQCADQTJqIQIgAxA1QQAhAAJAIAMoAjgiBkUNACAGKAIoIgZFDQAgAyAGEQAAIQALAkACQCAADhZNTEsBAQEBAQEBAQEBAQEBAQEBAQEAAQsgA0EpNgIcIAMgATYCFCADQawZNgIQIANBFTYCDEEAIQIMfgsg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c3BvbnNlIHN0YXR1cwBFeHBlY3RlZCBMRiBhZnRlciBoZWFkZXJzAEludmFsaWQgY2hhcmFjdGVyIGluIGNodW5rIGV4dGVuc2lvbnMAVXNlciBjYWxsYmFjayBlcnJvcgBgb25fcmVzZXRgIGNhbGxiYWNrIGVycm9yAGBvbl9jaHVua19oZWFkZXJgIGNhbGxiYWNrIGVycm9yAGBvbl9tZXNzYWdlX2JlZ2luYCBjYWxsYmFjayBlcnJvcgBgb25fY2h1bmtfZXh0ZW5zaW9uX3ZhbHVlYCBjYWxsYmFjayBlcnJvcgBgb25fc3RhdHVzX2NvbXBsZXRlYCBjYWxsYmFjayBlcnJvcgBgb25fdmVyc2lvbl9jb21wbGV0ZWAgY2FsbGJhY2sgZXJyb3IAYG9uX3VybF9jb21wbGV0ZWAgY2FsbGJhY2sgZXJyb3IAYG9uX3Byb3RvY29sX2NvbXBsZXRlYCBjYWxsYmFjayBlcnJvcgBgb25fY2h1bmtfY29tcGxldGVgIGNhbGxiYWNrIGVycm9yAGBvbl9oZWFkZXJfdmFsdWVfY29tcGxldGVgIGNhbGxiYWNrIGVycm9yAGBvbl9tZXNzYWdlX2NvbXBsZXRlYCBjYWxsYmFjayBlcnJvcgBgb25fbWV0aG9kX2NvbXBsZXRlYCBjYWxsYmFjayBlcnJvcgBgb25faGVhZGVyX2ZpZWxkX2NvbXBsZXRlYCBjYWxsYmFjayBlcnJvcgBgb25fY2h1bmtfZXh0ZW5zaW9uX25hbWVgIGNhbGxiYWNrIGVycm9yAFVuZXhwZWN0ZWQgY2hhciBpbiB1cmwgc2VydmVyAEludmFsaWQgaGVhZGVyIHZhbHVlIGNoYXIASW52YWxpZCBoZWFkZXIgZmllbGQgY2hhcgBTcGFuIGNhbGxiYWNrIGVycm9yIGluIG9uX3ZlcnNpb24ASW52YWxpZCBtaW5vciB2ZXJzaW9uAEludmFsaWQgbWFqb3IgdmVyc2lvbgBFeHBlY3RlZCBzcGFjZSBhZnRlciB2ZXJzaW9uAEV4cGVjdGVkIENSTEYgYWZ0ZXIgdmVyc2lvbgBJbnZhbGlkIEhUVFAgdmVyc2lvbgBJbnZhbGlkIGhlYWRlciB0b2tlbgBTcGFuIGNhbGxiYWNrIGVycm9yIGluIG9uX3VybABJbnZhbGlkIGNoYXJhY3RlcnMgaW4gdXJsAFVuZXhwZWN0ZWQgc3RhcnQgY2hhciBpbiB1cmwARG91YmxlIEAgaW4gdXJsAFNwYW4gY2FsbGJhY2sgZXJyb3IgaW4gb25fcHJvdG9jb2wARW1wdHkgQ29udGVudC1MZW5ndGgASW52YWxpZCBjaGFyYWN0ZXIgaW4gQ29udGVudC1MZW5ndGgAVHJhbnNmZXItRW5jb2RpbmcgY2FuJ3QgYmUgcHJlc2VudCB3aXRoIENvbnRlbnQtTGVuZ3RoAER1cGxpY2F0ZSBDb250ZW50LUxlbmd0aABJbnZhbGlkIGNoYXIgaW4gdXJsIHBhdGgAQ29udGVudC1MZW5ndGggY2FuJ3QgYmUgcHJlc2VudCB3aXRoIFRyYW5zZmVyLUVuY29kaW5nAE1pc3NpbmcgZXhwZWN0ZWQgQ1IgYWZ0ZXIgY2h1bmsgc2l6ZQBFeHBlY3RlZCBMRiBhZnRlciBjaHVuayBzaXplAEludmFsaWQgY2hhcmFjdGVyIGluIGNodW5rIHNpemUAU3BhbiBjYWxsYmFjayBlcnJvciBpbiBvbl9oZWFkZXJfdmFsdWUAU3BhbiBjYWxsYmFjayBlcnJvciBpbiBvbl9jaHVua19leHRlbnNpb25fdmFsdWUASW52YWxpZCBjaGFyYWN0ZXIgaW4gY2h1bmsgZXh0ZW5zaW9ucyB2YWx1ZQBVbmV4cGVjdGVkIHdoaXRlc3BhY2UgYWZ0ZXIgaGVhZGVyIHZhbHVlAE1pc3NpbmcgZXhwZWN0ZWQgQ1IgYWZ0ZXIgaGVhZGVyIHZhbHVlAE1pc3NpbmcgZXhwZWN0ZWQgTEYgYWZ0ZXIgaGVhZGVyIHZhbHVlAEludmFsaWQgYFRyYW5zZmVyLUVuY29kaW5nYCBoZWFkZXIgdmFsdWUATWlzc2luZyBleHBlY3RlZCBDUiBhZnRlciBjaHVuayBleHRlbnNpb24gdmFsdWUASW52YWxpZCBjaGFyYWN0ZXIgaW4gY2h1bmsgZXh0ZW5zaW9ucyBxdW90ZSB2YWx1ZQBJbnZhbGlkIHF1b3RlZC1wYWlyIGluIGNodW5rIGV4dGVuc2lvbnMgcXVvdGVkIHZhbHVlAEludmFsaWQgY2hhcmFjdGVyIGluIGNodW5rIGV4dGVuc2lvbnMgcXVvdGVkIHZhbHVlAFBhdXNlZCBieSBvbl9oZWFkZXJzX2NvbXBsZXRlAEludmFsaWQgRU9GIHN0YXRlAG9uX3Jlc2V0IHBhdXNlAG9uX2NodW5rX2hlYWRlciBwYXVzZQBvbl9tZXNzYWdlX2JlZ2luIHBhdXNlAG9uX2NodW5rX2V4dGVuc2lvbl92YWx1ZSBwYXVzZQBvbl9zdGF0dXNfY29tcGxldGUgcGF1c2UAb25fdmVyc2lvbl9jb21wbGV0ZSBwYXVzZQBvbl91cmxfY29tcGxldGUgcGF1c2UAb25fcHJvdG9jb2xfY29tcGxldGUgcGF1c2UAb25fY2h1bmtfY29tcGxldGUgcGF1c2UAb25faGVhZGVyX3ZhbHVlX2NvbXBsZXRlIHBhdXNlAG9uX21lc3NhZ2VfY29tcGxldGUgcGF1c2UAb25fbWV0aG9kX2NvbXBsZXRlIHBhdXNlAG9uX2hlYWRlcl9maWVsZF9jb21wbGV0ZSBwYXVzZQBvbl9jaHVua19leHRlbnNpb25fbmFtZSBwYXVzZQBVbmV4cGVjdGVkIHNwYWNlIGFmdGVyIHN0YXJ0IGxpbmUATWlzc2luZyBleHBlY3RlZCBDUiBhZnRlciByZXNwb25zZSBsaW5lAFNwYW4gY2FsbGJhY2sgZXJyb3IgaW4gb25fY2h1bmtfZXh0ZW5zaW9uX25hbWUASW52YWxpZCBjaGFyYWN0ZXIgaW4gY2h1bmsgZXh0ZW5zaW9ucyBuYW1lAE1pc3NpbmcgZXhwZWN0ZWQgQ1IgYWZ0ZXIgY2h1bmsgZXh0ZW5zaW9uIG5hbWUASW52YWxpZCBzdGF0dXMgY29kZQBQYXVzZSBvbiBDT05ORUNUL1VwZ3JhZGUAUGF1c2Ugb24gUFJJL1VwZ3JhZGUARXhwZWN0ZWQgSFRUUC8yIENvbm5lY3Rpb24gUHJlZmFjZQBTcGFuIGNhbGxiYWNrIGVycm9yIGluIG9uX21ldGhvZABFeHBlY3RlZCBzcGFjZSBhZnRlciBtZXRob2QAU3BhbiBjYWxsYmFjayBlcnJvciBpbiBvbl9oZWFkZXJfZmllbGQAUGF1c2VkAEludmFsaWQgd29yZCBlbmNvdW50ZXJlZABJbnZhbGlkIG1ldGhvZCBlbmNvdW50ZXJlZABNaXNzaW5nIGV4cGVjdGVkIENSIGFmdGVyIGNodW5rIGRhdGEARXhwZWN0ZWQgTEYgYWZ0ZXIgY2h1bmsgZGF0YQBVbmV4cGVjdGVkIGNoYXIgaW4gdXJsIHNjaGVtYQBSZXF1ZXN0IGhhcyBpbnZhbGlkIGBUcmFuc2Zlci1FbmNvZGluZ2AARGF0YSBhZnRlciBgQ29ubmVjdGlvbjogY2xvc2VgAFNXSVRDSF9QUk9YWQBVU0VfUFJPWFkATUtBQ1RJVklUWQBVTlBST0NFU1NBQkxFX0VOVElUWQBRVUVSWQBDT1BZAE1PVkVEX1BFUk1BTkVOVExZAFRPT19FQVJMWQBOT1RJRlkARkFJTEVEX0RFUEVOREVOQ1kAQkFEX0dBVEVXQVkAUExBWQBQVVQAQ0hFQ0tPVVQAR0FURVdBWV9USU1FT1VUAFJFUVVFU1RfVElNRU9VVABORVRXT1JLX0NPTk5FQ1RfVElNRU9VVABDT05ORUNUSU9OX1RJTUVPVVQATE9HSU5fVElNRU9VVABORVRXT1JLX1JFQURfVElNRU9VVABQT1NUAE1JU0RJUkVDVEVEX1JFUVVFU1QAQ0xJRU5UX0NMT1NFRF9SRVFVRVNUAENMSUVOVF9DTE9TRURfTE9BRF9CQUxBTkNFRF9SRVFVRVNUAEJBRF9SRVFVRVNUAEhUVFBfUkVRVUVTVF9TRU5UX1RPX0hUVFBTX1BPUlQAUkVQT1JUAElNX0FfVEVBUE9UAFJFU0VUX0NPTlRFTlQATk9fQ09OVEVOVABQQVJUSUFMX0NPTlRFTlQASFBFX0lOVkFMSURfQ09OU1RBTlQASFBFX0NCX1JFU0VUAEdFVABIUEVfU1RSSUNUAENPTkZMSUNUAFRFTVBPUkFSWV9SRURJUkVDVABQRVJNQU5FTlRfUkVESVJFQ1QAQ09OTkVDVABNVUxUSV9TVEFUVVMASFBFX0lOVkFMSURfU1RBVFVTAFRPT19NQU5ZX1JFUVVFU1RTAEVBUkxZX0hJTlRTAFVOQVZBSUxBQkxFX0ZPUl9MRUdBTF9SRUFTT05TAE9QVElPTlMAU1dJVENISU5HX1BST1RPQ09MUwBWQVJJQU5UX0FMU09fTkVHT1RJQVRFUwBNVUxUSVBMRV9DSE9JQ0VTAElOVEVSTkFMX1NFUlZFUl9FUlJPUgBXRUJfU0VSVkVSX1VOS05PV05fRVJST1IAUkFJTEdVTl9FUlJPUgBJREVOVElUWV9QUk9WSURFUl9BVVRIRU5USUNBVElPTl9FUlJPUgBTU0xfQ0VSVElGSUNBVEVfRVJST1IASU5WQUxJRF9YX0ZPUldBUkRFRF9GT1IAU0VUX1BBUkFNRVRFUgBHRVRfUEFSQU1FVEVSAEhQRV9VU0VSAFNFRV9PVEhFUgBIUEVfQ0JfQ0hVTktfSEVBREVSAEV4cGVjdGVkIExGIGFmdGVyIENSAE1LQ0FMRU5EQVIAU0VUVVAAV0VCX1NFUlZFUl9JU19ET1dOAFRFQVJET1dOAEhQRV9DTE9TRURfQ09OTkVDVElPTgBIRVVSSVNUSUNfRVhQSVJBVElPTgBESVNDT05ORUNURURfT1BFUkFUSU9OAE5PTl9BVVRIT1JJVEFUSVZFX0lORk9STUFUSU9OAEhQRV9JTlZBTElEX1ZFUlNJT04ASFBFX0NCX01FU1NBR0VfQkVHSU4AU0lURV9JU19GUk9aRU4ASFBFX0lOVkFMSURfSEVBREVSX1RPS0VOAElOVkFMSURfVE9LRU4ARk9SQklEREVOAEVOSEFOQ0VfWU9VUl9DQUxNAEhQRV9JTlZBTElEX1VSTABCTE9DS0VEX0JZX1BBUkVOVEFMX0NPTlRST0wATUtDT0wAQUNMAEhQRV9JTlRFUk5BTABSRVFVRVNUX0hFQURFUl9GSUVMRFNfVE9PX0xBUkdFX1VOT0ZGSUNJQUwASFBFX09LAFVOTElOSwBVTkxPQ0sAUFJJAFJFVFJZX1dJVEgASFBFX0lOVkFMSURfQ09OVEVOVF9MRU5HVEgASFBFX1VORVhQRUNURURfQ09OVEVOVF9MRU5HVEgARkxVU0gAUFJPUFBBVENIAE0tU0VBUkNIAFVSSV9UT09fTE9ORwBQUk9DRVNTSU5HAE1JU0NFTExBTkVPVVNfUEVSU0lTVEVOVF9XQVJOSU5HAE1JU0NFTExBTkVPVVNfV0FSTklORwBIUEVfSU5WQUxJRF9UUkFOU0ZFUl9FTkNPRElORwBFeHBlY3RlZCBDUkxGAEhQRV9JTlZBTElEX0NIVU5LX1NJWkUATU9WRQBDT05USU5VRQBIUEVfQ0JfU1RBVFVTX0NPTVBMRVRFAEhQRV9DQl9IRUFERVJTX0NPTVBMRVRFAEhQRV9DQl9WRVJTSU9OX0NPTVBMRVRFAEhQRV9DQl9VUkxfQ09NUExFVEUASFBFX0NCX1BST1RPQ09MX0NPTVBMRVRFAEhQRV9DQl9DSFVOS19DT01QTEVURQBIUEVfQ0JfSEVBREVSX1ZBTFVFX0NPTVBMRVRFAEhQRV9DQl9DSFVOS19FWFRFTlNJT05fVkFMVUVfQ09NUExFVEUASFBFX0NCX0NIVU5LX0VYVEVOU0lPTl9OQU1FX0NPTVBMRVRFAEhQRV9DQl9NRVNTQUdFX0NPTVBMRVRFAEhQRV9DQl9NRVRIT0RfQ09NUExFVEUASFBFX0NCX0hFQURFUl9GSUVMRF9DT01QTEVURQBERUxFVEUASFBFX0lOVkFMSURfRU9GX1NUQVRFAElOVkFMSURfU1NMX0NFUlRJRklDQVRFAFBBVVNFAE5PX1JFU1BPTlNFAFVOU1VQUE9SVEVEX01FRElBX1RZUEUAR09ORQBOT1RfQUNDRVBUQUJMRQBTRVJWSUNFX1VOQVZBSUxBQkxFAFJBTkdFX05PVF9TQVRJU0ZJQUJMRQBPUklHSU5fSVNfVU5SRUFDSEFCTEUAUkVTUE9OU0VfSVNfU1RBTEUAUFVSR0UATUVSR0UAUkVRVUVTVF9IRUFERVJfRklFTERTX1RPT19MQVJHRQBSRVFVRVNUX0hFQURFUl9UT09fTEFSR0UAUEFZTE9BRF9UT09fTEFSR0UASU5TVUZGSUNJRU5UX1NUT1JBR0UASFBFX1BBVVNFRF9VUEdSQURFAEhQRV9QQVVTRURfSDJfVVBHUkFERQBTT1VSQ0UAQU5OT1VOQ0UAVFJBQ0UASFBFX1VORVhQRUNURURfU1BBQ0UAREVTQ1JJQkUAVU5TVUJTQ1JJQkUAUkVDT1JEAEhQRV9JTlZBTElEX01FVEhPRABOT1RfRk9VTkQAUFJPUEZJTkQAVU5CSU5EAFJFQklORABVTkFVVEhPUklaRUQATUVUSE9EX05PVF9BTExPV0VEAEhUVFBfVkVSU0lPTl9OT1RfU1VQUE9SVEVEAEFMUkVBRFlfUkVQT1JURUQAQUNDRVBURUQATk9UX0lNUExFTUVOVEVEAExPT1BfREVURUNURUQASFBFX0NSX0VYUEVDVEVEAEhQRV9MRl9FWFBFQ1RFRABDUkVBVEVEAElNX1VTRUQASFBFX1BBVVNFRABUSU1FT1VUX09DQ1VSRUQAUEFZTUVOVF9SRVFVSVJFRABQUkVDT05ESVRJT05fUkVRVUlSRUQAUFJPWFlfQVVUSEVOVElDQVRJT05fUkVRVUlSRUQATkVUV09SS19BVVRIRU5USUNBVElPTl9SRVFVSVJFRABMRU5HVEhfUkVRVUlSRUQAU1NMX0NFUlRJRklDQVRFX1JFUVVJUkVEAFVQR1JBREVfUkVRVUlSRUQAUEFHRV9FWFBJUkVEAFBSRUNPTkRJVElPTl9GQUlMRUQARVhQRUNUQVRJT05fRkFJTEVEAFJFVkFMSURBVElPTl9GQUlMRUQAU1NMX0hBTkRTSEFLRV9GQUlMRUQATE9DS0VEAFRSQU5TRk9STUFUSU9OX0FQUExJRUQATk9UX01PRElGSUVEAE5PVF9FWFRFTkRFRABCQU5EV0lEVEhfTElNSVRfRVhDRUVERUQAU0lURV9JU19PVkVSTE9BREVEAEhFQUQARXhwZWN0ZWQgSFRUUC8sIFJUU1AvIG9yIElDRS8A5xUAAK8VAACkEgAAkhoAACYWAACeFAAA2xkAAHkVAAB+EgAA/hQAADYVAAALFgAA2BYAAPMSAABCGAAArBYAABIVAAAUFwAA7xcAAEgUAABxFwAAshoAAGsZAAB+GQAANRQAAIIaAABEFwAA/RYAAB4YAACHFwAAqhkAAJMSAAAHGAAALBcAAMoXAACkFwAA5xUAAOcVAABYFwAAOxgAAKASAAAtHAAAwxEAAEgRAADeEgAAQhMAAKQZAAD9EAAA9xUAAKUVAADvFgAA+BkAAEoWAABWFgAA9RUAAAoaAAAIGgAAARoAAKsVAABCEgAA1xAAAEwRAAAFGQAAVBYAAB4RAADKGQAAyBkAAE4WAAD/GAAAcRQAAPAVAADuFQAAlBkAAPwVAAC/GQAAmxkAAHwUAABDEQAAcBgAAJUUAAAnFAAAGRQAANUSAADUGQAARBYAAPcQAEG5OwsBAQBB0DsL4AEBAQIBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEDAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQBBuj0LBAEAAAIAQdE9C14DBAMDAwMDAAADAwADAwADAwMDAwMDAwMDAAUAAAAAAAMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAAAAAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMAAwADAEG6PwsEAQAAAgBB0T8LXgMAAwMDAwMAAAMDAAMDAAMDAwMDAwMDAwMABAAFAAAAAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMAAAADAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwADAAMAQbDBAAsNbG9zZWVlcC1hbGl2ZQBBycEACwEBAEHgwQAL4AEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQABAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQBBycMACwEBAEHgwwAL5wEBAQEBAQEBAQEBAQECAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQABAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAWNodW5rZWQAQfHFAAteAQABAQEBAQAAAQEAAQEAAQEBAQEBAQEBAQAAAAAAAAABAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQAAAAEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAAEAAQBB0McACyFlY3Rpb25lbnQtbGVuZ3Rob25yb3h5LWNvbm5lY3Rpb24AQYDIAAsgcmFuc2Zlci1lbmNvZGluZ3BncmFkZQ0KDQpTTQ0KDQoAQanIAAsFAQIAAQMAQcDIAAtfBAUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUAQanKAAsFAQIAAQMAQcDKAAtfBAUFBgUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUAQanMAAsEAQAAAQBBwcwAC14CAgACAgICAgICAgICAgICAgICAgICAgICAgICAgIAAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAEGpzgALBQECAAEDAEHAzgALXwQFAAAFBQUFBQUFBQUFBQYFBQUFBQUFBQUFBQUABQAHCAUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQAFAAUABQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUAAAAFAEGp0AALBQEBAAEBAEHA0AALAQEAQdrQAAtBAgAAAAAAAAMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAAAAAAAAAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMAQanSAAsFAQEAAQEAQcDSAAsBAQBBytIACwYCAAAAAAIAQeHSAAs6AwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMAAAAAAAADAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwBBoNQAC50BTk9VTkNFRUNLT1VUTkVDVEVURUNSSUJFTFVTSEVURUFEU0VBUkNIUkdFQ1RJVklUWUxFTkRBUlZFT1RJRllQVElPTlNDSFNFQVlTVEFUQ0hHRVVFUllPUkRJUkVDVE9SVFJDSFBBUkFNRVRFUlVSQ0VCU0NSSUJFQVJET1dOQUNFSU5ETktDS1VCU0NSSUJFVFRQQ0VUU1BBRFRQLw==";
	let wasmBuffer$1;
	Object.defineProperty(module, "exports", { get: () => {
		return wasmBuffer$1 ? wasmBuffer$1 : wasmBuffer$1 = Buffer$2.from(wasmBase64$1, "base64");
	} });
}) });

//#endregion
//#region node_modules/undici/lib/llhttp/llhttp_simd-wasm.js
var require_llhttp_simd_wasm = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/llhttp/llhttp_simd-wasm.js": ((exports, module) => {
	const { Buffer: Buffer$1 } = __require("node:buffer");
	const wasmBase64 = "AGFzbQEAAAABJwdgAX8Bf2ADf39/AX9gAn9/AGABfwBgBH9/f38Bf2AAAGADf39/AALLAQgDZW52GHdhc21fb25faGVhZGVyc19jb21wbGV0ZQAEA2VudhV3YXNtX29uX21lc3NhZ2VfYmVnaW4AAANlbnYLd2FzbV9vbl91cmwAAQNlbnYOd2FzbV9vbl9zdGF0dXMAAQNlbnYUd2FzbV9vbl9oZWFkZXJfZmllbGQAAQNlbnYUd2FzbV9vbl9oZWFkZXJfdmFsdWUAAQNlbnYMd2FzbV9vbl9ib2R5AAEDZW52GHdhc21fb25fbWVzc2FnZV9jb21wbGV0ZQAAAzU0BQYAAAMAAAAAAAADAQMAAwMDAAACAAAAAAICAgICAgICAgIBAQEBAQEBAQEBAwAAAwAAAAQFAXABExMFAwEAAgYIAX8BQcDZBAsHxQcoBm1lbW9yeQIAC19pbml0aWFsaXplAAgZX19pbmRpcmVjdF9mdW5jdGlvbl90YWJsZQEAC2xsaHR0cF9pbml0AAkYbGxodHRwX3Nob3VsZF9rZWVwX2FsaXZlADcMbGxodHRwX2FsbG9jAAsGbWFsbG9jADkLbGxodHRwX2ZyZWUADARmcmVlAAwPbGxodHRwX2dldF90eXBlAA0VbGxodHRwX2dldF9odHRwX21ham9yAA4VbGxodHRwX2dldF9odHRwX21pbm9yAA8RbGxodHRwX2dldF9tZXRob2QAEBZsbGh0dHBfZ2V0X3N0YXR1c19jb2RlABESbGxodHRwX2dldF91cGdyYWRlABIMbGxodHRwX3Jlc2V0ABMObGxodHRwX2V4ZWN1dGUAFBRsbGh0dHBfc2V0dGluZ3NfaW5pdAAVDWxsaHR0cF9maW5pc2gAFgxsbGh0dHBfcGF1c2UAFw1sbGh0dHBfcmVzdW1lABgbbGxodHRwX3Jlc3VtZV9hZnRlcl91cGdyYWRlABkQbGxodHRwX2dldF9lcnJubwAaF2xsaHR0cF9nZXRfZXJyb3JfcmVhc29uABsXbGxodHRwX3NldF9lcnJvcl9yZWFzb24AHBRsbGh0dHBfZ2V0X2Vycm9yX3BvcwAdEWxsaHR0cF9lcnJub19uYW1lAB4SbGxodHRwX21ldGhvZF9uYW1lAB8SbGxodHRwX3N0YXR1c19uYW1lACAabGxodHRw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AXRrQT5qIQILIAEgAjYCHCABQgA3AhAgAkECdEH01wBqIQACQEHI1QAoAgAiA0EBIAJ0IgdxRQRAIAAgATYCAEHI1QAgAyAHcjYCACABIAA2AhggASABNgIIIAEgATYCDAwBCyAEQRkgAkEBdmtBACACQR9HG3QhAiAAKAIAIQACQANAIAAiAygCBEF4cSAERg0BIAJBHXYhACACQQF0IQIgAyAAQQRxakEQaiIHKAIAIgANAAsgByABNgIAIAEgAzYCGCABIAE2AgwgASABNgIIDAELIAMoAggiACABNgIMIAMgATYCCCABQQA2AhggASADNgIMIAEgADYCCAtB5NUAQeTVACgCAEEBayIAQX8gABs2AgALCwcAIAAtACgLBwAgAC0AKgsHACAALQArCwcAIAAtACkLBwAgAC8BNAsHACAALQAwC0ABBH8gACgCGCEBIAAvAS4hAiAALQAoIQMgACgCOCEEIAAQOCAAIAQ2AjggACADOgAoIAAgAjsBLiAAIAE2AhgLhocCAwd/A34BeyABIAJqIQQCQCAAIgMoAgwiAA0AIAMoAgQEQCADIAE2AgQLIwBBEGsiCSQAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACfwJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkAC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DAgLQeMBDAcLQdcBDAYLQdgBDAULQdkBDAQLQdoBDAMLQdsBDAILQd0BDAELQdwBCyECA0ACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAIAMCfwJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACfwJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACfwJAAkACQAJAAkACQAJAAn8CQAJAAkACfwJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkAgAwJ/AkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJ/AkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkAC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A0EXNgIcIAMgADYCDCADIAFBAWo2AhQM+AILIABBFUYNI0EAIQIgA0EANgIcIAMgATYCFCADQdYMNgIQIANBIzYCDAz3AgsgAygCBCEAQQAhAiADQQA2AgQgAyAAIAEQNCIARQRAIAFBAWohAQwdCyADQRk2AhwgAyAANgIMIAMgAUEBajYCFAz2AgsgAygCBCEAQQAhAiADQQA2AgQgAyAAIAEQNCIARQRAIAFBAWohAQzvAgsgA0EaNgIcIAMgADYCDCADIAFBAWo2AhQM9QILIABBFUYNH0EAIQIgA0EANgIcIAMgATYCFCADQdAPNgIQIANBIjYCDAz0AgsgAygCBCEAIANBADYCBCADIAAgARAzIgBFBEAgAUEBaiEBDBsLIANBHDYCHCADIAA2AgwgAyABQQFqNgIUQQAhAgzzAgsgAygCBCEAIANBADYCBCADIAAgARAzIgBFBEAgAUEBaiEBDOsCCyADQR02AhwgAyAANgIMIAMgAUEBajYCFEEAIQIM8gILIABBO0cNASABQQFqIQELQSYhAgzXAgtBACECIANBADYCHCADIAE2AhQgA0GfFTYCECADQQw2AgwM7wILIAEgBEcEQANAIAEtAABBIEcNhAIgBCABQQFqIgFHDQALQSwhAgzvAgtBLCECDO4CCyABIARGBEBBNCECDO4CCwJAAkADQAJAIAEtAABBCmsOBAIAAAMACyAEIAFBAWoiAUcNAAtBNCECDO8CCyADKAIEIQAgA0EANgIEIAMgACABEDEiAEUNnwIgA0EyNgIcIAMgATYCFCADIAA2AgxBACECDO4CCyADKAIEIQAgA0EANgIEIAMgACABEDEiAEUEQCABQQFqIQEMnwILIANBMjYCHCADIAA2AgwgAyABQQFqNgIUQQAhAgztAgsgASAERwRAAkADQCABLQAAQTBrIgBB/wFxQQpPBEBBOiECDNcCCyADKQMgIgtCmbPmzJmz5swZVg0BIAMgC0IKfiIKNwMgIAogAK1C/wGDIgtCf4VWDQEgAyAKIAt8NwMgIAQgAUEBaiIBRw0AC0HAACECDO4CCyADKAIEIQAgA0EANgIEIAMgACABQQFqIgEQMSIADRcM4gILQcAAIQIM7AILIAEgBEYEQEHJACECDOwCCwJAA0ACQCABLQAAQQlrDhgAAqICogKpAqICogKiAqICogKiAqICogKiAqICogKiAqICogKiAqICogKiAgCiAgsgBCABQQFqIgFHDQALQckAIQIM7AILIAFBAWohASADQS9qLQAAQQFxDaUCIANBADYCHCADIAE2AhQgA0GXEDYCECADQQo2AgxBACECDOsCCyABIARHBEADQCABLQAAQSBHDRUgBCABQQFqIgFHDQALQfgAIQIM6wILQfgAIQIM6gILIANBAjoAKAw4C0EAIQIgA0EANgIcIANBvws2AhAgA0ECNgIMIAMgAUEBajYCFAzoAgtBACECDM4CC0ENIQIMzQILQRMhAgzMAgtBFSECDMsCC0EWIQIMygILQRghAgzJAgtBGSECDMgCC0EaIQIMxwILQRshAgzGAgtBHCECDMUCC0EdIQIMxAILQR4hAgzDAgtBHyECDMICC0EgIQIMwQILQSIhAgzAAgtBIyECDL8CC0ElIQIMvgILQeUAIQIMvQILIANBPTYCHCADIAE2AhQgAyAANgIMQQAhAgzVAgsgA0EbNgIcIAMgATYCFCADQaQcNgIQIANBFTYCDEEAIQIM1AILIANBIDYCHCADIAE2AhQgA0GYGjYCECADQRU2AgxBACECDNMCCyADQRM2AhwgAyABNgIUIANBmBo2AhAgA0EVNgIMQQAhAgzSAgsgA0ELNgIcIAMgATYCFCADQZgaNgIQIANBFTYCDEEAIQIM0QILIANBEDYCHCADIAE2AhQgA0GYGjYCECADQRU2AgxBACECDNACCyADQSA2AhwgAyABNgIUIANBpBw2AhAgA0EVNgIMQQAhAgzPAgsgA0ELNgIcIAMgATYCFCADQaQcNgIQIANBFTYCDEEAIQIMzgILIANBDDYCHCADIAE2AhQgA0GkHDYCECADQRU2AgxBACECDM0CC0EAIQIgA0EANgIcIAMgATYCFCADQd0ONgIQIANBEjYCDAzMAgsCQANAAkAgAS0AAEEKaw4EAAICAAILIAQgAUEBaiIBRw0AC0H9ASECDMwCCwJAAkAgAy0ANkEBRw0AQQAhAAJAIAMoAjgiAkUNACACKAJgIgJFDQAgAyACEQAAIQALIABFDQAgAEEVRw0BIANB/AE2AhwgAyABNgIUIANB3Bk2AhAgA0EVNgIMQQAhAgzNAgtB3AEhAgyzAgsgA0EANgIcIAMgATYCFCADQfkLNgIQIANBHzYCDEEAIQIMywILAkACQCADLQAoQQFrDgIEAQALQdsBIQIMsgILQdQBIQIMsQILIANBAjoAMUEAIQACQCADKAI4IgJFDQAgAigCACICRQ0AIAMgAhEAACEACyAARQRAQd0BIQIMsQILIABBFUcEQCADQQA2AhwgAyABNgIUIANBtAw2AhAgA0EQNgIMQQAhAgzKAgsgA0H7ATYCHCADIAE2AhQgA0GBGjYCECADQRU2AgxBACECDMkCCyABIARGBEBB+gEhAgzJAgsgAS0AAEHIAEYNASADQQE6ACgLQcABIQIMrgILQdoBIQIMrQILIAEgBEcEQCADQQw2AgggAyABNgIEQdkBIQIMrQILQfkBIQIMxQILIAEgBEYEQEH4ASECDMUCCyABLQAAQcgARw0EIAFBAWohAUHYASECDKsCCyABIARGBEBB9wEhAgzEAgsCQAJAIAEtAABBxQBrDhAABQUFBQUFBQUFBQUFBQUBBQsgAUEBaiEBQdYBIQIMqwILIAFBAWohAUHXASECDKoCC0H2ASECIAEgBEYNwgIgAygCACIAIAQgAWtqIQUgASAAa0ECaiEGAkADQCABLQAAIABButUAai0AAEcNAyAAQQJGDQEgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAU2AgAMwwILIAMoAgQhACADQgA3AwAgAyAAIAZBAWoiARAuIgBFBEBB4wEhAgyqAgsgA0H1ATYCHCADIAE2AhQgAyAANgIMQQAhAgzCAgtB9AEhAiABIARGDcECIAMoAgAiACAEIAFraiEFIAEgAGtBAWohBgJAA0AgAS0AACAAQbjVAGotAABHDQIgAEEBRg0BIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADMICCyADQYEEOwEoIAMoAgQhACADQgA3AwAgAyAAIAZBAWoiARAuIgANAwwCCyADQQA2AgALQQAhAiADQQA2AhwgAyABNgIUIANB5R82AhAgA0EINgIMDL8CC0HVASECDKUCCyADQfMBNgIcIAMgATYCFCADIAA2AgxBACECDL0CC0EAIQACQCADKAI4IgJFDQAgAigCQCICRQ0AIAMgAhEAACEACyAARQ1uIABBFUcEQCADQQA2AhwgAyABNgIUIANBgg82AhAgA0EgNgIMQQAhAgy9AgsgA0GPATYCHCADIAE2AhQgA0HsGzYCECADQRU2AgxBACECDLwCCyABIARHBEAgA0ENNgIIIAMgATYCBEHTASECDKMCC0HyASECDLsCCyABIARGBEBB8QEhAgy7AgsCQAJAAkAgAS0AAEHIAGsOCwABCAgICAgICAgCCAsgAUEBaiEBQdABIQIMowILIAFBAWohAUHRASECDKICCyABQQFqIQFB0gEhAgyhAgtB8AEhAiABIARGDbkCIAMoAgAiACAEIAFraiEGIAEgAGtBAmohBQNAIAEtAAAgAEG11QBqLQAARw0EIABBAkYNAyAAQQFqIQAgBCABQQFqIgFHDQALIAMgBjYCAAy5AgtB7wEhAiABIARGDbgCIAMoAgAiACAEIAFraiEGIAEgAGtBAWohBQNAIAEtAAAgAEGz1QBqLQAARw0DIABBAUYNAiAAQQFqIQAgBCABQQFqIgFHDQALIAMgBjYCAAy4AgtB7gEhAiABIARGDbcCIAMoAgAiACAEIAFraiEGIAEgAGtBAmohBQNAIAEtAAAgAEGw1QBqLQAARw0CIABBAkYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBjYCAAy3AgsgAygCBCEAIANCADcDACADIAAgBUEBaiIBECsiAEUNAiADQewBNgIcIAMgATYCFCADIAA2AgxBACECDLYCCyADQQA2AgALIAMoAgQhACADQQA2AgQgAyAAIAEQKyIARQ2cAiADQe0BNgIcIAMgATYCFCADIAA2AgxBACECDLQCC0HPASECDJoCC0EAIQACQCADKAI4IgJFDQAgAigCNCICRQ0AIAMgAhEAACEACwJAIAAEQCAAQRVGDQEgA0EANgIcIAMgATYCFCADQeoNNgIQIANBJjYCDEEAIQIMtAILQc4BIQIMmgILIANB6wE2AhwgAyABNgIUIANBgBs2AhAgA0EVNgIMQQAhAgyyAgsgASAERgRAQesBIQIMsgILIAEtAABBL0YEQCABQQFqIQEMAQsgA0EANgIcIAMgATYCFCADQbI4NgIQIANBCDYCDEEAIQIMsQILQc0BIQIMlwILIAEgBEcEQCADQQ42AgggAyABNgIEQcwBIQIMlwILQeoBIQIMrwILIAEgBEYEQEHpASECDK8CCyABLQAAQTBrIgBB/wFxQQpJBEAgAyAAOgAqIAFBAWohAUHLASECDJYCCyADKAIEIQAgA0EANgIEIAMgACABEC8iAEUNlwIgA0HoATYCHCADIAE2AhQgAyAANgIMQQAhAgyuAgsgASAERgRAQecBIQIMrgILAkAgAS0AAEEuRgRAIAFBAWohAQwBCyADKAIEIQAgA0EANgIEIAMgACABEC8iAEUNmAIgA0HmATYCHCADIAE2AhQgAyAANgIMQQAhAgyuAgtBygEhAgyUAgsgASAERgRAQeUBIQIMrQILQQAhAEEBIQVBASEHQQAhAgJAAkACQAJAAkACfwJAAkACQAJAAkACQAJAIAEtAABBMGsOCgoJAAECAwQFBggLC0ECDAYLQQMMBQtBBAwEC0EFDAMLQQYMAgtBBwwBC0EICyECQQAhBUEAIQcMAgtBCSECQQEhAEEAIQVBACEHDAELQQAhBUEBIQILIAMgAjoAKyABQQFqIQECQAJAIAMtAC5BEHENAAJAAkACQCADLQAqDgMBAAIECyAHRQ0DDAILIAANAQwCCyAFRQ0BCyADKAIEIQAgA0EANgIEIAMgACABEC8iAEUNAiADQeIBNgIcIAMgATYCFCADIAA2AgxBACECDK8CCyADKAIEIQAgA0EANgIEIAMgACABEC8iAEUNmgIgA0HjATYCHCADIAE2AhQgAyAANgIMQQAhAgyuAgsgAygCBCEAIANBADYCBCADIAAgARAvIgBFDZgCIANB5AE2AhwgAyABNgIUIAMgADYCDAytAgtByQEhAgyTAgtBACEAAkAgAygCOCICRQ0AIAIoAkQiAkUNACADIAIRAAAhAAsCQCAABEAgAEEVRg0BIANBADYCHCADIAE2AhQgA0GkDTYCECADQSE2AgxBACECDK0CC0HIASECDJMCCyADQeEBNgIcIAMgATYCFCADQdAaNgIQIANBFTYCDEEAIQIMqwILIAEgBEYEQEHhASECDKsCCwJAIAEtAABBIEYEQCADQQA7ATQgAUEBaiEBDAELIANBADYCHCADIAE2AhQgA0GZETYCECADQQk2AgxBACECDKsCC0HHASECDJECCyABIARGBEBB4AEhAgyqAgsCQCABLQAAQTBrQf8BcSICQQpJBEAgAUEBaiEBAkAgAy8BNCIAQZkzSw0AIAMgAEEKbCIAOwE0IABB/v8DcSACQf//A3NLDQAgAyAAIAJqOwE0DAILQQAhAiADQQA2AhwgAyABNgIUIANBlR42AhAgA0ENNgIMDKsCCyADQQA2AhwgAyABNgIUIANBlR42AhAgA0ENNgIMQQAhAgyqAgtBxgEhAgyQAgsgASAERgRAQd8BIQIMqQILAkAgAS0AAEEwa0H/AXEiAkEKSQRAIAFBAWohAQJAIAMvATQiAEGZM0sNACADIABBCmwiADsBNCAAQf7/A3EgAkH//wNzSw0AIAMgACACajsBNAwCC0EAIQIgA0EANgIcIAMgATYCFCADQZUeNgIQIANBDTYCDAyqAgsgA0EANgIcIAMgATYCFCADQZUeNgIQIANBDTYCDEEAIQIMqQILQcUBIQIMjwILIAEgBEYEQEHeASECDKgCCwJAIAEtAABBMGtB/wFxIgJBCkkEQCABQQFqIQECQCADLwE0IgBBmTNLDQAgAyAAQQpsIgA7ATQgAEH+/wNxIAJB//8Dc0sNACADIAAgAmo7ATQMAgtBACECIANBADYCHCADIAE2AhQgA0GVHjYCECADQQ02AgwMqQILIANBADYCHCADIAE2AhQgA0GVHjYCECADQQ02AgxBACECDKgCC0HEASECDI4CCyABIARGBEBB3QEhAgynAgsCQAJAAkACQCABLQAAQQprDhcCAwMAAwMDAwMDAwMDAwMDAwMDAwMDAQMLIAFBAWoMBQsgAUEBaiEBQcMBIQIMjwILIAFBAWohASADQS9qLQAAQQFxDQggA0EANgIcIAMgATYCFCADQY0LNgIQIANBDTYCDEEAIQIMpwILIANBADYCHCADIAE2AhQgA0GNCzYCECADQQ02AgxBACECDKYCCyABIARHBEAgA0EPNgIIIAMgATYCBEEBIQIMjQILQdwBIQIMpQILAkACQANAAkAgAS0AAEEKaw4EAgAAAwALIAQgAUEBaiIBRw0AC0HbASECDKYCCyADKAIEIQAgA0EANgIEIAMgACABEC0iAEUEQCABQQFqIQEMBAsgA0HaATYCHCADIAA2AgwgAyABQQFqNgIUQQAhAgylAgsgAygCBCEAIANBADYCBCADIAAgARAtIgANASABQQFqCyEBQcEBIQIMigILIANB2QE2AhwgAyAANgIMIAMgAUEBajYCFEEAIQIMogILQcIBIQIMiAILIANBL2otAABBAXENASADQQA2AhwgAyABNgIUIANB5Bw2AhAgA0EZNgIMQQAhAgygAgsgASAERgRAQdkBIQIMoAILAkACQAJAIAEtAABBCmsOBAECAgACCyABQQFqIQEMAgsgAUEBaiEBDAELIAMtAC5BwABxRQ0BC0EAIQACQCADKAI4IgJFDQAgAigCPCICRQ0AIAMgAhEAACEACyAARQ2gASAAQRVGBEAgA0HZADYCHCADIAE2AhQgA0G3GjYCECADQRU2AgxBACECDJ8CCyADQQA2AhwgAyABNgIUIANBgA02AhAgA0EbNgIMQQAhAgyeAgsgA0EANgIcIAMgATYCFCADQdwoNgIQIANBAjYCDEEAIQIMnQILIAEgBEcEQCADQQw2AgggAyABNgIEQb8BIQIMhAILQdgBIQIMnAILIAEgBEYEQEHXASECDJwCCwJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkAgAS0AAEHBAGsOFQABAgNaBAUGWlpaBwgJCgsMDQ4PEFoLIAFBAWohAUH7ACECDJICCyABQQFqIQFB/AAhAgyRAgsgAUEBaiEBQYEBIQIMkAILIAFBAWohAUGFASECDI8CCyABQQFqIQFBhgEhAgyOAgsgAUEBaiEBQYkBIQIMjQILIAFBAWohAUGKASECDIwCCyABQQFqIQFBjQEhAgyLAgsgAUEBaiEBQZYBIQIMigILIAFBAWohAUGXASECDIkCCyABQQFqIQFBmAEhAgyIAgsgAUEBaiEBQaUBIQIMhwILIAFBAWohAUGmASECDIYCCyABQQFqIQFBrAEhAgyFAgsgAUEBaiEBQbQBIQIMhAILIAFBAWohAUG3ASECDIMCCyABQQFqIQFBvgEhAgyCAgsgASAERgRAQdYBIQIMmwILIAEtAABBzgBHDUggAUEBaiEBQb0BIQIMgQILIAEgBEYEQEHVASECDJoCCwJAAkACQCABLQAAQcIAaw4SAEpKSkpKSkpKSgFKSkpKSkoCSgsgAUEBaiEBQbgBIQIMggILIAFBAWohAUG7ASECDIECCyABQQFqIQFBvAEhAgyAAgtB1AEhAiABIARGDZgCIAMoAgAiACAEIAFraiEFIAEgAGtBB2ohBgJAA0AgAS0AACAAQajVAGotAABHDUUgAEEHRg0BIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADJkCCyADQQA2AgAgBkEBaiEBQRsMRQsgASAERgRAQdMBIQIMmAILAkACQCABLQAAQckAaw4HAEdHR0dHAUcLIAFBAWohAUG5ASECDP8BCyABQQFqIQFBugEhAgz+AQtB0gEhAiABIARGDZYCIAMoAgAiACAEIAFraiEFIAEgAGtBAWohBgJAA0AgAS0AACAAQabVAGotAABHDUMgAEEBRg0BIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADJcCCyADQQA2AgAgBkEBaiEBQQ8MQwtB0QEhAiABIARGDZUCIAMoAgAiACAEIAFraiEFIAEgAGtBAWohBgJAA0AgAS0AACAAQaTVAGotAABHDUIgAEEBRg0BIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADJYCCyADQQA2AgAgBkEBaiEBQSAMQgtB0AEhAiABIARGDZQCIAMoAgAiACAEIAFraiEFIAEgAGtBAmohBgJAA0AgAS0AACAAQaHVAGotAABHDUEgAEECRg0BIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADJUCCyADQQA2AgAgBkEBaiEBQRIMQQsgASAERgRAQc8BIQIMlAILAkACQCABLQAAQcUAaw4OAENDQ0NDQ0NDQ0NDQwFDCyABQQFqIQFBtQEhAgz7AQsgAUEBaiEBQbYBIQIM+gELQc4BIQIgASAERg2SAiADKAIAIgAgBCABa2ohBSABIABrQQJqIQYCQANAIAEtAAAgAEGe1QBqLQAARw0/IABBAkYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAyTAgsgA0EANgIAIAZBAWohAUEHDD8LQc0BIQIgASAERg2RAiADKAIAIgAgBCABa2ohBSABIABrQQVqIQYCQANAIAEtAAAgAEGY1QBqLQAARw0+IABBBUYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAySAgsgA0EANgIAIAZBAWohAUEoDD4LIAEgBEYEQEHMASECDJECCwJAAkACQCABLQAAQcUAaw4RAEFBQUFBQUFBQQFBQUFBQQJBCyABQQFqIQFBsQEhAgz5AQsgAUEBaiEBQbIBIQIM+AELIAFBAWohAUGzASECDPcBC0HLASECIAEgBEYNjwIgAygCACIAIAQgAWtqIQUgASAAa0EGaiEGAkADQCABLQAAIABBkdUAai0AAEcNPCAAQQZGDQEgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAU2AgAMkAILIANBADYCACAGQQFqIQFBGgw8C0HKASECIAEgBEYNjgIgAygCACIAIAQgAWtqIQUgASAAa0EDaiEGAkADQCABLQAAIABBjdUAai0AAEcNOyAAQQNGDQEgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAU2AgAMjwILIANBADYCACAGQQFqIQFBIQw7CyABIARGBEBByQEhAgyOAgsCQAJAIAEtAABBwQBrDhQAPT09PT09PT09PT09PT09PT09AT0LIAFBAWohAUGtASECDPUBCyABQQFqIQFBsAEhAgz0AQsgASAERgRAQcgBIQIMjQILAkACQCABLQAAQdUAaw4LADw8PDw8PDw8PAE8CyABQQFqIQFBrgEhAgz0AQsgAUEBaiEBQa8BIQIM8wELQccBIQIgASAERg2LAiADKAIAIgAgBCABa2ohBSABIABrQQhqIQYCQANAIAEtAAAgAEGE1QBqLQAARw04IABBCEYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAyMAgsgA0EANgIAIAZBAWohAUEqDDgLIAEgBEYEQEHGASECDIsCCyABLQAAQdAARw04IAFBAWohAUElDDcLQcUBIQIgASAERg2JAiADKAIAIgAgBCABa2ohBSABIABrQQJqIQYCQANAIAEtAAAgAEGB1QBqLQAARw02IABBAkYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAyKAgsgA0EANgIAIAZBAWohAUEODDYLIAEgBEYEQEHEASECDIkCCyABLQAAQcUARw02IAFBAWohAUGrASECDO8BCyABIARGBEBBwwEhAgyIAgsCQAJAAkACQCABLQAAQcIAaw4PAAECOTk5OTk5OTk5OTkDOQsgAUEBaiEBQacBIQIM8QELIAFBAWohAUGoASECDPABCyABQQFqIQFBqQEhAgzvAQsgAUEBaiEBQaoBIQIM7gELQcIBIQIgASAERg2GAiADKAIAIgAgBCABa2ohBSABIABrQQJqIQYCQANAIAEtAAAgAEH+1ABqLQAARw0zIABBAkYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAyHAgsgA0EANgIAIAZBAWohAUEUDDMLQcEBIQIgASAERg2FAiADKAIAIgAgBCABa2ohBSABIABrQQRqIQYCQANA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BTYCAAzsAQsgA0EANgIAIAZBAWohAUEXDBgLQaYBIQIgASAERg3qASADKAIAIgAgBCABa2ohBSABIABrQQVqIQYCQANAIAEtAAAgAEHB1ABqLQAARw0XIABBBUYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAzrAQsgA0EANgIAIAZBAWohAUEYDBcLIAEgBEYEQEGlASECDOoBCwJAAkAgAS0AAEHJAGsOBwAZGRkZGQEZCyABQQFqIQFBiwEhAgzRAQsgAUEBaiEBQYwBIQIM0AELQaQBIQIgASAERg3oASADKAIAIgAgBCABa2ohBSABIABrQQFqIQYCQANAIAEtAAAgAEGm1QBqLQAARw0VIABBAUYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAzpAQsgA0EANgIAIAZBAWohAUEJDBULQaMBIQIgASAERg3nASADKAIAIgAgBCABa2ohBSABIABrQQFqIQYCQANAIAEtAAAgAEGk1QBqLQAARw0UIABBAUYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAzoAQsgA0EANgIAIAZBAWohAUEfDBQLQaIBIQIgASAERg3mASADKAIAIgAgBCABa2ohBSABIABrQQJqIQYCQANAIAEtAAAgAEG+1ABqLQAARw0TIABBAkYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAznAQsgA0EANgIAIAZBAWohAUECDBMLQaEBIQIgASAERg3lASADKAIAIgAgBCABa2ohBSABIABrQQFqIQYDQCABLQAAIABBvNQAai0AAEcNESAAQQFGDQIgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAU2AgAM5QELIAEgBEYEQEGgASECDOUBC0EBIAEtAABB3wBHDREaIAFBAWohAUGHASECDMsBCyADQQA2AgAgBkEBaiEBQYgBIQIMygELQZ8BIQIgASAERg3iASADKAIAIgAgBCABa2ohBSABIABrQQhqIQYCQANAIAEtAAAgAEGE1QBqLQAARw0PIABBCEYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAzjAQsgA0EANgIAIAZBAWohAUEpDA8LQZ4BIQIgASAERg3hASADKAIAIgAgBCABa2ohBSABIABrQQNqIQYCQANAIAEtAAAgAEG41ABqLQAARw0OIABBA0YNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAziAQsgA0EANgIAIAZBAWohAUEtDA4LIAEgBEYEQEGdASECDOEBCyABLQAAQcUARw0OIAFBAWohAUGEASECDMcBCyABIARGBEBBnAEhAgzgAQsCQAJAIAEtAABBzABrDggADw8PDw8PAQ8LIAFBAWohAUGCASECDMcBCyABQQFqIQFBgwEhAgzGAQtBmwEhAiABIARGDd4BIAMoAgAiACAEIAFraiEFIAEgAGtBBGohBgJAA0AgAS0AACAAQbPUAGotAABHDQsgAEEERg0BIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADN8BCyADQQA2AgAgBkEBaiEBQSMMCwtBmgEhAiABIARGDd0BIAMoAgAiACAEIAFraiEFIAEgAGtBAmohBgJAA0AgAS0AACAAQbDUAGotAABHDQogAEECRg0BIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADN4BCyADQQA2AgAgBkEBaiEBQQAMCgsgASAERgRAQZkBIQIM3QELAkACQCABLQAAQcgAaw4IAAwMDAwMDAEMCyABQQFqIQFB/QAhAgzEAQsgAUEBaiEBQYABIQIMwwELIAEgBEYEQEGYASECDNwBCwJAAkAgAS0AAEHOAGsOAwALAQsLIAFBAWohAUH+ACECDMMBCyABQQFqIQFB/wAhAgzCAQsgASAERgRAQZcBIQIM2wELIAEtAABB2QBHDQggAUEBaiEBQQgMBwtBlgEhAiABIARGDdkBIAMoAgAiACAEIAFraiEFIAEgAGtBA2ohBgJAA0AgAS0AACAAQazUAGotAABHDQYgAEEDRg0BIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADNoBCyADQQA2AgAgBkEBaiEBQQUMBgtBlQEhAiABIARGDdgBIAMoAgAiACAEIAFraiEFIAEgAGtBBWohBgJAA0AgAS0AACAAQabUAGotAABHDQUgAEEFRg0BIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADNkBCyADQQA2AgAgBkEBaiEBQRYMBQtBlAEhAiABIARGDdcBIAMoAgAiACAEIAFraiEFIAEgAGtBAmohBgJAA0AgAS0AACAAQaHVAGotAABHDQQgAEECRg0BIABBAWohACAEIAFBAWoiAUcNAAsgAyAFNgIADNgBCyADQQA2AgAgBkEBaiEBQRAMBAsgASAERgRAQZMBIQIM1wELAkACQCABLQAAQcMAaw4MAAYGBgYGBgYGBgYBBgsgAUEBaiEBQfkAIQIMvgELIAFBAWohAUH6ACECDL0BC0GSASECIAEgBEYN1QEgAygCACIAIAQgAWtqIQUgASAAa0EFaiEGAkADQCABLQAAIABBoNQAai0AAEcNAiAAQQVGDQEgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAU2AgAM1gELIANBADYCACAGQQFqIQFBJAwCCyADQQA2AgAMAgsgASAERgRAQZEBIQIM1AELIAEtAABBzABHDQEgAUEBaiEBQRMLOgApIAMoAgQhACADQQA2AgQgAyAAIAEQLiIADQIMAQtBACECIANBADYCHCADIAE2AhQgA0H+HzYCECADQQY2AgwM0QELQfgAIQIMtwELIANBkAE2AhwgAyABNgIUIAMgADYCDEEAIQIMzwELQQAhAAJAIAMoAjgiAkUNACACKAJAIgJFDQAgAyACEQAAIQALIABFDQAgAEEVRg0BIANBADYCHCADIAE2AhQgA0GCDzYCECADQSA2AgxBACECDM4BC0H3ACECDLQBCyADQY8BNgIcIAMgATYCFCADQewbNgIQIANBFTYCDEEAIQIMzAELIAEgBEYEQEGPASECDMwBCwJAIAEtAABBIEYEQCABQQFqIQEMAQsgA0EANgIcIAMgATYCFCADQZsfNgIQIANBBjYCDEEAIQIMzAELQQIhAgyyAQsDQCABLQAAQSBHDQIgBCABQQFqIgFHDQALQY4BIQIMygELIAEgBEYEQEGNASECDMoBCwJAIAEtAABBCWsOBEoAAEoAC0H1ACECDLABCyADLQApQQVGBEBB9gAhAgywAQtB9AAhAgyvAQsgASAERgRAQYwBIQIMyAELIANBEDYCCCADIAE2AgQMCgsgASAERgRAQYsBIQIMxwELAkAgAS0AAEEJaw4ERwAARwALQfMAIQIMrQELIAEgBEcEQCADQRA2AgggAyABNgIEQfEAIQIMrQELQYoBIQIMxQELAkAgASAERwRAA0AgAS0AAEGg0ABqLQAAIgBBA0cEQAJAIABBAWsOAkkABAtB8AAhAgyvAQsgBCABQQFqIgFHDQALQYgBIQIMxgELQYgBIQIMxQELIANBADYCHCADIAE2AhQgA0HbIDYCECADQQc2AgxBACECDMQBCyABIARGBEBBiQEhAgzEAQsCQAJAAkAgAS0AAEGg0gBqLQAAQQFrDgNGAgABC0HyACECDKwBCyADQQA2AhwgAyABNgIUIANBtBI2AhAgA0EHNgIMQQAhAgzEAQtB6gAhAgyqAQsgASAERwRAIAFBAWohAUHvACECDKoBC0GHASECDMIBCyAEIAEiAEYEQEGGASECDMIBCyAALQAAIgFBL0YEQCAAQQFqIQFB7gAhAgypAQsgAUEJayICQRdLDQEgACEBQQEgAnRBm4CABHENQQwBCyAEIAEiAEYEQEGFASECDMEBCyAALQAAQS9HDQAgAEEBaiEBDAMLQQAhAiADQQA2AhwgAyAANgIUIANB2yA2AhAgA0EHNgIMDL8BCwJAAkACQAJAAkADQCABLQAAQaDOAGotAAAiAEEFRwRAAkACQCAAQQFrDghHBQYHCAAEAQgLQesAIQIMrQELIAFBAWohAUHtACECDKwBCyAEIAFBAWoiAUcNAAtBhAEhAgzDAQsgAUEBagwUCyADKAIEIQAgA0EANgIEIAMgACABECwiAEUNHiADQdsANgIcIAMgATYCFCADIAA2AgxBACECDMEBCyADKAIEIQAgA0EANgIEIAMgACABECwiAEUNHiADQd0ANgIcIAMgATYCFCADIAA2AgxBACECDMABCyADKAIEIQAgA0EANgIEIAMgACABECwiAEUNHiADQfoANgIcIAMgATYCFCADIAA2AgxBACECDL8BCyADQQA2AhwgAyABNgIUIANB+Q82AhAgA0EHNgIMQQAhAgy+AQsgASAERgRAQYMBIQIMvgELAkAgAS0AAEGgzgBqLQAAQQFrDgg+BAUGAAgCAwcLIAFBAWohAQtBAyECDKMBCyABQQFqDA0LQQAhAiADQQA2AhwgA0HREjYCECADQQc2AgwgAyABQQFqNgIUDLoBCyADKAIEIQAgA0EANgIEIAMgACABECwiAEUNFiADQdsANgIcIAMgATYCFCADIAA2AgxBACECDLkBCyADKAIEIQAgA0EANgIEIAMgACABECwiAEUNFiADQd0ANgIcIAMgATYCFCADIAA2AgxBACECDLgBCyADKAIEIQAgA0EANgIEIAMgACABECwiAEUNFiADQfoANgIcIAMgATYCFCADIAA2AgxBACECDLcBCyADQQA2AhwgAyABNgIUIANB+Q82AhAgA0EHNgIMQQAhAgy2AQtB7AAhAgycAQsgASAERgRAQYIBIQIMtQELIAFBAWoMAgsgASAERgRAQYEBIQIMtAELIAFBAWoMAQsgASAERg0BIAFBAWoLIQFBBCECDJgBC0GAASECDLABCwNAIAEtAABBoMwAai0AACIAQQJHBEAgAEEBRwRAQekAIQIMmQELDDELIAQgAUEBaiIBRw0AC0H/ACECDK8BCyABIARGBEBB/gAhAgyvAQsCQCABLQAAQQlrDjcvAwYvBAYGBgYGBgYGBgYGBgYGBgYGBgUGBgIGBgYGBgYGBgYGBgYGBgYGBgYGBgYGBgYGBgYABgsgAUEBagshAUEFIQIMlAELIAFBAWoMBgsgAygCBCEAIANBADYCBCADIAAgARAsIgBFDQggA0HbADYCHCADIAE2AhQgAyAANgIMQQAhAgyrAQsgAygCBCEAIANBADYCBCADIAAgARAsIgBFDQggA0HdADYCHCADIAE2AhQgAyAANgIMQQAhAgyqAQsgAygCBCEAIANBADYCBCADIAAgARAsIgBFDQggA0H6ADYCHCADIAE2AhQgAyAANgIMQQAhAgypAQsgA0EANgIcIAMgATYCFCADQY0UNgIQIANBBzYCDEEAIQIMqAELAkACQAJAAkADQCABLQAAQaDKAGotAAAiAEEFRwRAAkAgAEEBaw4GLgMEBQYABgtB6AAhAgyUAQsgBCABQQFqIgFHDQALQf0AIQIMqwELIAMoAgQhACADQQA2AgQgAyAAIAEQLCIARQ0HIANB2wA2AhwgAyABNgIUIAMgADYCDEEAIQIMqgELIAMoAgQhACADQQA2AgQgAyAAIAEQLCIARQ0HIANB3QA2AhwgAyABNgIUIAMgADYCDEEAIQIMqQELIAMoAgQhACADQQA2AgQgAyAAIAEQLCIARQ0HIANB+gA2AhwgAyABNgIUIAMgADYCDEEAIQIMqAELIANBADYCHCADIAE2AhQgA0HkCDYCECADQQc2AgxBACECDKcBCyABIARGDQEgAUEBagshAUEGIQIMjAELQfwAIQIMpAELAkACQAJAAkADQCABLQAAQaDIAGotAAAiAEEFRwRAIABBAWsOBCkCAwQFCyAEIAFBAWoiAUcNAAtB+wAhAgynAQsgAygCBCEAIANBADYCBCADIAAgARAsIgBFDQMgA0HbADYCHCADIAE2AhQgAyAANgIMQQAhAgymAQsgAygCBCEAIANBADYCBCADIAAgARAsIgBFDQMgA0HdADYCHCADIAE2AhQgAyAANgIMQQAhAgylAQsgAygCBCEAIANBADYCBCADIAAgARAsIgBFDQMgA0H6ADYCHCADIAE2AhQgAyAANgIMQQAhAgykAQsgA0EANgIcIAMgATYCFCADQbwKNgIQIANBBzYCDEEAIQIMowELQc8AIQIMiQELQdEAIQIMiAELQecAIQIMhwELIAEgBEYEQEH6ACECDKABCwJAIAEtAABBCWsOBCAAACAACyABQQFqIQFB5gAhAgyGAQsgASAERgRAQfkAIQIMnwELAkAgAS0AAEEJaw4EHwAAHwALQQAhAAJAIAMoAjgiAkUNACACKAI4IgJFDQAgAyACEQAAIQALIABFBEBB4gEhAgyGAQsgAEEVRwRAIANBADYCHCADIAE2AhQgA0HJDTYCECADQRo2AgxBACECDJ8BCyADQfgANgIcIAMgATYCFCADQeoaNgIQIANBFTYCDEEAIQIMngELIAEgBEcEQCADQQ02AgggAyABNgIEQeQAIQIMhQELQfcAIQIMnQELIAEgBEYEQEH2ACECDJ0BCwJAAkACQCABLQAAQcgAaw4LAAELCwsLCwsLCwILCyABQQFqIQFB3QAhAgyFAQsgAUEBaiEBQeAAIQIMhAELIAFBAWohAUHjACECDIMBC0H1ACECIAEgBEYNmwEgAygCACIAIAQgAWtqIQUgASAAa0ECaiEGAkADQCABLQAAIABBtdUAai0AAEcNCCAAQQJGDQEgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAU2AgAMnAELIAMoAgQhACADQgA3AwAgAyAAIAZBAWoiARArIgAEQCADQfQANgIcIAMgATYCFCADIAA2AgxBACECDJwBC0HiACECDIIBC0EAIQACQCADKAI4IgJFDQAgAigCNCICRQ0AIAMgAhEAACEACwJAIAAEQCAAQRVGDQEgA0EANgIcIAMgATYCFCADQeoNNgIQIANBJjYCDEEAIQIMnAELQeEAIQIMggELIANB8wA2AhwgAyABNgIUIANBgBs2AhAgA0EVNgIMQQAhAgyaAQsgAy0AKSIAQSNrQQtJDQkCQCAAQQZLDQBBASAAdEHKAHFFDQAMCgtBACECIANBADYCHCADIAE2AhQgA0HtCTYCECADQQg2AgwMmQELQfIAIQIgASAERg2YASADKAIAIgAgBCABa2ohBSABIABrQQFqIQYCQANAIAEtAAAgAEGz1QBqLQAARw0FIABBAUYNASAAQQFqIQAgBCABQQFqIgFHDQALIAMgBTYCAAyZAQsgAygCBCEAIANCADcDACADIAAgBkEBaiIBECsiAARAIANB8QA2AhwgAyABNgIUIAMgADYCDEEAIQIMmQELQd8AIQIMfwtBACEAAkAgAygCOCICRQ0AIAIoAjQiAkUNACADIAIRAAAhAAsCQCAABEAgAEEVRg0BIANBADYCHCADIAE2AhQgA0HqDTYCECADQSY2AgxBACECDJkBC0HeACECDH8LIANB8AA2AhwgAyABNgIUIANBgBs2AhAgA0EVNgIMQQAhAgyXAQsgAy0AKUEhRg0GIANBADYCHCADIAE2AhQgA0GRCjYCECADQQg2AgxBACECDJYBC0HvACECIAEgBEYNlQEgAygCACIAIAQgAWtqIQUgASAAa0ECaiEGAkADQCABLQAAIABBsNUAai0AAEcNAiAAQQJGDQEgAEEBaiEAIAQgAUEBaiIBRw0ACyADIAU2AgAMlgELIAMoAgQhACADQgA3AwAgAyAAIAZBAWoiARArIgBFDQIgA0HtADYCHCADIAE2AhQgAyAANgIMQQAhAgyVAQsgA0EANgIACyADKAIEIQAgA0EANgIEIAMgACABECsiAEUNgAEgA0HuADYCHCADIAE2AhQgAyAANgIMQQAhAgyTAQtB3AAhAgx5C0EAIQACQCADKAI4IgJFDQAgAigCNCICRQ0AIAMgAhEAACEACwJAIAAEQCAAQRVGDQEgA0EANgIcIAMgATYCFCADQeoNNgIQIANBJjYCDEEAIQIMkwELQdsAIQIMeQsgA0HsADYCHCADIAE2AhQgA0GAGzYCECADQRU2AgxBACECDJEBCyADLQApIgBBI0kNACAAQS5GDQAgA0EANgIcIAMgATYCFCADQckJNgIQIANBCDYCDEEAIQIMkAELQdoAIQIMdgsgASAERgRAQesAIQIMjwELAkAgAS0AAEEvRgRAIAFBAWohAQwBCyAD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ciBIVFRQL3gueCByZXF1ZXN0AEludmFsaWQgbWV0aG9kIGZvciBSVFNQL3gueCByZXF1ZXN0AEV4cGVjdGVkIFNPVVJDRSBtZXRob2QgZm9yIElDRS94LnggcmVxdWVzdABJbnZhbGlkIGNoYXIgaW4gdXJsIGZyYWdtZW50IHN0YXJ0AEV4cGVjdGVkIGRvdABTcGFuIGNhbGxiYWNrIGVycm9yIGluIG9uX3N0YXR1cwBJbnZhbGlkIHJlc3BvbnNlIHN0YXR1cwBFeHBlY3RlZCBMRiBhZnRlciBoZWFkZXJzAEludmFsaWQgY2hhcmFjdGVyIGluIGNodW5rIGV4dGVuc2lvbnMAVXNlciBjYWxsYmFjayBlcnJvcgBgb25fcmVzZXRgIGNhbGxiYWNrIGVycm9yAGBvbl9jaHVua19oZWFkZXJgIGNhbGxiYWNrIGVycm9yAGBvbl9tZXNzYWdlX2JlZ2luYCBjYWxsYmFjayBlcnJvcgBgb25fY2h1bmtfZXh0ZW5zaW9uX3ZhbHVlYCBjYWxsYmFjayBlcnJvcgBgb25fc3RhdHVzX2NvbXBsZXRlYCBjYWxsYmFjayBlcnJvcgBgb25fdmVyc2lvbl9jb21wbGV0ZWAgY2FsbGJhY2sgZXJyb3IAYG9uX3VybF9jb21wbGV0ZWAgY2FsbGJhY2sgZXJyb3IAYG9uX3Byb3RvY29sX2NvbXBsZXRlYCBjYWxsYmFjayBlcnJvcgBgb25fY2h1bmtfY29tcGxldGVgIGNhbGxiYWNrIGVycm9yAGBvbl9oZWFkZXJfdmFsdWVfY29tcGxldGVgIGNhbGxiYWNrIGVycm9yAGBvbl9tZXNzYWdlX2NvbXBsZXRlYCBjYWxsYmFjayBlcnJvcgBgb25fbWV0aG9kX2NvbXBsZXRlYCBjYWxsYmFjayBlcnJvcgBgb25faGVhZGVyX2ZpZWxkX2NvbXBsZXRlYCBjYWxsYmFjayBlcnJvcgBgb25fY2h1bmtfZXh0ZW5zaW9uX25hbWVgIGNhbGxiYWNrIGVycm9yAFVuZXhwZWN0ZWQgY2hhciBpbiB1cmwgc2VydmVyAEludmFsaWQgaGVhZGVyIHZhbHVlIGNoYXIASW52YWxpZCBoZWFkZXIgZmllbGQgY2hhcgBTcGFuIGNhbGxiYWNrIGVycm9yIGluIG9uX3ZlcnNpb24ASW52YWxpZCBtaW5vciB2ZXJzaW9uAEludmFsaWQgbWFqb3IgdmVyc2lvbgBFeHBlY3RlZCBzcGFjZSBhZnRlciB2ZXJzaW9uAEV4cGVjdGVkIENSTEYgYWZ0ZXIgdmVyc2lvbgBJbnZhbGlkIEhUVFAgdmVyc2lvbgBJbnZhbGlkIGhlYWRlciB0b2tlbgBTcGFuIGNhbGxiYWNrIGVycm9yIGluIG9uX3VybABJbnZhbGlkIGNoYXJhY3RlcnMgaW4gdXJsAFVuZXhwZWN0ZWQgc3RhcnQgY2hhciBpbiB1cmwARG91YmxlIEAgaW4gdXJsAFNwYW4gY2FsbGJhY2sgZXJyb3IgaW4gb25fcHJvdG9jb2wARW1wdHkgQ29udGVudC1MZW5ndGgASW52YWxpZCBjaGFyYWN0ZXIgaW4gQ29udGVudC1MZW5ndGgAVHJhbnNmZXItRW5jb2RpbmcgY2FuJ3QgYmUgcHJlc2VudCB3aXRoIENvbnRlbnQtTGVuZ3RoAER1cGxpY2F0ZSBDb250ZW50LUxlbmd0aABJbnZhbGlkIGNoYXIgaW4gdXJsIHBhdGgAQ29udGVudC1MZW5ndGggY2FuJ3QgYmUgcHJlc2VudCB3aXRoIFRyYW5zZmVyLUVuY29kaW5nAE1pc3NpbmcgZXhwZWN0ZWQgQ1IgYWZ0ZXIgY2h1bmsgc2l6ZQBFeHBlY3RlZCBMRiBhZnRlciBjaHVuayBzaXplAEludmFsaWQgY2hhcmFjdGVyIGluIGNodW5rIHNpemUAU3BhbiBjYWxsYmFjayBlcnJvciBpbiBvbl9oZWFkZXJfdmFsdWUAU3BhbiBjYWxsYmFjayBlcnJvciBpbiBvbl9jaHVua19leHRlbnNpb25fdmFsdWUASW52YWxpZCBjaGFyYWN0ZXIgaW4gY2h1bmsgZXh0ZW5zaW9ucyB2YWx1ZQBVbmV4cGVjdGVkIHdoaXRlc3BhY2UgYWZ0ZXIgaGVhZGVyIHZhbHVlAE1pc3NpbmcgZXhwZWN0ZWQgQ1IgYWZ0ZXIgaGVhZGVyIHZhbHVlAE1pc3NpbmcgZXhwZWN0ZWQgTEYgYWZ0ZXIgaGVhZGVyIHZhbHVlAEludmFsaWQgYFRyYW5zZmVyLUVuY29kaW5nYCBoZWFkZXIgdmFsdWUATWlzc2luZyBleHBlY3RlZCBDUiBhZnRlciBjaHVuayBleHRlbnNpb24gdmFsdWUASW52YWxpZCBjaGFyYWN0ZXIgaW4gY2h1bmsgZXh0ZW5zaW9ucyBxdW90ZSB2YWx1ZQBJbnZhbGlkIHF1b3RlZC1wYWlyIGluIGNodW5rIGV4dGVuc2lvbnMgcXVvdGVkIHZhbHVlAEludmFsaWQgY2hhcmFjdGVyIGluIGNodW5rIGV4dGVuc2lvbnMgcXVvdGVkIHZhbHVlAFBhdXNlZCBieSBvbl9oZWFkZXJzX2NvbXBsZXRlAEludmFsaWQgRU9GIHN0YXRlAG9uX3Jlc2V0IHBhdXNlAG9uX2NodW5rX2hlYWRlciBwYXVzZQBvbl9tZXNzYWdlX2JlZ2luIHBhdXNlAG9uX2NodW5rX2V4dGVuc2lvbl92YWx1ZSBwYXVzZQBvbl9zdGF0dXNfY29tcGxldGUgcGF1c2UAb25fdmVyc2lvbl9jb21wbGV0ZSBwYXVzZQBvbl91cmxfY29tcGxldGUgcGF1c2UAb25fcHJvdG9jb2xfY29tcGxldGUgcGF1c2UAb25fY2h1bmtfY29tcGxldGUgcGF1c2UAb25faGVhZGVyX3ZhbHVlX2NvbXBsZXRlIHBhdXNlAG9uX21lc3NhZ2VfY29tcGxldGUgcGF1c2UAb25fbWV0aG9kX2NvbXBsZXRlIHBhdXNlAG9uX2hlYWRlcl9maWVsZF9jb21wbGV0ZSBwYXVzZQBvbl9jaHVua19leHRlbnNpb25fbmFtZSBwYXVzZQBVbmV4cGVjdGVkIHNwYWNlIGFmdGVyIHN0YXJ0IGxpbmUATWlzc2luZyBleHBlY3RlZCBDUiBhZnRlciByZXNwb25zZSBsaW5lAFNwYW4gY2FsbGJhY2sgZXJyb3IgaW4gb25fY2h1bmtfZXh0ZW5zaW9uX25hbWUASW52YWxpZCBjaGFyYWN0ZXIgaW4gY2h1bmsgZXh0ZW5zaW9ucyBuYW1lAE1pc3NpbmcgZXhwZWN0ZWQgQ1IgYWZ0ZXIgY2h1bmsgZXh0ZW5zaW9uIG5hbWUASW52YWxpZCBzdGF0dXMgY29kZQBQYXVzZSBvbiBDT05ORUNUL1VwZ3JhZGUAUGF1c2Ugb24gUFJJL1VwZ3JhZGUARXhwZWN0ZWQgSFRUUC8yIENvbm5lY3Rpb24gUHJlZmFjZQBTcGFuIGNhbGxiYWNrIGVycm9yIGluIG9uX21ldGhvZABFeHBlY3RlZCBzcGFjZSBhZnRlciBtZXRob2QAU3BhbiBjYWxsYmFjayBlcnJvciBpbiBvbl9oZWFkZXJfZmllbGQAUGF1c2VkAEludmFsaWQgd29yZCBlbmNvdW50ZXJlZABJbnZhbGlkIG1ldGhvZCBlbmNvdW50ZXJlZABNaXNzaW5nIGV4cGVjdGVkIENSIGFmdGVyIGNodW5rIGRhdGEARXhwZWN0ZWQgTEYgYWZ0ZXIgY2h1bmsgZGF0YQBVbmV4cGVjdGVkIGNoYXIgaW4gdXJsIHNjaGVtYQBSZXF1ZXN0IGhhcyBpbnZhbGlkIGBUcmFuc2Zlci1FbmNvZGluZ2AARGF0YSBhZnRlciBgQ29ubmVjdGlvbjogY2xvc2VgAFNXSVRDSF9QUk9YWQBVU0VfUFJPWFkATUtBQ1RJVklUWQBVTlBST0NFU1NBQkxFX0VOVElUWQBRVUVSWQBDT1BZAE1PVkVEX1BFUk1BTkVOVExZAFRPT19FQVJMWQBOT1RJRlkARkFJTEVEX0RFUEVOREVOQ1kAQkFEX0dBVEVXQVkAUExBWQBQVVQAQ0hFQ0tPVVQAR0FURVdBWV9USU1FT1VUAFJFUVVFU1RfVElNRU9VVABORVRXT1JLX0NPTk5FQ1RfVElNRU9VVABDT05ORUNUSU9OX1RJTUVPVVQATE9HSU5fVElNRU9VVABORVRXT1JLX1JFQURfVElNRU9VVABQT1NUAE1JU0RJUkVDVEVEX1JFUVVFU1QAQ0xJRU5UX0NMT1NFRF9SRVFVRVNUAENMSUVOVF9DTE9TRURfTE9BRF9CQUxBTkNFRF9SRVFVRVNUAEJBRF9SRVFVRVNUAEhUVFBfUkVRVUVTVF9TRU5UX1RPX0hUVFBTX1BPUlQAUkVQT1JUAElNX0FfVEVBUE9UAFJFU0VUX0NPTlRFTlQATk9fQ09OVEVOVABQQVJUSUFMX0NPTlRFTlQASFBFX0lOVkFMSURfQ09OU1RBTlQASFBFX0NCX1JFU0VUAEdFVABIUEVfU1RSSUNUAENPTkZMSUNUAFRFTVBPUkFSWV9SRURJUkVDVABQRVJNQU5FTlRfUkVESVJFQ1QAQ09OTkVDVABNVUxUSV9TVEFUVVMASFBFX0lOVkFMSURfU1RBVFVTAFRPT19NQU5ZX1JFUVVFU1RTAEVBUkxZX0hJTlRTAFVOQVZBSUxBQkxFX0ZPUl9MRUdBTF9SRUFTT05TAE9QVElPTlMAU1dJVENISU5HX1BST1RPQ09MUwBWQVJJQU5UX0FMU09fTkVHT1RJQVRFUwBNVUxUSVBMRV9DSE9JQ0VTAElOVEVSTkFMX1NFUlZFUl9FUlJPUgBXRUJfU0VSVkVSX1VOS05PV05fRVJST1IAUkFJTEdVTl9FUlJPUgBJREVOVElUWV9QUk9WSURFUl9BVVRIRU5USUNBVElPTl9FUlJPUgBTU0xfQ0VSVElGSUNBVEVfRVJST1IASU5WQUxJRF9YX0ZPUldBUkRFRF9GT1IAU0VUX1BBUkFNRVRFUgBHRVRfUEFSQU1FVEVSAEhQRV9VU0VSAFNFRV9PVEhFUgBIUEVfQ0JfQ0hVTktfSEVBREVSAEV4cGVjdGVkIExGIGFmdGVyIENSAE1LQ0FMRU5EQVIAU0VUVVAAV0VCX1NFUlZFUl9JU19ET1dOAFRFQVJET1dOAEhQRV9DTE9TRURfQ09OTkVDVElPTgBIRVVSSVNUSUNfRVhQSVJBVElPTgBESVNDT05ORUNURURfT1BFUkFUSU9OAE5PTl9BVVRIT1JJVEFUSVZFX0lORk9STUFUSU9OAEhQRV9JTlZBTElEX1ZFUlNJT04ASFBFX0NCX01FU1NBR0VfQkVHSU4AU0lURV9JU19GUk9aRU4ASFBFX0lOVkFMSURfSEVBREVSX1RPS0VOAElOVkFMSURfVE9LRU4ARk9SQklEREVOAEVOSEFOQ0VfWU9VUl9DQUxNAEhQRV9JTlZBTElEX1VSTABCTE9DS0VEX0JZX1BBUkVOVEFMX0NPTlRST0wATUtDT0wAQUNMAEhQRV9JTlRFUk5BTABSRVFVRVNUX0hFQURFUl9GSUVMRFNfVE9PX0xBUkdFX1VOT0ZGSUNJQUwASFBFX09LAFVOTElOSwBVTkxPQ0sAUFJJAFJFVFJZX1dJVEgASFBFX0lOVkFMSURfQ09OVEVOVF9MRU5HVEgASFBFX1VORVhQRUNURURfQ09OVEVOVF9MRU5HVEgARkxVU0gAUFJPUFBBVENIAE0tU0VBUkNIAFVSSV9UT09fTE9ORwBQUk9DRVNTSU5HAE1JU0NFTExBTkVPVVNfUEVSU0lTVEVOVF9XQVJOSU5HAE1JU0NFTExBTkVPVVNfV0FSTklORwBIUEVfSU5WQUxJRF9UUkFOU0ZFUl9FTkNPRElORwBFeHBlY3RlZCBDUkxGAEhQRV9JTlZBTElEX0NIVU5LX1NJWkUATU9WRQBDT05USU5VRQBIUEVfQ0JfU1RBVFVTX0NPTVBMRVRFAEhQRV9DQl9IRUFERVJTX0NPTVBMRVRFAEhQRV9DQl9WRVJTSU9OX0NPTVBMRVRFAEhQRV9DQl9VUkxfQ09NUExFVEUASFBFX0NCX1BST1RPQ09MX0NPTVBMRVRFAEhQRV9DQl9DSFVOS19DT01QTEVURQBIUEVfQ0JfSEVBREVSX1ZBTFVFX0NPTVBMRVRFAEhQRV9DQl9DSFVOS19FWFRFTlNJT05fVkFMVUVfQ09NUExFVEUASFBFX0NCX0NIVU5LX0VYVEVOU0lPTl9OQU1FX0NPTVBMRVRFAEhQRV9DQl9NRVNTQUdFX0NPTVBMRVRFAEhQRV9DQl9NRVRIT0RfQ09NUExFVEUASFBFX0NCX0hFQURFUl9GSUVMRF9DT01QTEVURQBERUxFVEUASFBFX0lOVkFMSURfRU9GX1NUQVRFAElOVkFMSURfU1NMX0NFUlRJRklDQVRFAFBBVVNFAE5PX1JFU1BPTlNFAFVOU1VQUE9SVEVEX01FRElBX1RZUEUAR09ORQBOT1RfQUNDRVBUQUJMRQBTRVJWSUNFX1VOQVZBSUxBQkxFAFJBTkdFX05PVF9TQVRJU0ZJQUJMRQBPUklHSU5fSVNfVU5SRUFDSEFCTEUAUkVTUE9OU0VfSVNfU1RBTEUAUFVSR0UATUVSR0UAUkVRVUVTVF9IRUFERVJfRklFTERTX1RPT19MQVJHRQBSRVFVRVNUX0hFQURFUl9UT09fTEFSR0UAUEFZTE9BRF9UT09fTEFSR0UASU5TVUZGSUNJRU5UX1NUT1JBR0UASFBFX1BBVVNFRF9VUEdSQURFAEhQRV9QQVVTRURfSDJfVVBHUkFERQBTT1VSQ0UAQU5OT1VOQ0UAVFJBQ0UASFBFX1VORVhQRUNURURfU1BBQ0UAREVTQ1JJQkUAVU5TVUJTQ1JJQkUAUkVDT1JEAEhQRV9JTlZBTElEX01FVEhPRABOT1RfRk9VTkQAUFJPUEZJTkQAVU5CSU5EAFJFQklORABVTkFVVEhPUklaRUQATUVUSE9EX05PVF9BTExPV0VEAEhUVFBfVkVSU0lPTl9OT1RfU1VQUE9SVEVEAEFMUkVBRFlfUkVQT1JURUQAQUNDRVBURUQATk9UX0lNUExFTUVOVEVEAExPT1BfREVURUNURUQASFBFX0NSX0VYUEVDVEVEAEhQRV9MRl9FWFBFQ1RFRABDUkVBVEVEAElNX1VTRUQASFBFX1BBVVNFRABUSU1FT1VUX09DQ1VSRUQAUEFZTUVOVF9SRVFVSVJFRABQUkVDT05ESVRJT05fUkVRVUlSRUQAUFJPWFlfQVVUSEVOVElDQVRJT05fUkVRVUlSRUQATkVUV09SS19BVVRIRU5USUNBVElPTl9SRVFVSVJFRABMRU5HVEhfUkVRVUlSRUQAU1NMX0NFUlRJRklDQVRFX1JFUVVJUkVEAFVQR1JBREVfUkVRVUlSRUQAUEFHRV9FWFBJUkVEAFBSRUNPTkRJVElPTl9GQUlMRUQARVhQRUNUQVRJT05fRkFJTEVEAFJFVkFMSURBVElPTl9GQUlMRUQAU1NMX0hBTkRTSEFLRV9GQUlMRUQATE9DS0VEAFRSQU5TRk9STUFUSU9OX0FQUExJRUQATk9UX01PRElGSUVEAE5PVF9FWFRFTkRFRABCQU5EV0lEVEhfTElNSVRfRVhDRUVERUQAU0lURV9JU19PVkVSTE9BREVEAEhFQUQARXhwZWN0ZWQgSFRUUC8sIFJUU1AvIG9yIElDRS8A5xUAAK8VAACkEgAAkhoAACYWAACeFAAA2xkAAHkVAAB+EgAA/hQAADYVAAALFgAA2BYAAPMSAABCGAAArBYAABIVAAAUFwAA7xcAAEgUAABxFwAAshoAAGsZAAB+GQAANRQAAIIaAABEFwAA/RYAAB4YAACHFwAAqhkAAJMSAAAHGAAALBcAAMoXAACkFwAA5xUAAOcVAABYFwAAOxgAAKASAAAtHAAAwxEAAEgRAADeEgAAQhMAAKQZAAD9EAAA9xUAAKUVAADvFgAA+BkAAEoWAABWFgAA9RUAAAoaAAAIGgAAARoAAKsVAABCEgAA1xAAAEwRAAAFGQAAVBYAAB4RAADKGQAAyBkAAE4WAAD/GAAAcRQAAPAVAADuFQAAlBkAAPwVAAC/GQAAmxkAAHwUAABDEQAAcBgAAJUUAAAnFAAAGRQAANUSAADUGQAARBYAAPcQAEG5OwsBAQBB0DsL4AEBAQIBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEDAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQBBuj0LBAEAAAIAQdE9C14DBAMDAwMDAAADAwADAwADAwMDAwMDAwMDAAUAAAAAAAMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAAAAAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMAAwADAEG6PwsEAQAAAgBB0T8LXgMAAwMDAwMAAAMDAAMDAAMDAwMDAwMDAwMABAAFAAAAAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMAAAADAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwADAAMAQbDBAAsNbG9zZWVlcC1hbGl2ZQBBycEACwEBAEHgwQAL4AEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQABAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQBBycMACwEBAEHgwwAL5wEBAQEBAQEBAQEBAQECAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQABAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAWNodW5rZWQAQfHFAAteAQABAQEBAQAAAQEAAQEAAQEBAQEBAQEBAQAAAAAAAAABAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQAAAAEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAAEAAQBB0McACyFlY3Rpb25lbnQtbGVuZ3Rob25yb3h5LWNvbm5lY3Rpb24AQYDIAAsgcmFuc2Zlci1lbmNvZGluZ3BncmFkZQ0KDQpTTQ0KDQoAQanIAAsFAQIAAQMAQcDIAAtfBAUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUAQanKAAsFAQIAAQMAQcDKAAtfBAUFBgUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUAQanMAAsEAQAAAQBBwcwAC14CAgACAgICAgICAgICAgICAgICAgICAgICAgICAgIAAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAEGpzgALBQECAAEDAEHAzgALXwQFAAAFBQUFBQUFBQUFBQYFBQUFBQUFBQUFBQUABQAHCAUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQAFAAUABQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUAAAAFAEGp0AALBQEBAAEBAEHA0AALAQEAQdrQAAtBAgAAAAAAAAMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAAAAAAAAAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMAQanSAAsFAQEAAQEAQcDSAAsBAQBBytIACwYCAAAAAAIAQeHSAAs6AwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMAAAAAAAADAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwBBoNQAC50BTk9VTkNFRUNLT1VUTkVDVEVURUNSSUJFTFVTSEVURUFEU0VBUkNIUkdFQ1RJVklUWUxFTkRBUlZFT1RJRllQVElPTlNDSFNFQVlTVEFUQ0hHRVVFUllPUkRJUkVDVE9SVFJDSFBBUkFNRVRFUlVSQ0VCU0NSSUJFQVJET1dOQUNFSU5ETktDS1VCU0NSSUJFVFRQQ0VUU1BBRFRQLw==";
	let wasmBuffer;
	Object.defineProperty(module, "exports", { get: () => {
		return wasmBuffer ? wasmBuffer : wasmBuffer = Buffer$1.from(wasmBase64, "base64");
	} });
}) });

//#endregion
//#region node_modules/undici/lib/web/fetch/constants.js
var require_constants$2 = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/fetch/constants.js": ((exports, module) => {
	const corsSafeListedMethods = [
		"GET",
		"HEAD",
		"POST"
	];
	const corsSafeListedMethodsSet$1 = new Set(corsSafeListedMethods);
	const nullBodyStatus$2 = [
		101,
		204,
		205,
		304
	];
	const redirectStatus = [
		301,
		302,
		303,
		307,
		308
	];
	const redirectStatusSet$3 = new Set(redirectStatus);
	/**
	* @see https://fetch.spec.whatwg.org/#block-bad-port
	*/
	const badPorts = [
		"1",
		"7",
		"9",
		"11",
		"13",
		"15",
		"17",
		"19",
		"20",
		"21",
		"22",
		"23",
		"25",
		"37",
		"42",
		"43",
		"53",
		"69",
		"77",
		"79",
		"87",
		"95",
		"101",
		"102",
		"103",
		"104",
		"109",
		"110",
		"111",
		"113",
		"115",
		"117",
		"119",
		"123",
		"135",
		"137",
		"139",
		"143",
		"161",
		"179",
		"389",
		"427",
		"465",
		"512",
		"513",
		"514",
		"515",
		"526",
		"530",
		"531",
		"532",
		"540",
		"548",
		"554",
		"556",
		"563",
		"587",
		"601",
		"636",
		"989",
		"990",
		"993",
		"995",
		"1719",
		"1720",
		"1723",
		"2049",
		"3659",
		"4045",
		"4190",
		"5060",
		"5061",
		"6000",
		"6566",
		"6665",
		"6666",
		"6667",
		"6668",
		"6669",
		"6679",
		"6697",
		"10080"
	];
	const badPortsSet$1 = new Set(badPorts);
	/**
	* @see https://w3c.github.io/webappsec-referrer-policy/#referrer-policy-header
	*/
	const referrerPolicyTokens$1 = [
		"no-referrer",
		"no-referrer-when-downgrade",
		"same-origin",
		"origin",
		"strict-origin",
		"origin-when-cross-origin",
		"strict-origin-when-cross-origin",
		"unsafe-url"
	];
	/**
	* @see https://w3c.github.io/webappsec-referrer-policy/#referrer-policies
	*/
	const referrerPolicy$1 = ["", ...referrerPolicyTokens$1];
	const referrerPolicyTokensSet = new Set(referrerPolicyTokens$1);
	const requestRedirect$1 = [
		"follow",
		"manual",
		"error"
	];
	const safeMethods = [
		"GET",
		"HEAD",
		"OPTIONS",
		"TRACE"
	];
	const safeMethodsSet$1 = new Set(safeMethods);
	const requestMode$1 = [
		"navigate",
		"same-origin",
		"no-cors",
		"cors"
	];
	const requestCredentials$1 = [
		"omit",
		"same-origin",
		"include"
	];
	const requestCache$1 = [
		"default",
		"no-store",
		"reload",
		"no-cache",
		"force-cache",
		"only-if-cached"
	];
	/**
	* @see https://fetch.spec.whatwg.org/#request-body-header-name
	*/
	const requestBodyHeader$1 = [
		"content-encoding",
		"content-language",
		"content-location",
		"content-type",
		"content-length"
	];
	/**
	* @see https://fetch.spec.whatwg.org/#enumdef-requestduplex
	*/
	const requestDuplex$1 = ["half"];
	/**
	* @see http://fetch.spec.whatwg.org/#forbidden-method
	*/
	const forbiddenMethods = [
		"CONNECT",
		"TRACE",
		"TRACK"
	];
	const forbiddenMethodsSet$1 = new Set(forbiddenMethods);
	const subresource = [
		"audio",
		"audioworklet",
		"font",
		"image",
		"manifest",
		"paintworklet",
		"script",
		"style",
		"track",
		"video",
		"xslt",
		""
	];
	const subresourceSet$1 = new Set(subresource);
	module.exports = {
		subresource,
		forbiddenMethods,
		requestBodyHeader: requestBodyHeader$1,
		referrerPolicy: referrerPolicy$1,
		requestRedirect: requestRedirect$1,
		requestMode: requestMode$1,
		requestCredentials: requestCredentials$1,
		requestCache: requestCache$1,
		redirectStatus,
		corsSafeListedMethods,
		nullBodyStatus: nullBodyStatus$2,
		safeMethods,
		badPorts,
		requestDuplex: requestDuplex$1,
		subresourceSet: subresourceSet$1,
		badPortsSet: badPortsSet$1,
		redirectStatusSet: redirectStatusSet$3,
		corsSafeListedMethodsSet: corsSafeListedMethodsSet$1,
		safeMethodsSet: safeMethodsSet$1,
		forbiddenMethodsSet: forbiddenMethodsSet$1,
		referrerPolicyTokens: referrerPolicyTokensSet
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/fetch/global.js
var require_global$1 = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/fetch/global.js": ((exports, module) => {
	const globalOrigin = Symbol.for("undici.globalOrigin.1");
	function getGlobalOrigin$2() {
		return globalThis[globalOrigin];
	}
	function setGlobalOrigin$1(newOrigin) {
		if (newOrigin === void 0) {
			Object.defineProperty(globalThis, globalOrigin, {
				value: void 0,
				writable: true,
				enumerable: false,
				configurable: false
			});
			return;
		}
		const parsedURL = new URL(newOrigin);
		if (parsedURL.protocol !== "http:" && parsedURL.protocol !== "https:") throw new TypeError(`Only http & https urls are allowed, received ${parsedURL.protocol}`);
		Object.defineProperty(globalThis, globalOrigin, {
			value: parsedURL,
			writable: true,
			enumerable: false,
			configurable: false
		});
	}
	module.exports = {
		getGlobalOrigin: getGlobalOrigin$2,
		setGlobalOrigin: setGlobalOrigin$1
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/fetch/data-url.js
var require_data_url = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/fetch/data-url.js": ((exports, module) => {
	const assert$27 = __require("node:assert");
	const encoder = new TextEncoder();
	/**
	* @see https://mimesniff.spec.whatwg.org/#http-token-code-point
	*/
	const HTTP_TOKEN_CODEPOINTS$1 = /^[!#$%&'*+\-.^_|~A-Za-z0-9]+$/;
	const HTTP_WHITESPACE_REGEX = /[\u000A\u000D\u0009\u0020]/;
	const ASCII_WHITESPACE_REPLACE_REGEX = /[\u0009\u000A\u000C\u000D\u0020]/g;
	/**
	* @see https://mimesniff.spec.whatwg.org/#http-quoted-string-token-code-point
	*/
	const HTTP_QUOTED_STRING_TOKENS = /^[\u0009\u0020-\u007E\u0080-\u00FF]+$/;
	/** @param {URL} dataURL */
	function dataURLProcessor$1(dataURL) {
		assert$27(dataURL.protocol === "data:");
		let input = URLSerializer$4(dataURL, true);
		input = input.slice(5);
		const position = { position: 0 };
		let mimeType = collectASequenceOfCodePointsFast$2(",", input, position);
		const mimeTypeLength = mimeType.length;
		mimeType = removeASCIIWhitespace(mimeType, true, true);
		if (position.position >= input.length) return "failure";
		position.position++;
		const encodedBody = input.slice(mimeTypeLength + 1);
		let body = stringPercentDecode(encodedBody);
		if (/;(\u0020){0,}base64$/i.test(mimeType)) {
			const stringBody = isomorphicDecode$1(body);
			body = forgivingBase64(stringBody);
			if (body === "failure") return "failure";
			mimeType = mimeType.slice(0, -6);
			mimeType = mimeType.replace(/(\u0020)+$/, "");
			mimeType = mimeType.slice(0, -1);
		}
		if (mimeType.startsWith(";")) mimeType = "text/plain" + mimeType;
		let mimeTypeRecord = parseMIMEType$3(mimeType);
		if (mimeTypeRecord === "failure") mimeTypeRecord = parseMIMEType$3("text/plain;charset=US-ASCII");
		return {
			mimeType: mimeTypeRecord,
			body
		};
	}
	/**
	* @param {URL} url
	* @param {boolean} excludeFragment
	*/
	function URLSerializer$4(url, excludeFragment = false) {
		if (!excludeFragment) return url.href;
		const href = url.href;
		const hashLength = url.hash.length;
		const serialized = hashLength === 0 ? href : href.substring(0, href.length - hashLength);
		if (!hashLength && href.endsWith("#")) return serialized.slice(0, -1);
		return serialized;
	}
	/**
	* @param {(char: string) => boolean} condition
	* @param {string} input
	* @param {{ position: number }} position
	*/
	function collectASequenceOfCodePoints$1(condition, input, position) {
		let result = "";
		while (position.position < input.length && condition(input[position.position])) {
			result += input[position.position];
			position.position++;
		}
		return result;
	}
	/**
	* A faster collectASequenceOfCodePoints that only works when comparing a single character.
	* @param {string} char
	* @param {string} input
	* @param {{ position: number }} position
	*/
	function collectASequenceOfCodePointsFast$2(char, input, position) {
		const idx = input.indexOf(char, position.position);
		const start = position.position;
		if (idx === -1) {
			position.position = input.length;
			return input.slice(start);
		}
		position.position = idx;
		return input.slice(start, position.position);
	}
	/** @param {string} input */
	function stringPercentDecode(input) {
		const bytes = encoder.encode(input);
		return percentDecode(bytes);
	}
	/**
	* @param {number} byte
	*/
	function isHexCharByte(byte) {
		return byte >= 48 && byte <= 57 || byte >= 65 && byte <= 70 || byte >= 97 && byte <= 102;
	}
	/**
	* @param {number} byte
	*/
	function hexByteToNumber(byte) {
		return byte >= 48 && byte <= 57 ? byte - 48 : (byte & 223) - 55;
	}
	/** @param {Uint8Array} input */
	function percentDecode(input) {
		const length = input.length;
		/** @type {Uint8Array} */
		const output = new Uint8Array(length);
		let j = 0;
		for (let i = 0; i < length; ++i) {
			const byte = input[i];
			if (byte !== 37) output[j++] = byte;
			else if (byte === 37 && !(isHexCharByte(input[i + 1]) && isHexCharByte(input[i + 2]))) output[j++] = 37;
			else {
				output[j++] = hexByteToNumber(input[i + 1]) << 4 | hexByteToNumber(input[i + 2]);
				i += 2;
			}
		}
		return length === j ? output : output.subarray(0, j);
	}
	/** @param {string} input */
	function parseMIMEType$3(input) {
		input = removeHTTPWhitespace$1(input, true, true);
		const position = { position: 0 };
		const type = collectASequenceOfCodePointsFast$2("/", input, position);
		if (type.length === 0 || !HTTP_TOKEN_CODEPOINTS$1.test(type)) return "failure";
		if (position.position >= input.length) return "failure";
		position.position++;
		let subtype = collectASequenceOfCodePointsFast$2(";", input, position);
		subtype = removeHTTPWhitespace$1(subtype, false, true);
		if (subtype.length === 0 || !HTTP_TOKEN_CODEPOINTS$1.test(subtype)) return "failure";
		const typeLowercase = type.toLowerCase();
		const subtypeLowercase = subtype.toLowerCase();
		const mimeType = {
			type: typeLowercase,
			subtype: subtypeLowercase,
			parameters: /* @__PURE__ */ new Map(),
			essence: `${typeLowercase}/${subtypeLowercase}`
		};
		while (position.position < input.length) {
			position.position++;
			collectASequenceOfCodePoints$1((char) => HTTP_WHITESPACE_REGEX.test(char), input, position);
			let parameterName = collectASequenceOfCodePoints$1((char) => char !== ";" && char !== "=", input, position);
			parameterName = parameterName.toLowerCase();
			if (position.position < input.length) {
				if (input[position.position] === ";") continue;
				position.position++;
			}
			if (position.position >= input.length) break;
			let parameterValue = null;
			if (input[position.position] === "\"") {
				parameterValue = collectAnHTTPQuotedString$1(input, position, true);
				collectASequenceOfCodePointsFast$2(";", input, position);
			} else {
				parameterValue = collectASequenceOfCodePointsFast$2(";", input, position);
				parameterValue = removeHTTPWhitespace$1(parameterValue, false, true);
				if (parameterValue.length === 0) continue;
			}
			if (parameterName.length !== 0 && HTTP_TOKEN_CODEPOINTS$1.test(parameterName) && (parameterValue.length === 0 || HTTP_QUOTED_STRING_TOKENS.test(parameterValue)) && !mimeType.parameters.has(parameterName)) mimeType.parameters.set(parameterName, parameterValue);
		}
		return mimeType;
	}
	/** @param {string} data */
	function forgivingBase64(data) {
		data = data.replace(ASCII_WHITESPACE_REPLACE_REGEX, "");
		let dataLength = data.length;
		if (dataLength % 4 === 0) {
			if (data.charCodeAt(dataLength - 1) === 61) {
				--dataLength;
				if (data.charCodeAt(dataLength - 1) === 61) --dataLength;
			}
		}
		if (dataLength % 4 === 1) return "failure";
		if (/[^+/0-9A-Za-z]/.test(data.length === dataLength ? data : data.substring(0, dataLength))) return "failure";
		const buffer$1 = Buffer.from(data, "base64");
		return new Uint8Array(buffer$1.buffer, buffer$1.byteOffset, buffer$1.byteLength);
	}
	/**
	* @param {string} input
	* @param {{ position: number }} position
	* @param {boolean} [extractValue=false]
	*/
	function collectAnHTTPQuotedString$1(input, position, extractValue = false) {
		const positionStart = position.position;
		let value = "";
		assert$27(input[position.position] === "\"");
		position.position++;
		while (true) {
			value += collectASequenceOfCodePoints$1((char) => char !== "\"" && char !== "\\", input, position);
			if (position.position >= input.length) break;
			const quoteOrBackslash = input[position.position];
			position.position++;
			if (quoteOrBackslash === "\\") {
				if (position.position >= input.length) {
					value += "\\";
					break;
				}
				value += input[position.position];
				position.position++;
			} else {
				assert$27(quoteOrBackslash === "\"");
				break;
			}
		}
		if (extractValue) return value;
		return input.slice(positionStart, position.position);
	}
	/**
	* @see https://mimesniff.spec.whatwg.org/#serialize-a-mime-type
	*/
	function serializeAMimeType$3(mimeType) {
		assert$27(mimeType !== "failure");
		const { parameters, essence } = mimeType;
		let serialization = essence;
		for (let [name, value] of parameters.entries()) {
			serialization += ";";
			serialization += name;
			serialization += "=";
			if (!HTTP_TOKEN_CODEPOINTS$1.test(value)) {
				value = value.replace(/(\\|")/g, "\\$1");
				value = "\"" + value;
				value += "\"";
			}
			serialization += value;
		}
		return serialization;
	}
	/**
	* @see https://fetch.spec.whatwg.org/#http-whitespace
	* @param {number} char
	*/
	function isHTTPWhiteSpace(char) {
		return char === 13 || char === 10 || char === 9 || char === 32;
	}
	/**
	* @see https://fetch.spec.whatwg.org/#http-whitespace
	* @param {string} str
	* @param {boolean} [leading=true]
	* @param {boolean} [trailing=true]
	*/
	function removeHTTPWhitespace$1(str, leading = true, trailing = true) {
		return removeChars$2(str, leading, trailing, isHTTPWhiteSpace);
	}
	/**
	* @see https://infra.spec.whatwg.org/#ascii-whitespace
	* @param {number} char
	*/
	function isASCIIWhitespace(char) {
		return char === 13 || char === 10 || char === 9 || char === 12 || char === 32;
	}
	/**
	* @see https://infra.spec.whatwg.org/#strip-leading-and-trailing-ascii-whitespace
	* @param {string} str
	* @param {boolean} [leading=true]
	* @param {boolean} [trailing=true]
	*/
	function removeASCIIWhitespace(str, leading = true, trailing = true) {
		return removeChars$2(str, leading, trailing, isASCIIWhitespace);
	}
	/**
	* @param {string} str
	* @param {boolean} leading
	* @param {boolean} trailing
	* @param {(charCode: number) => boolean} predicate
	* @returns
	*/
	function removeChars$2(str, leading, trailing, predicate) {
		let lead = 0;
		let trail = str.length - 1;
		if (leading) while (lead < str.length && predicate(str.charCodeAt(lead))) lead++;
		if (trailing) while (trail > 0 && predicate(str.charCodeAt(trail))) trail--;
		return lead === 0 && trail === str.length - 1 ? str : str.slice(lead, trail + 1);
	}
	/**
	* @see https://infra.spec.whatwg.org/#isomorphic-decode
	* @param {Uint8Array} input
	* @returns {string}
	*/
	function isomorphicDecode$1(input) {
		const length = input.length;
		if (65535 > length) return String.fromCharCode.apply(null, input);
		let result = "";
		let i = 0;
		let addition = 65535;
		while (i < length) {
			if (i + addition > length) addition = length - i;
			result += String.fromCharCode.apply(null, input.subarray(i, i += addition));
		}
		return result;
	}
	/**
	* @see https://mimesniff.spec.whatwg.org/#minimize-a-supported-mime-type
	* @param {Exclude<ReturnType<typeof parseMIMEType>, 'failure'>} mimeType
	*/
	function minimizeSupportedMimeType$1(mimeType) {
		switch (mimeType.essence) {
			case "application/ecmascript":
			case "application/javascript":
			case "application/x-ecmascript":
			case "application/x-javascript":
			case "text/ecmascript":
			case "text/javascript":
			case "text/javascript1.0":
			case "text/javascript1.1":
			case "text/javascript1.2":
			case "text/javascript1.3":
			case "text/javascript1.4":
			case "text/javascript1.5":
			case "text/jscript":
			case "text/livescript":
			case "text/x-ecmascript":
			case "text/x-javascript": return "text/javascript";
			case "application/json":
			case "text/json": return "application/json";
			case "image/svg+xml": return "image/svg+xml";
			case "text/xml":
			case "application/xml": return "application/xml";
		}
		if (mimeType.subtype.endsWith("+json")) return "application/json";
		if (mimeType.subtype.endsWith("+xml")) return "application/xml";
		return "";
	}
	module.exports = {
		dataURLProcessor: dataURLProcessor$1,
		URLSerializer: URLSerializer$4,
		collectASequenceOfCodePoints: collectASequenceOfCodePoints$1,
		collectASequenceOfCodePointsFast: collectASequenceOfCodePointsFast$2,
		stringPercentDecode,
		parseMIMEType: parseMIMEType$3,
		collectAnHTTPQuotedString: collectAnHTTPQuotedString$1,
		serializeAMimeType: serializeAMimeType$3,
		removeChars: removeChars$2,
		removeHTTPWhitespace: removeHTTPWhitespace$1,
		minimizeSupportedMimeType: minimizeSupportedMimeType$1,
		HTTP_TOKEN_CODEPOINTS: HTTP_TOKEN_CODEPOINTS$1,
		isomorphicDecode: isomorphicDecode$1
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/webidl/index.js
var require_webidl = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/webidl/index.js": ((exports, module) => {
	const { types, inspect } = __require("node:util");
	const { markAsUncloneable } = __require("node:worker_threads");
	const UNDEFINED = 1;
	const BOOLEAN = 2;
	const STRING = 3;
	const SYMBOL = 4;
	const NUMBER = 5;
	const BIGINT = 6;
	const NULL = 7;
	const OBJECT = 8;
	const FunctionPrototypeSymbolHasInstance = Function.call.bind(Function.prototype[Symbol.hasInstance]);
	/** @type {import('../../../types/webidl').Webidl} */
	const webidl$16 = {
		converters: {},
		util: {},
		errors: {},
		is: {}
	};
	/**
	* @description Instantiate an error.
	*
	* @param {Object} opts
	* @param {string} opts.header
	* @param {string} opts.message
	* @returns {TypeError}
	*/
	webidl$16.errors.exception = function(message) {
		return /* @__PURE__ */ new TypeError(`${message.header}: ${message.message}`);
	};
	/**
	* @description Instantiate an error when conversion from one type to another has failed.
	*
	* @param {Object} opts
	* @param {string} opts.prefix
	* @param {string} opts.argument
	* @param {string[]} opts.types
	* @returns {TypeError}
	*/
	webidl$16.errors.conversionFailed = function(opts) {
		const plural = opts.types.length === 1 ? "" : " one of";
		const message = `${opts.argument} could not be converted to${plural}: ${opts.types.join(", ")}.`;
		return webidl$16.errors.exception({
			header: opts.prefix,
			message
		});
	};
	/**
	* @description Instantiate an error when an invalid argument is provided
	*
	* @param {Object} context
	* @param {string} context.prefix
	* @param {string} context.value
	* @param {string} context.type
	* @returns {TypeError}
	*/
	webidl$16.errors.invalidArgument = function(context) {
		return webidl$16.errors.exception({
			header: context.prefix,
			message: `"${context.value}" is an invalid ${context.type}.`
		});
	};
	webidl$16.brandCheck = function(V, I) {
		if (!FunctionPrototypeSymbolHasInstance(I, V)) {
			const err = /* @__PURE__ */ new TypeError("Illegal invocation");
			err.code = "ERR_INVALID_THIS";
			throw err;
		}
	};
	webidl$16.brandCheckMultiple = function(List) {
		const prototypes = List.map((c) => webidl$16.util.MakeTypeAssertion(c));
		return (V) => {
			if (prototypes.every((typeCheck) => !typeCheck(V))) {
				const err = /* @__PURE__ */ new TypeError("Illegal invocation");
				err.code = "ERR_INVALID_THIS";
				throw err;
			}
		};
	};
	webidl$16.argumentLengthCheck = function({ length }, min, ctx) {
		if (length < min) throw webidl$16.errors.exception({
			message: `${min} argument${min !== 1 ? "s" : ""} required, but${length ? " only" : ""} ${length} found.`,
			header: ctx
		});
	};
	webidl$16.illegalConstructor = function() {
		throw webidl$16.errors.exception({
			header: "TypeError",
			message: "Illegal constructor"
		});
	};
	webidl$16.util.MakeTypeAssertion = function(I) {
		return (O) => FunctionPrototypeSymbolHasInstance(I, O);
	};
	webidl$16.util.Type = function(V) {
		switch (typeof V) {
			case "undefined": return UNDEFINED;
			case "boolean": return BOOLEAN;
			case "string": return STRING;
			case "symbol": return SYMBOL;
			case "number": return NUMBER;
			case "bigint": return BIGINT;
			case "function":
			case "object":
				if (V === null) return NULL;
				return OBJECT;
		}
	};
	webidl$16.util.Types = {
		UNDEFINED,
		BOOLEAN,
		STRING,
		SYMBOL,
		NUMBER,
		BIGINT,
		NULL,
		OBJECT
	};
	webidl$16.util.TypeValueToString = function(o) {
		switch (webidl$16.util.Type(o)) {
			case UNDEFINED: return "Undefined";
			case BOOLEAN: return "Boolean";
			case STRING: return "String";
			case SYMBOL: return "Symbol";
			case NUMBER: return "Number";
			case BIGINT: return "BigInt";
			case NULL: return "Null";
			case OBJECT: return "Object";
		}
	};
	webidl$16.util.markAsUncloneable = markAsUncloneable || (() => {});
	webidl$16.util.ConvertToInt = function(V, bitLength, signedness, opts) {
		let upperBound;
		let lowerBound;
		if (bitLength === 64) {
			upperBound = Math.pow(2, 53) - 1;
			if (signedness === "unsigned") lowerBound = 0;
			else lowerBound = Math.pow(-2, 53) + 1;
		} else if (signedness === "unsigned") {
			lowerBound = 0;
			upperBound = Math.pow(2, bitLength) - 1;
		} else {
			lowerBound = Math.pow(-2, bitLength) - 1;
			upperBound = Math.pow(2, bitLength - 1) - 1;
		}
		let x = Number(V);
		if (x === 0) x = 0;
		if (opts?.enforceRange === true) {
			if (Number.isNaN(x) || x === Number.POSITIVE_INFINITY || x === Number.NEGATIVE_INFINITY) throw webidl$16.errors.exception({
				header: "Integer conversion",
				message: `Could not convert ${webidl$16.util.Stringify(V)} to an integer.`
			});
			x = webidl$16.util.IntegerPart(x);
			if (x < lowerBound || x > upperBound) throw webidl$16.errors.exception({
				header: "Integer conversion",
				message: `Value must be between ${lowerBound}-${upperBound}, got ${x}.`
			});
			return x;
		}
		if (!Number.isNaN(x) && opts?.clamp === true) {
			x = Math.min(Math.max(x, lowerBound), upperBound);
			if (Math.floor(x) % 2 === 0) x = Math.floor(x);
			else x = Math.ceil(x);
			return x;
		}
		if (Number.isNaN(x) || x === 0 && Object.is(0, x) || x === Number.POSITIVE_INFINITY || x === Number.NEGATIVE_INFINITY) return 0;
		x = webidl$16.util.IntegerPart(x);
		x = x % Math.pow(2, bitLength);
		if (signedness === "signed" && x >= Math.pow(2, bitLength) - 1) return x - Math.pow(2, bitLength);
		return x;
	};
	webidl$16.util.IntegerPart = function(n) {
		const r = Math.floor(Math.abs(n));
		if (n < 0) return -1 * r;
		return r;
	};
	webidl$16.util.Stringify = function(V) {
		switch (webidl$16.util.Type(V)) {
			case SYMBOL: return `Symbol(${V.description})`;
			case OBJECT: return inspect(V);
			case STRING: return `"${V}"`;
			case BIGINT: return `${V}n`;
			default: return `${V}`;
		}
	};
	webidl$16.sequenceConverter = function(converter) {
		return (V, prefix, argument, Iterable) => {
			if (webidl$16.util.Type(V) !== OBJECT) throw webidl$16.errors.exception({
				header: prefix,
				message: `${argument} (${webidl$16.util.Stringify(V)}) is not iterable.`
			});
			/** @type {Generator} */
			const method = typeof Iterable === "function" ? Iterable() : V?.[Symbol.iterator]?.();
			const seq = [];
			let index = 0;
			if (method === void 0 || typeof method.next !== "function") throw webidl$16.errors.exception({
				header: prefix,
				message: `${argument} is not iterable.`
			});
			while (true) {
				const { done, value } = method.next();
				if (done) break;
				seq.push(converter(value, prefix, `${argument}[${index++}]`));
			}
			return seq;
		};
	};
	webidl$16.recordConverter = function(keyConverter, valueConverter) {
		return (O, prefix, argument) => {
			if (webidl$16.util.Type(O) !== OBJECT) throw webidl$16.errors.exception({
				header: prefix,
				message: `${argument} ("${webidl$16.util.TypeValueToString(O)}") is not an Object.`
			});
			const result = {};
			if (!types.isProxy(O)) {
				const keys$1 = [...Object.getOwnPropertyNames(O), ...Object.getOwnPropertySymbols(O)];
				for (const key of keys$1) {
					const keyName = webidl$16.util.Stringify(key);
					const typedKey = keyConverter(key, prefix, `Key ${keyName} in ${argument}`);
					result[typedKey] = valueConverter(O[key], prefix, `${argument}[${keyName}]`);
				}
				return result;
			}
			const keys = Reflect.ownKeys(O);
			for (const key of keys) if (Reflect.getOwnPropertyDescriptor(O, key)?.enumerable) {
				const typedKey = keyConverter(key, prefix, argument);
				result[typedKey] = valueConverter(O[key], prefix, argument);
			}
			return result;
		};
	};
	webidl$16.interfaceConverter = function(TypeCheck, name) {
		return (V, prefix, argument) => {
			if (!TypeCheck(V)) throw webidl$16.errors.exception({
				header: prefix,
				message: `Expected ${argument} ("${webidl$16.util.Stringify(V)}") to be an instance of ${name}.`
			});
			return V;
		};
	};
	webidl$16.dictionaryConverter = function(converters) {
		return (dictionary, prefix, argument) => {
			const dict = {};
			if (dictionary != null && webidl$16.util.Type(dictionary) !== OBJECT) throw webidl$16.errors.exception({
				header: prefix,
				message: `Expected ${dictionary} to be one of: Null, Undefined, Object.`
			});
			for (const options of converters) {
				const { key, defaultValue, required, converter } = options;
				if (required === true) {
					if (dictionary == null || !Object.hasOwn(dictionary, key)) throw webidl$16.errors.exception({
						header: prefix,
						message: `Missing required key "${key}".`
					});
				}
				let value = dictionary?.[key];
				const hasDefault = defaultValue !== void 0;
				if (hasDefault && value === void 0) value = defaultValue();
				if (required || hasDefault || value !== void 0) {
					value = converter(value, prefix, `${argument}.${key}`);
					if (options.allowedValues && !options.allowedValues.includes(value)) throw webidl$16.errors.exception({
						header: prefix,
						message: `${value} is not an accepted type. Expected one of ${options.allowedValues.join(", ")}.`
					});
					dict[key] = value;
				}
			}
			return dict;
		};
	};
	webidl$16.nullableConverter = function(converter) {
		return (V, prefix, argument) => {
			if (V === null) return V;
			return converter(V, prefix, argument);
		};
	};
	/**
	* @param {*} value
	* @returns {boolean}
	*/
	webidl$16.is.USVString = function(value) {
		return typeof value === "string" && value.isWellFormed();
	};
	webidl$16.is.ReadableStream = webidl$16.util.MakeTypeAssertion(ReadableStream);
	webidl$16.is.Blob = webidl$16.util.MakeTypeAssertion(Blob);
	webidl$16.is.URLSearchParams = webidl$16.util.MakeTypeAssertion(URLSearchParams);
	webidl$16.is.File = webidl$16.util.MakeTypeAssertion(File);
	webidl$16.is.URL = webidl$16.util.MakeTypeAssertion(URL);
	webidl$16.is.AbortSignal = webidl$16.util.MakeTypeAssertion(AbortSignal);
	webidl$16.is.MessagePort = webidl$16.util.MakeTypeAssertion(MessagePort);
	webidl$16.converters.DOMString = function(V, prefix, argument, opts) {
		if (V === null && opts?.legacyNullToEmptyString) return "";
		if (typeof V === "symbol") throw webidl$16.errors.exception({
			header: prefix,
			message: `${argument} is a symbol, which cannot be converted to a DOMString.`
		});
		return String(V);
	};
	webidl$16.converters.ByteString = function(V, prefix, argument) {
		if (typeof V === "symbol") throw webidl$16.errors.exception({
			header: prefix,
			message: `${argument} is a symbol, which cannot be converted to a ByteString.`
		});
		const x = String(V);
		for (let index = 0; index < x.length; index++) if (x.charCodeAt(index) > 255) throw new TypeError(`Cannot convert argument to a ByteString because the character at index ${index} has a value of ${x.charCodeAt(index)} which is greater than 255.`);
		return x;
	};
	/**
	* @param {unknown} value
	* @returns {string}
	* @see https://webidl.spec.whatwg.org/#es-USVString
	*/
	webidl$16.converters.USVString = function(value) {
		if (typeof value === "string") return value.toWellFormed();
		return `${value}`.toWellFormed();
	};
	webidl$16.converters.boolean = function(V) {
		return Boolean(V);
	};
	webidl$16.converters.any = function(V) {
		return V;
	};
	webidl$16.converters["long long"] = function(V, prefix, argument) {
		return webidl$16.util.ConvertToInt(V, 64, "signed", void 0, prefix, argument);
	};
	webidl$16.converters["unsigned long long"] = function(V, prefix, argument) {
		return webidl$16.util.ConvertToInt(V, 64, "unsigned", void 0, prefix, argument);
	};
	webidl$16.converters["unsigned long"] = function(V, prefix, argument) {
		return webidl$16.util.ConvertToInt(V, 32, "unsigned", void 0, prefix, argument);
	};
	webidl$16.converters["unsigned short"] = function(V, prefix, argument, opts) {
		return webidl$16.util.ConvertToInt(V, 16, "unsigned", opts, prefix, argument);
	};
	webidl$16.converters.ArrayBuffer = function(V, prefix, argument, opts) {
		if (webidl$16.util.Type(V) !== OBJECT || !types.isAnyArrayBuffer(V)) throw webidl$16.errors.conversionFailed({
			prefix,
			argument: `${argument} ("${webidl$16.util.Stringify(V)}")`,
			types: ["ArrayBuffer"]
		});
		if (opts?.allowShared === false && types.isSharedArrayBuffer(V)) throw webidl$16.errors.exception({
			header: "ArrayBuffer",
			message: "SharedArrayBuffer is not allowed."
		});
		if (V.resizable || V.growable) throw webidl$16.errors.exception({
			header: "ArrayBuffer",
			message: "Received a resizable ArrayBuffer."
		});
		return V;
	};
	webidl$16.converters.TypedArray = function(V, T, prefix, name, opts) {
		if (webidl$16.util.Type(V) !== OBJECT || !types.isTypedArray(V) || V.constructor.name !== T.name) throw webidl$16.errors.conversionFailed({
			prefix,
			argument: `${name} ("${webidl$16.util.Stringify(V)}")`,
			types: [T.name]
		});
		if (opts?.allowShared === false && types.isSharedArrayBuffer(V.buffer)) throw webidl$16.errors.exception({
			header: "ArrayBuffer",
			message: "SharedArrayBuffer is not allowed."
		});
		if (V.buffer.resizable || V.buffer.growable) throw webidl$16.errors.exception({
			header: "ArrayBuffer",
			message: "Received a resizable ArrayBuffer."
		});
		return V;
	};
	webidl$16.converters.DataView = function(V, prefix, name, opts) {
		if (webidl$16.util.Type(V) !== OBJECT || !types.isDataView(V)) throw webidl$16.errors.exception({
			header: prefix,
			message: `${name} is not a DataView.`
		});
		if (opts?.allowShared === false && types.isSharedArrayBuffer(V.buffer)) throw webidl$16.errors.exception({
			header: "ArrayBuffer",
			message: "SharedArrayBuffer is not allowed."
		});
		if (V.buffer.resizable || V.buffer.growable) throw webidl$16.errors.exception({
			header: "ArrayBuffer",
			message: "Received a resizable ArrayBuffer."
		});
		return V;
	};
	webidl$16.converters["sequence<ByteString>"] = webidl$16.sequenceConverter(webidl$16.converters.ByteString);
	webidl$16.converters["sequence<sequence<ByteString>>"] = webidl$16.sequenceConverter(webidl$16.converters["sequence<ByteString>"]);
	webidl$16.converters["record<ByteString, ByteString>"] = webidl$16.recordConverter(webidl$16.converters.ByteString, webidl$16.converters.ByteString);
	webidl$16.converters.Blob = webidl$16.interfaceConverter(webidl$16.is.Blob, "Blob");
	webidl$16.converters.AbortSignal = webidl$16.interfaceConverter(webidl$16.is.AbortSignal, "AbortSignal");
	module.exports = { webidl: webidl$16 };
}) });

//#endregion
//#region node_modules/undici/lib/web/fetch/util.js
var require_util$4 = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/fetch/util.js": ((exports, module) => {
	const { Transform: Transform$2 } = __require("node:stream");
	const zlib$1 = __require("node:zlib");
	const { redirectStatusSet: redirectStatusSet$2, referrerPolicyTokens, badPortsSet } = require_constants$2();
	const { getGlobalOrigin: getGlobalOrigin$1 } = require_global$1();
	const { collectASequenceOfCodePoints, collectAnHTTPQuotedString, removeChars: removeChars$1, parseMIMEType: parseMIMEType$2 } = require_data_url();
	const { performance: performance$1 } = __require("node:perf_hooks");
	const { ReadableStreamFrom: ReadableStreamFrom$2, isValidHTTPToken: isValidHTTPToken$1, normalizedMethodRecordsBase: normalizedMethodRecordsBase$1 } = require_util$5();
	const assert$26 = __require("node:assert");
	const { isUint8Array } = __require("node:util/types");
	const { webidl: webidl$15 } = require_webidl();
	function responseURL(response) {
		const urlList = response.urlList;
		const length = urlList.length;
		return length === 0 ? null : urlList[length - 1].toString();
	}
	function responseLocationURL$1(response, requestFragment) {
		if (!redirectStatusSet$2.has(response.status)) return null;
		let location = response.headersList.get("location", true);
		if (location !== null && isValidHeaderValue$1(location)) {
			if (!isValidEncodedURL(location)) location = normalizeBinaryStringToUtf8(location);
			location = new URL(location, responseURL(response));
		}
		if (location && !location.hash) location.hash = requestFragment;
		return location;
	}
	/**
	* @see https://www.rfc-editor.org/rfc/rfc1738#section-2.2
	* @param {string} url
	* @returns {boolean}
	*/
	function isValidEncodedURL(url) {
		for (let i = 0; i < url.length; ++i) {
			const code = url.charCodeAt(i);
			if (code > 126 || code < 32) return false;
		}
		return true;
	}
	/**
	* If string contains non-ASCII characters, assumes it's UTF-8 encoded and decodes it.
	* Since UTF-8 is a superset of ASCII, this will work for ASCII strings as well.
	* @param {string} value
	* @returns {string}
	*/
	function normalizeBinaryStringToUtf8(value) {
		return Buffer.from(value, "binary").toString("utf8");
	}
	/** @returns {URL} */
	function requestCurrentURL$1(request$2) {
		return request$2.urlList[request$2.urlList.length - 1];
	}
	function requestBadPort$1(request$2) {
		const url = requestCurrentURL$1(request$2);
		if (urlIsHttpHttpsScheme$2(url) && badPortsSet.has(url.port)) return "blocked";
		return "allowed";
	}
	function isErrorLike$2(object) {
		return object instanceof Error || object?.constructor?.name === "Error" || object?.constructor?.name === "DOMException";
	}
	function isValidReasonPhrase$1(statusText) {
		for (let i = 0; i < statusText.length; ++i) {
			const c = statusText.charCodeAt(i);
			if (!(c === 9 || c >= 32 && c <= 126 || c >= 128 && c <= 255)) return false;
		}
		return true;
	}
	/**
	* @see https://fetch.spec.whatwg.org/#header-name
	* @param {string} potentialValue
	*/
	const isValidHeaderName$2 = isValidHTTPToken$1;
	/**
	* @see https://fetch.spec.whatwg.org/#header-value
	* @param {string} potentialValue
	*/
	function isValidHeaderValue$1(potentialValue) {
		return (potentialValue[0] === "	" || potentialValue[0] === " " || potentialValue[potentialValue.length - 1] === "	" || potentialValue[potentialValue.length - 1] === " " || potentialValue.includes("\n") || potentialValue.includes("\r") || potentialValue.includes("\0")) === false;
	}
	/**
	* Parse a referrer policy from a Referrer-Policy header
	* @see https://w3c.github.io/webappsec-referrer-policy/#parse-referrer-policy-from-header
	*/
	function parseReferrerPolicy(actualResponse) {
		const policyHeader = (actualResponse.headersList.get("referrer-policy", true) ?? "").split(",");
		let policy = "";
		if (policyHeader.length) for (let i = policyHeader.length; i !== 0; i--) {
			const token = policyHeader[i - 1].trim();
			if (referrerPolicyTokens.has(token)) {
				policy = token;
				break;
			}
		}
		return policy;
	}
	/**
	* Given a request request and a response actualResponse, this algorithm
	* updates request’s referrer policy according to the Referrer-Policy
	* header (if any) in actualResponse.
	* @see https://w3c.github.io/webappsec-referrer-policy/#set-requests-referrer-policy-on-redirect
	* @param {import('./request').Request} request
	* @param {import('./response').Response} actualResponse
	*/
	function setRequestReferrerPolicyOnRedirect$1(request$2, actualResponse) {
		const policy = parseReferrerPolicy(actualResponse);
		if (policy !== "") request$2.referrerPolicy = policy;
	}
	function crossOriginResourcePolicyCheck$1() {
		return "allowed";
	}
	function corsCheck$1() {
		return "success";
	}
	function TAOCheck$1() {
		return "success";
	}
	function appendFetchMetadata$1(httpRequest) {
		let header = null;
		header = httpRequest.mode;
		httpRequest.headersList.set("sec-fetch-mode", header, true);
	}
	function appendRequestOriginHeader$1(request$2) {
		let serializedOrigin = request$2.origin;
		if (serializedOrigin === "client" || serializedOrigin === void 0) return;
		if (request$2.responseTainting === "cors" || request$2.mode === "websocket") request$2.headersList.append("origin", serializedOrigin, true);
		else if (request$2.method !== "GET" && request$2.method !== "HEAD") {
			switch (request$2.referrerPolicy) {
				case "no-referrer":
					serializedOrigin = null;
					break;
				case "no-referrer-when-downgrade":
				case "strict-origin":
				case "strict-origin-when-cross-origin":
					if (request$2.origin && urlHasHttpsScheme$1(request$2.origin) && !urlHasHttpsScheme$1(requestCurrentURL$1(request$2))) serializedOrigin = null;
					break;
				case "same-origin":
					if (!sameOrigin$2(request$2, requestCurrentURL$1(request$2))) serializedOrigin = null;
					break;
				default:
			}
			request$2.headersList.append("origin", serializedOrigin, true);
		}
	}
	function coarsenTime(timestamp, crossOriginIsolatedCapability) {
		return timestamp;
	}
	function clampAndCoarsenConnectionTimingInfo$1(connectionTimingInfo, defaultStartTime, crossOriginIsolatedCapability) {
		if (!connectionTimingInfo?.startTime || connectionTimingInfo.startTime < defaultStartTime) return {
			domainLookupStartTime: defaultStartTime,
			domainLookupEndTime: defaultStartTime,
			connectionStartTime: defaultStartTime,
			connectionEndTime: defaultStartTime,
			secureConnectionStartTime: defaultStartTime,
			ALPNNegotiatedProtocol: connectionTimingInfo?.ALPNNegotiatedProtocol
		};
		return {
			domainLookupStartTime: coarsenTime(connectionTimingInfo.domainLookupStartTime, crossOriginIsolatedCapability),
			domainLookupEndTime: coarsenTime(connectionTimingInfo.domainLookupEndTime, crossOriginIsolatedCapability),
			connectionStartTime: coarsenTime(connectionTimingInfo.connectionStartTime, crossOriginIsolatedCapability),
			connectionEndTime: coarsenTime(connectionTimingInfo.connectionEndTime, crossOriginIsolatedCapability),
			secureConnectionStartTime: coarsenTime(connectionTimingInfo.secureConnectionStartTime, crossOriginIsolatedCapability),
			ALPNNegotiatedProtocol: connectionTimingInfo.ALPNNegotiatedProtocol
		};
	}
	function coarsenedSharedCurrentTime$1(crossOriginIsolatedCapability) {
		return coarsenTime(performance$1.now(), crossOriginIsolatedCapability);
	}
	function createOpaqueTimingInfo$1(timingInfo) {
		return {
			startTime: timingInfo.startTime ?? 0,
			redirectStartTime: 0,
			redirectEndTime: 0,
			postRedirectStartTime: timingInfo.startTime ?? 0,
			finalServiceWorkerStartTime: 0,
			finalNetworkResponseStartTime: 0,
			finalNetworkRequestStartTime: 0,
			endTime: 0,
			encodedBodySize: 0,
			decodedBodySize: 0,
			finalConnectionTimingInfo: null
		};
	}
	function makePolicyContainer$1() {
		return { referrerPolicy: "strict-origin-when-cross-origin" };
	}
	function clonePolicyContainer$1(policyContainer) {
		return { referrerPolicy: policyContainer.referrerPolicy };
	}
	/**
	* Determine request’s Referrer
	*
	* @see https://w3c.github.io/webappsec-referrer-policy/#determine-requests-referrer
	*/
	function determineRequestsReferrer$1(request$2) {
		const policy = request$2.referrerPolicy;
		assert$26(policy);
		let referrerSource = null;
		if (request$2.referrer === "client") {
			const globalOrigin$1 = getGlobalOrigin$1();
			if (!globalOrigin$1 || globalOrigin$1.origin === "null") return "no-referrer";
			referrerSource = new URL(globalOrigin$1);
		} else if (webidl$15.is.URL(request$2.referrer)) referrerSource = request$2.referrer;
		let referrerURL = stripURLForReferrer(referrerSource);
		const referrerOrigin = stripURLForReferrer(referrerSource, true);
		if (referrerURL.toString().length > 4096) referrerURL = referrerOrigin;
		switch (policy) {
			case "no-referrer": return "no-referrer";
			case "origin":
				if (referrerOrigin != null) return referrerOrigin;
				return stripURLForReferrer(referrerSource, true);
			case "unsafe-url": return referrerURL;
			case "strict-origin": {
				const currentURL = requestCurrentURL$1(request$2);
				if (isURLPotentiallyTrustworthy(referrerURL) && !isURLPotentiallyTrustworthy(currentURL)) return "no-referrer";
				return referrerOrigin;
			}
			case "strict-origin-when-cross-origin": {
				const currentURL = requestCurrentURL$1(request$2);
				if (sameOrigin$2(referrerURL, currentURL)) return referrerURL;
				if (isURLPotentiallyTrustworthy(referrerURL) && !isURLPotentiallyTrustworthy(currentURL)) return "no-referrer";
				return referrerOrigin;
			}
			case "same-origin":
				if (sameOrigin$2(request$2, referrerURL)) return referrerURL;
				return "no-referrer";
			case "origin-when-cross-origin":
				if (sameOrigin$2(request$2, referrerURL)) return referrerURL;
				return referrerOrigin;
			case "no-referrer-when-downgrade": {
				const currentURL = requestCurrentURL$1(request$2);
				if (isURLPotentiallyTrustworthy(referrerURL) && !isURLPotentiallyTrustworthy(currentURL)) return "no-referrer";
				return referrerOrigin;
			}
		}
	}
	/**
	* Certain portions of URLs must not be included when sending a URL as the
	* value of a `Referer` header: a URLs fragment, username, and password
	* components must be stripped from the URL before it’s sent out. This
	* algorithm accepts a origin-only flag, which defaults to false. If set to
	* true, the algorithm will additionally remove the URL’s path and query
	* components, leaving only the scheme, host, and port.
	*
	* @see https://w3c.github.io/webappsec-referrer-policy/#strip-url
	* @param {URL} url
	* @param {boolean} [originOnly=false]
	*/
	function stripURLForReferrer(url, originOnly = false) {
		assert$26(webidl$15.is.URL(url));
		url = new URL(url);
		if (urlIsLocal$1(url)) return "no-referrer";
		url.username = "";
		url.password = "";
		url.hash = "";
		if (originOnly === true) {
			url.pathname = "";
			url.search = "";
		}
		return url;
	}
	const potentialleTrustworthyIPv4RegExp = /* @__PURE__ */ new RegExp("^(?:(?:127\\.)(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\\.){2}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[1-9]))$");
	const potentialleTrustworthyIPv6RegExp = /* @__PURE__ */ new RegExp("^(?:(?:(?:0{1,4}):){7}(?:(?:0{0,3}1))|(?:(?:0{1,4}):){1,6}(?::(?:0{0,3}1))|(?:::(?:0{0,3}1))|)$");
	/**
	* Check if host matches one of the CIDR notations 127.0.0.0/8 or ::1/128.
	*
	* @param {string} origin
	* @returns {boolean}
	*/
	function isOriginIPPotentiallyTrustworthy(origin) {
		if (origin.includes(":")) {
			if (origin[0] === "[" && origin[origin.length - 1] === "]") origin = origin.slice(1, -1);
			return potentialleTrustworthyIPv6RegExp.test(origin);
		}
		return potentialleTrustworthyIPv4RegExp.test(origin);
	}
	/**
	* A potentially trustworthy origin is one which a user agent can generally
	* trust as delivering data securely.
	*
	* Return value `true` means `Potentially Trustworthy`.
	* Return value `false` means `Not Trustworthy`.
	*
	* @see https://w3c.github.io/webappsec-secure-contexts/#is-origin-trustworthy
	* @param {string} origin
	* @returns {boolean}
	*/
	function isOriginPotentiallyTrustworthy(origin) {
		if (origin == null || origin === "null") return false;
		origin = new URL(origin);
		if (origin.protocol === "https:" || origin.protocol === "wss:") return true;
		if (isOriginIPPotentiallyTrustworthy(origin.hostname)) return true;
		if (origin.hostname === "localhost" || origin.hostname === "localhost.") return true;
		if (origin.hostname.endsWith(".localhost") || origin.hostname.endsWith(".localhost.")) return true;
		if (origin.protocol === "file:") return true;
		return false;
	}
	/**
	* A potentially trustworthy URL is one which either inherits context from its
	* creator (about:blank, about:srcdoc, data) or one whose origin is a
	* potentially trustworthy origin.
	*
	* Return value `true` means `Potentially Trustworthy`.
	* Return value `false` means `Not Trustworthy`.
	*
	* @see https://www.w3.org/TR/secure-contexts/#is-url-trustworthy
	* @param {URL} url
	* @returns {boolean}
	*/
	function isURLPotentiallyTrustworthy(url) {
		if (!webidl$15.is.URL(url)) return false;
		if (url.href === "about:blank" || url.href === "about:srcdoc") return true;
		if (url.protocol === "data:") return true;
		if (url.protocol === "blob:") return true;
		return isOriginPotentiallyTrustworthy(url.origin);
	}
	function tryUpgradeRequestToAPotentiallyTrustworthyURL$1(request$2) {}
	/**
	* @link {https://html.spec.whatwg.org/multipage/origin.html#same-origin}
	* @param {URL} A
	* @param {URL} B
	*/
	function sameOrigin$2(A, B) {
		if (A.origin === B.origin && A.origin === "null") return true;
		if (A.protocol === B.protocol && A.hostname === B.hostname && A.port === B.port) return true;
		return false;
	}
	function isAborted$2(fetchParams) {
		return fetchParams.controller.state === "aborted";
	}
	function isCancelled$2(fetchParams) {
		return fetchParams.controller.state === "aborted" || fetchParams.controller.state === "terminated";
	}
	/**
	* @see https://fetch.spec.whatwg.org/#concept-method-normalize
	* @param {string} method
	*/
	function normalizeMethod(method) {
		return normalizedMethodRecordsBase$1[method.toLowerCase()] ?? method;
	}
	function serializeJavascriptValueToJSONString$1(value) {
		const result = JSON.stringify(value);
		if (result === void 0) throw new TypeError("Value is not JSON serializable");
		assert$26(typeof result === "string");
		return result;
	}
	const esIteratorPrototype = Object.getPrototypeOf(Object.getPrototypeOf([][Symbol.iterator]()));
	/**
	* @see https://webidl.spec.whatwg.org/#dfn-iterator-prototype-object
	* @param {string} name name of the instance
	* @param {((target: any) => any)} kInternalIterator
	* @param {string | number} [keyIndex]
	* @param {string | number} [valueIndex]
	*/
	function createIterator(name, kInternalIterator, keyIndex = 0, valueIndex = 1) {
		class FastIterableIterator {
			/** @type {any} */
			#target;
			/** @type {'key' | 'value' | 'key+value'} */
			#kind;
			/** @type {number} */
			#index;
			/**
			* @see https://webidl.spec.whatwg.org/#dfn-default-iterator-object
			* @param {unknown} target
			* @param {'key' | 'value' | 'key+value'} kind
			*/
			constructor(target, kind) {
				this.#target = target;
				this.#kind = kind;
				this.#index = 0;
			}
			next() {
				if (typeof this !== "object" || this === null || !(#target in this)) throw new TypeError(`'next' called on an object that does not implement interface ${name} Iterator.`);
				const index = this.#index;
				const values = kInternalIterator(this.#target);
				const len = values.length;
				if (index >= len) return {
					value: void 0,
					done: true
				};
				const { [keyIndex]: key, [valueIndex]: value } = values[index];
				this.#index = index + 1;
				let result;
				switch (this.#kind) {
					case "key":
						result = key;
						break;
					case "value":
						result = value;
						break;
					case "key+value":
						result = [key, value];
						break;
				}
				return {
					value: result,
					done: false
				};
			}
		}
		delete FastIterableIterator.prototype.constructor;
		Object.setPrototypeOf(FastIterableIterator.prototype, esIteratorPrototype);
		Object.defineProperties(FastIterableIterator.prototype, {
			[Symbol.toStringTag]: {
				writable: false,
				enumerable: false,
				configurable: true,
				value: `${name} Iterator`
			},
			next: {
				writable: true,
				enumerable: true,
				configurable: true
			}
		});
		/**
		* @param {unknown} target
		* @param {'key' | 'value' | 'key+value'} kind
		* @returns {IterableIterator<any>}
		*/
		return function(target, kind) {
			return new FastIterableIterator(target, kind);
		};
	}
	/**
	* @see https://webidl.spec.whatwg.org/#dfn-iterator-prototype-object
	* @param {string} name name of the instance
	* @param {any} object class
	* @param {(target: any) => any} kInternalIterator
	* @param {string | number} [keyIndex]
	* @param {string | number} [valueIndex]
	*/
	function iteratorMixin$2(name, object, kInternalIterator, keyIndex = 0, valueIndex = 1) {
		const makeIterator = createIterator(name, kInternalIterator, keyIndex, valueIndex);
		const properties = {
			keys: {
				writable: true,
				enumerable: true,
				configurable: true,
				value: function keys() {
					webidl$15.brandCheck(this, object);
					return makeIterator(this, "key");
				}
			},
			values: {
				writable: true,
				enumerable: true,
				configurable: true,
				value: function values() {
					webidl$15.brandCheck(this, object);
					return makeIterator(this, "value");
				}
			},
			entries: {
				writable: true,
				enumerable: true,
				configurable: true,
				value: function entries() {
					webidl$15.brandCheck(this, object);
					return makeIterator(this, "key+value");
				}
			},
			forEach: {
				writable: true,
				enumerable: true,
				configurable: true,
				value: function forEach(callbackfn, thisArg = globalThis) {
					webidl$15.brandCheck(this, object);
					webidl$15.argumentLengthCheck(arguments, 1, `${name}.forEach`);
					if (typeof callbackfn !== "function") throw new TypeError(`Failed to execute 'forEach' on '${name}': parameter 1 is not of type 'Function'.`);
					for (const { 0: key, 1: value } of makeIterator(this, "key+value")) callbackfn.call(thisArg, value, key, this);
				}
			}
		};
		return Object.defineProperties(object.prototype, {
			...properties,
			[Symbol.iterator]: {
				writable: true,
				enumerable: false,
				configurable: true,
				value: properties.entries.value
			}
		});
	}
	/**
	* @param {import('./body').ExtractBodyResult} body
	* @param {(bytes: Uint8Array) => void} processBody
	* @param {(error: Error) => void} processBodyError
	* @returns {void}
	*
	* @see https://fetch.spec.whatwg.org/#body-fully-read
	*/
	function fullyReadBody$2(body, processBody, processBodyError) {
		const successSteps = processBody;
		const errorSteps = processBodyError;
		try {
			const reader = body.stream.getReader();
			readAllBytes$1(reader, successSteps, errorSteps);
		} catch (e) {
			errorSteps(e);
		}
	}
	/**
	* @param {ReadableStreamController<Uint8Array>} controller
	*/
	function readableStreamClose$2(controller) {
		try {
			controller.close();
			controller.byobRequest?.respond(0);
		} catch (err) {
			if (!err.message.includes("Controller is already closed") && !err.message.includes("ReadableStream is already closed")) throw err;
		}
	}
	const invalidIsomorphicEncodeValueRegex = /[^\x00-\xFF]/;
	/**
	* @see https://infra.spec.whatwg.org/#isomorphic-encode
	* @param {string} input
	*/
	function isomorphicEncode$2(input) {
		assert$26(!invalidIsomorphicEncodeValueRegex.test(input));
		return input;
	}
	/**
	* @see https://streams.spec.whatwg.org/#readablestreamdefaultreader-read-all-bytes
	* @see https://streams.spec.whatwg.org/#read-loop
	* @param {ReadableStream<Uint8Array<ArrayBuffer>>} reader
	* @param {(bytes: Uint8Array) => void} successSteps
	* @param {(error: Error) => void} failureSteps
	* @returns {Promise<void>}
	*/
	async function readAllBytes$1(reader, successSteps, failureSteps) {
		try {
			const bytes = [];
			let byteLength = 0;
			do {
				const { done, value: chunk } = await reader.read();
				if (done) {
					successSteps(Buffer.concat(bytes, byteLength));
					return;
				}
				if (!isUint8Array(chunk)) {
					failureSteps(/* @__PURE__ */ new TypeError("Received non-Uint8Array chunk"));
					return;
				}
				bytes.push(chunk);
				byteLength += chunk.length;
			} while (true);
		} catch (e) {
			failureSteps(e);
		}
	}
	/**
	* @see https://fetch.spec.whatwg.org/#is-local
	* @param {URL} url
	* @returns {boolean}
	*/
	function urlIsLocal$1(url) {
		assert$26("protocol" in url);
		const protocol = url.protocol;
		return protocol === "about:" || protocol === "blob:" || protocol === "data:";
	}
	/**
	* @param {string|URL} url
	* @returns {boolean}
	*/
	function urlHasHttpsScheme$1(url) {
		return typeof url === "string" && url[5] === ":" && url[0] === "h" && url[1] === "t" && url[2] === "t" && url[3] === "p" && url[4] === "s" || url.protocol === "https:";
	}
	/**
	* @see https://fetch.spec.whatwg.org/#http-scheme
	* @param {URL} url
	*/
	function urlIsHttpHttpsScheme$2(url) {
		assert$26("protocol" in url);
		const protocol = url.protocol;
		return protocol === "http:" || protocol === "https:";
	}
	/**
	* @typedef {Object} RangeHeaderValue
	* @property {number|null} rangeStartValue
	* @property {number|null} rangeEndValue
	*/
	/**
	* @see https://fetch.spec.whatwg.org/#simple-range-header-value
	* @param {string} value
	* @param {boolean} allowWhitespace
	* @return {RangeHeaderValue|'failure'}
	*/
	function simpleRangeHeaderValue$1(value, allowWhitespace) {
		const data = value;
		if (!data.startsWith("bytes")) return "failure";
		const position = { position: 5 };
		if (allowWhitespace) collectASequenceOfCodePoints((char) => char === "	" || char === " ", data, position);
		if (data.charCodeAt(position.position) !== 61) return "failure";
		position.position++;
		if (allowWhitespace) collectASequenceOfCodePoints((char) => char === "	" || char === " ", data, position);
		const rangeStart = collectASequenceOfCodePoints((char) => {
			const code = char.charCodeAt(0);
			return code >= 48 && code <= 57;
		}, data, position);
		const rangeStartValue = rangeStart.length ? Number(rangeStart) : null;
		if (allowWhitespace) collectASequenceOfCodePoints((char) => char === "	" || char === " ", data, position);
		if (data.charCodeAt(position.position) !== 45) return "failure";
		position.position++;
		if (allowWhitespace) collectASequenceOfCodePoints((char) => char === "	" || char === " ", data, position);
		const rangeEnd = collectASequenceOfCodePoints((char) => {
			const code = char.charCodeAt(0);
			return code >= 48 && code <= 57;
		}, data, position);
		const rangeEndValue = rangeEnd.length ? Number(rangeEnd) : null;
		if (position.position < data.length) return "failure";
		if (rangeEndValue === null && rangeStartValue === null) return "failure";
		if (rangeStartValue > rangeEndValue) return "failure";
		return {
			rangeStartValue,
			rangeEndValue
		};
	}
	/**
	* @see https://fetch.spec.whatwg.org/#build-a-content-range
	* @param {number} rangeStart
	* @param {number} rangeEnd
	* @param {number} fullLength
	*/
	function buildContentRange$1(rangeStart, rangeEnd, fullLength) {
		let contentRange = "bytes ";
		contentRange += isomorphicEncode$2(`${rangeStart}`);
		contentRange += "-";
		contentRange += isomorphicEncode$2(`${rangeEnd}`);
		contentRange += "/";
		contentRange += isomorphicEncode$2(`${fullLength}`);
		return contentRange;
	}
	var InflateStream = class extends Transform$2 {
		#zlibOptions;
		/** @param {zlib.ZlibOptions} [zlibOptions] */
		constructor(zlibOptions) {
			super();
			this.#zlibOptions = zlibOptions;
		}
		_transform(chunk, encoding, callback) {
			if (!this._inflateStream) {
				if (chunk.length === 0) {
					callback();
					return;
				}
				this._inflateStream = (chunk[0] & 15) === 8 ? zlib$1.createInflate(this.#zlibOptions) : zlib$1.createInflateRaw(this.#zlibOptions);
				this._inflateStream.on("data", this.push.bind(this));
				this._inflateStream.on("end", () => this.push(null));
				this._inflateStream.on("error", (err) => this.destroy(err));
			}
			this._inflateStream.write(chunk, encoding, callback);
		}
		_final(callback) {
			if (this._inflateStream) {
				this._inflateStream.end();
				this._inflateStream = null;
			}
			callback();
		}
	};
	/**
	* @param {zlib.ZlibOptions} [zlibOptions]
	* @returns {InflateStream}
	*/
	function createInflate$2(zlibOptions) {
		return new InflateStream(zlibOptions);
	}
	/**
	* @see https://fetch.spec.whatwg.org/#concept-header-extract-mime-type
	* @param {import('./headers').HeadersList} headers
	*/
	function extractMimeType$2(headers) {
		let charset = null;
		let essence = null;
		let mimeType = null;
		const values = getDecodeSplit$1("content-type", headers);
		if (values === null) return "failure";
		for (const value of values) {
			const temporaryMimeType = parseMIMEType$2(value);
			if (temporaryMimeType === "failure" || temporaryMimeType.essence === "*/*") continue;
			mimeType = temporaryMimeType;
			if (mimeType.essence !== essence) {
				charset = null;
				if (mimeType.parameters.has("charset")) charset = mimeType.parameters.get("charset");
				essence = mimeType.essence;
			} else if (!mimeType.parameters.has("charset") && charset !== null) mimeType.parameters.set("charset", charset);
		}
		if (mimeType == null) return "failure";
		return mimeType;
	}
	/**
	* @see https://fetch.spec.whatwg.org/#header-value-get-decode-and-split
	* @param {string|null} value
	*/
	function gettingDecodingSplitting(value) {
		const input = value;
		const position = { position: 0 };
		const values = [];
		let temporaryValue = "";
		while (position.position < input.length) {
			temporaryValue += collectASequenceOfCodePoints((char) => char !== "\"" && char !== ",", input, position);
			if (position.position < input.length) if (input.charCodeAt(position.position) === 34) {
				temporaryValue += collectAnHTTPQuotedString(input, position);
				if (position.position < input.length) continue;
			} else {
				assert$26(input.charCodeAt(position.position) === 44);
				position.position++;
			}
			temporaryValue = removeChars$1(temporaryValue, true, true, (char) => char === 9 || char === 32);
			values.push(temporaryValue);
			temporaryValue = "";
		}
		return values;
	}
	/**
	* @see https://fetch.spec.whatwg.org/#concept-header-list-get-decode-split
	* @param {string} name lowercase header name
	* @param {import('./headers').HeadersList} list
	*/
	function getDecodeSplit$1(name, list) {
		const value = list.get(name, true);
		if (value === null) return null;
		return gettingDecodingSplitting(value);
	}
	const textDecoder = new TextDecoder();
	/**
	* @see https://encoding.spec.whatwg.org/#utf-8-decode
	* @param {Buffer} buffer
	*/
	function utf8DecodeBytes$3(buffer$1) {
		if (buffer$1.length === 0) return "";
		if (buffer$1[0] === 239 && buffer$1[1] === 187 && buffer$1[2] === 191) buffer$1 = buffer$1.subarray(3);
		return textDecoder.decode(buffer$1);
	}
	var EnvironmentSettingsObjectBase = class {
		get baseUrl() {
			return getGlobalOrigin$1();
		}
		get origin() {
			return this.baseUrl?.origin;
		}
		policyContainer = makePolicyContainer$1();
	};
	var EnvironmentSettingsObject = class {
		settingsObject = new EnvironmentSettingsObjectBase();
	};
	const environmentSettingsObject$4 = new EnvironmentSettingsObject();
	module.exports = {
		isAborted: isAborted$2,
		isCancelled: isCancelled$2,
		isValidEncodedURL,
		ReadableStreamFrom: ReadableStreamFrom$2,
		tryUpgradeRequestToAPotentiallyTrustworthyURL: tryUpgradeRequestToAPotentiallyTrustworthyURL$1,
		clampAndCoarsenConnectionTimingInfo: clampAndCoarsenConnectionTimingInfo$1,
		coarsenedSharedCurrentTime: coarsenedSharedCurrentTime$1,
		determineRequestsReferrer: determineRequestsReferrer$1,
		makePolicyContainer: makePolicyContainer$1,
		clonePolicyContainer: clonePolicyContainer$1,
		appendFetchMetadata: appendFetchMetadata$1,
		appendRequestOriginHeader: appendRequestOriginHeader$1,
		TAOCheck: TAOCheck$1,
		corsCheck: corsCheck$1,
		crossOriginResourcePolicyCheck: crossOriginResourcePolicyCheck$1,
		createOpaqueTimingInfo: createOpaqueTimingInfo$1,
		setRequestReferrerPolicyOnRedirect: setRequestReferrerPolicyOnRedirect$1,
		isValidHTTPToken: isValidHTTPToken$1,
		requestBadPort: requestBadPort$1,
		requestCurrentURL: requestCurrentURL$1,
		responseURL,
		responseLocationURL: responseLocationURL$1,
		isURLPotentiallyTrustworthy,
		isValidReasonPhrase: isValidReasonPhrase$1,
		sameOrigin: sameOrigin$2,
		normalizeMethod,
		serializeJavascriptValueToJSONString: serializeJavascriptValueToJSONString$1,
		iteratorMixin: iteratorMixin$2,
		createIterator,
		isValidHeaderName: isValidHeaderName$2,
		isValidHeaderValue: isValidHeaderValue$1,
		isErrorLike: isErrorLike$2,
		fullyReadBody: fullyReadBody$2,
		readableStreamClose: readableStreamClose$2,
		isomorphicEncode: isomorphicEncode$2,
		urlIsLocal: urlIsLocal$1,
		urlHasHttpsScheme: urlHasHttpsScheme$1,
		urlIsHttpHttpsScheme: urlIsHttpHttpsScheme$2,
		readAllBytes: readAllBytes$1,
		simpleRangeHeaderValue: simpleRangeHeaderValue$1,
		buildContentRange: buildContentRange$1,
		createInflate: createInflate$2,
		extractMimeType: extractMimeType$2,
		getDecodeSplit: getDecodeSplit$1,
		utf8DecodeBytes: utf8DecodeBytes$3,
		environmentSettingsObject: environmentSettingsObject$4,
		isOriginIPPotentiallyTrustworthy
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/fetch/formdata.js
var require_formdata = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/fetch/formdata.js": ((exports, module) => {
	const { iteratorMixin: iteratorMixin$1 } = require_util$4();
	const { kEnumerableProperty: kEnumerableProperty$10 } = require_util$5();
	const { webidl: webidl$14 } = require_webidl();
	const nodeUtil$2 = __require("node:util");
	var FormData$1 = class FormData$1 {
		#state = [];
		constructor(form = void 0) {
			webidl$14.util.markAsUncloneable(this);
			if (form !== void 0) throw webidl$14.errors.conversionFailed({
				prefix: "FormData constructor",
				argument: "Argument 1",
				types: ["undefined"]
			});
		}
		append(name, value, filename = void 0) {
			webidl$14.brandCheck(this, FormData$1);
			const prefix = "FormData.append";
			webidl$14.argumentLengthCheck(arguments, 2, prefix);
			name = webidl$14.converters.USVString(name);
			if (arguments.length === 3 || webidl$14.is.Blob(value)) {
				value = webidl$14.converters.Blob(value, prefix, "value");
				if (filename !== void 0) filename = webidl$14.converters.USVString(filename);
			} else value = webidl$14.converters.USVString(value);
			const entry = makeEntry$1(name, value, filename);
			this.#state.push(entry);
		}
		delete(name) {
			webidl$14.brandCheck(this, FormData$1);
			webidl$14.argumentLengthCheck(arguments, 1, "FormData.delete");
			name = webidl$14.converters.USVString(name);
			this.#state = this.#state.filter((entry) => entry.name !== name);
		}
		get(name) {
			webidl$14.brandCheck(this, FormData$1);
			webidl$14.argumentLengthCheck(arguments, 1, "FormData.get");
			name = webidl$14.converters.USVString(name);
			const idx = this.#state.findIndex((entry) => entry.name === name);
			if (idx === -1) return null;
			return this.#state[idx].value;
		}
		getAll(name) {
			webidl$14.brandCheck(this, FormData$1);
			webidl$14.argumentLengthCheck(arguments, 1, "FormData.getAll");
			name = webidl$14.converters.USVString(name);
			return this.#state.filter((entry) => entry.name === name).map((entry) => entry.value);
		}
		has(name) {
			webidl$14.brandCheck(this, FormData$1);
			webidl$14.argumentLengthCheck(arguments, 1, "FormData.has");
			name = webidl$14.converters.USVString(name);
			return this.#state.findIndex((entry) => entry.name === name) !== -1;
		}
		set(name, value, filename = void 0) {
			webidl$14.brandCheck(this, FormData$1);
			const prefix = "FormData.set";
			webidl$14.argumentLengthCheck(arguments, 2, prefix);
			name = webidl$14.converters.USVString(name);
			if (arguments.length === 3 || webidl$14.is.Blob(value)) {
				value = webidl$14.converters.Blob(value, prefix, "value");
				if (filename !== void 0) filename = webidl$14.converters.USVString(filename);
			} else value = webidl$14.converters.USVString(value);
			const entry = makeEntry$1(name, value, filename);
			const idx = this.#state.findIndex((entry$1) => entry$1.name === name);
			if (idx !== -1) this.#state = [
				...this.#state.slice(0, idx),
				entry,
				...this.#state.slice(idx + 1).filter((entry$1) => entry$1.name !== name)
			];
			else this.#state.push(entry);
		}
		[nodeUtil$2.inspect.custom](depth, options) {
			const state = this.#state.reduce((a, b) => {
				if (a[b.name]) if (Array.isArray(a[b.name])) a[b.name].push(b.value);
				else a[b.name] = [a[b.name], b.value];
				else a[b.name] = b.value;
				return a;
			}, { __proto__: null });
			options.depth ??= depth;
			options.colors ??= true;
			const output = nodeUtil$2.formatWithOptions(options, state);
			return `FormData ${output.slice(output.indexOf("]") + 2)}`;
		}
		/**
		* @param {FormData} formData
		*/
		static getFormDataState(formData) {
			return formData.#state;
		}
		/**
		* @param {FormData} formData
		* @param {any[]} newState
		*/
		static setFormDataState(formData, newState) {
			formData.#state = newState;
		}
	};
	const { getFormDataState, setFormDataState: setFormDataState$1 } = FormData$1;
	Reflect.deleteProperty(FormData$1, "getFormDataState");
	Reflect.deleteProperty(FormData$1, "setFormDataState");
	iteratorMixin$1("FormData", FormData$1, getFormDataState, "name", "value");
	Object.defineProperties(FormData$1.prototype, {
		append: kEnumerableProperty$10,
		delete: kEnumerableProperty$10,
		get: kEnumerableProperty$10,
		getAll: kEnumerableProperty$10,
		has: kEnumerableProperty$10,
		set: kEnumerableProperty$10,
		[Symbol.toStringTag]: {
			value: "FormData",
			configurable: true
		}
	});
	/**
	* @see https://html.spec.whatwg.org/multipage/form-control-infrastructure.html#create-an-entry
	* @param {string} name
	* @param {string|Blob} value
	* @param {?string} filename
	* @returns
	*/
	function makeEntry$1(name, value, filename) {
		if (typeof value === "string") {} else {
			if (!webidl$14.is.File(value)) value = new File([value], "blob", { type: value.type });
			if (filename !== void 0) {
				/** @type {FilePropertyBag} */
				const options = {
					type: value.type,
					lastModified: value.lastModified
				};
				value = new File([value], filename, options);
			}
		}
		return {
			name,
			value
		};
	}
	webidl$14.is.FormData = webidl$14.util.MakeTypeAssertion(FormData$1);
	module.exports = {
		FormData: FormData$1,
		makeEntry: makeEntry$1,
		setFormDataState: setFormDataState$1
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/fetch/formdata-parser.js
var require_formdata_parser = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/fetch/formdata-parser.js": ((exports, module) => {
	const { bufferToLowerCasedHeaderName: bufferToLowerCasedHeaderName$1 } = require_util$5();
	const { utf8DecodeBytes: utf8DecodeBytes$2 } = require_util$4();
	const { HTTP_TOKEN_CODEPOINTS, isomorphicDecode } = require_data_url();
	const { makeEntry } = require_formdata();
	const { webidl: webidl$13 } = require_webidl();
	const assert$25 = __require("node:assert");
	const formDataNameBuffer = Buffer.from("form-data; name=\"");
	const filenameBuffer = Buffer.from("filename");
	const dd = Buffer.from("--");
	const ddcrlf = Buffer.from("--\r\n");
	/**
	* @param {string} chars
	*/
	function isAsciiString(chars) {
		for (let i = 0; i < chars.length; ++i) if ((chars.charCodeAt(i) & -128) !== 0) return false;
		return true;
	}
	/**
	* @see https://andreubotella.github.io/multipart-form-data/#multipart-form-data-boundary
	* @param {string} boundary
	*/
	function validateBoundary(boundary) {
		const length = boundary.length;
		if (length < 27 || length > 70) return false;
		for (let i = 0; i < length; ++i) {
			const cp = boundary.charCodeAt(i);
			if (!(cp >= 48 && cp <= 57 || cp >= 65 && cp <= 90 || cp >= 97 && cp <= 122 || cp === 39 || cp === 45 || cp === 95)) return false;
		}
		return true;
	}
	/**
	* @see https://andreubotella.github.io/multipart-form-data/#multipart-form-data-parser
	* @param {Buffer} input
	* @param {ReturnType<import('./data-url')['parseMIMEType']>} mimeType
	*/
	function multipartFormDataParser$1(input, mimeType) {
		assert$25(mimeType !== "failure" && mimeType.essence === "multipart/form-data");
		const boundaryString = mimeType.parameters.get("boundary");
		if (boundaryString === void 0) throw parsingError("missing boundary in content-type header");
		const boundary = Buffer.from(`--${boundaryString}`, "utf8");
		const entryList = [];
		const position = { position: 0 };
		while (input[position.position] === 13 && input[position.position + 1] === 10) position.position += 2;
		let trailing = input.length;
		while (input[trailing - 1] === 10 && input[trailing - 2] === 13) trailing -= 2;
		if (trailing !== input.length) input = input.subarray(0, trailing);
		while (true) {
			if (input.subarray(position.position, position.position + boundary.length).equals(boundary)) position.position += boundary.length;
			else throw parsingError("expected a value starting with -- and the boundary");
			if (position.position === input.length - 2 && bufferStartsWith(input, dd, position) || position.position === input.length - 4 && bufferStartsWith(input, ddcrlf, position)) return entryList;
			if (input[position.position] !== 13 || input[position.position + 1] !== 10) throw parsingError("expected CRLF");
			position.position += 2;
			let { name, filename, contentType, encoding } = parseMultipartFormDataHeaders(input, position);
			position.position += 2;
			let body;
			{
				const boundaryIndex = input.indexOf(boundary.subarray(2), position.position);
				if (boundaryIndex === -1) throw parsingError("expected boundary after body");
				body = input.subarray(position.position, boundaryIndex - 4);
				position.position += body.length;
				if (encoding === "base64") body = Buffer.from(body.toString(), "base64");
			}
			if (input[position.position] !== 13 || input[position.position + 1] !== 10) throw parsingError("expected CRLF");
			else position.position += 2;
			let value;
			if (filename !== null) {
				contentType ??= "text/plain";
				if (!isAsciiString(contentType)) contentType = "";
				value = new File([body], filename, { type: contentType });
			} else value = utf8DecodeBytes$2(Buffer.from(body));
			assert$25(webidl$13.is.USVString(name));
			assert$25(typeof value === "string" && webidl$13.is.USVString(value) || webidl$13.is.File(value));
			entryList.push(makeEntry(name, value, filename));
		}
	}
	/**
	* @see https://andreubotella.github.io/multipart-form-data/#parse-multipart-form-data-headers
	* @param {Buffer} input
	* @param {{ position: number }} position
	*/
	function parseMultipartFormDataHeaders(input, position) {
		let name = null;
		let filename = null;
		let contentType = null;
		let encoding = null;
		while (true) {
			if (input[position.position] === 13 && input[position.position + 1] === 10) {
				if (name === null) throw parsingError("header name is null");
				return {
					name,
					filename,
					contentType,
					encoding
				};
			}
			let headerName = collectASequenceOfBytes((char) => char !== 10 && char !== 13 && char !== 58, input, position);
			headerName = removeChars(headerName, true, true, (char) => char === 9 || char === 32);
			if (!HTTP_TOKEN_CODEPOINTS.test(headerName.toString())) throw parsingError("header name does not match the field-name token production");
			if (input[position.position] !== 58) throw parsingError("expected :");
			position.position++;
			collectASequenceOfBytes((char) => char === 32 || char === 9, input, position);
			switch (bufferToLowerCasedHeaderName$1(headerName)) {
				case "content-disposition":
					name = filename = null;
					if (!bufferStartsWith(input, formDataNameBuffer, position)) throw parsingError("expected form-data; name=\" for content-disposition header");
					position.position += 17;
					name = parseMultipartFormDataName(input, position);
					if (input[position.position] === 59 && input[position.position + 1] === 32) {
						const at = { position: position.position + 2 };
						if (bufferStartsWith(input, filenameBuffer, at)) if (input[at.position + 8] === 42) {
							at.position += 10;
							collectASequenceOfBytes((char) => char === 32 || char === 9, input, at);
							const headerValue = collectASequenceOfBytes((char) => char !== 32 && char !== 13 && char !== 10, input, at);
							if (headerValue[0] !== 117 && headerValue[0] !== 85 || headerValue[1] !== 116 && headerValue[1] !== 84 || headerValue[2] !== 102 && headerValue[2] !== 70 || headerValue[3] !== 45 || headerValue[4] !== 56) throw parsingError("unknown encoding, expected utf-8''");
							filename = decodeURIComponent(new TextDecoder().decode(headerValue.subarray(7)));
							position.position = at.position;
						} else {
							position.position += 11;
							collectASequenceOfBytes((char) => char === 32 || char === 9, input, position);
							position.position++;
							filename = parseMultipartFormDataName(input, position);
						}
					}
					break;
				case "content-type": {
					let headerValue = collectASequenceOfBytes((char) => char !== 10 && char !== 13, input, position);
					headerValue = removeChars(headerValue, false, true, (char) => char === 9 || char === 32);
					contentType = isomorphicDecode(headerValue);
					break;
				}
				case "content-transfer-encoding": {
					let headerValue = collectASequenceOfBytes((char) => char !== 10 && char !== 13, input, position);
					headerValue = removeChars(headerValue, false, true, (char) => char === 9 || char === 32);
					encoding = isomorphicDecode(headerValue);
					break;
				}
				default: collectASequenceOfBytes((char) => char !== 10 && char !== 13, input, position);
			}
			if (input[position.position] !== 13 && input[position.position + 1] !== 10) throw parsingError("expected CRLF");
			else position.position += 2;
		}
	}
	/**
	* @see https://andreubotella.github.io/multipart-form-data/#parse-a-multipart-form-data-name
	* @param {Buffer} input
	* @param {{ position: number }} position
	*/
	function parseMultipartFormDataName(input, position) {
		assert$25(input[position.position - 1] === 34);
		/** @type {string | Buffer} */
		let name = collectASequenceOfBytes((char) => char !== 10 && char !== 13 && char !== 34, input, position);
		if (input[position.position] !== 34) throw parsingError("expected \"");
		else position.position++;
		name = new TextDecoder().decode(name).replace(/%0A/gi, "\n").replace(/%0D/gi, "\r").replace(/%22/g, "\"");
		return name;
	}
	/**
	* @param {(char: number) => boolean} condition
	* @param {Buffer} input
	* @param {{ position: number }} position
	*/
	function collectASequenceOfBytes(condition, input, position) {
		let start = position.position;
		while (start < input.length && condition(input[start])) ++start;
		return input.subarray(position.position, position.position = start);
	}
	/**
	* @param {Buffer} buf
	* @param {boolean} leading
	* @param {boolean} trailing
	* @param {(charCode: number) => boolean} predicate
	* @returns {Buffer}
	*/
	function removeChars(buf, leading, trailing, predicate) {
		let lead = 0;
		let trail = buf.length - 1;
		if (leading) while (lead < buf.length && predicate(buf[lead])) lead++;
		if (trailing) while (trail > 0 && predicate(buf[trail])) trail--;
		return lead === 0 && trail === buf.length - 1 ? buf : buf.subarray(lead, trail + 1);
	}
	/**
	* Checks if {@param buffer} starts with {@param start}
	* @param {Buffer} buffer
	* @param {Buffer} start
	* @param {{ position: number }} position
	*/
	function bufferStartsWith(buffer$1, start, position) {
		if (buffer$1.length < start.length) return false;
		for (let i = 0; i < start.length; i++) if (start[i] !== buffer$1[position.position + i]) return false;
		return true;
	}
	function parsingError(cause) {
		return new TypeError("Failed to parse body as FormData.", { cause: new TypeError(cause) });
	}
	module.exports = {
		multipartFormDataParser: multipartFormDataParser$1,
		validateBoundary
	};
}) });

//#endregion
//#region node_modules/undici/lib/util/promise.js
var require_promise = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/util/promise.js": ((exports, module) => {
	/**
	* @template {*} T
	* @typedef {Object} DeferredPromise
	* @property {Promise<T>} promise
	* @property {(value?: T) => void} resolve
	* @property {(reason?: any) => void} reject
	*/
	/**
	* @template {*} T
	* @returns {DeferredPromise<T>} An object containing a promise and its resolve/reject methods.
	*/
	function createDeferredPromise$4() {
		let res;
		let rej;
		return {
			promise: new Promise((resolve$1, reject) => {
				res = resolve$1;
				rej = reject;
			}),
			resolve: res,
			reject: rej
		};
	}
	module.exports = { createDeferredPromise: createDeferredPromise$4 };
}) });

//#endregion
//#region node_modules/undici/lib/web/fetch/body.js
var require_body = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/fetch/body.js": ((exports, module) => {
	const util$19 = require_util$5();
	const { ReadableStreamFrom: ReadableStreamFrom$1, readableStreamClose: readableStreamClose$1, fullyReadBody: fullyReadBody$1, extractMimeType: extractMimeType$1, utf8DecodeBytes: utf8DecodeBytes$1 } = require_util$4();
	const { FormData, setFormDataState } = require_formdata();
	const { webidl: webidl$12 } = require_webidl();
	const assert$24 = __require("node:assert");
	const { isErrored: isErrored$1, isDisturbed: isDisturbed$2 } = __require("node:stream");
	const { isArrayBuffer: isArrayBuffer$3 } = __require("node:util/types");
	const { serializeAMimeType: serializeAMimeType$2 } = require_data_url();
	const { multipartFormDataParser } = require_formdata_parser();
	const { createDeferredPromise: createDeferredPromise$3 } = require_promise();
	let random;
	try {
		const crypto$4 = __require("node:crypto");
		random = (max) => crypto$4.randomInt(0, max);
	} catch {
		random = (max) => Math.floor(Math.random() * max);
	}
	const textEncoder$1 = new TextEncoder();
	function noop$8() {}
	const streamRegistry$1 = new FinalizationRegistry((weakRef) => {
		const stream$2 = weakRef.deref();
		if (stream$2 && !stream$2.locked && !isDisturbed$2(stream$2) && !isErrored$1(stream$2)) stream$2.cancel("Response object has been garbage collected").catch(noop$8);
	});
	/**
	* Extract a body with type from a byte sequence or BodyInit object
	*
	* @param {import('../../../types').BodyInit} object - The BodyInit object to extract from
	* @param {boolean} [keepalive=false] - If true, indicates that the body
	* @returns {[{stream: ReadableStream, source: any, length: number | null}, string | null]} - Returns a tuple containing the body and its type
	*
	* @see https://fetch.spec.whatwg.org/#concept-bodyinit-extract
	*/
	function extractBody$5(object, keepalive = false) {
		let stream$2 = null;
		if (webidl$12.is.ReadableStream(object)) stream$2 = object;
		else if (webidl$12.is.Blob(object)) stream$2 = object.stream();
		else stream$2 = new ReadableStream({
			async pull(controller) {
				const buffer$1 = typeof source === "string" ? textEncoder$1.encode(source) : source;
				if (buffer$1.byteLength) controller.enqueue(buffer$1);
				queueMicrotask(() => readableStreamClose$1(controller));
			},
			start() {},
			type: "bytes"
		});
		assert$24(webidl$12.is.ReadableStream(stream$2));
		let action = null;
		let source = null;
		let length = null;
		let type = null;
		if (typeof object === "string") {
			source = object;
			type = "text/plain;charset=UTF-8";
		} else if (webidl$12.is.URLSearchParams(object)) {
			source = object.toString();
			type = "application/x-www-form-urlencoded;charset=UTF-8";
		} else if (isArrayBuffer$3(object)) source = new Uint8Array(object.slice());
		else if (ArrayBuffer.isView(object)) source = new Uint8Array(object.buffer.slice(object.byteOffset, object.byteOffset + object.byteLength));
		else if (webidl$12.is.FormData(object)) {
			const boundary = `----formdata-undici-0${`${random(1e11)}`.padStart(11, "0")}`;
			const prefix = `--${boundary}\r\nContent-Disposition: form-data`;
			/*! formdata-polyfill. MIT License. Jimmy Wärting <https://jimmy.warting.se/opensource> */
			const formdataEscape = (str) => str.replace(/\n/g, "%0A").replace(/\r/g, "%0D").replace(/"/g, "%22");
			const normalizeLinefeeds = (value) => value.replace(/\r?\n|\r/g, "\r\n");
			const blobParts = [];
			const rn = new Uint8Array([13, 10]);
			length = 0;
			let hasUnknownSizeValue = false;
			for (const [name, value] of object) if (typeof value === "string") {
				const chunk$1 = textEncoder$1.encode(prefix + `; name="${formdataEscape(normalizeLinefeeds(name))}"\r\n\r\n${normalizeLinefeeds(value)}\r\n`);
				blobParts.push(chunk$1);
				length += chunk$1.byteLength;
			} else {
				const chunk$1 = textEncoder$1.encode(`${prefix}; name="${formdataEscape(normalizeLinefeeds(name))}"` + (value.name ? `; filename="${formdataEscape(value.name)}"` : "") + `\r
Content-Type: ${value.type || "application/octet-stream"}\r\n\r\n`);
				blobParts.push(chunk$1, value, rn);
				if (typeof value.size === "number") length += chunk$1.byteLength + value.size + rn.byteLength;
				else hasUnknownSizeValue = true;
			}
			const chunk = textEncoder$1.encode(`--${boundary}--\r\n`);
			blobParts.push(chunk);
			length += chunk.byteLength;
			if (hasUnknownSizeValue) length = null;
			source = object;
			action = async function* () {
				for (const part of blobParts) if (part.stream) yield* part.stream();
				else yield part;
			};
			type = `multipart/form-data; boundary=${boundary}`;
		} else if (webidl$12.is.Blob(object)) {
			source = object;
			length = object.size;
			if (object.type) type = object.type;
		} else if (typeof object[Symbol.asyncIterator] === "function") {
			if (keepalive) throw new TypeError("keepalive");
			if (util$19.isDisturbed(object) || object.locked) throw new TypeError("Response body object should not be disturbed or locked");
			stream$2 = webidl$12.is.ReadableStream(object) ? object : ReadableStreamFrom$1(object);
		}
		if (typeof source === "string" || util$19.isBuffer(source)) length = Buffer.byteLength(source);
		if (action != null) {
			let iterator;
			stream$2 = new ReadableStream({
				async start() {
					iterator = action(object)[Symbol.asyncIterator]();
				},
				async pull(controller) {
					const { value, done } = await iterator.next();
					if (done) queueMicrotask(() => {
						controller.close();
						controller.byobRequest?.respond(0);
					});
					else if (!isErrored$1(stream$2)) {
						const buffer$1 = new Uint8Array(value);
						if (buffer$1.byteLength) controller.enqueue(buffer$1);
					}
					return controller.desiredSize > 0;
				},
				async cancel(reason) {
					await iterator.return();
				},
				type: "bytes"
			});
		}
		return [{
			stream: stream$2,
			source,
			length
		}, type];
	}
	/**
	* @typedef {object} ExtractBodyResult
	* @property {ReadableStream<Uint8Array<ArrayBuffer>>} stream - The ReadableStream containing the body data
	* @property {any} source - The original source of the body data
	* @property {number | null} length - The length of the body data, or null
	*/
	/**
	* Safely extract a body with type from a byte sequence or BodyInit object.
	*
	* @param {import('../../../types').BodyInit} object - The BodyInit object to extract from
	* @param {boolean} [keepalive=false] - If true, indicates that the body
	* @returns {[ExtractBodyResult, string | null]} - Returns a tuple containing the body and its type
	*
	* @see https://fetch.spec.whatwg.org/#bodyinit-safely-extract
	*/
	function safelyExtractBody$1(object, keepalive = false) {
		if (webidl$12.is.ReadableStream(object)) {
			assert$24(!util$19.isDisturbed(object), "The body has already been consumed.");
			assert$24(!object.locked, "The stream is locked.");
		}
		return extractBody$5(object, keepalive);
	}
	function cloneBody$2(body) {
		const { 0: out1, 1: out2 } = body.stream.tee();
		body.stream = out1;
		return {
			stream: out2,
			length: body.length,
			source: body.source
		};
	}
	function throwIfAborted(state) {
		if (state.aborted) throw new DOMException("The operation was aborted.", "AbortError");
	}
	function bodyMixinMethods(instance, getInternalState) {
		return {
			blob() {
				return consumeBody(this, (bytes) => {
					let mimeType = bodyMimeType(getInternalState(this));
					if (mimeType === null) mimeType = "";
					else if (mimeType) mimeType = serializeAMimeType$2(mimeType);
					return new Blob([bytes], { type: mimeType });
				}, instance, getInternalState);
			},
			arrayBuffer() {
				return consumeBody(this, (bytes) => {
					return new Uint8Array(bytes).buffer;
				}, instance, getInternalState);
			},
			text() {
				return consumeBody(this, utf8DecodeBytes$1, instance, getInternalState);
			},
			json() {
				return consumeBody(this, parseJSONFromBytes, instance, getInternalState);
			},
			formData() {
				return consumeBody(this, (value) => {
					const mimeType = bodyMimeType(getInternalState(this));
					if (mimeType !== null) switch (mimeType.essence) {
						case "multipart/form-data": {
							const parsed = multipartFormDataParser(value, mimeType);
							const fd = new FormData();
							setFormDataState(fd, parsed);
							return fd;
						}
						case "application/x-www-form-urlencoded": {
							const entries = new URLSearchParams(value.toString());
							const fd = new FormData();
							for (const [name, value$1] of entries) fd.append(name, value$1);
							return fd;
						}
					}
					throw new TypeError("Content-Type was not one of \"multipart/form-data\" or \"application/x-www-form-urlencoded\".");
				}, instance, getInternalState);
			},
			bytes() {
				return consumeBody(this, (bytes) => {
					return new Uint8Array(bytes);
				}, instance, getInternalState);
			}
		};
	}
	function mixinBody$2(prototype, getInternalState) {
		Object.assign(prototype.prototype, bodyMixinMethods(prototype, getInternalState));
	}
	/**
	* @see https://fetch.spec.whatwg.org/#concept-body-consume-body
	* @param {any} object internal state
	* @param {(value: unknown) => unknown} convertBytesToJSValue
	* @param {any} instance
	* @param {(target: any) => any} getInternalState
	*/
	async function consumeBody(object, convertBytesToJSValue, instance, getInternalState) {
		webidl$12.brandCheck(object, instance);
		const state = getInternalState(object);
		if (bodyUnusable$2(state)) throw new TypeError("Body is unusable: Body has already been read");
		throwIfAborted(state);
		const promise = createDeferredPromise$3();
		const errorSteps = (error) => promise.reject(error);
		const successSteps = (data) => {
			try {
				promise.resolve(convertBytesToJSValue(data));
			} catch (e) {
				errorSteps(e);
			}
		};
		if (state.body == null) {
			successSteps(Buffer.allocUnsafe(0));
			return promise.promise;
		}
		fullyReadBody$1(state.body, successSteps, errorSteps);
		return promise.promise;
	}
	/**
	* @see https://fetch.spec.whatwg.org/#body-unusable
	* @param {any} object internal state
	*/
	function bodyUnusable$2(object) {
		const body = object.body;
		return body != null && (body.stream.locked || util$19.isDisturbed(body.stream));
	}
	/**
	* @see https://infra.spec.whatwg.org/#parse-json-bytes-to-a-javascript-value
	* @param {Uint8Array} bytes
	*/
	function parseJSONFromBytes(bytes) {
		return JSON.parse(utf8DecodeBytes$1(bytes));
	}
	/**
	* @see https://fetch.spec.whatwg.org/#concept-body-mime-type
	* @param {any} requestOrResponse internal state
	*/
	function bodyMimeType(requestOrResponse) {
		/** @type {import('./headers').HeadersList} */
		const headers = requestOrResponse.headersList;
		const mimeType = extractMimeType$1(headers);
		if (mimeType === "failure") return null;
		return mimeType;
	}
	module.exports = {
		extractBody: extractBody$5,
		safelyExtractBody: safelyExtractBody$1,
		cloneBody: cloneBody$2,
		mixinBody: mixinBody$2,
		streamRegistry: streamRegistry$1,
		bodyUnusable: bodyUnusable$2
	};
}) });

//#endregion
//#region node_modules/undici/lib/dispatcher/client-h1.js
var require_client_h1 = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/dispatcher/client-h1.js": ((exports, module) => {
	const assert$23 = __require("node:assert");
	const util$18 = require_util$5();
	const { channels: channels$4 } = require_diagnostics();
	const timers = require_timers();
	const { RequestContentLengthMismatchError: RequestContentLengthMismatchError$1, ResponseContentLengthMismatchError, RequestAbortedError: RequestAbortedError$7, HeadersTimeoutError, HeadersOverflowError, SocketError: SocketError$3, InformationalError: InformationalError$3, BodyTimeoutError, HTTPParserError, ResponseExceededMaxSizeError } = require_errors();
	const { kUrl: kUrl$6, kReset: kReset$1, kClient: kClient$3, kParser, kBlocking, kRunning: kRunning$4, kPending: kPending$3, kSize: kSize$4, kWriting, kQueue: kQueue$3, kNoRef, kKeepAliveDefaultTimeout: kKeepAliveDefaultTimeout$1, kHostHeader: kHostHeader$1, kPendingIdx: kPendingIdx$2, kRunningIdx: kRunningIdx$2, kError: kError$2, kPipelining: kPipelining$1, kSocket: kSocket$1, kKeepAliveTimeoutValue: kKeepAliveTimeoutValue$1, kMaxHeadersSize: kMaxHeadersSize$1, kKeepAliveMaxTimeout: kKeepAliveMaxTimeout$1, kKeepAliveTimeoutThreshold: kKeepAliveTimeoutThreshold$1, kHeadersTimeout: kHeadersTimeout$1, kBodyTimeout: kBodyTimeout$2, kStrictContentLength: kStrictContentLength$2, kMaxRequests: kMaxRequests$1, kCounter: kCounter$1, kMaxResponseSize: kMaxResponseSize$1, kOnError: kOnError$2, kResume: kResume$3, kHTTPContext: kHTTPContext$2, kClosed: kClosed$2 } = require_symbols();
	const constants = require_constants$3();
	const EMPTY_BUF = Buffer.alloc(0);
	const FastBuffer = Buffer[Symbol.species];
	const removeAllListeners = util$18.removeAllListeners;
	let extractBody$4;
	function lazyllhttp() {
		const llhttpWasmData = process.env.JEST_WORKER_ID ? require_llhttp_wasm() : void 0;
		let mod;
		try {
			mod = new WebAssembly.Module(require_llhttp_simd_wasm());
		} catch {
			/* istanbul ignore next */
			mod = new WebAssembly.Module(llhttpWasmData || require_llhttp_wasm());
		}
		return new WebAssembly.Instance(mod, { env: {
			wasm_on_url: (p, at, len) => {
				/* istanbul ignore next */
				return 0;
			},
			wasm_on_status: (p, at, len) => {
				assert$23(currentParser.ptr === p);
				const start = at - currentBufferPtr + currentBufferRef.byteOffset;
				return currentParser.onStatus(new FastBuffer(currentBufferRef.buffer, start, len));
			},
			wasm_on_message_begin: (p) => {
				assert$23(currentParser.ptr === p);
				return currentParser.onMessageBegin();
			},
			wasm_on_header_field: (p, at, len) => {
				assert$23(currentParser.ptr === p);
				const start = at - currentBufferPtr + currentBufferRef.byteOffset;
				return currentParser.onHeaderField(new FastBuffer(currentBufferRef.buffer, start, len));
			},
			wasm_on_header_value: (p, at, len) => {
				assert$23(currentParser.ptr === p);
				const start = at - currentBufferPtr + currentBufferRef.byteOffset;
				return currentParser.onHeaderValue(new FastBuffer(currentBufferRef.buffer, start, len));
			},
			wasm_on_headers_complete: (p, statusCode, upgrade$1, shouldKeepAlive) => {
				assert$23(currentParser.ptr === p);
				return currentParser.onHeadersComplete(statusCode, upgrade$1 === 1, shouldKeepAlive === 1);
			},
			wasm_on_body: (p, at, len) => {
				assert$23(currentParser.ptr === p);
				const start = at - currentBufferPtr + currentBufferRef.byteOffset;
				return currentParser.onBody(new FastBuffer(currentBufferRef.buffer, start, len));
			},
			wasm_on_message_complete: (p) => {
				assert$23(currentParser.ptr === p);
				return currentParser.onMessageComplete();
			}
		} });
	}
	let llhttpInstance = null;
	/**
	* @type {Parser|null}
	*/
	let currentParser = null;
	let currentBufferRef = null;
	/**
	* @type {number}
	*/
	let currentBufferSize = 0;
	let currentBufferPtr = null;
	const USE_NATIVE_TIMER = 0;
	const USE_FAST_TIMER = 1;
	const TIMEOUT_HEADERS = 2 | USE_FAST_TIMER;
	const TIMEOUT_BODY = 4 | USE_FAST_TIMER;
	const TIMEOUT_KEEP_ALIVE = 8 | USE_NATIVE_TIMER;
	var Parser = class {
		/**
		* @param {import('./client.js')} client
		* @param {import('net').Socket} socket
		* @param {*} llhttp
		*/
		constructor(client, socket, { exports: exports$1 }) {
			this.llhttp = exports$1;
			this.ptr = this.llhttp.llhttp_alloc(constants.TYPE.RESPONSE);
			this.client = client;
			/**
			* @type {import('net').Socket}
			*/
			this.socket = socket;
			this.timeout = null;
			this.timeoutValue = null;
			this.timeoutType = null;
			this.statusCode = 0;
			this.statusText = "";
			this.upgrade = false;
			this.headers = [];
			this.headersSize = 0;
			this.headersMaxSize = client[kMaxHeadersSize$1];
			this.shouldKeepAlive = false;
			this.paused = false;
			this.resume = this.resume.bind(this);
			this.bytesRead = 0;
			this.keepAlive = "";
			this.contentLength = "";
			this.connection = "";
			this.maxResponseSize = client[kMaxResponseSize$1];
		}
		setTimeout(delay$2, type) {
			if (delay$2 !== this.timeoutValue || type & USE_FAST_TIMER ^ this.timeoutType & USE_FAST_TIMER) {
				if (this.timeout) {
					timers.clearTimeout(this.timeout);
					this.timeout = null;
				}
				if (delay$2) if (type & USE_FAST_TIMER) this.timeout = timers.setFastTimeout(onParserTimeout, delay$2, new WeakRef(this));
				else {
					this.timeout = setTimeout(onParserTimeout, delay$2, new WeakRef(this));
					this.timeout?.unref();
				}
				this.timeoutValue = delay$2;
			} else if (this.timeout) {
				// istanbul ignore else: only for jest
				if (this.timeout.refresh) this.timeout.refresh();
			}
			this.timeoutType = type;
		}
		resume() {
			if (this.socket.destroyed || !this.paused) return;
			assert$23(this.ptr != null);
			assert$23(currentParser === null);
			this.llhttp.llhttp_resume(this.ptr);
			assert$23(this.timeoutType === TIMEOUT_BODY);
			if (this.timeout) {
				// istanbul ignore else: only for jest
				if (this.timeout.refresh) this.timeout.refresh();
			}
			this.paused = false;
			this.execute(this.socket.read() || EMPTY_BUF);
			this.readMore();
		}
		readMore() {
			while (!this.paused && this.ptr) {
				const chunk = this.socket.read();
				if (chunk === null) break;
				this.execute(chunk);
			}
		}
		/**
		* @param {Buffer} chunk
		*/
		execute(chunk) {
			assert$23(currentParser === null);
			assert$23(this.ptr != null);
			assert$23(!this.paused);
			const { socket, llhttp } = this;
			if (chunk.length > currentBufferSize) {
				if (currentBufferPtr) llhttp.free(currentBufferPtr);
				currentBufferSize = Math.ceil(chunk.length / 4096) * 4096;
				currentBufferPtr = llhttp.malloc(currentBufferSize);
			}
			new Uint8Array(llhttp.memory.buffer, currentBufferPtr, currentBufferSize).set(chunk);
			try {
				let ret;
				try {
					currentBufferRef = chunk;
					currentParser = this;
					ret = llhttp.llhttp_execute(this.ptr, currentBufferPtr, chunk.length);
				} catch (err) {
					/* istanbul ignore next: difficult to make a test case for */
					throw err;
				} finally {
					currentParser = null;
					currentBufferRef = null;
				}
				if (ret !== constants.ERROR.OK) {
					const data = chunk.subarray(llhttp.llhttp_get_error_pos(this.ptr) - currentBufferPtr);
					if (ret === constants.ERROR.PAUSED_UPGRADE) this.onUpgrade(data);
					else if (ret === constants.ERROR.PAUSED) {
						this.paused = true;
						socket.unshift(data);
					} else {
						const ptr = llhttp.llhttp_get_error_reason(this.ptr);
						let message = "";
						/* istanbul ignore else: difficult to make a test case for */
						if (ptr) {
							const len = new Uint8Array(llhttp.memory.buffer, ptr).indexOf(0);
							message = "Response does not match the HTTP/1.1 protocol (" + Buffer.from(llhttp.memory.buffer, ptr, len).toString() + ")";
						}
						throw new HTTPParserError(message, constants.ERROR[ret], data);
					}
				}
			} catch (err) {
				util$18.destroy(socket, err);
			}
		}
		destroy() {
			assert$23(currentParser === null);
			assert$23(this.ptr != null);
			this.llhttp.llhttp_free(this.ptr);
			this.ptr = null;
			this.timeout && timers.clearTimeout(this.timeout);
			this.timeout = null;
			this.timeoutValue = null;
			this.timeoutType = null;
			this.paused = false;
		}
		/**
		* @param {Buffer} buf
		* @returns {0}
		*/
		onStatus(buf) {
			this.statusText = buf.toString();
			return 0;
		}
		/**
		* @returns {0|-1}
		*/
		onMessageBegin() {
			const { socket, client } = this;
			/* istanbul ignore next: difficult to make a test case for */
			if (socket.destroyed) return -1;
			const request$2 = client[kQueue$3][client[kRunningIdx$2]];
			if (!request$2) return -1;
			request$2.onResponseStarted();
			return 0;
		}
		/**
		* @param {Buffer} buf
		* @returns {number}
		*/
		onHeaderField(buf) {
			const len = this.headers.length;
			if ((len & 1) === 0) this.headers.push(buf);
			else this.headers[len - 1] = Buffer.concat([this.headers[len - 1], buf]);
			this.trackHeader(buf.length);
			return 0;
		}
		/**
		* @param {Buffer} buf
		* @returns {number}
		*/
		onHeaderValue(buf) {
			let len = this.headers.length;
			if ((len & 1) === 1) {
				this.headers.push(buf);
				len += 1;
			} else this.headers[len - 1] = Buffer.concat([this.headers[len - 1], buf]);
			const key = this.headers[len - 2];
			if (key.length === 10) {
				const headerName = util$18.bufferToLowerCasedHeaderName(key);
				if (headerName === "keep-alive") this.keepAlive += buf.toString();
				else if (headerName === "connection") this.connection += buf.toString();
			} else if (key.length === 14 && util$18.bufferToLowerCasedHeaderName(key) === "content-length") this.contentLength += buf.toString();
			this.trackHeader(buf.length);
			return 0;
		}
		/**
		* @param {number} len
		*/
		trackHeader(len) {
			this.headersSize += len;
			if (this.headersSize >= this.headersMaxSize) util$18.destroy(this.socket, new HeadersOverflowError());
		}
		/**
		* @param {Buffer} head
		*/
		onUpgrade(head) {
			const { upgrade: upgrade$1, client, socket, headers, statusCode } = this;
			assert$23(upgrade$1);
			assert$23(client[kSocket$1] === socket);
			assert$23(!socket.destroyed);
			assert$23(!this.paused);
			assert$23((headers.length & 1) === 0);
			const request$2 = client[kQueue$3][client[kRunningIdx$2]];
			assert$23(request$2);
			assert$23(request$2.upgrade || request$2.method === "CONNECT");
			this.statusCode = 0;
			this.statusText = "";
			this.shouldKeepAlive = false;
			this.headers = [];
			this.headersSize = 0;
			socket.unshift(head);
			socket[kParser].destroy();
			socket[kParser] = null;
			socket[kClient$3] = null;
			socket[kError$2] = null;
			removeAllListeners(socket);
			client[kSocket$1] = null;
			client[kHTTPContext$2] = null;
			client[kQueue$3][client[kRunningIdx$2]++] = null;
			client.emit("disconnect", client[kUrl$6], [client], new InformationalError$3("upgrade"));
			try {
				request$2.onUpgrade(statusCode, headers, socket);
			} catch (err) {
				util$18.destroy(socket, err);
			}
			client[kResume$3]();
		}
		/**
		* @param {number} statusCode
		* @param {boolean} upgrade
		* @param {boolean} shouldKeepAlive
		* @returns {number}
		*/
		onHeadersComplete(statusCode, upgrade$1, shouldKeepAlive) {
			const { client, socket, headers, statusText } = this;
			/* istanbul ignore next: difficult to make a test case for */
			if (socket.destroyed) return -1;
			const request$2 = client[kQueue$3][client[kRunningIdx$2]];
			/* istanbul ignore next: difficult to make a test case for */
			if (!request$2) return -1;
			assert$23(!this.upgrade);
			assert$23(this.statusCode < 200);
			if (statusCode === 100) {
				util$18.destroy(socket, new SocketError$3("bad response", util$18.getSocketInfo(socket)));
				return -1;
			}
			if (upgrade$1 && !request$2.upgrade) {
				util$18.destroy(socket, new SocketError$3("bad upgrade", util$18.getSocketInfo(socket)));
				return -1;
			}
			assert$23(this.timeoutType === TIMEOUT_HEADERS);
			this.statusCode = statusCode;
			this.shouldKeepAlive = shouldKeepAlive || request$2.method === "HEAD" && !socket[kReset$1] && this.connection.toLowerCase() === "keep-alive";
			if (this.statusCode >= 200) {
				const bodyTimeout = request$2.bodyTimeout != null ? request$2.bodyTimeout : client[kBodyTimeout$2];
				this.setTimeout(bodyTimeout, TIMEOUT_BODY);
			} else if (this.timeout) {
				// istanbul ignore else: only for jest
				if (this.timeout.refresh) this.timeout.refresh();
			}
			if (request$2.method === "CONNECT") {
				assert$23(client[kRunning$4] === 1);
				this.upgrade = true;
				return 2;
			}
			if (upgrade$1) {
				assert$23(client[kRunning$4] === 1);
				this.upgrade = true;
				return 2;
			}
			assert$23((this.headers.length & 1) === 0);
			this.headers = [];
			this.headersSize = 0;
			if (this.shouldKeepAlive && client[kPipelining$1]) {
				const keepAliveTimeout = this.keepAlive ? util$18.parseKeepAliveTimeout(this.keepAlive) : null;
				if (keepAliveTimeout != null) {
					const timeout = Math.min(keepAliveTimeout - client[kKeepAliveTimeoutThreshold$1], client[kKeepAliveMaxTimeout$1]);
					if (timeout <= 0) socket[kReset$1] = true;
					else client[kKeepAliveTimeoutValue$1] = timeout;
				} else client[kKeepAliveTimeoutValue$1] = client[kKeepAliveDefaultTimeout$1];
			} else socket[kReset$1] = true;
			const pause = request$2.onHeaders(statusCode, headers, this.resume, statusText) === false;
			if (request$2.aborted) return -1;
			if (request$2.method === "HEAD") return 1;
			if (statusCode < 200) return 1;
			if (socket[kBlocking]) {
				socket[kBlocking] = false;
				client[kResume$3]();
			}
			return pause ? constants.ERROR.PAUSED : 0;
		}
		/**
		* @param {Buffer} buf
		* @returns {number}
		*/
		onBody(buf) {
			const { client, socket, statusCode, maxResponseSize } = this;
			if (socket.destroyed) return -1;
			const request$2 = client[kQueue$3][client[kRunningIdx$2]];
			assert$23(request$2);
			assert$23(this.timeoutType === TIMEOUT_BODY);
			if (this.timeout) {
				// istanbul ignore else: only for jest
				if (this.timeout.refresh) this.timeout.refresh();
			}
			assert$23(statusCode >= 200);
			if (maxResponseSize > -1 && this.bytesRead + buf.length > maxResponseSize) {
				util$18.destroy(socket, new ResponseExceededMaxSizeError());
				return -1;
			}
			this.bytesRead += buf.length;
			if (request$2.onData(buf) === false) return constants.ERROR.PAUSED;
			return 0;
		}
		/**
		* @returns {number}
		*/
		onMessageComplete() {
			const { client, socket, statusCode, upgrade: upgrade$1, headers, contentLength, bytesRead, shouldKeepAlive } = this;
			if (socket.destroyed && (!statusCode || shouldKeepAlive)) return -1;
			if (upgrade$1) return 0;
			assert$23(statusCode >= 100);
			assert$23((this.headers.length & 1) === 0);
			const request$2 = client[kQueue$3][client[kRunningIdx$2]];
			assert$23(request$2);
			this.statusCode = 0;
			this.statusText = "";
			this.bytesRead = 0;
			this.contentLength = "";
			this.keepAlive = "";
			this.connection = "";
			this.headers = [];
			this.headersSize = 0;
			if (statusCode < 200) return 0;
			/* istanbul ignore next: should be handled by llhttp? */
			if (request$2.method !== "HEAD" && contentLength && bytesRead !== parseInt(contentLength, 10)) {
				util$18.destroy(socket, new ResponseContentLengthMismatchError());
				return -1;
			}
			request$2.onComplete(headers);
			client[kQueue$3][client[kRunningIdx$2]++] = null;
			if (socket[kWriting]) {
				assert$23(client[kRunning$4] === 0);
				util$18.destroy(socket, new InformationalError$3("reset"));
				return constants.ERROR.PAUSED;
			} else if (!shouldKeepAlive) {
				util$18.destroy(socket, new InformationalError$3("reset"));
				return constants.ERROR.PAUSED;
			} else if (socket[kReset$1] && client[kRunning$4] === 0) {
				util$18.destroy(socket, new InformationalError$3("reset"));
				return constants.ERROR.PAUSED;
			} else if (client[kPipelining$1] == null || client[kPipelining$1] === 1) setImmediate(client[kResume$3]);
			else client[kResume$3]();
			return 0;
		}
	};
	function onParserTimeout(parser) {
		const { socket, timeoutType, client, paused } = parser.deref();
		/* istanbul ignore else */
		if (timeoutType === TIMEOUT_HEADERS) {
			if (!socket[kWriting] || socket.writableNeedDrain || client[kRunning$4] > 1) {
				assert$23(!paused, "cannot be paused while waiting for headers");
				util$18.destroy(socket, new HeadersTimeoutError());
			}
		} else if (timeoutType === TIMEOUT_BODY) {
			if (!paused) util$18.destroy(socket, new BodyTimeoutError());
		} else if (timeoutType === TIMEOUT_KEEP_ALIVE) {
			assert$23(client[kRunning$4] === 0 && client[kKeepAliveTimeoutValue$1]);
			util$18.destroy(socket, new InformationalError$3("socket idle timeout"));
		}
	}
	/**
	* @param {import ('./client.js')} client
	* @param {import('net').Socket} socket
	* @returns
	*/
	async function connectH1$1(client, socket) {
		client[kSocket$1] = socket;
		if (!llhttpInstance) llhttpInstance = lazyllhttp();
		if (socket.errored) throw socket.errored;
		if (socket.destroyed) throw new SocketError$3("destroyed");
		socket[kNoRef] = false;
		socket[kWriting] = false;
		socket[kReset$1] = false;
		socket[kBlocking] = false;
		socket[kParser] = new Parser(client, socket, llhttpInstance);
		util$18.addListener(socket, "error", onHttpSocketError);
		util$18.addListener(socket, "readable", onHttpSocketReadable);
		util$18.addListener(socket, "end", onHttpSocketEnd);
		util$18.addListener(socket, "close", onHttpSocketClose);
		socket[kClosed$2] = false;
		socket.on("close", onSocketClose$1);
		return {
			version: "h1",
			defaultPipelining: 1,
			write(request$2) {
				return writeH1(client, request$2);
			},
			resume() {
				resumeH1(client);
			},
			destroy(err, callback) {
				if (socket[kClosed$2]) queueMicrotask(callback);
				else {
					socket.on("close", callback);
					socket.destroy(err);
				}
			},
			get destroyed() {
				return socket.destroyed;
			},
			busy(request$2) {
				if (socket[kWriting] || socket[kReset$1] || socket[kBlocking]) return true;
				if (request$2) {
					if (client[kRunning$4] > 0 && !request$2.idempotent) return true;
					if (client[kRunning$4] > 0 && (request$2.upgrade || request$2.method === "CONNECT")) return true;
					if (client[kRunning$4] > 0 && util$18.bodyLength(request$2.body) !== 0 && (util$18.isStream(request$2.body) || util$18.isAsyncIterable(request$2.body) || util$18.isFormDataLike(request$2.body))) return true;
				}
				return false;
			}
		};
	}
	function onHttpSocketError(err) {
		assert$23(err.code !== "ERR_TLS_CERT_ALTNAME_INVALID");
		const parser = this[kParser];
		if (err.code === "ECONNRESET" && parser.statusCode && !parser.shouldKeepAlive) {
			parser.onMessageComplete();
			return;
		}
		this[kError$2] = err;
		this[kClient$3][kOnError$2](err);
	}
	function onHttpSocketReadable() {
		this[kParser]?.readMore();
	}
	function onHttpSocketEnd() {
		const parser = this[kParser];
		if (parser.statusCode && !parser.shouldKeepAlive) {
			parser.onMessageComplete();
			return;
		}
		util$18.destroy(this, new SocketError$3("other side closed", util$18.getSocketInfo(this)));
	}
	function onHttpSocketClose() {
		const parser = this[kParser];
		if (parser) {
			if (!this[kError$2] && parser.statusCode && !parser.shouldKeepAlive) parser.onMessageComplete();
			this[kParser].destroy();
			this[kParser] = null;
		}
		const err = this[kError$2] || new SocketError$3("closed", util$18.getSocketInfo(this));
		const client = this[kClient$3];
		client[kSocket$1] = null;
		client[kHTTPContext$2] = null;
		if (client.destroyed) {
			assert$23(client[kPending$3] === 0);
			const requests = client[kQueue$3].splice(client[kRunningIdx$2]);
			for (let i = 0; i < requests.length; i++) {
				const request$2 = requests[i];
				util$18.errorRequest(client, request$2, err);
			}
		} else if (client[kRunning$4] > 0 && err.code !== "UND_ERR_INFO") {
			const request$2 = client[kQueue$3][client[kRunningIdx$2]];
			client[kQueue$3][client[kRunningIdx$2]++] = null;
			util$18.errorRequest(client, request$2, err);
		}
		client[kPendingIdx$2] = client[kRunningIdx$2];
		assert$23(client[kRunning$4] === 0);
		client.emit("disconnect", client[kUrl$6], [client], err);
		client[kResume$3]();
	}
	function onSocketClose$1() {
		this[kClosed$2] = true;
	}
	/**
	* @param {import('./client.js')} client
	*/
	function resumeH1(client) {
		const socket = client[kSocket$1];
		if (socket && !socket.destroyed) {
			if (client[kSize$4] === 0) {
				if (!socket[kNoRef] && socket.unref) {
					socket.unref();
					socket[kNoRef] = true;
				}
			} else if (socket[kNoRef] && socket.ref) {
				socket.ref();
				socket[kNoRef] = false;
			}
			if (client[kSize$4] === 0) {
				if (socket[kParser].timeoutType !== TIMEOUT_KEEP_ALIVE) socket[kParser].setTimeout(client[kKeepAliveTimeoutValue$1], TIMEOUT_KEEP_ALIVE);
			} else if (client[kRunning$4] > 0 && socket[kParser].statusCode < 200) {
				if (socket[kParser].timeoutType !== TIMEOUT_HEADERS) {
					const request$2 = client[kQueue$3][client[kRunningIdx$2]];
					const headersTimeout = request$2.headersTimeout != null ? request$2.headersTimeout : client[kHeadersTimeout$1];
					socket[kParser].setTimeout(headersTimeout, TIMEOUT_HEADERS);
				}
			}
		}
	}
	function shouldSendContentLength$1(method) {
		return method !== "GET" && method !== "HEAD" && method !== "OPTIONS" && method !== "TRACE" && method !== "CONNECT";
	}
	/**
	* @param {import('./client.js')} client
	* @param {import('../core/request.js')} request
	* @returns
	*/
	function writeH1(client, request$2) {
		const { method, path: path$1, host, upgrade: upgrade$1, blocking, reset } = request$2;
		let { body, headers, contentLength } = request$2;
		const expectsPayload = method === "PUT" || method === "POST" || method === "PATCH" || method === "QUERY" || method === "PROPFIND" || method === "PROPPATCH";
		if (util$18.isFormDataLike(body)) {
			if (!extractBody$4) extractBody$4 = require_body().extractBody;
			const [bodyStream, contentType] = extractBody$4(body);
			if (request$2.contentType == null) headers.push("content-type", contentType);
			body = bodyStream.stream;
			contentLength = bodyStream.length;
		} else if (util$18.isBlobLike(body) && request$2.contentType == null && body.type) headers.push("content-type", body.type);
		if (body && typeof body.read === "function") body.read(0);
		const bodyLength$1 = util$18.bodyLength(body);
		contentLength = bodyLength$1 ?? contentLength;
		if (contentLength === null) contentLength = request$2.contentLength;
		if (contentLength === 0 && !expectsPayload) contentLength = null;
		if (shouldSendContentLength$1(method) && contentLength > 0 && request$2.contentLength !== null && request$2.contentLength !== contentLength) {
			if (client[kStrictContentLength$2]) {
				util$18.errorRequest(client, request$2, new RequestContentLengthMismatchError$1());
				return false;
			}
			process.emitWarning(new RequestContentLengthMismatchError$1());
		}
		const socket = client[kSocket$1];
		/**
		* @param {Error} [err]
		* @returns {void}
		*/
		const abort$1 = (err) => {
			if (request$2.aborted || request$2.completed) return;
			util$18.errorRequest(client, request$2, err || new RequestAbortedError$7());
			util$18.destroy(body);
			util$18.destroy(socket, new InformationalError$3("aborted"));
		};
		try {
			request$2.onConnect(abort$1);
		} catch (err) {
			util$18.errorRequest(client, request$2, err);
		}
		if (request$2.aborted) return false;
		if (method === "HEAD") socket[kReset$1] = true;
		if (upgrade$1 || method === "CONNECT") socket[kReset$1] = true;
		if (reset != null) socket[kReset$1] = reset;
		if (client[kMaxRequests$1] && socket[kCounter$1]++ >= client[kMaxRequests$1]) socket[kReset$1] = true;
		if (blocking) socket[kBlocking] = true;
		let header = `${method} ${path$1} HTTP/1.1\r\n`;
		if (typeof host === "string") header += `host: ${host}\r\n`;
		else header += client[kHostHeader$1];
		if (upgrade$1) header += `connection: upgrade\r\nupgrade: ${upgrade$1}\r\n`;
		else if (client[kPipelining$1] && !socket[kReset$1]) header += "connection: keep-alive\r\n";
		else header += "connection: close\r\n";
		if (Array.isArray(headers)) for (let n = 0; n < headers.length; n += 2) {
			const key = headers[n + 0];
			const val = headers[n + 1];
			if (Array.isArray(val)) for (let i = 0; i < val.length; i++) header += `${key}: ${val[i]}\r\n`;
			else header += `${key}: ${val}\r\n`;
		}
		if (channels$4.sendHeaders.hasSubscribers) channels$4.sendHeaders.publish({
			request: request$2,
			headers: header,
			socket
		});
		/* istanbul ignore else: assertion */
		if (!body || bodyLength$1 === 0) writeBuffer$1(abort$1, null, client, request$2, socket, contentLength, header, expectsPayload);
		else if (util$18.isBuffer(body)) writeBuffer$1(abort$1, body, client, request$2, socket, contentLength, header, expectsPayload);
		else if (util$18.isBlobLike(body)) if (typeof body.stream === "function") writeIterable$1(abort$1, body.stream(), client, request$2, socket, contentLength, header, expectsPayload);
		else writeBlob$1(abort$1, body, client, request$2, socket, contentLength, header, expectsPayload);
		else if (util$18.isStream(body)) writeStream$1(abort$1, body, client, request$2, socket, contentLength, header, expectsPayload);
		else if (util$18.isIterable(body)) writeIterable$1(abort$1, body, client, request$2, socket, contentLength, header, expectsPayload);
		else assert$23(false);
		return true;
	}
	/**
	* @param {AbortCallback} abort
	* @param {import('stream').Stream} body
	* @param {import('./client.js')} client
	* @param {import('../core/request.js')} request
	* @param {import('net').Socket} socket
	* @param {number} contentLength
	* @param {string} header
	* @param {boolean} expectsPayload
	*/
	function writeStream$1(abort$1, body, client, request$2, socket, contentLength, header, expectsPayload) {
		assert$23(contentLength !== 0 || client[kRunning$4] === 0, "stream body cannot be pipelined");
		let finished$2 = false;
		const writer = new AsyncWriter({
			abort: abort$1,
			socket,
			request: request$2,
			contentLength,
			client,
			expectsPayload,
			header
		});
		/**
		* @param {Buffer} chunk
		* @returns {void}
		*/
		const onData = function(chunk) {
			if (finished$2) return;
			try {
				if (!writer.write(chunk) && this.pause) this.pause();
			} catch (err) {
				util$18.destroy(this, err);
			}
		};
		/**
		* @returns {void}
		*/
		const onDrain = function() {
			if (finished$2) return;
			if (body.resume) body.resume();
		};
		/**
		* @returns {void}
		*/
		const onClose = function() {
			queueMicrotask(() => {
				body.removeListener("error", onFinished);
			});
			if (!finished$2) {
				const err = new RequestAbortedError$7();
				queueMicrotask(() => onFinished(err));
			}
		};
		/**
		* @param {Error} [err]
		* @returns
		*/
		const onFinished = function(err) {
			if (finished$2) return;
			finished$2 = true;
			assert$23(socket.destroyed || socket[kWriting] && client[kRunning$4] <= 1);
			socket.off("drain", onDrain).off("error", onFinished);
			body.removeListener("data", onData).removeListener("end", onFinished).removeListener("close", onClose);
			if (!err) try {
				writer.end();
			} catch (er) {
				err = er;
			}
			writer.destroy(err);
			if (err && (err.code !== "UND_ERR_INFO" || err.message !== "reset")) util$18.destroy(body, err);
			else util$18.destroy(body);
		};
		body.on("data", onData).on("end", onFinished).on("error", onFinished).on("close", onClose);
		if (body.resume) body.resume();
		socket.on("drain", onDrain).on("error", onFinished);
		if (body.errorEmitted ?? body.errored) setImmediate(onFinished, body.errored);
		else if (body.endEmitted ?? body.readableEnded) setImmediate(onFinished, null);
		if (body.closeEmitted ?? body.closed) setImmediate(onClose);
	}
	/**
	* @typedef AbortCallback
	* @type {Function}
	* @param {Error} [err]
	* @returns {void}
	*/
	/**
	* @param {AbortCallback} abort
	* @param {Uint8Array|null} body
	* @param {import('./client.js')} client
	* @param {import('../core/request.js')} request
	* @param {import('net').Socket} socket
	* @param {number} contentLength
	* @param {string} header
	* @param {boolean} expectsPayload
	* @returns {void}
	*/
	function writeBuffer$1(abort$1, body, client, request$2, socket, contentLength, header, expectsPayload) {
		try {
			if (!body) if (contentLength === 0) socket.write(`${header}content-length: 0\r\n\r\n`, "latin1");
			else {
				assert$23(contentLength === null, "no body must not have content length");
				socket.write(`${header}\r\n`, "latin1");
			}
			else if (util$18.isBuffer(body)) {
				assert$23(contentLength === body.byteLength, "buffer body must have content length");
				socket.cork();
				socket.write(`${header}content-length: ${contentLength}\r\n\r\n`, "latin1");
				socket.write(body);
				socket.uncork();
				request$2.onBodySent(body);
				if (!expectsPayload && request$2.reset !== false) socket[kReset$1] = true;
			}
			request$2.onRequestSent();
			client[kResume$3]();
		} catch (err) {
			abort$1(err);
		}
	}
	/**
	* @param {AbortCallback} abort
	* @param {Blob} body
	* @param {import('./client.js')} client
	* @param {import('../core/request.js')} request
	* @param {import('net').Socket} socket
	* @param {number} contentLength
	* @param {string} header
	* @param {boolean} expectsPayload
	* @returns {Promise<void>}
	*/
	async function writeBlob$1(abort$1, body, client, request$2, socket, contentLength, header, expectsPayload) {
		assert$23(contentLength === body.size, "blob body must have content length");
		try {
			if (contentLength != null && contentLength !== body.size) throw new RequestContentLengthMismatchError$1();
			const buffer$1 = Buffer.from(await body.arrayBuffer());
			socket.cork();
			socket.write(`${header}content-length: ${contentLength}\r\n\r\n`, "latin1");
			socket.write(buffer$1);
			socket.uncork();
			request$2.onBodySent(buffer$1);
			request$2.onRequestSent();
			if (!expectsPayload && request$2.reset !== false) socket[kReset$1] = true;
			client[kResume$3]();
		} catch (err) {
			abort$1(err);
		}
	}
	/**
	* @param {AbortCallback} abort
	* @param {Iterable} body
	* @param {import('./client.js')} client
	* @param {import('../core/request.js')} request
	* @param {import('net').Socket} socket
	* @param {number} contentLength
	* @param {string} header
	* @param {boolean} expectsPayload
	* @returns {Promise<void>}
	*/
	async function writeIterable$1(abort$1, body, client, request$2, socket, contentLength, header, expectsPayload) {
		assert$23(contentLength !== 0 || client[kRunning$4] === 0, "iterator body cannot be pipelined");
		let callback = null;
		function onDrain() {
			if (callback) {
				const cb = callback;
				callback = null;
				cb();
			}
		}
		const waitForDrain = () => new Promise((resolve$1, reject) => {
			assert$23(callback === null);
			if (socket[kError$2]) reject(socket[kError$2]);
			else callback = resolve$1;
		});
		socket.on("close", onDrain).on("drain", onDrain);
		const writer = new AsyncWriter({
			abort: abort$1,
			socket,
			request: request$2,
			contentLength,
			client,
			expectsPayload,
			header
		});
		try {
			for await (const chunk of body) {
				if (socket[kError$2]) throw socket[kError$2];
				if (!writer.write(chunk)) await waitForDrain();
			}
			writer.end();
		} catch (err) {
			writer.destroy(err);
		} finally {
			socket.off("close", onDrain).off("drain", onDrain);
		}
	}
	var AsyncWriter = class {
		/**
		*
		* @param {object} arg
		* @param {AbortCallback} arg.abort
		* @param {import('net').Socket} arg.socket
		* @param {import('../core/request.js')} arg.request
		* @param {number} arg.contentLength
		* @param {import('./client.js')} arg.client
		* @param {boolean} arg.expectsPayload
		* @param {string} arg.header
		*/
		constructor({ abort: abort$1, socket, request: request$2, contentLength, client, expectsPayload, header }) {
			this.socket = socket;
			this.request = request$2;
			this.contentLength = contentLength;
			this.client = client;
			this.bytesWritten = 0;
			this.expectsPayload = expectsPayload;
			this.header = header;
			this.abort = abort$1;
			socket[kWriting] = true;
		}
		/**
		* @param {Buffer} chunk
		* @returns
		*/
		write(chunk) {
			const { socket, request: request$2, contentLength, client, bytesWritten, expectsPayload, header } = this;
			if (socket[kError$2]) throw socket[kError$2];
			if (socket.destroyed) return false;
			const len = Buffer.byteLength(chunk);
			if (!len) return true;
			if (contentLength !== null && bytesWritten + len > contentLength) {
				if (client[kStrictContentLength$2]) throw new RequestContentLengthMismatchError$1();
				process.emitWarning(new RequestContentLengthMismatchError$1());
			}
			socket.cork();
			if (bytesWritten === 0) {
				if (!expectsPayload && request$2.reset !== false) socket[kReset$1] = true;
				if (contentLength === null) socket.write(`${header}transfer-encoding: chunked\r\n`, "latin1");
				else socket.write(`${header}content-length: ${contentLength}\r\n\r\n`, "latin1");
			}
			if (contentLength === null) socket.write(`\r\n${len.toString(16)}\r\n`, "latin1");
			this.bytesWritten += len;
			const ret = socket.write(chunk);
			socket.uncork();
			request$2.onBodySent(chunk);
			if (!ret) {
				if (socket[kParser].timeout && socket[kParser].timeoutType === TIMEOUT_HEADERS) {
					// istanbul ignore else: only for jest
					if (socket[kParser].timeout.refresh) socket[kParser].timeout.refresh();
				}
			}
			return ret;
		}
		/**
		* @returns {void}
		*/
		end() {
			const { socket, contentLength, client, bytesWritten, expectsPayload, header, request: request$2 } = this;
			request$2.onRequestSent();
			socket[kWriting] = false;
			if (socket[kError$2]) throw socket[kError$2];
			if (socket.destroyed) return;
			if (bytesWritten === 0) if (expectsPayload) socket.write(`${header}content-length: 0\r\n\r\n`, "latin1");
			else socket.write(`${header}\r\n`, "latin1");
			else if (contentLength === null) socket.write("\r\n0\r\n\r\n", "latin1");
			if (contentLength !== null && bytesWritten !== contentLength) if (client[kStrictContentLength$2]) throw new RequestContentLengthMismatchError$1();
			else process.emitWarning(new RequestContentLengthMismatchError$1());
			if (socket[kParser].timeout && socket[kParser].timeoutType === TIMEOUT_HEADERS) {
				// istanbul ignore else: only for jest
				if (socket[kParser].timeout.refresh) socket[kParser].timeout.refresh();
			}
			client[kResume$3]();
		}
		/**
		* @param {Error} [err]
		* @returns {void}
		*/
		destroy(err) {
			const { socket, client, abort: abort$1 } = this;
			socket[kWriting] = false;
			if (err) {
				assert$23(client[kRunning$4] <= 1, "pipeline should only contain this request");
				abort$1(err);
			}
		}
	};
	module.exports = connectH1$1;
}) });

//#endregion
//#region node_modules/undici/lib/dispatcher/client-h2.js
var require_client_h2 = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/dispatcher/client-h2.js": ((exports, module) => {
	const assert$22 = __require("node:assert");
	const { pipeline: pipeline$4 } = __require("node:stream");
	const util$17 = require_util$5();
	const { RequestContentLengthMismatchError, RequestAbortedError: RequestAbortedError$6, SocketError: SocketError$2, InformationalError: InformationalError$2 } = require_errors();
	const { kUrl: kUrl$5, kReset, kClient: kClient$2, kRunning: kRunning$3, kPending: kPending$2, kQueue: kQueue$2, kPendingIdx: kPendingIdx$1, kRunningIdx: kRunningIdx$1, kError: kError$1, kSocket, kStrictContentLength: kStrictContentLength$1, kOnError: kOnError$1, kMaxConcurrentStreams: kMaxConcurrentStreams$1, kHTTP2Session, kResume: kResume$2, kSize: kSize$3, kHTTPContext: kHTTPContext$1, kClosed: kClosed$1, kBodyTimeout: kBodyTimeout$1 } = require_symbols();
	const { channels: channels$3 } = require_diagnostics();
	const kOpenStreams = Symbol("open streams");
	let extractBody$3;
	/** @type {import('http2')} */
	let http2;
	try {
		http2 = __require("node:http2");
	} catch {
		http2 = { constants: {} };
	}
	const { constants: { HTTP2_HEADER_AUTHORITY, HTTP2_HEADER_METHOD, HTTP2_HEADER_PATH, HTTP2_HEADER_SCHEME, HTTP2_HEADER_CONTENT_LENGTH, HTTP2_HEADER_EXPECT, HTTP2_HEADER_STATUS } } = http2;
	function parseH2Headers(headers) {
		const result = [];
		for (const [name, value] of Object.entries(headers)) if (Array.isArray(value)) for (const subvalue of value) result.push(Buffer.from(name), Buffer.from(subvalue));
		else result.push(Buffer.from(name), Buffer.from(value));
		return result;
	}
	async function connectH2$1(client, socket) {
		client[kSocket] = socket;
		const session = http2.connect(client[kUrl$5], {
			createConnection: () => socket,
			peerMaxConcurrentStreams: client[kMaxConcurrentStreams$1],
			settings: { enablePush: false }
		});
		session[kOpenStreams] = 0;
		session[kClient$2] = client;
		session[kSocket] = socket;
		session[kHTTP2Session] = null;
		util$17.addListener(session, "error", onHttp2SessionError);
		util$17.addListener(session, "frameError", onHttp2FrameError);
		util$17.addListener(session, "end", onHttp2SessionEnd);
		util$17.addListener(session, "goaway", onHttp2SessionGoAway);
		util$17.addListener(session, "close", onHttp2SessionClose);
		session.unref();
		client[kHTTP2Session] = session;
		socket[kHTTP2Session] = session;
		util$17.addListener(socket, "error", onHttp2SocketError);
		util$17.addListener(socket, "end", onHttp2SocketEnd);
		util$17.addListener(socket, "close", onHttp2SocketClose);
		socket[kClosed$1] = false;
		socket.on("close", onSocketClose);
		return {
			version: "h2",
			defaultPipelining: Infinity,
			write(request$2) {
				return writeH2(client, request$2);
			},
			resume() {
				resumeH2(client);
			},
			destroy(err, callback) {
				if (socket[kClosed$1]) queueMicrotask(callback);
				else socket.destroy(err).on("close", callback);
			},
			get destroyed() {
				return socket.destroyed;
			},
			busy() {
				return false;
			}
		};
	}
	function resumeH2(client) {
		const socket = client[kSocket];
		if (socket?.destroyed === false) if (client[kSize$3] === 0 || client[kMaxConcurrentStreams$1] === 0) {
			socket.unref();
			client[kHTTP2Session].unref();
		} else {
			socket.ref();
			client[kHTTP2Session].ref();
		}
	}
	function onHttp2SessionError(err) {
		assert$22(err.code !== "ERR_TLS_CERT_ALTNAME_INVALID");
		this[kSocket][kError$1] = err;
		this[kClient$2][kOnError$1](err);
	}
	function onHttp2FrameError(type, code, id) {
		if (id === 0) {
			const err = new InformationalError$2(`HTTP/2: "frameError" received - type ${type}, code ${code}`);
			this[kSocket][kError$1] = err;
			this[kClient$2][kOnError$1](err);
		}
	}
	function onHttp2SessionEnd() {
		const err = new SocketError$2("other side closed", util$17.getSocketInfo(this[kSocket]));
		this.destroy(err);
		util$17.destroy(this[kSocket], err);
	}
	/**
	* This is the root cause of #3011
	* We need to handle GOAWAY frames properly, and trigger the session close
	* along with the socket right away
	*
	* @this {import('http2').ClientHttp2Session}
	* @param {number} errorCode
	*/
	function onHttp2SessionGoAway(errorCode) {
		const err = this[kError$1] || new SocketError$2(`HTTP/2: "GOAWAY" frame received with code ${errorCode}`, util$17.getSocketInfo(this[kSocket]));
		const client = this[kClient$2];
		client[kSocket] = null;
		client[kHTTPContext$1] = null;
		this.close();
		this[kHTTP2Session] = null;
		util$17.destroy(this[kSocket], err);
		if (client[kRunningIdx$1] < client[kQueue$2].length) {
			const request$2 = client[kQueue$2][client[kRunningIdx$1]];
			client[kQueue$2][client[kRunningIdx$1]++] = null;
			util$17.errorRequest(client, request$2, err);
			client[kPendingIdx$1] = client[kRunningIdx$1];
		}
		assert$22(client[kRunning$3] === 0);
		client.emit("disconnect", client[kUrl$5], [client], err);
		client.emit("connectionError", client[kUrl$5], [client], err);
		client[kResume$2]();
	}
	function onHttp2SessionClose() {
		const { [kClient$2]: client } = this;
		const { [kSocket]: socket } = client;
		const err = this[kSocket][kError$1] || this[kError$1] || new SocketError$2("closed", util$17.getSocketInfo(socket));
		client[kSocket] = null;
		client[kHTTPContext$1] = null;
		if (client.destroyed) {
			assert$22(client[kPending$2] === 0);
			const requests = client[kQueue$2].splice(client[kRunningIdx$1]);
			for (let i = 0; i < requests.length; i++) {
				const request$2 = requests[i];
				util$17.errorRequest(client, request$2, err);
			}
		}
	}
	function onHttp2SocketClose() {
		const err = this[kError$1] || new SocketError$2("closed", util$17.getSocketInfo(this));
		const client = this[kHTTP2Session][kClient$2];
		client[kSocket] = null;
		client[kHTTPContext$1] = null;
		if (this[kHTTP2Session] !== null) this[kHTTP2Session].destroy(err);
		client[kPendingIdx$1] = client[kRunningIdx$1];
		assert$22(client[kRunning$3] === 0);
		client.emit("disconnect", client[kUrl$5], [client], err);
		client[kResume$2]();
	}
	function onHttp2SocketError(err) {
		assert$22(err.code !== "ERR_TLS_CERT_ALTNAME_INVALID");
		this[kError$1] = err;
		this[kClient$2][kOnError$1](err);
	}
	function onHttp2SocketEnd() {
		util$17.destroy(this, new SocketError$2("other side closed", util$17.getSocketInfo(this)));
	}
	function onSocketClose() {
		this[kClosed$1] = true;
	}
	function shouldSendContentLength(method) {
		return method !== "GET" && method !== "HEAD" && method !== "OPTIONS" && method !== "TRACE" && method !== "CONNECT";
	}
	function writeH2(client, request$2) {
		const requestTimeout = request$2.bodyTimeout ?? client[kBodyTimeout$1];
		const session = client[kHTTP2Session];
		const { method, path: path$1, host, upgrade: upgrade$1, expectContinue, signal, headers: reqHeaders } = request$2;
		let { body } = request$2;
		if (upgrade$1) {
			util$17.errorRequest(client, request$2, /* @__PURE__ */ new Error("Upgrade not supported for H2"));
			return false;
		}
		const headers = {};
		for (let n = 0; n < reqHeaders.length; n += 2) {
			const key = reqHeaders[n + 0];
			const val = reqHeaders[n + 1];
			if (Array.isArray(val)) for (let i = 0; i < val.length; i++) if (headers[key]) headers[key] += `, ${val[i]}`;
			else headers[key] = val[i];
			else if (headers[key]) headers[key] += `, ${val}`;
			else headers[key] = val;
		}
		/** @type {import('node:http2').ClientHttp2Stream} */
		let stream$2 = null;
		const { hostname: hostname$1, port: port$1 } = client[kUrl$5];
		headers[HTTP2_HEADER_AUTHORITY] = host || `${hostname$1}${port$1 ? `:${port$1}` : ""}`;
		headers[HTTP2_HEADER_METHOD] = method;
		const abort$1 = (err) => {
			if (request$2.aborted || request$2.completed) return;
			err = err || new RequestAbortedError$6();
			util$17.errorRequest(client, request$2, err);
			if (stream$2 != null) {
				stream$2.removeAllListeners("data");
				stream$2.close();
				client[kOnError$1](err);
				client[kResume$2]();
			}
			util$17.destroy(body, err);
		};
		try {
			request$2.onConnect(abort$1);
		} catch (err) {
			util$17.errorRequest(client, request$2, err);
		}
		if (request$2.aborted) return false;
		if (method === "CONNECT") {
			session.ref();
			stream$2 = session.request(headers, {
				endStream: false,
				signal
			});
			if (!stream$2.pending) {
				request$2.onUpgrade(null, null, stream$2);
				++session[kOpenStreams];
				client[kQueue$2][client[kRunningIdx$1]++] = null;
			} else stream$2.once("ready", () => {
				request$2.onUpgrade(null, null, stream$2);
				++session[kOpenStreams];
				client[kQueue$2][client[kRunningIdx$1]++] = null;
			});
			stream$2.once("close", () => {
				session[kOpenStreams] -= 1;
				if (session[kOpenStreams] === 0) session.unref();
			});
			stream$2.setTimeout(requestTimeout);
			return true;
		}
		headers[HTTP2_HEADER_PATH] = path$1;
		headers[HTTP2_HEADER_SCHEME] = "https";
		const expectsPayload = method === "PUT" || method === "POST" || method === "PATCH";
		if (body && typeof body.read === "function") body.read(0);
		let contentLength = util$17.bodyLength(body);
		if (util$17.isFormDataLike(body)) {
			extractBody$3 ??= require_body().extractBody;
			const [bodyStream, contentType] = extractBody$3(body);
			headers["content-type"] = contentType;
			body = bodyStream.stream;
			contentLength = bodyStream.length;
		}
		if (contentLength == null) contentLength = request$2.contentLength;
		if (contentLength === 0 || !expectsPayload) contentLength = null;
		if (shouldSendContentLength(method) && contentLength > 0 && request$2.contentLength != null && request$2.contentLength !== contentLength) {
			if (client[kStrictContentLength$1]) {
				util$17.errorRequest(client, request$2, new RequestContentLengthMismatchError());
				return false;
			}
			process.emitWarning(new RequestContentLengthMismatchError());
		}
		if (contentLength != null) {
			assert$22(body, "no body must not have content length");
			headers[HTTP2_HEADER_CONTENT_LENGTH] = `${contentLength}`;
		}
		session.ref();
		if (channels$3.sendHeaders.hasSubscribers) {
			let header = "";
			for (const key in headers) header += `${key}: ${headers[key]}\r\n`;
			channels$3.sendHeaders.publish({
				request: request$2,
				headers: header,
				socket: session[kSocket]
			});
		}
		const shouldEndStream = method === "GET" || method === "HEAD" || body === null;
		if (expectContinue) {
			headers[HTTP2_HEADER_EXPECT] = "100-continue";
			stream$2 = session.request(headers, {
				endStream: shouldEndStream,
				signal
			});
			stream$2.once("continue", writeBodyH2);
		} else {
			stream$2 = session.request(headers, {
				endStream: shouldEndStream,
				signal
			});
			writeBodyH2();
		}
		++session[kOpenStreams];
		stream$2.setTimeout(requestTimeout);
		stream$2.once("response", (headers$1) => {
			const { [HTTP2_HEADER_STATUS]: statusCode,...realHeaders } = headers$1;
			request$2.onResponseStarted();
			if (request$2.aborted) {
				stream$2.removeAllListeners("data");
				return;
			}
			if (request$2.onHeaders(Number(statusCode), parseH2Headers(realHeaders), stream$2.resume.bind(stream$2), "") === false) stream$2.pause();
		});
		stream$2.on("data", (chunk) => {
			if (request$2.onData(chunk) === false) stream$2.pause();
		});
		stream$2.once("end", (err) => {
			stream$2.removeAllListeners("data");
			if (stream$2.state?.state == null || stream$2.state.state < 6) {
				if (!request$2.aborted && !request$2.completed) request$2.onComplete({});
				client[kQueue$2][client[kRunningIdx$1]++] = null;
				client[kResume$2]();
			} else {
				--session[kOpenStreams];
				if (session[kOpenStreams] === 0) session.unref();
				abort$1(err ?? new InformationalError$2("HTTP/2: stream half-closed (remote)"));
				client[kQueue$2][client[kRunningIdx$1]++] = null;
				client[kPendingIdx$1] = client[kRunningIdx$1];
				client[kResume$2]();
			}
		});
		stream$2.once("close", () => {
			stream$2.removeAllListeners("data");
			session[kOpenStreams] -= 1;
			if (session[kOpenStreams] === 0) session.unref();
		});
		stream$2.once("error", function(err) {
			stream$2.removeAllListeners("data");
			abort$1(err);
		});
		stream$2.once("frameError", (type, code) => {
			stream$2.removeAllListeners("data");
			abort$1(new InformationalError$2(`HTTP/2: "frameError" received - type ${type}, code ${code}`));
		});
		stream$2.on("aborted", () => {
			stream$2.removeAllListeners("data");
		});
		stream$2.on("timeout", () => {
			const err = new InformationalError$2(`HTTP/2: "stream timeout after ${requestTimeout}"`);
			stream$2.removeAllListeners("data");
			session[kOpenStreams] -= 1;
			if (session[kOpenStreams] === 0) session.unref();
			abort$1(err);
		});
		stream$2.once("trailers", (trailers) => {
			if (request$2.aborted || request$2.completed) return;
			request$2.onComplete(trailers);
		});
		return true;
		function writeBodyH2() {
			/* istanbul ignore else: assertion */
			if (!body || contentLength === 0) writeBuffer(abort$1, stream$2, null, client, request$2, client[kSocket], contentLength, expectsPayload);
			else if (util$17.isBuffer(body)) writeBuffer(abort$1, stream$2, body, client, request$2, client[kSocket], contentLength, expectsPayload);
			else if (util$17.isBlobLike(body)) if (typeof body.stream === "function") writeIterable(abort$1, stream$2, body.stream(), client, request$2, client[kSocket], contentLength, expectsPayload);
			else writeBlob(abort$1, stream$2, body, client, request$2, client[kSocket], contentLength, expectsPayload);
			else if (util$17.isStream(body)) writeStream(abort$1, client[kSocket], expectsPayload, stream$2, body, client, request$2, contentLength);
			else if (util$17.isIterable(body)) writeIterable(abort$1, stream$2, body, client, request$2, client[kSocket], contentLength, expectsPayload);
			else assert$22(false);
		}
	}
	function writeBuffer(abort$1, h2stream, body, client, request$2, socket, contentLength, expectsPayload) {
		try {
			if (body != null && util$17.isBuffer(body)) {
				assert$22(contentLength === body.byteLength, "buffer body must have content length");
				h2stream.cork();
				h2stream.write(body);
				h2stream.uncork();
				h2stream.end();
				request$2.onBodySent(body);
			}
			if (!expectsPayload) socket[kReset] = true;
			request$2.onRequestSent();
			client[kResume$2]();
		} catch (error) {
			abort$1(error);
		}
	}
	function writeStream(abort$1, socket, expectsPayload, h2stream, body, client, request$2, contentLength) {
		assert$22(contentLength !== 0 || client[kRunning$3] === 0, "stream body cannot be pipelined");
		const pipe = pipeline$4(body, h2stream, (err) => {
			if (err) {
				util$17.destroy(pipe, err);
				abort$1(err);
			} else {
				util$17.removeAllListeners(pipe);
				request$2.onRequestSent();
				if (!expectsPayload) socket[kReset] = true;
				client[kResume$2]();
			}
		});
		util$17.addListener(pipe, "data", onPipeData);
		function onPipeData(chunk) {
			request$2.onBodySent(chunk);
		}
	}
	async function writeBlob(abort$1, h2stream, body, client, request$2, socket, contentLength, expectsPayload) {
		assert$22(contentLength === body.size, "blob body must have content length");
		try {
			if (contentLength != null && contentLength !== body.size) throw new RequestContentLengthMismatchError();
			const buffer$1 = Buffer.from(await body.arrayBuffer());
			h2stream.cork();
			h2stream.write(buffer$1);
			h2stream.uncork();
			h2stream.end();
			request$2.onBodySent(buffer$1);
			request$2.onRequestSent();
			if (!expectsPayload) socket[kReset] = true;
			client[kResume$2]();
		} catch (err) {
			abort$1(err);
		}
	}
	async function writeIterable(abort$1, h2stream, body, client, request$2, socket, contentLength, expectsPayload) {
		assert$22(contentLength !== 0 || client[kRunning$3] === 0, "iterator body cannot be pipelined");
		let callback = null;
		function onDrain() {
			if (callback) {
				const cb = callback;
				callback = null;
				cb();
			}
		}
		const waitForDrain = () => new Promise((resolve$1, reject) => {
			assert$22(callback === null);
			if (socket[kError$1]) reject(socket[kError$1]);
			else callback = resolve$1;
		});
		h2stream.on("close", onDrain).on("drain", onDrain);
		try {
			for await (const chunk of body) {
				if (socket[kError$1]) throw socket[kError$1];
				const res = h2stream.write(chunk);
				request$2.onBodySent(chunk);
				if (!res) await waitForDrain();
			}
			h2stream.end();
			request$2.onRequestSent();
			if (!expectsPayload) socket[kReset] = true;
			client[kResume$2]();
		} catch (err) {
			abort$1(err);
		} finally {
			h2stream.off("close", onDrain).off("drain", onDrain);
		}
	}
	module.exports = connectH2$1;
}) });

//#endregion
//#region node_modules/undici/lib/dispatcher/client.js
var require_client = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/dispatcher/client.js": ((exports, module) => {
	const assert$21 = __require("node:assert");
	const net = __require("node:net");
	const http = __require("node:http");
	const util$16 = require_util$5();
	const { ClientStats } = require_stats();
	const { channels: channels$2 } = require_diagnostics();
	const Request$3 = require_request$1();
	const DispatcherBase$5 = require_dispatcher_base();
	const { InvalidArgumentError: InvalidArgumentError$25, InformationalError: InformationalError$1, ClientDestroyedError } = require_errors();
	const buildConnector$3 = require_connect();
	const { kUrl: kUrl$4, kServerName, kClient: kClient$1, kBusy: kBusy$1, kConnect, kResuming, kRunning: kRunning$2, kPending: kPending$1, kSize: kSize$2, kQueue: kQueue$1, kConnected: kConnected$3, kConnecting, kNeedDrain: kNeedDrain$3, kKeepAliveDefaultTimeout, kHostHeader, kPendingIdx, kRunningIdx, kError, kPipelining, kKeepAliveTimeoutValue, kMaxHeadersSize, kKeepAliveMaxTimeout, kKeepAliveTimeoutThreshold, kHeadersTimeout, kBodyTimeout, kStrictContentLength, kConnector, kMaxRequests, kCounter, kClose: kClose$7, kDestroy: kDestroy$5, kDispatch: kDispatch$4, kLocalAddress, kMaxResponseSize, kOnError, kHTTPContext, kMaxConcurrentStreams, kResume: kResume$1 } = require_symbols();
	const connectH1 = require_client_h1();
	const connectH2 = require_client_h2();
	const kClosedResolve$1 = Symbol("kClosedResolve");
	const getDefaultNodeMaxHeaderSize = http && http.maxHeaderSize && Number.isInteger(http.maxHeaderSize) && http.maxHeaderSize > 0 ? () => http.maxHeaderSize : () => {
		throw new InvalidArgumentError$25("http module not available or http.maxHeaderSize invalid");
	};
	const noop$7 = () => {};
	function getPipelining(client) {
		return client[kPipelining] ?? client[kHTTPContext]?.defaultPipelining ?? 1;
	}
	/**
	* @type {import('../../types/client.js').default}
	*/
	var Client$6 = class extends DispatcherBase$5 {
		/**
		*
		* @param {string|URL} url
		* @param {import('../../types/client.js').Client.Options} options
		*/
		constructor(url, { maxHeaderSize, headersTimeout, socketTimeout, requestTimeout, connectTimeout, bodyTimeout, idleTimeout, keepAlive, keepAliveTimeout, maxKeepAliveTimeout, keepAliveMaxTimeout, keepAliveTimeoutThreshold, socketPath, pipelining, tls: tls$1, strictContentLength, maxCachedSessions, connect: connect$3, maxRequestsPerClient, localAddress, maxResponseSize, autoSelectFamily, autoSelectFamilyAttemptTimeout, maxConcurrentStreams, allowH2 } = {}) {
			if (keepAlive !== void 0) throw new InvalidArgumentError$25("unsupported keepAlive, use pipelining=0 instead");
			if (socketTimeout !== void 0) throw new InvalidArgumentError$25("unsupported socketTimeout, use headersTimeout & bodyTimeout instead");
			if (requestTimeout !== void 0) throw new InvalidArgumentError$25("unsupported requestTimeout, use headersTimeout & bodyTimeout instead");
			if (idleTimeout !== void 0) throw new InvalidArgumentError$25("unsupported idleTimeout, use keepAliveTimeout instead");
			if (maxKeepAliveTimeout !== void 0) throw new InvalidArgumentError$25("unsupported maxKeepAliveTimeout, use keepAliveMaxTimeout instead");
			if (maxHeaderSize != null) {
				if (!Number.isInteger(maxHeaderSize) || maxHeaderSize < 1) throw new InvalidArgumentError$25("invalid maxHeaderSize");
			} else maxHeaderSize = getDefaultNodeMaxHeaderSize();
			if (socketPath != null && typeof socketPath !== "string") throw new InvalidArgumentError$25("invalid socketPath");
			if (connectTimeout != null && (!Number.isFinite(connectTimeout) || connectTimeout < 0)) throw new InvalidArgumentError$25("invalid connectTimeout");
			if (keepAliveTimeout != null && (!Number.isFinite(keepAliveTimeout) || keepAliveTimeout <= 0)) throw new InvalidArgumentError$25("invalid keepAliveTimeout");
			if (keepAliveMaxTimeout != null && (!Number.isFinite(keepAliveMaxTimeout) || keepAliveMaxTimeout <= 0)) throw new InvalidArgumentError$25("invalid keepAliveMaxTimeout");
			if (keepAliveTimeoutThreshold != null && !Number.isFinite(keepAliveTimeoutThreshold)) throw new InvalidArgumentError$25("invalid keepAliveTimeoutThreshold");
			if (headersTimeout != null && (!Number.isInteger(headersTimeout) || headersTimeout < 0)) throw new InvalidArgumentError$25("headersTimeout must be a positive integer or zero");
			if (bodyTimeout != null && (!Number.isInteger(bodyTimeout) || bodyTimeout < 0)) throw new InvalidArgumentError$25("bodyTimeout must be a positive integer or zero");
			if (connect$3 != null && typeof connect$3 !== "function" && typeof connect$3 !== "object") throw new InvalidArgumentError$25("connect must be a function or an object");
			if (maxRequestsPerClient != null && (!Number.isInteger(maxRequestsPerClient) || maxRequestsPerClient < 0)) throw new InvalidArgumentError$25("maxRequestsPerClient must be a positive number");
			if (localAddress != null && (typeof localAddress !== "string" || net.isIP(localAddress) === 0)) throw new InvalidArgumentError$25("localAddress must be valid string IP address");
			if (maxResponseSize != null && (!Number.isInteger(maxResponseSize) || maxResponseSize < -1)) throw new InvalidArgumentError$25("maxResponseSize must be a positive number");
			if (autoSelectFamilyAttemptTimeout != null && (!Number.isInteger(autoSelectFamilyAttemptTimeout) || autoSelectFamilyAttemptTimeout < -1)) throw new InvalidArgumentError$25("autoSelectFamilyAttemptTimeout must be a positive number");
			if (allowH2 != null && typeof allowH2 !== "boolean") throw new InvalidArgumentError$25("allowH2 must be a valid boolean value");
			if (maxConcurrentStreams != null && (typeof maxConcurrentStreams !== "number" || maxConcurrentStreams < 1)) throw new InvalidArgumentError$25("maxConcurrentStreams must be a positive integer, greater than 0");
			super();
			if (typeof connect$3 !== "function") connect$3 = buildConnector$3({
				...tls$1,
				maxCachedSessions,
				allowH2,
				socketPath,
				timeout: connectTimeout,
				...typeof autoSelectFamily === "boolean" ? {
					autoSelectFamily,
					autoSelectFamilyAttemptTimeout
				} : void 0,
				...connect$3
			});
			this[kUrl$4] = util$16.parseOrigin(url);
			this[kConnector] = connect$3;
			this[kPipelining] = pipelining != null ? pipelining : 1;
			this[kMaxHeadersSize] = maxHeaderSize;
			this[kKeepAliveDefaultTimeout] = keepAliveTimeout == null ? 4e3 : keepAliveTimeout;
			this[kKeepAliveMaxTimeout] = keepAliveMaxTimeout == null ? 6e5 : keepAliveMaxTimeout;
			this[kKeepAliveTimeoutThreshold] = keepAliveTimeoutThreshold == null ? 2e3 : keepAliveTimeoutThreshold;
			this[kKeepAliveTimeoutValue] = this[kKeepAliveDefaultTimeout];
			this[kServerName] = null;
			this[kLocalAddress] = localAddress != null ? localAddress : null;
			this[kResuming] = 0;
			this[kNeedDrain$3] = 0;
			this[kHostHeader] = `host: ${this[kUrl$4].hostname}${this[kUrl$4].port ? `:${this[kUrl$4].port}` : ""}\r\n`;
			this[kBodyTimeout] = bodyTimeout != null ? bodyTimeout : 3e5;
			this[kHeadersTimeout] = headersTimeout != null ? headersTimeout : 3e5;
			this[kStrictContentLength] = strictContentLength == null ? true : strictContentLength;
			this[kMaxRequests] = maxRequestsPerClient;
			this[kClosedResolve$1] = null;
			this[kMaxResponseSize] = maxResponseSize > -1 ? maxResponseSize : -1;
			this[kMaxConcurrentStreams] = maxConcurrentStreams != null ? maxConcurrentStreams : 100;
			this[kHTTPContext] = null;
			this[kQueue$1] = [];
			this[kRunningIdx] = 0;
			this[kPendingIdx] = 0;
			this[kResume$1] = (sync) => resume(this, sync);
			this[kOnError] = (err) => onError(this, err);
		}
		get pipelining() {
			return this[kPipelining];
		}
		set pipelining(value) {
			this[kPipelining] = value;
			this[kResume$1](true);
		}
		get stats() {
			return new ClientStats(this);
		}
		get [kPending$1]() {
			return this[kQueue$1].length - this[kPendingIdx];
		}
		get [kRunning$2]() {
			return this[kPendingIdx] - this[kRunningIdx];
		}
		get [kSize$2]() {
			return this[kQueue$1].length - this[kRunningIdx];
		}
		get [kConnected$3]() {
			return !!this[kHTTPContext] && !this[kConnecting] && !this[kHTTPContext].destroyed;
		}
		get [kBusy$1]() {
			return Boolean(this[kHTTPContext]?.busy(null) || this[kSize$2] >= (getPipelining(this) || 1) || this[kPending$1] > 0);
		}
		/* istanbul ignore: only used for test */
		[kConnect](cb) {
			connect$2(this);
			this.once("connect", cb);
		}
		[kDispatch$4](opts, handler) {
			const origin = opts.origin || this[kUrl$4].origin;
			const request$2 = new Request$3(origin, opts, handler);
			this[kQueue$1].push(request$2);
			if (this[kResuming]) {} else if (util$16.bodyLength(request$2.body) == null && util$16.isIterable(request$2.body)) {
				this[kResuming] = 1;
				queueMicrotask(() => resume(this));
			} else this[kResume$1](true);
			if (this[kResuming] && this[kNeedDrain$3] !== 2 && this[kBusy$1]) this[kNeedDrain$3] = 2;
			return this[kNeedDrain$3] < 2;
		}
		async [kClose$7]() {
			return new Promise((resolve$1) => {
				if (this[kSize$2]) this[kClosedResolve$1] = resolve$1;
				else resolve$1(null);
			});
		}
		async [kDestroy$5](err) {
			return new Promise((resolve$1) => {
				const requests = this[kQueue$1].splice(this[kPendingIdx]);
				for (let i = 0; i < requests.length; i++) {
					const request$2 = requests[i];
					util$16.errorRequest(this, request$2, err);
				}
				const callback = () => {
					if (this[kClosedResolve$1]) {
						this[kClosedResolve$1]();
						this[kClosedResolve$1] = null;
					}
					resolve$1(null);
				};
				if (this[kHTTPContext]) {
					this[kHTTPContext].destroy(err, callback);
					this[kHTTPContext] = null;
				} else queueMicrotask(callback);
				this[kResume$1]();
			});
		}
	};
	function onError(client, err) {
		if (client[kRunning$2] === 0 && err.code !== "UND_ERR_INFO" && err.code !== "UND_ERR_SOCKET") {
			assert$21(client[kPendingIdx] === client[kRunningIdx]);
			const requests = client[kQueue$1].splice(client[kRunningIdx]);
			for (let i = 0; i < requests.length; i++) {
				const request$2 = requests[i];
				util$16.errorRequest(client, request$2, err);
			}
			assert$21(client[kSize$2] === 0);
		}
	}
	/**
	* @param {Client} client
	* @returns
	*/
	async function connect$2(client) {
		assert$21(!client[kConnecting]);
		assert$21(!client[kHTTPContext]);
		let { host, hostname: hostname$1, protocol, port: port$1 } = client[kUrl$4];
		if (hostname$1[0] === "[") {
			const idx = hostname$1.indexOf("]");
			assert$21(idx !== -1);
			const ip = hostname$1.substring(1, idx);
			assert$21(net.isIPv6(ip));
			hostname$1 = ip;
		}
		client[kConnecting] = true;
		if (channels$2.beforeConnect.hasSubscribers) channels$2.beforeConnect.publish({
			connectParams: {
				host,
				hostname: hostname$1,
				protocol,
				port: port$1,
				version: client[kHTTPContext]?.version,
				servername: client[kServerName],
				localAddress: client[kLocalAddress]
			},
			connector: client[kConnector]
		});
		try {
			const socket = await new Promise((resolve$1, reject) => {
				client[kConnector]({
					host,
					hostname: hostname$1,
					protocol,
					port: port$1,
					servername: client[kServerName],
					localAddress: client[kLocalAddress]
				}, (err, socket$1) => {
					if (err) reject(err);
					else resolve$1(socket$1);
				});
			});
			if (client.destroyed) {
				util$16.destroy(socket.on("error", noop$7), new ClientDestroyedError());
				return;
			}
			assert$21(socket);
			try {
				client[kHTTPContext] = socket.alpnProtocol === "h2" ? await connectH2(client, socket) : await connectH1(client, socket);
			} catch (err) {
				socket.destroy().on("error", noop$7);
				throw err;
			}
			client[kConnecting] = false;
			socket[kCounter] = 0;
			socket[kMaxRequests] = client[kMaxRequests];
			socket[kClient$1] = client;
			socket[kError] = null;
			if (channels$2.connected.hasSubscribers) channels$2.connected.publish({
				connectParams: {
					host,
					hostname: hostname$1,
					protocol,
					port: port$1,
					version: client[kHTTPContext]?.version,
					servername: client[kServerName],
					localAddress: client[kLocalAddress]
				},
				connector: client[kConnector],
				socket
			});
			client.emit("connect", client[kUrl$4], [client]);
		} catch (err) {
			if (client.destroyed) return;
			client[kConnecting] = false;
			if (channels$2.connectError.hasSubscribers) channels$2.connectError.publish({
				connectParams: {
					host,
					hostname: hostname$1,
					protocol,
					port: port$1,
					version: client[kHTTPContext]?.version,
					servername: client[kServerName],
					localAddress: client[kLocalAddress]
				},
				connector: client[kConnector],
				error: err
			});
			if (err.code === "ERR_TLS_CERT_ALTNAME_INVALID") {
				assert$21(client[kRunning$2] === 0);
				while (client[kPending$1] > 0 && client[kQueue$1][client[kPendingIdx]].servername === client[kServerName]) {
					const request$2 = client[kQueue$1][client[kPendingIdx]++];
					util$16.errorRequest(client, request$2, err);
				}
			} else onError(client, err);
			client.emit("connectionError", client[kUrl$4], [client], err);
		}
		client[kResume$1]();
	}
	function emitDrain(client) {
		client[kNeedDrain$3] = 0;
		client.emit("drain", client[kUrl$4], [client]);
	}
	function resume(client, sync) {
		if (client[kResuming] === 2) return;
		client[kResuming] = 2;
		_resume(client, sync);
		client[kResuming] = 0;
		if (client[kRunningIdx] > 256) {
			client[kQueue$1].splice(0, client[kRunningIdx]);
			client[kPendingIdx] -= client[kRunningIdx];
			client[kRunningIdx] = 0;
		}
	}
	function _resume(client, sync) {
		while (true) {
			if (client.destroyed) {
				assert$21(client[kPending$1] === 0);
				return;
			}
			if (client[kClosedResolve$1] && !client[kSize$2]) {
				client[kClosedResolve$1]();
				client[kClosedResolve$1] = null;
				return;
			}
			if (client[kHTTPContext]) client[kHTTPContext].resume();
			if (client[kBusy$1]) client[kNeedDrain$3] = 2;
			else if (client[kNeedDrain$3] === 2) {
				if (sync) {
					client[kNeedDrain$3] = 1;
					queueMicrotask(() => emitDrain(client));
				} else emitDrain(client);
				continue;
			}
			if (client[kPending$1] === 0) return;
			if (client[kRunning$2] >= (getPipelining(client) || 1)) return;
			const request$2 = client[kQueue$1][client[kPendingIdx]];
			if (client[kUrl$4].protocol === "https:" && client[kServerName] !== request$2.servername) {
				if (client[kRunning$2] > 0) return;
				client[kServerName] = request$2.servername;
				client[kHTTPContext]?.destroy(new InformationalError$1("servername changed"), () => {
					client[kHTTPContext] = null;
					resume(client);
				});
			}
			if (client[kConnecting]) return;
			if (!client[kHTTPContext]) {
				connect$2(client);
				return;
			}
			if (client[kHTTPContext].destroyed) return;
			if (client[kHTTPContext].busy(request$2)) return;
			if (!request$2.aborted && client[kHTTPContext].write(request$2)) client[kPendingIdx]++;
			else client[kQueue$1].splice(client[kPendingIdx], 1);
		}
	}
	module.exports = Client$6;
}) });

//#endregion
//#region node_modules/undici/lib/dispatcher/fixed-queue.js
var require_fixed_queue = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/dispatcher/fixed-queue.js": ((exports, module) => {
	const kSize$1 = 2048;
	const kMask = kSize$1 - 1;
	/**
	* @type {FixedCircularBuffer}
	* @template T
	*/
	var FixedCircularBuffer = class {
		constructor() {
			/**
			* @type {number}
			*/
			this.bottom = 0;
			/**
			* @type {number}
			*/
			this.top = 0;
			/**
			* @type {Array<T|undefined>}
			*/
			this.list = new Array(kSize$1).fill(void 0);
			/**
			* @type {T|null}
			*/
			this.next = null;
		}
		/**
		* @returns {boolean}
		*/
		isEmpty() {
			return this.top === this.bottom;
		}
		/**
		* @returns {boolean}
		*/
		isFull() {
			return (this.top + 1 & kMask) === this.bottom;
		}
		/**
		* @param {T} data
		* @returns {void}
		*/
		push(data) {
			this.list[this.top] = data;
			this.top = this.top + 1 & kMask;
		}
		/**
		* @returns {T|null}
		*/
		shift() {
			const nextItem = this.list[this.bottom];
			if (nextItem === void 0) return null;
			this.list[this.bottom] = void 0;
			this.bottom = this.bottom + 1 & kMask;
			return nextItem;
		}
	};
	/**
	* @template T
	*/
	module.exports = class FixedQueue$2 {
		constructor() {
			/**
			* @type {FixedCircularBuffer<T>}
			*/
			this.head = this.tail = new FixedCircularBuffer();
		}
		/**
		* @returns {boolean}
		*/
		isEmpty() {
			return this.head.isEmpty();
		}
		/**
		* @param {T} data
		*/
		push(data) {
			if (this.head.isFull()) this.head = this.head.next = new FixedCircularBuffer();
			this.head.push(data);
		}
		/**
		* @returns {T|null}
		*/
		shift() {
			const tail$1 = this.tail;
			const next = tail$1.shift();
			if (tail$1.isEmpty() && tail$1.next !== null) {
				this.tail = tail$1.next;
				tail$1.next = null;
			}
			return next;
		}
	};
}) });

//#endregion
//#region node_modules/undici/lib/dispatcher/pool-base.js
var require_pool_base = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/dispatcher/pool-base.js": ((exports, module) => {
	const { PoolStats } = require_stats();
	const DispatcherBase$4 = require_dispatcher_base();
	const FixedQueue$1 = require_fixed_queue();
	const { kConnected: kConnected$2, kSize, kRunning: kRunning$1, kPending, kQueued, kBusy, kFree, kUrl: kUrl$3, kClose: kClose$6, kDestroy: kDestroy$4, kDispatch: kDispatch$3 } = require_symbols();
	const kClients$4 = Symbol("clients");
	const kNeedDrain$2 = Symbol("needDrain");
	const kQueue = Symbol("queue");
	const kClosedResolve = Symbol("closed resolve");
	const kOnDrain$1 = Symbol("onDrain");
	const kOnConnect$1 = Symbol("onConnect");
	const kOnDisconnect$1 = Symbol("onDisconnect");
	const kOnConnectionError$1 = Symbol("onConnectionError");
	const kGetDispatcher$2 = Symbol("get dispatcher");
	const kAddClient$2 = Symbol("add client");
	const kRemoveClient$2 = Symbol("remove client");
	var PoolBase$2 = class extends DispatcherBase$4 {
		constructor() {
			super();
			this[kQueue] = new FixedQueue$1();
			this[kClients$4] = [];
			this[kQueued] = 0;
			const pool = this;
			this[kOnDrain$1] = function onDrain(origin, targets) {
				const queue = pool[kQueue];
				let needDrain = false;
				while (!needDrain) {
					const item = queue.shift();
					if (!item) break;
					pool[kQueued]--;
					needDrain = !this.dispatch(item.opts, item.handler);
				}
				this[kNeedDrain$2] = needDrain;
				if (!this[kNeedDrain$2] && pool[kNeedDrain$2]) {
					pool[kNeedDrain$2] = false;
					pool.emit("drain", origin, [pool, ...targets]);
				}
				if (pool[kClosedResolve] && queue.isEmpty()) Promise.all(pool[kClients$4].map((c) => c.close())).then(pool[kClosedResolve]);
			};
			this[kOnConnect$1] = (origin, targets) => {
				pool.emit("connect", origin, [pool, ...targets]);
			};
			this[kOnDisconnect$1] = (origin, targets, err) => {
				pool.emit("disconnect", origin, [pool, ...targets], err);
			};
			this[kOnConnectionError$1] = (origin, targets, err) => {
				pool.emit("connectionError", origin, [pool, ...targets], err);
			};
		}
		get [kBusy]() {
			return this[kNeedDrain$2];
		}
		get [kConnected$2]() {
			return this[kClients$4].filter((client) => client[kConnected$2]).length;
		}
		get [kFree]() {
			return this[kClients$4].filter((client) => client[kConnected$2] && !client[kNeedDrain$2]).length;
		}
		get [kPending]() {
			let ret = this[kQueued];
			for (const { [kPending]: pending } of this[kClients$4]) ret += pending;
			return ret;
		}
		get [kRunning$1]() {
			let ret = 0;
			for (const { [kRunning$1]: running } of this[kClients$4]) ret += running;
			return ret;
		}
		get [kSize]() {
			let ret = this[kQueued];
			for (const { [kSize]: size } of this[kClients$4]) ret += size;
			return ret;
		}
		get stats() {
			return new PoolStats(this);
		}
		async [kClose$6]() {
			if (this[kQueue].isEmpty()) await Promise.all(this[kClients$4].map((c) => c.close()));
			else await new Promise((resolve$1) => {
				this[kClosedResolve] = resolve$1;
			});
		}
		async [kDestroy$4](err) {
			while (true) {
				const item = this[kQueue].shift();
				if (!item) break;
				item.handler.onError(err);
			}
			await Promise.all(this[kClients$4].map((c) => c.destroy(err)));
		}
		[kDispatch$3](opts, handler) {
			const dispatcher = this[kGetDispatcher$2]();
			if (!dispatcher) {
				this[kNeedDrain$2] = true;
				this[kQueue].push({
					opts,
					handler
				});
				this[kQueued]++;
			} else if (!dispatcher.dispatch(opts, handler)) {
				dispatcher[kNeedDrain$2] = true;
				this[kNeedDrain$2] = !this[kGetDispatcher$2]();
			}
			return !this[kNeedDrain$2];
		}
		[kAddClient$2](client) {
			client.on("drain", this[kOnDrain$1]).on("connect", this[kOnConnect$1]).on("disconnect", this[kOnDisconnect$1]).on("connectionError", this[kOnConnectionError$1]);
			this[kClients$4].push(client);
			if (this[kNeedDrain$2]) queueMicrotask(() => {
				if (this[kNeedDrain$2]) this[kOnDrain$1](client[kUrl$3], [this, client]);
			});
			return this;
		}
		[kRemoveClient$2](client) {
			client.close(() => {
				const idx = this[kClients$4].indexOf(client);
				if (idx !== -1) this[kClients$4].splice(idx, 1);
			});
			this[kNeedDrain$2] = this[kClients$4].some((dispatcher) => !dispatcher[kNeedDrain$2] && dispatcher.closed !== true && dispatcher.destroyed !== true);
		}
	};
	module.exports = {
		PoolBase: PoolBase$2,
		kClients: kClients$4,
		kNeedDrain: kNeedDrain$2,
		kAddClient: kAddClient$2,
		kRemoveClient: kRemoveClient$2,
		kGetDispatcher: kGetDispatcher$2
	};
}) });

//#endregion
//#region node_modules/undici/lib/dispatcher/pool.js
var require_pool = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/dispatcher/pool.js": ((exports, module) => {
	const { PoolBase: PoolBase$1, kClients: kClients$3, kNeedDrain: kNeedDrain$1, kAddClient: kAddClient$1, kGetDispatcher: kGetDispatcher$1, kRemoveClient: kRemoveClient$1 } = require_pool_base();
	const Client$5 = require_client();
	const { InvalidArgumentError: InvalidArgumentError$24 } = require_errors();
	const util$15 = require_util$5();
	const { kUrl: kUrl$2 } = require_symbols();
	const buildConnector$2 = require_connect();
	const kOptions$3 = Symbol("options");
	const kConnections = Symbol("connections");
	const kFactory$3 = Symbol("factory");
	function defaultFactory$3(origin, opts) {
		return new Client$5(origin, opts);
	}
	var Pool$5 = class extends PoolBase$1 {
		constructor(origin, { connections, factory = defaultFactory$3, connect: connect$3, connectTimeout, tls: tls$1, maxCachedSessions, socketPath, autoSelectFamily, autoSelectFamilyAttemptTimeout, allowH2, clientTtl,...options } = {}) {
			if (connections != null && (!Number.isFinite(connections) || connections < 0)) throw new InvalidArgumentError$24("invalid connections");
			if (typeof factory !== "function") throw new InvalidArgumentError$24("factory must be a function.");
			if (connect$3 != null && typeof connect$3 !== "function" && typeof connect$3 !== "object") throw new InvalidArgumentError$24("connect must be a function or an object");
			super();
			if (typeof connect$3 !== "function") connect$3 = buildConnector$2({
				...tls$1,
				maxCachedSessions,
				allowH2,
				socketPath,
				timeout: connectTimeout,
				...typeof autoSelectFamily === "boolean" ? {
					autoSelectFamily,
					autoSelectFamilyAttemptTimeout
				} : void 0,
				...connect$3
			});
			this[kConnections] = connections || null;
			this[kUrl$2] = util$15.parseOrigin(origin);
			this[kOptions$3] = {
				...util$15.deepClone(options),
				connect: connect$3,
				allowH2,
				clientTtl
			};
			this[kOptions$3].interceptors = options.interceptors ? { ...options.interceptors } : void 0;
			this[kFactory$3] = factory;
			this.on("connect", (origin$1, targets) => {
				if (clientTtl != null && clientTtl > 0) for (const target of targets) Object.assign(target, { ttl: Date.now() });
			});
			this.on("connectionError", (origin$1, targets, error) => {
				for (const target of targets) {
					const idx = this[kClients$3].indexOf(target);
					if (idx !== -1) this[kClients$3].splice(idx, 1);
				}
			});
		}
		[kGetDispatcher$1]() {
			const clientTtlOption = this[kOptions$3].clientTtl;
			for (const client of this[kClients$3]) if (clientTtlOption != null && clientTtlOption > 0 && client.ttl && Date.now() - client.ttl > clientTtlOption) this[kRemoveClient$1](client);
			else if (!client[kNeedDrain$1]) return client;
			if (!this[kConnections] || this[kClients$3].length < this[kConnections]) {
				const dispatcher = this[kFactory$3](this[kUrl$2], this[kOptions$3]);
				this[kAddClient$1](dispatcher);
				return dispatcher;
			}
		}
	};
	module.exports = Pool$5;
}) });

//#endregion
//#region node_modules/undici/lib/dispatcher/balanced-pool.js
var require_balanced_pool = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/dispatcher/balanced-pool.js": ((exports, module) => {
	const { BalancedPoolMissingUpstreamError, InvalidArgumentError: InvalidArgumentError$23 } = require_errors();
	const { PoolBase, kClients: kClients$2, kNeedDrain, kAddClient, kRemoveClient, kGetDispatcher } = require_pool_base();
	const Pool$4 = require_pool();
	const { kUrl: kUrl$1 } = require_symbols();
	const { parseOrigin } = require_util$5();
	const kFactory$2 = Symbol("factory");
	const kOptions$2 = Symbol("options");
	const kGreatestCommonDivisor = Symbol("kGreatestCommonDivisor");
	const kCurrentWeight = Symbol("kCurrentWeight");
	const kIndex = Symbol("kIndex");
	const kWeight = Symbol("kWeight");
	const kMaxWeightPerServer = Symbol("kMaxWeightPerServer");
	const kErrorPenalty = Symbol("kErrorPenalty");
	/**
	* Calculate the greatest common divisor of two numbers by
	* using the Euclidean algorithm.
	*
	* @param {number} a
	* @param {number} b
	* @returns {number}
	*/
	function getGreatestCommonDivisor(a, b) {
		if (a === 0) return b;
		while (b !== 0) {
			const t = b;
			b = a % b;
			a = t;
		}
		return a;
	}
	function defaultFactory$2(origin, opts) {
		return new Pool$4(origin, opts);
	}
	var BalancedPool$1 = class extends PoolBase {
		constructor(upstreams = [], { factory = defaultFactory$2,...opts } = {}) {
			if (typeof factory !== "function") throw new InvalidArgumentError$23("factory must be a function.");
			super();
			this[kOptions$2] = opts;
			this[kIndex] = -1;
			this[kCurrentWeight] = 0;
			this[kMaxWeightPerServer] = this[kOptions$2].maxWeightPerServer || 100;
			this[kErrorPenalty] = this[kOptions$2].errorPenalty || 15;
			if (!Array.isArray(upstreams)) upstreams = [upstreams];
			this[kFactory$2] = factory;
			for (const upstream of upstreams) this.addUpstream(upstream);
			this._updateBalancedPoolStats();
		}
		addUpstream(upstream) {
			const upstreamOrigin = parseOrigin(upstream).origin;
			if (this[kClients$2].find((pool$1) => pool$1[kUrl$1].origin === upstreamOrigin && pool$1.closed !== true && pool$1.destroyed !== true)) return this;
			const pool = this[kFactory$2](upstreamOrigin, Object.assign({}, this[kOptions$2]));
			this[kAddClient](pool);
			pool.on("connect", () => {
				pool[kWeight] = Math.min(this[kMaxWeightPerServer], pool[kWeight] + this[kErrorPenalty]);
			});
			pool.on("connectionError", () => {
				pool[kWeight] = Math.max(1, pool[kWeight] - this[kErrorPenalty]);
				this._updateBalancedPoolStats();
			});
			pool.on("disconnect", (...args) => {
				const err = args[2];
				if (err && err.code === "UND_ERR_SOCKET") {
					pool[kWeight] = Math.max(1, pool[kWeight] - this[kErrorPenalty]);
					this._updateBalancedPoolStats();
				}
			});
			for (const client of this[kClients$2]) client[kWeight] = this[kMaxWeightPerServer];
			this._updateBalancedPoolStats();
			return this;
		}
		_updateBalancedPoolStats() {
			let result = 0;
			for (let i = 0; i < this[kClients$2].length; i++) result = getGreatestCommonDivisor(this[kClients$2][i][kWeight], result);
			this[kGreatestCommonDivisor] = result;
		}
		removeUpstream(upstream) {
			const upstreamOrigin = parseOrigin(upstream).origin;
			const pool = this[kClients$2].find((pool$1) => pool$1[kUrl$1].origin === upstreamOrigin && pool$1.closed !== true && pool$1.destroyed !== true);
			if (pool) this[kRemoveClient](pool);
			return this;
		}
		get upstreams() {
			return this[kClients$2].filter((dispatcher) => dispatcher.closed !== true && dispatcher.destroyed !== true).map((p) => p[kUrl$1].origin);
		}
		[kGetDispatcher]() {
			if (this[kClients$2].length === 0) throw new BalancedPoolMissingUpstreamError();
			if (!this[kClients$2].find((dispatcher) => !dispatcher[kNeedDrain] && dispatcher.closed !== true && dispatcher.destroyed !== true)) return;
			if (this[kClients$2].map((pool) => pool[kNeedDrain]).reduce((a, b) => a && b, true)) return;
			let counter = 0;
			let maxWeightIndex = this[kClients$2].findIndex((pool) => !pool[kNeedDrain]);
			while (counter++ < this[kClients$2].length) {
				this[kIndex] = (this[kIndex] + 1) % this[kClients$2].length;
				const pool = this[kClients$2][this[kIndex]];
				if (pool[kWeight] > this[kClients$2][maxWeightIndex][kWeight] && !pool[kNeedDrain]) maxWeightIndex = this[kIndex];
				if (this[kIndex] === 0) {
					this[kCurrentWeight] = this[kCurrentWeight] - this[kGreatestCommonDivisor];
					if (this[kCurrentWeight] <= 0) this[kCurrentWeight] = this[kMaxWeightPerServer];
				}
				if (pool[kWeight] >= this[kCurrentWeight] && !pool[kNeedDrain]) return pool;
			}
			this[kCurrentWeight] = this[kClients$2][maxWeightIndex][kWeight];
			this[kIndex] = maxWeightIndex;
			return this[kClients$2][maxWeightIndex];
		}
	};
	module.exports = BalancedPool$1;
}) });

//#endregion
//#region node_modules/undici/lib/dispatcher/agent.js
var require_agent = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/dispatcher/agent.js": ((exports, module) => {
	const { InvalidArgumentError: InvalidArgumentError$22 } = require_errors();
	const { kClients: kClients$1, kRunning, kClose: kClose$5, kDestroy: kDestroy$3, kDispatch: kDispatch$2, kUrl } = require_symbols();
	const DispatcherBase$3 = require_dispatcher_base();
	const Pool$3 = require_pool();
	const Client$4 = require_client();
	const util$14 = require_util$5();
	const kOnConnect = Symbol("onConnect");
	const kOnDisconnect = Symbol("onDisconnect");
	const kOnConnectionError = Symbol("onConnectionError");
	const kOnDrain = Symbol("onDrain");
	const kFactory$1 = Symbol("factory");
	const kOptions$1 = Symbol("options");
	function defaultFactory$1(origin, opts) {
		return opts && opts.connections === 1 ? new Client$4(origin, opts) : new Pool$3(origin, opts);
	}
	var Agent$6 = class extends DispatcherBase$3 {
		constructor({ factory = defaultFactory$1, connect: connect$3,...options } = {}) {
			if (typeof factory !== "function") throw new InvalidArgumentError$22("factory must be a function.");
			if (connect$3 != null && typeof connect$3 !== "function" && typeof connect$3 !== "object") throw new InvalidArgumentError$22("connect must be a function or an object");
			super();
			if (connect$3 && typeof connect$3 !== "function") connect$3 = { ...connect$3 };
			this[kOptions$1] = {
				...util$14.deepClone(options),
				connect: connect$3
			};
			this[kFactory$1] = factory;
			this[kClients$1] = /* @__PURE__ */ new Map();
			this[kOnDrain] = (origin, targets) => {
				this.emit("drain", origin, [this, ...targets]);
			};
			this[kOnConnect] = (origin, targets) => {
				this.emit("connect", origin, [this, ...targets]);
			};
			this[kOnDisconnect] = (origin, targets, err) => {
				this.emit("disconnect", origin, [this, ...targets], err);
			};
			this[kOnConnectionError] = (origin, targets, err) => {
				this.emit("connectionError", origin, [this, ...targets], err);
			};
		}
		get [kRunning]() {
			let ret = 0;
			for (const { dispatcher } of this[kClients$1].values()) ret += dispatcher[kRunning];
			return ret;
		}
		[kDispatch$2](opts, handler) {
			let key;
			if (opts.origin && (typeof opts.origin === "string" || opts.origin instanceof URL)) key = String(opts.origin);
			else throw new InvalidArgumentError$22("opts.origin must be a non-empty string or URL.");
			const result = this[kClients$1].get(key);
			let dispatcher = result && result.dispatcher;
			if (!dispatcher) {
				const closeClientIfUnused = (connected) => {
					const result$1 = this[kClients$1].get(key);
					if (result$1) {
						if (connected) result$1.count -= 1;
						if (result$1.count <= 0) {
							this[kClients$1].delete(key);
							result$1.dispatcher.close();
						}
					}
				};
				dispatcher = this[kFactory$1](opts.origin, this[kOptions$1]).on("drain", this[kOnDrain]).on("connect", (origin, targets) => {
					const result$1 = this[kClients$1].get(key);
					if (result$1) result$1.count += 1;
					this[kOnConnect](origin, targets);
				}).on("disconnect", (origin, targets, err) => {
					closeClientIfUnused(true);
					this[kOnDisconnect](origin, targets, err);
				}).on("connectionError", (origin, targets, err) => {
					closeClientIfUnused(false);
					this[kOnConnectionError](origin, targets, err);
				});
				this[kClients$1].set(key, {
					count: 0,
					dispatcher
				});
			}
			return dispatcher.dispatch(opts, handler);
		}
		async [kClose$5]() {
			const closePromises = [];
			for (const { dispatcher } of this[kClients$1].values()) closePromises.push(dispatcher.close());
			this[kClients$1].clear();
			await Promise.all(closePromises);
		}
		async [kDestroy$3](err) {
			const destroyPromises = [];
			for (const { dispatcher } of this[kClients$1].values()) destroyPromises.push(dispatcher.destroy(err));
			this[kClients$1].clear();
			await Promise.all(destroyPromises);
		}
		get stats() {
			const allClientStats = {};
			for (const { dispatcher } of this[kClients$1].values()) if (dispatcher.stats) allClientStats[dispatcher[kUrl].origin] = dispatcher.stats;
			return allClientStats;
		}
	};
	module.exports = Agent$6;
}) });

//#endregion
//#region node_modules/undici/lib/dispatcher/proxy-agent.js
var require_proxy_agent = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/dispatcher/proxy-agent.js": ((exports, module) => {
	const { kProxy, kClose: kClose$4, kDestroy: kDestroy$2, kDispatch: kDispatch$1 } = require_symbols();
	const Agent$5 = require_agent();
	const Pool$2 = require_pool();
	const DispatcherBase$2 = require_dispatcher_base();
	const { InvalidArgumentError: InvalidArgumentError$21, RequestAbortedError: RequestAbortedError$5, SecureProxyConnectionError } = require_errors();
	const buildConnector$1 = require_connect();
	const Client$3 = require_client();
	const kAgent$1 = Symbol("proxy agent");
	const kClient = Symbol("proxy client");
	const kProxyHeaders = Symbol("proxy headers");
	const kRequestTls = Symbol("request tls settings");
	const kProxyTls = Symbol("proxy tls settings");
	const kConnectEndpoint = Symbol("connect endpoint function");
	const kTunnelProxy = Symbol("tunnel proxy");
	function defaultProtocolPort(protocol) {
		return protocol === "https:" ? 443 : 80;
	}
	function defaultFactory(origin, opts) {
		return new Pool$2(origin, opts);
	}
	const noop$6 = () => {};
	function defaultAgentFactory(origin, opts) {
		if (opts.connections === 1) return new Client$3(origin, opts);
		return new Pool$2(origin, opts);
	}
	var Http1ProxyWrapper = class extends DispatcherBase$2 {
		#client;
		constructor(proxyUrl, { headers = {}, connect: connect$3, factory }) {
			super();
			if (!proxyUrl) throw new InvalidArgumentError$21("Proxy URL is mandatory");
			this[kProxyHeaders] = headers;
			if (factory) this.#client = factory(proxyUrl, { connect: connect$3 });
			else this.#client = new Client$3(proxyUrl, { connect: connect$3 });
		}
		[kDispatch$1](opts, handler) {
			const onHeaders = handler.onHeaders;
			handler.onHeaders = function(statusCode, data, resume$1) {
				if (statusCode === 407) {
					if (typeof handler.onError === "function") handler.onError(new InvalidArgumentError$21("Proxy Authentication Required (407)"));
					return;
				}
				if (onHeaders) onHeaders.call(this, statusCode, data, resume$1);
			};
			const { origin, path: path$1 = "/", headers = {} } = opts;
			opts.path = origin + path$1;
			if (!("host" in headers) && !("Host" in headers)) {
				const { host } = new URL(origin);
				headers.host = host;
			}
			opts.headers = {
				...this[kProxyHeaders],
				...headers
			};
			return this.#client[kDispatch$1](opts, handler);
		}
		async [kClose$4]() {
			return this.#client.close();
		}
		async [kDestroy$2](err) {
			return this.#client.destroy(err);
		}
	};
	var ProxyAgent$2 = class extends DispatcherBase$2 {
		constructor(opts) {
			if (!opts || typeof opts === "object" && !(opts instanceof URL) && !opts.uri) throw new InvalidArgumentError$21("Proxy uri is mandatory");
			const { clientFactory = defaultFactory } = opts;
			if (typeof clientFactory !== "function") throw new InvalidArgumentError$21("Proxy opts.clientFactory must be a function.");
			const { proxyTunnel = true } = opts;
			super();
			const url = this.#getUrl(opts);
			const { href, origin, port: port$1, protocol, username, password, hostname: proxyHostname } = url;
			this[kProxy] = {
				uri: href,
				protocol
			};
			this[kRequestTls] = opts.requestTls;
			this[kProxyTls] = opts.proxyTls;
			this[kProxyHeaders] = opts.headers || {};
			this[kTunnelProxy] = proxyTunnel;
			if (opts.auth && opts.token) throw new InvalidArgumentError$21("opts.auth cannot be used in combination with opts.token");
			else if (opts.auth) this[kProxyHeaders]["proxy-authorization"] = `Basic ${opts.auth}`;
			else if (opts.token) this[kProxyHeaders]["proxy-authorization"] = opts.token;
			else if (username && password) this[kProxyHeaders]["proxy-authorization"] = `Basic ${Buffer.from(`${decodeURIComponent(username)}:${decodeURIComponent(password)}`).toString("base64")}`;
			const connect$3 = buildConnector$1({ ...opts.proxyTls });
			this[kConnectEndpoint] = buildConnector$1({ ...opts.requestTls });
			const agentFactory = opts.factory || defaultAgentFactory;
			const factory = (origin$1, options) => {
				const { protocol: protocol$1 } = new URL(origin$1);
				if (!this[kTunnelProxy] && protocol$1 === "http:" && this[kProxy].protocol === "http:") return new Http1ProxyWrapper(this[kProxy].uri, {
					headers: this[kProxyHeaders],
					connect: connect$3,
					factory: agentFactory
				});
				return agentFactory(origin$1, options);
			};
			this[kClient] = clientFactory(url, { connect: connect$3 });
			this[kAgent$1] = new Agent$5({
				...opts,
				factory,
				connect: async (opts$1, callback) => {
					let requestedPath = opts$1.host;
					if (!opts$1.port) requestedPath += `:${defaultProtocolPort(opts$1.protocol)}`;
					try {
						const { socket, statusCode } = await this[kClient].connect({
							origin,
							port: port$1,
							path: requestedPath,
							signal: opts$1.signal,
							headers: {
								...this[kProxyHeaders],
								host: opts$1.host,
								...opts$1.connections == null || opts$1.connections > 0 ? { "proxy-connection": "keep-alive" } : {}
							},
							servername: this[kProxyTls]?.servername || proxyHostname
						});
						if (statusCode !== 200) {
							socket.on("error", noop$6).destroy();
							callback(new RequestAbortedError$5(`Proxy response (${statusCode}) !== 200 when HTTP Tunneling`));
						}
						if (opts$1.protocol !== "https:") {
							callback(null, socket);
							return;
						}
						let servername;
						if (this[kRequestTls]) servername = this[kRequestTls].servername;
						else servername = opts$1.servername;
						this[kConnectEndpoint]({
							...opts$1,
							servername,
							httpSocket: socket
						}, callback);
					} catch (err) {
						if (err.code === "ERR_TLS_CERT_ALTNAME_INVALID") callback(new SecureProxyConnectionError(err));
						else callback(err);
					}
				}
			});
		}
		dispatch(opts, handler) {
			const headers = buildHeaders(opts.headers);
			throwIfProxyAuthIsSent(headers);
			if (headers && !("host" in headers) && !("Host" in headers)) {
				const { host } = new URL(opts.origin);
				headers.host = host;
			}
			return this[kAgent$1].dispatch({
				...opts,
				headers
			}, handler);
		}
		/**
		* @param {import('../../types/proxy-agent').ProxyAgent.Options | string | URL} opts
		* @returns {URL}
		*/
		#getUrl(opts) {
			if (typeof opts === "string") return new URL(opts);
			else if (opts instanceof URL) return opts;
			else return new URL(opts.uri);
		}
		async [kClose$4]() {
			await this[kAgent$1].close();
			await this[kClient].close();
		}
		async [kDestroy$2]() {
			await this[kAgent$1].destroy();
			await this[kClient].destroy();
		}
	};
	/**
	* @param {string[] | Record<string, string>} headers
	* @returns {Record<string, string>}
	*/
	function buildHeaders(headers) {
		if (Array.isArray(headers)) {
			/** @type {Record<string, string>} */
			const headersPair = {};
			for (let i = 0; i < headers.length; i += 2) headersPair[headers[i]] = headers[i + 1];
			return headersPair;
		}
		return headers;
	}
	/**
	* @param {Record<string, string>} headers
	*
	* Previous versions of ProxyAgent suggests the Proxy-Authorization in request headers
	* Nevertheless, it was changed and to avoid a security vulnerability by end users
	* this check was created.
	* It should be removed in the next major version for performance reasons
	*/
	function throwIfProxyAuthIsSent(headers) {
		if (headers && Object.keys(headers).find((key) => key.toLowerCase() === "proxy-authorization")) throw new InvalidArgumentError$21("Proxy-Authorization should be sent in ProxyAgent constructor");
	}
	module.exports = ProxyAgent$2;
}) });

//#endregion
//#region node_modules/undici/lib/dispatcher/env-http-proxy-agent.js
var require_env_http_proxy_agent = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/dispatcher/env-http-proxy-agent.js": ((exports, module) => {
	const DispatcherBase$1 = require_dispatcher_base();
	const { kClose: kClose$3, kDestroy: kDestroy$1, kClosed, kDestroyed, kDispatch, kNoProxyAgent, kHttpProxyAgent, kHttpsProxyAgent } = require_symbols();
	const ProxyAgent$1 = require_proxy_agent();
	const Agent$4 = require_agent();
	const DEFAULT_PORTS = {
		"http:": 80,
		"https:": 443
	};
	var EnvHttpProxyAgent$1 = class extends DispatcherBase$1 {
		#noProxyValue = null;
		#noProxyEntries = null;
		#opts = null;
		constructor(opts = {}) {
			super();
			this.#opts = opts;
			const { httpProxy, httpsProxy, noProxy,...agentOpts } = opts;
			this[kNoProxyAgent] = new Agent$4(agentOpts);
			const HTTP_PROXY = httpProxy ?? process.env.http_proxy ?? process.env.HTTP_PROXY;
			if (HTTP_PROXY) this[kHttpProxyAgent] = new ProxyAgent$1({
				...agentOpts,
				uri: HTTP_PROXY
			});
			else this[kHttpProxyAgent] = this[kNoProxyAgent];
			const HTTPS_PROXY = httpsProxy ?? process.env.https_proxy ?? process.env.HTTPS_PROXY;
			if (HTTPS_PROXY) this[kHttpsProxyAgent] = new ProxyAgent$1({
				...agentOpts,
				uri: HTTPS_PROXY
			});
			else this[kHttpsProxyAgent] = this[kHttpProxyAgent];
			this.#parseNoProxy();
		}
		[kDispatch](opts, handler) {
			const url = new URL(opts.origin);
			return this.#getProxyAgentForUrl(url).dispatch(opts, handler);
		}
		async [kClose$3]() {
			await this[kNoProxyAgent].close();
			if (!this[kHttpProxyAgent][kClosed]) await this[kHttpProxyAgent].close();
			if (!this[kHttpsProxyAgent][kClosed]) await this[kHttpsProxyAgent].close();
		}
		async [kDestroy$1](err) {
			await this[kNoProxyAgent].destroy(err);
			if (!this[kHttpProxyAgent][kDestroyed]) await this[kHttpProxyAgent].destroy(err);
			if (!this[kHttpsProxyAgent][kDestroyed]) await this[kHttpsProxyAgent].destroy(err);
		}
		#getProxyAgentForUrl(url) {
			let { protocol, host: hostname$1, port: port$1 } = url;
			hostname$1 = hostname$1.replace(/:\d*$/, "").toLowerCase();
			port$1 = Number.parseInt(port$1, 10) || DEFAULT_PORTS[protocol] || 0;
			if (!this.#shouldProxy(hostname$1, port$1)) return this[kNoProxyAgent];
			if (protocol === "https:") return this[kHttpsProxyAgent];
			return this[kHttpProxyAgent];
		}
		#shouldProxy(hostname$1, port$1) {
			if (this.#noProxyChanged) this.#parseNoProxy();
			if (this.#noProxyEntries.length === 0) return true;
			if (this.#noProxyValue === "*") return false;
			for (let i = 0; i < this.#noProxyEntries.length; i++) {
				const entry = this.#noProxyEntries[i];
				if (entry.port && entry.port !== port$1) continue;
				if (!/^[.*]/.test(entry.hostname)) {
					if (hostname$1 === entry.hostname) return false;
				} else if (hostname$1.endsWith(entry.hostname.replace(/^\*/, ""))) return false;
			}
			return true;
		}
		#parseNoProxy() {
			const noProxyValue = this.#opts.noProxy ?? this.#noProxyEnv;
			const noProxySplit = noProxyValue.split(/[,\s]/);
			const noProxyEntries = [];
			for (let i = 0; i < noProxySplit.length; i++) {
				const entry = noProxySplit[i];
				if (!entry) continue;
				const parsed = entry.match(/^(.+):(\d+)$/);
				noProxyEntries.push({
					hostname: (parsed ? parsed[1] : entry).toLowerCase(),
					port: parsed ? Number.parseInt(parsed[2], 10) : 0
				});
			}
			this.#noProxyValue = noProxyValue;
			this.#noProxyEntries = noProxyEntries;
		}
		get #noProxyChanged() {
			if (this.#opts.noProxy !== void 0) return false;
			return this.#noProxyValue !== this.#noProxyEnv;
		}
		get #noProxyEnv() {
			return process.env.no_proxy ?? process.env.NO_PROXY ?? "";
		}
	};
	module.exports = EnvHttpProxyAgent$1;
}) });

//#endregion
//#region node_modules/undici/lib/handler/retry-handler.js
var require_retry_handler = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/handler/retry-handler.js": ((exports, module) => {
	const assert$20 = __require("node:assert");
	const { kRetryHandlerDefaultRetry } = require_symbols();
	const { RequestRetryError } = require_errors();
	const WrapHandler$2 = require_wrap_handler();
	const { isDisturbed: isDisturbed$1, parseRangeHeader, wrapRequestBody } = require_util$5();
	function calculateRetryAfterHeader(retryAfter) {
		const retryTime = new Date(retryAfter).getTime();
		return isNaN(retryTime) ? 0 : retryTime - Date.now();
	}
	var RetryHandler$3 = class RetryHandler$3 {
		constructor(opts, { dispatch, handler }) {
			const { retryOptions,...dispatchOpts } = opts;
			const { retry: retryFn, maxRetries, maxTimeout, minTimeout, timeoutFactor, methods, errorCodes, retryAfter, statusCodes, throwOnError } = retryOptions ?? {};
			this.error = null;
			this.dispatch = dispatch;
			this.handler = WrapHandler$2.wrap(handler);
			this.opts = {
				...dispatchOpts,
				body: wrapRequestBody(opts.body)
			};
			this.retryOpts = {
				throwOnError: throwOnError ?? true,
				retry: retryFn ?? RetryHandler$3[kRetryHandlerDefaultRetry],
				retryAfter: retryAfter ?? true,
				maxTimeout: maxTimeout ?? 30 * 1e3,
				minTimeout: minTimeout ?? 500,
				timeoutFactor: timeoutFactor ?? 2,
				maxRetries: maxRetries ?? 5,
				methods: methods ?? [
					"GET",
					"HEAD",
					"OPTIONS",
					"PUT",
					"DELETE",
					"TRACE"
				],
				statusCodes: statusCodes ?? [
					500,
					502,
					503,
					504,
					429
				],
				errorCodes: errorCodes ?? [
					"ECONNRESET",
					"ECONNREFUSED",
					"ENOTFOUND",
					"ENETDOWN",
					"ENETUNREACH",
					"EHOSTDOWN",
					"EHOSTUNREACH",
					"EPIPE",
					"UND_ERR_SOCKET"
				]
			};
			this.retryCount = 0;
			this.retryCountCheckpoint = 0;
			this.headersSent = false;
			this.start = 0;
			this.end = null;
			this.etag = null;
		}
		onResponseStartWithRetry(controller, statusCode, headers, statusMessage, err) {
			if (this.retryOpts.throwOnError) {
				if (this.retryOpts.statusCodes.includes(statusCode) === false) {
					this.headersSent = true;
					this.handler.onResponseStart?.(controller, statusCode, headers, statusMessage);
				} else this.error = err;
				return;
			}
			if (isDisturbed$1(this.opts.body)) {
				this.headersSent = true;
				this.handler.onResponseStart?.(controller, statusCode, headers, statusMessage);
				return;
			}
			function shouldRetry(passedErr) {
				if (passedErr) {
					this.headersSent = true;
					this.headersSent = true;
					this.handler.onResponseStart?.(controller, statusCode, headers, statusMessage);
					controller.resume();
					return;
				}
				this.error = err;
				controller.resume();
			}
			controller.pause();
			this.retryOpts.retry(err, {
				state: { counter: this.retryCount },
				opts: {
					retryOptions: this.retryOpts,
					...this.opts
				}
			}, shouldRetry.bind(this));
		}
		onRequestStart(controller, context) {
			if (!this.headersSent) this.handler.onRequestStart?.(controller, context);
		}
		onRequestUpgrade(controller, statusCode, headers, socket) {
			this.handler.onRequestUpgrade?.(controller, statusCode, headers, socket);
		}
		static [kRetryHandlerDefaultRetry](err, { state, opts }, cb) {
			const { statusCode, code, headers } = err;
			const { method, retryOptions } = opts;
			const { maxRetries, minTimeout, maxTimeout, timeoutFactor, statusCodes, errorCodes, methods } = retryOptions;
			const { counter } = state;
			if (code && code !== "UND_ERR_REQ_RETRY" && !errorCodes.includes(code)) {
				cb(err);
				return;
			}
			if (Array.isArray(methods) && !methods.includes(method)) {
				cb(err);
				return;
			}
			if (statusCode != null && Array.isArray(statusCodes) && !statusCodes.includes(statusCode)) {
				cb(err);
				return;
			}
			if (counter > maxRetries) {
				cb(err);
				return;
			}
			let retryAfterHeader = headers?.["retry-after"];
			if (retryAfterHeader) {
				retryAfterHeader = Number(retryAfterHeader);
				retryAfterHeader = Number.isNaN(retryAfterHeader) ? calculateRetryAfterHeader(headers["retry-after"]) : retryAfterHeader * 1e3;
			}
			const retryTimeout = retryAfterHeader > 0 ? Math.min(retryAfterHeader, maxTimeout) : Math.min(minTimeout * timeoutFactor ** (counter - 1), maxTimeout);
			setTimeout(() => cb(null), retryTimeout);
		}
		onResponseStart(controller, statusCode, headers, statusMessage) {
			this.error = null;
			this.retryCount += 1;
			if (statusCode >= 300) {
				const err = new RequestRetryError("Request failed", statusCode, {
					headers,
					data: { count: this.retryCount }
				});
				this.onResponseStartWithRetry(controller, statusCode, headers, statusMessage, err);
				return;
			}
			if (this.headersSent) {
				if (statusCode !== 206 && (this.start > 0 || statusCode !== 200)) throw new RequestRetryError("server does not support the range header and the payload was partially consumed", statusCode, {
					headers,
					data: { count: this.retryCount }
				});
				const contentRange = parseRangeHeader(headers["content-range"]);
				if (!contentRange) throw new RequestRetryError("Content-Range mismatch", statusCode, {
					headers,
					data: { count: this.retryCount }
				});
				if (this.etag != null && this.etag !== headers.etag) throw new RequestRetryError("ETag mismatch", statusCode, {
					headers,
					data: { count: this.retryCount }
				});
				const { start, size, end = size ? size - 1 : null } = contentRange;
				assert$20(this.start === start, "content-range mismatch");
				assert$20(this.end == null || this.end === end, "content-range mismatch");
				return;
			}
			if (this.end == null) {
				if (statusCode === 206) {
					const range = parseRangeHeader(headers["content-range"]);
					if (range == null) {
						this.headersSent = true;
						this.handler.onResponseStart?.(controller, statusCode, headers, statusMessage);
						return;
					}
					const { start, size, end = size ? size - 1 : null } = range;
					assert$20(start != null && Number.isFinite(start), "content-range mismatch");
					assert$20(end != null && Number.isFinite(end), "invalid content-length");
					this.start = start;
					this.end = end;
				}
				if (this.end == null) {
					const contentLength = headers["content-length"];
					this.end = contentLength != null ? Number(contentLength) - 1 : null;
				}
				assert$20(Number.isFinite(this.start));
				assert$20(this.end == null || Number.isFinite(this.end), "invalid content-length");
				this.resume = true;
				this.etag = headers.etag != null ? headers.etag : null;
				if (this.etag != null && this.etag[0] === "W" && this.etag[1] === "/") this.etag = null;
				this.headersSent = true;
				this.handler.onResponseStart?.(controller, statusCode, headers, statusMessage);
			} else throw new RequestRetryError("Request failed", statusCode, {
				headers,
				data: { count: this.retryCount }
			});
		}
		onResponseData(controller, chunk) {
			if (this.error) return;
			this.start += chunk.length;
			this.handler.onResponseData?.(controller, chunk);
		}
		onResponseEnd(controller, trailers) {
			if (this.error && this.retryOpts.throwOnError) throw this.error;
			if (!this.error) {
				this.retryCount = 0;
				return this.handler.onResponseEnd?.(controller, trailers);
			}
			this.retry(controller);
		}
		retry(controller) {
			if (this.start !== 0) {
				const headers = { range: `bytes=${this.start}-${this.end ?? ""}` };
				if (this.etag != null) headers["if-match"] = this.etag;
				this.opts = {
					...this.opts,
					headers: {
						...this.opts.headers,
						...headers
					}
				};
			}
			try {
				this.retryCountCheckpoint = this.retryCount;
				this.dispatch(this.opts, this);
			} catch (err) {
				this.handler.onResponseError?.(controller, err);
			}
		}
		onResponseError(controller, err) {
			if (controller?.aborted || isDisturbed$1(this.opts.body)) {
				this.handler.onResponseError?.(controller, err);
				return;
			}
			function shouldRetry(returnedErr) {
				if (!returnedErr) {
					this.retry(controller);
					return;
				}
				this.handler?.onResponseError?.(controller, returnedErr);
			}
			if (this.retryCount - this.retryCountCheckpoint > 0) this.retryCount = this.retryCountCheckpoint + (this.retryCount - this.retryCountCheckpoint);
			else this.retryCount += 1;
			this.retryOpts.retry(err, {
				state: { counter: this.retryCount },
				opts: {
					retryOptions: this.retryOpts,
					...this.opts
				}
			}, shouldRetry.bind(this));
		}
	};
	module.exports = RetryHandler$3;
}) });

//#endregion
//#region node_modules/undici/lib/dispatcher/retry-agent.js
var require_retry_agent = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/dispatcher/retry-agent.js": ((exports, module) => {
	const Dispatcher$2 = require_dispatcher();
	const RetryHandler$2 = require_retry_handler();
	var RetryAgent$1 = class extends Dispatcher$2 {
		#agent = null;
		#options = null;
		constructor(agent, options = {}) {
			super(options);
			this.#agent = agent;
			this.#options = options;
		}
		dispatch(opts, handler) {
			const retry = new RetryHandler$2({
				...opts,
				retryOptions: this.#options
			}, {
				dispatch: this.#agent.dispatch.bind(this.#agent),
				handler
			});
			return this.#agent.dispatch(opts, retry);
		}
		close() {
			return this.#agent.close();
		}
		destroy() {
			return this.#agent.destroy();
		}
	};
	module.exports = RetryAgent$1;
}) });

//#endregion
//#region node_modules/undici/lib/dispatcher/h2c-client.js
var require_h2c_client = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/dispatcher/h2c-client.js": ((exports, module) => {
	const { connect: connect$1 } = __require("node:net");
	const { kClose: kClose$2, kDestroy } = require_symbols();
	const { InvalidArgumentError: InvalidArgumentError$20 } = require_errors();
	const util$13 = require_util$5();
	const Client$2 = require_client();
	const DispatcherBase = require_dispatcher_base();
	var H2CClient$1 = class extends DispatcherBase {
		#client = null;
		constructor(origin, clientOpts) {
			super();
			if (typeof origin === "string") origin = new URL(origin);
			if (origin.protocol !== "http:") throw new InvalidArgumentError$20("h2c-client: Only h2c protocol is supported");
			const { connect: connect$3, maxConcurrentStreams, pipelining,...opts } = clientOpts ?? {};
			let defaultMaxConcurrentStreams = 100;
			let defaultPipelining = 100;
			if (maxConcurrentStreams != null && Number.isInteger(maxConcurrentStreams) && maxConcurrentStreams > 0) defaultMaxConcurrentStreams = maxConcurrentStreams;
			if (pipelining != null && Number.isInteger(pipelining) && pipelining > 0) defaultPipelining = pipelining;
			if (defaultPipelining > defaultMaxConcurrentStreams) throw new InvalidArgumentError$20("h2c-client: pipelining cannot be greater than maxConcurrentStreams");
			this.#client = new Client$2(origin, {
				...opts,
				connect: this.#buildConnector(connect$3),
				maxConcurrentStreams: defaultMaxConcurrentStreams,
				pipelining: defaultPipelining,
				allowH2: true
			});
		}
		#buildConnector(connectOpts) {
			return (opts, callback) => {
				const timeout = connectOpts?.connectOpts ?? 1e4;
				const { hostname: hostname$1, port: port$1, pathname } = opts;
				const socket = connect$1({
					...opts,
					host: hostname$1,
					port: port$1,
					pathname
				});
				if (opts.keepAlive == null || opts.keepAlive) {
					const keepAliveInitialDelay = opts.keepAliveInitialDelay == null ? 6e4 : opts.keepAliveInitialDelay;
					socket.setKeepAlive(true, keepAliveInitialDelay);
				}
				socket.alpnProtocol = "h2";
				const clearConnectTimeout = util$13.setupConnectTimeout(new WeakRef(socket), {
					timeout,
					hostname: hostname$1,
					port: port$1
				});
				socket.setNoDelay(true).once("connect", function() {
					queueMicrotask(clearConnectTimeout);
					if (callback) {
						const cb = callback;
						callback = null;
						cb(null, this);
					}
				}).on("error", function(err) {
					queueMicrotask(clearConnectTimeout);
					if (callback) {
						const cb = callback;
						callback = null;
						cb(err);
					}
				});
				return socket;
			};
		}
		dispatch(opts, handler) {
			return this.#client.dispatch(opts, handler);
		}
		async [kClose$2]() {
			await this.#client.close();
		}
		async [kDestroy]() {
			await this.#client.destroy();
		}
	};
	module.exports = H2CClient$1;
}) });

//#endregion
//#region node_modules/undici/lib/api/readable.js
var require_readable = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/api/readable.js": ((exports, module) => {
	const assert$19 = __require("node:assert");
	const { Readable: Readable$4 } = __require("node:stream");
	const { RequestAbortedError: RequestAbortedError$4, NotSupportedError, InvalidArgumentError: InvalidArgumentError$19, AbortError: AbortError$1 } = require_errors();
	const util$12 = require_util$5();
	const { ReadableStreamFrom } = require_util$5();
	const kConsume = Symbol("kConsume");
	const kReading = Symbol("kReading");
	const kBody$1 = Symbol("kBody");
	const kAbort = Symbol("kAbort");
	const kContentType = Symbol("kContentType");
	const kContentLength$1 = Symbol("kContentLength");
	const kUsed = Symbol("kUsed");
	const kBytesRead = Symbol("kBytesRead");
	const noop$5 = () => {};
	/**
	* @class
	* @extends {Readable}
	* @see https://fetch.spec.whatwg.org/#body
	*/
	var BodyReadable = class extends Readable$4 {
		/**
		* @param {object} opts
		* @param {(this: Readable, size: number) => void} opts.resume
		* @param {() => (void | null)} opts.abort
		* @param {string} [opts.contentType = '']
		* @param {number} [opts.contentLength]
		* @param {number} [opts.highWaterMark = 64 * 1024]
		*/
		constructor({ resume: resume$1, abort: abort$1, contentType = "", contentLength, highWaterMark = 64 * 1024 }) {
			super({
				autoDestroy: true,
				read: resume$1,
				highWaterMark
			});
			this._readableState.dataEmitted = false;
			this[kAbort] = abort$1;
			/** @type {Consume | null} */
			this[kConsume] = null;
			/** @type {number} */
			this[kBytesRead] = 0;
			/** @type {ReadableStream|null} */
			this[kBody$1] = null;
			/** @type {boolean} */
			this[kUsed] = false;
			/** @type {string} */
			this[kContentType] = contentType;
			/** @type {number|null} */
			this[kContentLength$1] = Number.isFinite(contentLength) ? contentLength : null;
			/**
			* Is stream being consumed through Readable API?
			* This is an optimization so that we avoid checking
			* for 'data' and 'readable' listeners in the hot path
			* inside push().
			*
			* @type {boolean}
			*/
			this[kReading] = false;
		}
		/**
		* @param {Error|null} err
		* @param {(error:(Error|null)) => void} callback
		* @returns {void}
		*/
		_destroy(err, callback) {
			if (!err && !this._readableState.endEmitted) err = new RequestAbortedError$4();
			if (err) this[kAbort]();
			if (!this[kUsed]) setImmediate(callback, err);
			else callback(err);
		}
		/**
		* @param {string|symbol} event
		* @param {(...args: any[]) => void} listener
		* @returns {this}
		*/
		on(event, listener) {
			if (event === "data" || event === "readable") {
				this[kReading] = true;
				this[kUsed] = true;
			}
			return super.on(event, listener);
		}
		/**
		* @param {string|symbol} event
		* @param {(...args: any[]) => void} listener
		* @returns {this}
		*/
		addListener(event, listener) {
			return this.on(event, listener);
		}
		/**
		* @param {string|symbol} event
		* @param {(...args: any[]) => void} listener
		* @returns {this}
		*/
		off(event, listener) {
			const ret = super.off(event, listener);
			if (event === "data" || event === "readable") this[kReading] = this.listenerCount("data") > 0 || this.listenerCount("readable") > 0;
			return ret;
		}
		/**
		* @param {string|symbol} event
		* @param {(...args: any[]) => void} listener
		* @returns {this}
		*/
		removeListener(event, listener) {
			return this.off(event, listener);
		}
		/**
		* @param {Buffer|null} chunk
		* @returns {boolean}
		*/
		push(chunk) {
			if (chunk) {
				this[kBytesRead] += chunk.length;
				if (this[kConsume]) {
					consumePush(this[kConsume], chunk);
					return this[kReading] ? super.push(chunk) : true;
				}
			}
			return super.push(chunk);
		}
		/**
		* Consumes and returns the body as a string.
		*
		* @see https://fetch.spec.whatwg.org/#dom-body-text
		* @returns {Promise<string>}
		*/
		text() {
			return consume(this, "text");
		}
		/**
		* Consumes and returns the body as a JavaScript Object.
		*
		* @see https://fetch.spec.whatwg.org/#dom-body-json
		* @returns {Promise<unknown>}
		*/
		json() {
			return consume(this, "json");
		}
		/**
		* Consumes and returns the body as a Blob
		*
		* @see https://fetch.spec.whatwg.org/#dom-body-blob
		* @returns {Promise<Blob>}
		*/
		blob() {
			return consume(this, "blob");
		}
		/**
		* Consumes and returns the body as an Uint8Array.
		*
		* @see https://fetch.spec.whatwg.org/#dom-body-bytes
		* @returns {Promise<Uint8Array>}
		*/
		bytes() {
			return consume(this, "bytes");
		}
		/**
		* Consumes and returns the body as an ArrayBuffer.
		*
		* @see https://fetch.spec.whatwg.org/#dom-body-arraybuffer
		* @returns {Promise<ArrayBuffer>}
		*/
		arrayBuffer() {
			return consume(this, "arrayBuffer");
		}
		/**
		* Not implemented
		*
		* @see https://fetch.spec.whatwg.org/#dom-body-formdata
		* @throws {NotSupportedError}
		*/
		async formData() {
			throw new NotSupportedError();
		}
		/**
		* Returns true if the body is not null and the body has been consumed.
		* Otherwise, returns false.
		*
		* @see https://fetch.spec.whatwg.org/#dom-body-bodyused
		* @readonly
		* @returns {boolean}
		*/
		get bodyUsed() {
			return util$12.isDisturbed(this);
		}
		/**
		* @see https://fetch.spec.whatwg.org/#dom-body-body
		* @readonly
		* @returns {ReadableStream}
		*/
		get body() {
			if (!this[kBody$1]) {
				this[kBody$1] = ReadableStreamFrom(this);
				if (this[kConsume]) {
					this[kBody$1].getReader();
					assert$19(this[kBody$1].locked);
				}
			}
			return this[kBody$1];
		}
		/**
		* Dumps the response body by reading `limit` number of bytes.
		* @param {object} opts
		* @param {number} [opts.limit = 131072] Number of bytes to read.
		* @param {AbortSignal} [opts.signal] An AbortSignal to cancel the dump.
		* @returns {Promise<null>}
		*/
		async dump(opts) {
			const signal = opts?.signal;
			if (signal != null && (typeof signal !== "object" || !("aborted" in signal))) throw new InvalidArgumentError$19("signal must be an AbortSignal");
			const limit = opts?.limit && Number.isFinite(opts.limit) ? opts.limit : 128 * 1024;
			signal?.throwIfAborted();
			if (this._readableState.closeEmitted) return null;
			return await new Promise((resolve$1, reject) => {
				if (this[kContentLength$1] && this[kContentLength$1] > limit || this[kBytesRead] > limit) this.destroy(new AbortError$1());
				if (signal) {
					const onAbort = () => {
						this.destroy(signal.reason ?? new AbortError$1());
					};
					signal.addEventListener("abort", onAbort);
					this.on("close", function() {
						signal.removeEventListener("abort", onAbort);
						if (signal.aborted) reject(signal.reason ?? new AbortError$1());
						else resolve$1(null);
					});
				} else this.on("close", resolve$1);
				this.on("error", noop$5).on("data", () => {
					if (this[kBytesRead] > limit) this.destroy();
				}).resume();
			});
		}
		/**
		* @param {BufferEncoding} encoding
		* @returns {this}
		*/
		setEncoding(encoding) {
			if (Buffer.isEncoding(encoding)) this._readableState.encoding = encoding;
			return this;
		}
	};
	/**
	* @see https://streams.spec.whatwg.org/#readablestream-locked
	* @param {BodyReadable} bodyReadable
	* @returns {boolean}
	*/
	function isLocked(bodyReadable) {
		return bodyReadable[kBody$1]?.locked === true || bodyReadable[kConsume] !== null;
	}
	/**
	* @see https://fetch.spec.whatwg.org/#body-unusable
	* @param {BodyReadable} bodyReadable
	* @returns {boolean}
	*/
	function isUnusable(bodyReadable) {
		return util$12.isDisturbed(bodyReadable) || isLocked(bodyReadable);
	}
	/**
	* @typedef {'text' | 'json' | 'blob' | 'bytes' | 'arrayBuffer'} ConsumeType
	*/
	/**
	* @template {ConsumeType} T
	* @typedef {T extends 'text' ? string :
	*           T extends 'json' ? unknown :
	*           T extends 'blob' ? Blob :
	*           T extends 'arrayBuffer' ? ArrayBuffer :
	*           T extends 'bytes' ? Uint8Array :
	*           never
	* } ConsumeReturnType
	*/
	/**
	* @typedef {object} Consume
	* @property {ConsumeType} type
	* @property {BodyReadable} stream
	* @property {((value?: any) => void)} resolve
	* @property {((err: Error) => void)} reject
	* @property {number} length
	* @property {Buffer[]} body
	*/
	/**
	* @template {ConsumeType} T
	* @param {BodyReadable} stream
	* @param {T} type
	* @returns {Promise<ConsumeReturnType<T>>}
	*/
	function consume(stream$2, type) {
		assert$19(!stream$2[kConsume]);
		return new Promise((resolve$1, reject) => {
			if (isUnusable(stream$2)) {
				const rState = stream$2._readableState;
				if (rState.destroyed && rState.closeEmitted === false) stream$2.on("error", reject).on("close", () => {
					reject(/* @__PURE__ */ new TypeError("unusable"));
				});
				else reject(rState.errored ?? /* @__PURE__ */ new TypeError("unusable"));
			} else queueMicrotask(() => {
				stream$2[kConsume] = {
					type,
					stream: stream$2,
					resolve: resolve$1,
					reject,
					length: 0,
					body: []
				};
				stream$2.on("error", function(err) {
					consumeFinish(this[kConsume], err);
				}).on("close", function() {
					if (this[kConsume].body !== null) consumeFinish(this[kConsume], new RequestAbortedError$4());
				});
				consumeStart(stream$2[kConsume]);
			});
		});
	}
	/**
	* @param {Consume} consume
	* @returns {void}
	*/
	function consumeStart(consume$1) {
		if (consume$1.body === null) return;
		const { _readableState: state } = consume$1.stream;
		if (state.bufferIndex) {
			const start = state.bufferIndex;
			const end = state.buffer.length;
			for (let n = start; n < end; n++) consumePush(consume$1, state.buffer[n]);
		} else for (const chunk of state.buffer) consumePush(consume$1, chunk);
		if (state.endEmitted) consumeEnd(this[kConsume], this._readableState.encoding);
		else consume$1.stream.on("end", function() {
			consumeEnd(this[kConsume], this._readableState.encoding);
		});
		consume$1.stream.resume();
		while (consume$1.stream.read() != null);
	}
	/**
	* @param {Buffer[]} chunks
	* @param {number} length
	* @param {BufferEncoding} [encoding='utf8']
	* @returns {string}
	*/
	function chunksDecode(chunks, length, encoding) {
		if (chunks.length === 0 || length === 0) return "";
		const buffer$1 = chunks.length === 1 ? chunks[0] : Buffer.concat(chunks, length);
		const bufferLength = buffer$1.length;
		const start = bufferLength > 2 && buffer$1[0] === 239 && buffer$1[1] === 187 && buffer$1[2] === 191 ? 3 : 0;
		if (!encoding || encoding === "utf8" || encoding === "utf-8") return buffer$1.utf8Slice(start, bufferLength);
		else return buffer$1.subarray(start, bufferLength).toString(encoding);
	}
	/**
	* @param {Buffer[]} chunks
	* @param {number} length
	* @returns {Uint8Array}
	*/
	function chunksConcat(chunks, length) {
		if (chunks.length === 0 || length === 0) return new Uint8Array(0);
		if (chunks.length === 1) return new Uint8Array(chunks[0]);
		const buffer$1 = new Uint8Array(Buffer.allocUnsafeSlow(length).buffer);
		let offset = 0;
		for (let i = 0; i < chunks.length; ++i) {
			const chunk = chunks[i];
			buffer$1.set(chunk, offset);
			offset += chunk.length;
		}
		return buffer$1;
	}
	/**
	* @param {Consume} consume
	* @param {BufferEncoding} encoding
	* @returns {void}
	*/
	function consumeEnd(consume$1, encoding) {
		const { type, body, resolve: resolve$1, stream: stream$2, length } = consume$1;
		try {
			if (type === "text") resolve$1(chunksDecode(body, length, encoding));
			else if (type === "json") resolve$1(JSON.parse(chunksDecode(body, length, encoding)));
			else if (type === "arrayBuffer") resolve$1(chunksConcat(body, length).buffer);
			else if (type === "blob") resolve$1(new Blob(body, { type: stream$2[kContentType] }));
			else if (type === "bytes") resolve$1(chunksConcat(body, length));
			consumeFinish(consume$1);
		} catch (err) {
			stream$2.destroy(err);
		}
	}
	/**
	* @param {Consume} consume
	* @param {Buffer} chunk
	* @returns {void}
	*/
	function consumePush(consume$1, chunk) {
		consume$1.length += chunk.length;
		consume$1.body.push(chunk);
	}
	/**
	* @param {Consume} consume
	* @param {Error} [err]
	* @returns {void}
	*/
	function consumeFinish(consume$1, err) {
		if (consume$1.body === null) return;
		if (err) consume$1.reject(err);
		else consume$1.resolve();
		consume$1.type = null;
		consume$1.stream = null;
		consume$1.resolve = null;
		consume$1.reject = null;
		consume$1.length = 0;
		consume$1.body = null;
	}
	module.exports = {
		Readable: BodyReadable,
		chunksDecode
	};
}) });

//#endregion
//#region node_modules/undici/lib/api/api-request.js
var require_api_request = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/api/api-request.js": ((exports, module) => {
	const assert$18 = __require("node:assert");
	const { AsyncResource: AsyncResource$4 } = __require("node:async_hooks");
	const { Readable: Readable$3 } = require_readable();
	const { InvalidArgumentError: InvalidArgumentError$18, RequestAbortedError: RequestAbortedError$3 } = require_errors();
	const util$11 = require_util$5();
	function noop$4() {}
	var RequestHandler = class extends AsyncResource$4 {
		constructor(opts, callback) {
			if (!opts || typeof opts !== "object") throw new InvalidArgumentError$18("invalid opts");
			const { signal, method, opaque, body, onInfo, responseHeaders, highWaterMark } = opts;
			try {
				if (typeof callback !== "function") throw new InvalidArgumentError$18("invalid callback");
				if (highWaterMark && (typeof highWaterMark !== "number" || highWaterMark < 0)) throw new InvalidArgumentError$18("invalid highWaterMark");
				if (signal && typeof signal.on !== "function" && typeof signal.addEventListener !== "function") throw new InvalidArgumentError$18("signal must be an EventEmitter or EventTarget");
				if (method === "CONNECT") throw new InvalidArgumentError$18("invalid method");
				if (onInfo && typeof onInfo !== "function") throw new InvalidArgumentError$18("invalid onInfo callback");
				super("UNDICI_REQUEST");
			} catch (err) {
				if (util$11.isStream(body)) util$11.destroy(body.on("error", noop$4), err);
				throw err;
			}
			this.method = method;
			this.responseHeaders = responseHeaders || null;
			this.opaque = opaque || null;
			this.callback = callback;
			this.res = null;
			this.abort = null;
			this.body = body;
			this.trailers = {};
			this.context = null;
			this.onInfo = onInfo || null;
			this.highWaterMark = highWaterMark;
			this.reason = null;
			this.removeAbortListener = null;
			if (signal?.aborted) this.reason = signal.reason ?? new RequestAbortedError$3();
			else if (signal) this.removeAbortListener = util$11.addAbortListener(signal, () => {
				this.reason = signal.reason ?? new RequestAbortedError$3();
				if (this.res) util$11.destroy(this.res.on("error", noop$4), this.reason);
				else if (this.abort) this.abort(this.reason);
			});
		}
		onConnect(abort$1, context) {
			if (this.reason) {
				abort$1(this.reason);
				return;
			}
			assert$18(this.callback);
			this.abort = abort$1;
			this.context = context;
		}
		onHeaders(statusCode, rawHeaders, resume$1, statusMessage) {
			const { callback, opaque, abort: abort$1, context, responseHeaders, highWaterMark } = this;
			const headers = responseHeaders === "raw" ? util$11.parseRawHeaders(rawHeaders) : util$11.parseHeaders(rawHeaders);
			if (statusCode < 200) {
				if (this.onInfo) this.onInfo({
					statusCode,
					headers
				});
				return;
			}
			const parsedHeaders = responseHeaders === "raw" ? util$11.parseHeaders(rawHeaders) : headers;
			const contentType = parsedHeaders["content-type"];
			const contentLength = parsedHeaders["content-length"];
			const res = new Readable$3({
				resume: resume$1,
				abort: abort$1,
				contentType,
				contentLength: this.method !== "HEAD" && contentLength ? Number(contentLength) : null,
				highWaterMark
			});
			if (this.removeAbortListener) {
				res.on("close", this.removeAbortListener);
				this.removeAbortListener = null;
			}
			this.callback = null;
			this.res = res;
			if (callback !== null) this.runInAsyncScope(callback, null, null, {
				statusCode,
				headers,
				trailers: this.trailers,
				opaque,
				body: res,
				context
			});
		}
		onData(chunk) {
			return this.res.push(chunk);
		}
		onComplete(trailers) {
			util$11.parseHeaders(trailers, this.trailers);
			this.res.push(null);
		}
		onError(err) {
			const { res, callback, body, opaque } = this;
			if (callback) {
				this.callback = null;
				queueMicrotask(() => {
					this.runInAsyncScope(callback, null, err, { opaque });
				});
			}
			if (res) {
				this.res = null;
				queueMicrotask(() => {
					util$11.destroy(res.on("error", noop$4), err);
				});
			}
			if (body) {
				this.body = null;
				if (util$11.isStream(body)) {
					body.on("error", noop$4);
					util$11.destroy(body, err);
				}
			}
			if (this.removeAbortListener) {
				this.removeAbortListener();
				this.removeAbortListener = null;
			}
		}
	};
	function request$1(opts, callback) {
		if (callback === void 0) return new Promise((resolve$1, reject) => {
			request$1.call(this, opts, (err, data) => {
				return err ? reject(err) : resolve$1(data);
			});
		});
		try {
			const handler = new RequestHandler(opts, callback);
			this.dispatch(opts, handler);
		} catch (err) {
			if (typeof callback !== "function") throw err;
			const opaque = opts?.opaque;
			queueMicrotask(() => callback(err, { opaque }));
		}
	}
	module.exports = request$1;
	module.exports.RequestHandler = RequestHandler;
}) });

//#endregion
//#region node_modules/undici/lib/api/abort-signal.js
var require_abort_signal = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/api/abort-signal.js": ((exports, module) => {
	const { addAbortListener: addAbortListener$1 } = require_util$5();
	const { RequestAbortedError: RequestAbortedError$2 } = require_errors();
	const kListener = Symbol("kListener");
	const kSignal = Symbol("kSignal");
	function abort(self) {
		if (self.abort) self.abort(self[kSignal]?.reason);
		else self.reason = self[kSignal]?.reason ?? new RequestAbortedError$2();
		removeSignal$4(self);
	}
	function addSignal$4(self, signal) {
		self.reason = null;
		self[kSignal] = null;
		self[kListener] = null;
		if (!signal) return;
		if (signal.aborted) {
			abort(self);
			return;
		}
		self[kSignal] = signal;
		self[kListener] = () => {
			abort(self);
		};
		addAbortListener$1(self[kSignal], self[kListener]);
	}
	function removeSignal$4(self) {
		if (!self[kSignal]) return;
		if ("removeEventListener" in self[kSignal]) self[kSignal].removeEventListener("abort", self[kListener]);
		else self[kSignal].removeListener("abort", self[kListener]);
		self[kSignal] = null;
		self[kListener] = null;
	}
	module.exports = {
		addSignal: addSignal$4,
		removeSignal: removeSignal$4
	};
}) });

//#endregion
//#region node_modules/undici/lib/api/api-stream.js
var require_api_stream = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/api/api-stream.js": ((exports, module) => {
	const assert$17 = __require("node:assert");
	const { finished: finished$1 } = __require("node:stream");
	const { AsyncResource: AsyncResource$3 } = __require("node:async_hooks");
	const { InvalidArgumentError: InvalidArgumentError$17, InvalidReturnValueError: InvalidReturnValueError$1 } = require_errors();
	const util$10 = require_util$5();
	const { addSignal: addSignal$3, removeSignal: removeSignal$3 } = require_abort_signal();
	function noop$3() {}
	var StreamHandler = class extends AsyncResource$3 {
		constructor(opts, factory, callback) {
			if (!opts || typeof opts !== "object") throw new InvalidArgumentError$17("invalid opts");
			const { signal, method, opaque, body, onInfo, responseHeaders } = opts;
			try {
				if (typeof callback !== "function") throw new InvalidArgumentError$17("invalid callback");
				if (typeof factory !== "function") throw new InvalidArgumentError$17("invalid factory");
				if (signal && typeof signal.on !== "function" && typeof signal.addEventListener !== "function") throw new InvalidArgumentError$17("signal must be an EventEmitter or EventTarget");
				if (method === "CONNECT") throw new InvalidArgumentError$17("invalid method");
				if (onInfo && typeof onInfo !== "function") throw new InvalidArgumentError$17("invalid onInfo callback");
				super("UNDICI_STREAM");
			} catch (err) {
				if (util$10.isStream(body)) util$10.destroy(body.on("error", noop$3), err);
				throw err;
			}
			this.responseHeaders = responseHeaders || null;
			this.opaque = opaque || null;
			this.factory = factory;
			this.callback = callback;
			this.res = null;
			this.abort = null;
			this.context = null;
			this.trailers = null;
			this.body = body;
			this.onInfo = onInfo || null;
			if (util$10.isStream(body)) body.on("error", (err) => {
				this.onError(err);
			});
			addSignal$3(this, signal);
		}
		onConnect(abort$1, context) {
			if (this.reason) {
				abort$1(this.reason);
				return;
			}
			assert$17(this.callback);
			this.abort = abort$1;
			this.context = context;
		}
		onHeaders(statusCode, rawHeaders, resume$1, statusMessage) {
			const { factory, opaque, context, responseHeaders } = this;
			const headers = responseHeaders === "raw" ? util$10.parseRawHeaders(rawHeaders) : util$10.parseHeaders(rawHeaders);
			if (statusCode < 200) {
				if (this.onInfo) this.onInfo({
					statusCode,
					headers
				});
				return;
			}
			this.factory = null;
			if (factory === null) return;
			const res = this.runInAsyncScope(factory, null, {
				statusCode,
				headers,
				opaque,
				context
			});
			if (!res || typeof res.write !== "function" || typeof res.end !== "function" || typeof res.on !== "function") throw new InvalidReturnValueError$1("expected Writable");
			finished$1(res, { readable: false }, (err) => {
				const { callback, res: res$1, opaque: opaque$1, trailers, abort: abort$1 } = this;
				this.res = null;
				if (err || !res$1?.readable) util$10.destroy(res$1, err);
				this.callback = null;
				this.runInAsyncScope(callback, null, err || null, {
					opaque: opaque$1,
					trailers
				});
				if (err) abort$1();
			});
			res.on("drain", resume$1);
			this.res = res;
			return (res.writableNeedDrain !== void 0 ? res.writableNeedDrain : res._writableState?.needDrain) !== true;
		}
		onData(chunk) {
			const { res } = this;
			return res ? res.write(chunk) : true;
		}
		onComplete(trailers) {
			const { res } = this;
			removeSignal$3(this);
			if (!res) return;
			this.trailers = util$10.parseHeaders(trailers);
			res.end();
		}
		onError(err) {
			const { res, callback, opaque, body } = this;
			removeSignal$3(this);
			this.factory = null;
			if (res) {
				this.res = null;
				util$10.destroy(res, err);
			} else if (callback) {
				this.callback = null;
				queueMicrotask(() => {
					this.runInAsyncScope(callback, null, err, { opaque });
				});
			}
			if (body) {
				this.body = null;
				util$10.destroy(body, err);
			}
		}
	};
	function stream(opts, factory, callback) {
		if (callback === void 0) return new Promise((resolve$1, reject) => {
			stream.call(this, opts, factory, (err, data) => {
				return err ? reject(err) : resolve$1(data);
			});
		});
		try {
			const handler = new StreamHandler(opts, factory, callback);
			this.dispatch(opts, handler);
		} catch (err) {
			if (typeof callback !== "function") throw err;
			const opaque = opts?.opaque;
			queueMicrotask(() => callback(err, { opaque }));
		}
	}
	module.exports = stream;
}) });

//#endregion
//#region node_modules/undici/lib/api/api-pipeline.js
var require_api_pipeline = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/api/api-pipeline.js": ((exports, module) => {
	const { Readable: Readable$2, Duplex, PassThrough } = __require("node:stream");
	const assert$16 = __require("node:assert");
	const { AsyncResource: AsyncResource$2 } = __require("node:async_hooks");
	const { InvalidArgumentError: InvalidArgumentError$16, InvalidReturnValueError, RequestAbortedError: RequestAbortedError$1 } = require_errors();
	const util$9 = require_util$5();
	const { addSignal: addSignal$2, removeSignal: removeSignal$2 } = require_abort_signal();
	function noop$2() {}
	const kResume = Symbol("resume");
	var PipelineRequest = class extends Readable$2 {
		constructor() {
			super({ autoDestroy: true });
			this[kResume] = null;
		}
		_read() {
			const { [kResume]: resume$1 } = this;
			if (resume$1) {
				this[kResume] = null;
				resume$1();
			}
		}
		_destroy(err, callback) {
			this._read();
			callback(err);
		}
	};
	var PipelineResponse = class extends Readable$2 {
		constructor(resume$1) {
			super({ autoDestroy: true });
			this[kResume] = resume$1;
		}
		_read() {
			this[kResume]();
		}
		_destroy(err, callback) {
			if (!err && !this._readableState.endEmitted) err = new RequestAbortedError$1();
			callback(err);
		}
	};
	var PipelineHandler = class extends AsyncResource$2 {
		constructor(opts, handler) {
			if (!opts || typeof opts !== "object") throw new InvalidArgumentError$16("invalid opts");
			if (typeof handler !== "function") throw new InvalidArgumentError$16("invalid handler");
			const { signal, method, opaque, onInfo, responseHeaders } = opts;
			if (signal && typeof signal.on !== "function" && typeof signal.addEventListener !== "function") throw new InvalidArgumentError$16("signal must be an EventEmitter or EventTarget");
			if (method === "CONNECT") throw new InvalidArgumentError$16("invalid method");
			if (onInfo && typeof onInfo !== "function") throw new InvalidArgumentError$16("invalid onInfo callback");
			super("UNDICI_PIPELINE");
			this.opaque = opaque || null;
			this.responseHeaders = responseHeaders || null;
			this.handler = handler;
			this.abort = null;
			this.context = null;
			this.onInfo = onInfo || null;
			this.req = new PipelineRequest().on("error", noop$2);
			this.ret = new Duplex({
				readableObjectMode: opts.objectMode,
				autoDestroy: true,
				read: () => {
					const { body } = this;
					if (body?.resume) body.resume();
				},
				write: (chunk, encoding, callback) => {
					const { req } = this;
					if (req.push(chunk, encoding) || req._readableState.destroyed) callback();
					else req[kResume] = callback;
				},
				destroy: (err, callback) => {
					const { body, req, res, ret, abort: abort$1 } = this;
					if (!err && !ret._readableState.endEmitted) err = new RequestAbortedError$1();
					if (abort$1 && err) abort$1();
					util$9.destroy(body, err);
					util$9.destroy(req, err);
					util$9.destroy(res, err);
					removeSignal$2(this);
					callback(err);
				}
			}).on("prefinish", () => {
				const { req } = this;
				req.push(null);
			});
			this.res = null;
			addSignal$2(this, signal);
		}
		onConnect(abort$1, context) {
			const { res } = this;
			if (this.reason) {
				abort$1(this.reason);
				return;
			}
			assert$16(!res, "pipeline cannot be retried");
			this.abort = abort$1;
			this.context = context;
		}
		onHeaders(statusCode, rawHeaders, resume$1) {
			const { opaque, handler, context } = this;
			if (statusCode < 200) {
				if (this.onInfo) {
					const headers = this.responseHeaders === "raw" ? util$9.parseRawHeaders(rawHeaders) : util$9.parseHeaders(rawHeaders);
					this.onInfo({
						statusCode,
						headers
					});
				}
				return;
			}
			this.res = new PipelineResponse(resume$1);
			let body;
			try {
				this.handler = null;
				const headers = this.responseHeaders === "raw" ? util$9.parseRawHeaders(rawHeaders) : util$9.parseHeaders(rawHeaders);
				body = this.runInAsyncScope(handler, null, {
					statusCode,
					headers,
					opaque,
					body: this.res,
					context
				});
			} catch (err) {
				this.res.on("error", noop$2);
				throw err;
			}
			if (!body || typeof body.on !== "function") throw new InvalidReturnValueError("expected Readable");
			body.on("data", (chunk) => {
				const { ret, body: body$1 } = this;
				if (!ret.push(chunk) && body$1.pause) body$1.pause();
			}).on("error", (err) => {
				const { ret } = this;
				util$9.destroy(ret, err);
			}).on("end", () => {
				const { ret } = this;
				ret.push(null);
			}).on("close", () => {
				const { ret } = this;
				if (!ret._readableState.ended) util$9.destroy(ret, new RequestAbortedError$1());
			});
			this.body = body;
		}
		onData(chunk) {
			const { res } = this;
			return res.push(chunk);
		}
		onComplete(trailers) {
			const { res } = this;
			res.push(null);
		}
		onError(err) {
			const { ret } = this;
			this.handler = null;
			util$9.destroy(ret, err);
		}
	};
	function pipeline$3(opts, handler) {
		try {
			const pipelineHandler = new PipelineHandler(opts, handler);
			this.dispatch({
				...opts,
				body: pipelineHandler.req
			}, pipelineHandler);
			return pipelineHandler.ret;
		} catch (err) {
			return new PassThrough().destroy(err);
		}
	}
	module.exports = pipeline$3;
}) });

//#endregion
//#region node_modules/undici/lib/api/api-upgrade.js
var require_api_upgrade = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/api/api-upgrade.js": ((exports, module) => {
	const { InvalidArgumentError: InvalidArgumentError$15, SocketError: SocketError$1 } = require_errors();
	const { AsyncResource: AsyncResource$1 } = __require("node:async_hooks");
	const assert$15 = __require("node:assert");
	const util$8 = require_util$5();
	const { addSignal: addSignal$1, removeSignal: removeSignal$1 } = require_abort_signal();
	var UpgradeHandler = class extends AsyncResource$1 {
		constructor(opts, callback) {
			if (!opts || typeof opts !== "object") throw new InvalidArgumentError$15("invalid opts");
			if (typeof callback !== "function") throw new InvalidArgumentError$15("invalid callback");
			const { signal, opaque, responseHeaders } = opts;
			if (signal && typeof signal.on !== "function" && typeof signal.addEventListener !== "function") throw new InvalidArgumentError$15("signal must be an EventEmitter or EventTarget");
			super("UNDICI_UPGRADE");
			this.responseHeaders = responseHeaders || null;
			this.opaque = opaque || null;
			this.callback = callback;
			this.abort = null;
			this.context = null;
			addSignal$1(this, signal);
		}
		onConnect(abort$1, context) {
			if (this.reason) {
				abort$1(this.reason);
				return;
			}
			assert$15(this.callback);
			this.abort = abort$1;
			this.context = null;
		}
		onHeaders() {
			throw new SocketError$1("bad upgrade", null);
		}
		onUpgrade(statusCode, rawHeaders, socket) {
			assert$15(statusCode === 101);
			const { callback, opaque, context } = this;
			removeSignal$1(this);
			this.callback = null;
			const headers = this.responseHeaders === "raw" ? util$8.parseRawHeaders(rawHeaders) : util$8.parseHeaders(rawHeaders);
			this.runInAsyncScope(callback, null, null, {
				headers,
				socket,
				opaque,
				context
			});
		}
		onError(err) {
			const { callback, opaque } = this;
			removeSignal$1(this);
			if (callback) {
				this.callback = null;
				queueMicrotask(() => {
					this.runInAsyncScope(callback, null, err, { opaque });
				});
			}
		}
	};
	function upgrade(opts, callback) {
		if (callback === void 0) return new Promise((resolve$1, reject) => {
			upgrade.call(this, opts, (err, data) => {
				return err ? reject(err) : resolve$1(data);
			});
		});
		try {
			const upgradeHandler = new UpgradeHandler(opts, callback);
			const upgradeOpts = {
				...opts,
				method: opts.method || "GET",
				upgrade: opts.protocol || "Websocket"
			};
			this.dispatch(upgradeOpts, upgradeHandler);
		} catch (err) {
			if (typeof callback !== "function") throw err;
			const opaque = opts?.opaque;
			queueMicrotask(() => callback(err, { opaque }));
		}
	}
	module.exports = upgrade;
}) });

//#endregion
//#region node_modules/undici/lib/api/api-connect.js
var require_api_connect = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/api/api-connect.js": ((exports, module) => {
	const assert$14 = __require("node:assert");
	const { AsyncResource } = __require("node:async_hooks");
	const { InvalidArgumentError: InvalidArgumentError$14, SocketError } = require_errors();
	const util$7 = require_util$5();
	const { addSignal, removeSignal } = require_abort_signal();
	var ConnectHandler = class extends AsyncResource {
		constructor(opts, callback) {
			if (!opts || typeof opts !== "object") throw new InvalidArgumentError$14("invalid opts");
			if (typeof callback !== "function") throw new InvalidArgumentError$14("invalid callback");
			const { signal, opaque, responseHeaders } = opts;
			if (signal && typeof signal.on !== "function" && typeof signal.addEventListener !== "function") throw new InvalidArgumentError$14("signal must be an EventEmitter or EventTarget");
			super("UNDICI_CONNECT");
			this.opaque = opaque || null;
			this.responseHeaders = responseHeaders || null;
			this.callback = callback;
			this.abort = null;
			addSignal(this, signal);
		}
		onConnect(abort$1, context) {
			if (this.reason) {
				abort$1(this.reason);
				return;
			}
			assert$14(this.callback);
			this.abort = abort$1;
			this.context = context;
		}
		onHeaders() {
			throw new SocketError("bad connect", null);
		}
		onUpgrade(statusCode, rawHeaders, socket) {
			const { callback, opaque, context } = this;
			removeSignal(this);
			this.callback = null;
			let headers = rawHeaders;
			if (headers != null) headers = this.responseHeaders === "raw" ? util$7.parseRawHeaders(rawHeaders) : util$7.parseHeaders(rawHeaders);
			this.runInAsyncScope(callback, null, null, {
				statusCode,
				headers,
				socket,
				opaque,
				context
			});
		}
		onError(err) {
			const { callback, opaque } = this;
			removeSignal(this);
			if (callback) {
				this.callback = null;
				queueMicrotask(() => {
					this.runInAsyncScope(callback, null, err, { opaque });
				});
			}
		}
	};
	function connect(opts, callback) {
		if (callback === void 0) return new Promise((resolve$1, reject) => {
			connect.call(this, opts, (err, data) => {
				return err ? reject(err) : resolve$1(data);
			});
		});
		try {
			const connectHandler = new ConnectHandler(opts, callback);
			const connectOptions = {
				...opts,
				method: "CONNECT"
			};
			this.dispatch(connectOptions, connectHandler);
		} catch (err) {
			if (typeof callback !== "function") throw err;
			const opaque = opts?.opaque;
			queueMicrotask(() => callback(err, { opaque }));
		}
	}
	module.exports = connect;
}) });

//#endregion
//#region node_modules/undici/lib/api/index.js
var require_api = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/api/index.js": ((exports, module) => {
	module.exports.request = require_api_request();
	module.exports.stream = require_api_stream();
	module.exports.pipeline = require_api_pipeline();
	module.exports.upgrade = require_api_upgrade();
	module.exports.connect = require_api_connect();
}) });

//#endregion
//#region node_modules/undici/lib/mock/mock-errors.js
var require_mock_errors = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/mock/mock-errors.js": ((exports, module) => {
	const { UndiciError: UndiciError$3 } = require_errors();
	/**
	* The request does not match any registered mock dispatches.
	*/
	var MockNotMatchedError$1 = class extends UndiciError$3 {
		constructor(message) {
			super(message);
			this.name = "MockNotMatchedError";
			this.message = message || "The request does not match any registered mock dispatches";
			this.code = "UND_MOCK_ERR_MOCK_NOT_MATCHED";
		}
	};
	module.exports = { MockNotMatchedError: MockNotMatchedError$1 };
}) });

//#endregion
//#region node_modules/undici/lib/mock/mock-symbols.js
var require_mock_symbols = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/mock/mock-symbols.js": ((exports, module) => {
	module.exports = {
		kAgent: Symbol("agent"),
		kOptions: Symbol("options"),
		kFactory: Symbol("factory"),
		kDispatches: Symbol("dispatches"),
		kDispatchKey: Symbol("dispatch key"),
		kDefaultHeaders: Symbol("default headers"),
		kDefaultTrailers: Symbol("default trailers"),
		kContentLength: Symbol("content length"),
		kMockAgent: Symbol("mock agent"),
		kMockAgentSet: Symbol("mock agent set"),
		kMockAgentGet: Symbol("mock agent get"),
		kMockDispatch: Symbol("mock dispatch"),
		kClose: Symbol("close"),
		kOriginalClose: Symbol("original agent close"),
		kOriginalDispatch: Symbol("original dispatch"),
		kOrigin: Symbol("origin"),
		kIsMockActive: Symbol("is mock active"),
		kNetConnect: Symbol("net connect"),
		kGetNetConnect: Symbol("get net connect"),
		kConnected: Symbol("connected"),
		kIgnoreTrailingSlash: Symbol("ignore trailing slash"),
		kMockAgentMockCallHistoryInstance: Symbol("mock agent mock call history name"),
		kMockAgentRegisterCallHistory: Symbol("mock agent register mock call history"),
		kMockAgentAddCallHistoryLog: Symbol("mock agent add call history log"),
		kMockAgentIsCallHistoryEnabled: Symbol("mock agent is call history enabled"),
		kMockAgentAcceptsNonStandardSearchParameters: Symbol("mock agent accepts non standard search parameters"),
		kMockCallHistoryAddLog: Symbol("mock call history add log")
	};
}) });

//#endregion
//#region node_modules/undici/lib/mock/mock-utils.js
var require_mock_utils = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/mock/mock-utils.js": ((exports, module) => {
	const { MockNotMatchedError } = require_mock_errors();
	const { kDispatches: kDispatches$4, kMockAgent: kMockAgent$2, kOriginalDispatch: kOriginalDispatch$2, kOrigin: kOrigin$2, kGetNetConnect: kGetNetConnect$1 } = require_mock_symbols();
	const { serializePathWithQuery: serializePathWithQuery$2 } = require_util$5();
	const { STATUS_CODES: STATUS_CODES$1 } = __require("node:http");
	const { types: { isPromise } } = __require("node:util");
	const { InvalidArgumentError: InvalidArgumentError$13 } = require_errors();
	function matchValue$1(match, value) {
		if (typeof match === "string") return match === value;
		if (match instanceof RegExp) return match.test(value);
		if (typeof match === "function") return match(value) === true;
		return false;
	}
	function lowerCaseEntries(headers) {
		return Object.fromEntries(Object.entries(headers).map(([headerName, headerValue]) => {
			return [headerName.toLocaleLowerCase(), headerValue];
		}));
	}
	/**
	* @param {import('../../index').Headers|string[]|Record<string, string>} headers
	* @param {string} key
	*/
	function getHeaderByName(headers, key) {
		if (Array.isArray(headers)) {
			for (let i = 0; i < headers.length; i += 2) if (headers[i].toLocaleLowerCase() === key.toLocaleLowerCase()) return headers[i + 1];
			return;
		} else if (typeof headers.get === "function") return headers.get(key);
		else return lowerCaseEntries(headers)[key.toLocaleLowerCase()];
	}
	/** @param {string[]} headers */
	function buildHeadersFromArray(headers) {
		const clone = headers.slice();
		const entries = [];
		for (let index = 0; index < clone.length; index += 2) entries.push([clone[index], clone[index + 1]]);
		return Object.fromEntries(entries);
	}
	function matchHeaders(mockDispatch$1, headers) {
		if (typeof mockDispatch$1.headers === "function") {
			if (Array.isArray(headers)) headers = buildHeadersFromArray(headers);
			return mockDispatch$1.headers(headers ? lowerCaseEntries(headers) : {});
		}
		if (typeof mockDispatch$1.headers === "undefined") return true;
		if (typeof headers !== "object" || typeof mockDispatch$1.headers !== "object") return false;
		for (const [matchHeaderName, matchHeaderValue] of Object.entries(mockDispatch$1.headers)) {
			const headerValue = getHeaderByName(headers, matchHeaderName);
			if (!matchValue$1(matchHeaderValue, headerValue)) return false;
		}
		return true;
	}
	function normalizeSearchParams$1(query) {
		if (typeof query !== "string") return query;
		const originalQp = new URLSearchParams(query);
		const normalizedQp = new URLSearchParams();
		for (let [key, value] of originalQp.entries()) {
			key = key.replace("[]", "");
			if (/^(['"]).*\1$/.test(value)) {
				normalizedQp.append(key, value);
				continue;
			}
			if (value.includes(",")) {
				const values = value.split(",");
				for (const v of values) normalizedQp.append(key, v);
				continue;
			}
			normalizedQp.append(key, value);
		}
		return normalizedQp;
	}
	function safeUrl(path$1) {
		if (typeof path$1 !== "string") return path$1;
		const pathSegments = path$1.split("?", 3);
		if (pathSegments.length !== 2) return path$1;
		const qp = new URLSearchParams(pathSegments.pop());
		qp.sort();
		return [...pathSegments, qp.toString()].join("?");
	}
	function matchKey(mockDispatch$1, { path: path$1, method, body, headers }) {
		const pathMatch = matchValue$1(mockDispatch$1.path, path$1);
		const methodMatch = matchValue$1(mockDispatch$1.method, method);
		const bodyMatch = typeof mockDispatch$1.body !== "undefined" ? matchValue$1(mockDispatch$1.body, body) : true;
		const headersMatch = matchHeaders(mockDispatch$1, headers);
		return pathMatch && methodMatch && bodyMatch && headersMatch;
	}
	function getResponseData$1(data) {
		if (Buffer.isBuffer(data)) return data;
		else if (data instanceof Uint8Array) return data;
		else if (data instanceof ArrayBuffer) return data;
		else if (typeof data === "object") return JSON.stringify(data);
		else if (data) return data.toString();
		else return "";
	}
	function getMockDispatch(mockDispatches, key) {
		const basePath = key.query ? serializePathWithQuery$2(key.path, key.query) : key.path;
		const resolvedPath = typeof basePath === "string" ? safeUrl(basePath) : basePath;
		const resolvedPathWithoutTrailingSlash = removeTrailingSlash(resolvedPath);
		let matchedMockDispatches = mockDispatches.filter(({ consumed }) => !consumed).filter(({ path: path$1, ignoreTrailingSlash }) => {
			return ignoreTrailingSlash ? matchValue$1(removeTrailingSlash(safeUrl(path$1)), resolvedPathWithoutTrailingSlash) : matchValue$1(safeUrl(path$1), resolvedPath);
		});
		if (matchedMockDispatches.length === 0) throw new MockNotMatchedError(`Mock dispatch not matched for path '${resolvedPath}'`);
		matchedMockDispatches = matchedMockDispatches.filter(({ method }) => matchValue$1(method, key.method));
		if (matchedMockDispatches.length === 0) throw new MockNotMatchedError(`Mock dispatch not matched for method '${key.method}' on path '${resolvedPath}'`);
		matchedMockDispatches = matchedMockDispatches.filter(({ body }) => typeof body !== "undefined" ? matchValue$1(body, key.body) : true);
		if (matchedMockDispatches.length === 0) throw new MockNotMatchedError(`Mock dispatch not matched for body '${key.body}' on path '${resolvedPath}'`);
		matchedMockDispatches = matchedMockDispatches.filter((mockDispatch$1) => matchHeaders(mockDispatch$1, key.headers));
		if (matchedMockDispatches.length === 0) {
			const headers = typeof key.headers === "object" ? JSON.stringify(key.headers) : key.headers;
			throw new MockNotMatchedError(`Mock dispatch not matched for headers '${headers}' on path '${resolvedPath}'`);
		}
		return matchedMockDispatches[0];
	}
	function addMockDispatch$1(mockDispatches, key, data, opts) {
		const baseData = {
			timesInvoked: 0,
			times: 1,
			persist: false,
			consumed: false,
			...opts
		};
		const replyData = typeof data === "function" ? { callback: data } : { ...data };
		const newMockDispatch = {
			...baseData,
			...key,
			pending: true,
			data: {
				error: null,
				...replyData
			}
		};
		mockDispatches.push(newMockDispatch);
		return newMockDispatch;
	}
	function deleteMockDispatch(mockDispatches, key) {
		const index = mockDispatches.findIndex((dispatch) => {
			if (!dispatch.consumed) return false;
			return matchKey(dispatch, key);
		});
		if (index !== -1) mockDispatches.splice(index, 1);
	}
	/**
	* @param {string} path Path to remove trailing slash from
	*/
	function removeTrailingSlash(path$1) {
		while (path$1.endsWith("/")) path$1 = path$1.slice(0, -1);
		if (path$1.length === 0) path$1 = "/";
		return path$1;
	}
	function buildKey$1(opts) {
		const { path: path$1, method, body, headers, query } = opts;
		return {
			path: path$1,
			method,
			body,
			headers,
			query
		};
	}
	function generateKeyValues(data) {
		const keys = Object.keys(data);
		const result = [];
		for (let i = 0; i < keys.length; ++i) {
			const key = keys[i];
			const value = data[key];
			const name = Buffer.from(`${key}`);
			if (Array.isArray(value)) for (let j = 0; j < value.length; ++j) result.push(name, Buffer.from(`${value[j]}`));
			else result.push(name, Buffer.from(`${value}`));
		}
		return result;
	}
	/**
	* @see https://developer.mozilla.org/en-US/docs/Web/HTTP/Status
	* @param {number} statusCode
	*/
	function getStatusText(statusCode) {
		return STATUS_CODES$1[statusCode] || "unknown";
	}
	async function getResponse(body) {
		const buffers = [];
		for await (const data of body) buffers.push(data);
		return Buffer.concat(buffers).toString("utf8");
	}
	/**
	* Mock dispatch function used to simulate undici dispatches
	*/
	function mockDispatch(opts, handler) {
		const key = buildKey$1(opts);
		const mockDispatch$1 = getMockDispatch(this[kDispatches$4], key);
		mockDispatch$1.timesInvoked++;
		if (mockDispatch$1.data.callback) mockDispatch$1.data = {
			...mockDispatch$1.data,
			...mockDispatch$1.data.callback(opts)
		};
		const { data: { statusCode, data, headers, trailers, error }, delay: delay$2, persist } = mockDispatch$1;
		const { timesInvoked, times } = mockDispatch$1;
		mockDispatch$1.consumed = !persist && timesInvoked >= times;
		mockDispatch$1.pending = timesInvoked < times;
		if (error !== null) {
			deleteMockDispatch(this[kDispatches$4], key);
			handler.onError(error);
			return true;
		}
		if (typeof delay$2 === "number" && delay$2 > 0) setTimeout(() => {
			handleReply(this[kDispatches$4]);
		}, delay$2);
		else handleReply(this[kDispatches$4]);
		function handleReply(mockDispatches, _data = data) {
			const optsHeaders = Array.isArray(opts.headers) ? buildHeadersFromArray(opts.headers) : opts.headers;
			const body = typeof _data === "function" ? _data({
				...opts,
				headers: optsHeaders
			}) : _data;
			if (isPromise(body)) {
				body.then((newData) => handleReply(mockDispatches, newData));
				return;
			}
			const responseData = getResponseData$1(body);
			const responseHeaders = generateKeyValues(headers);
			const responseTrailers = generateKeyValues(trailers);
			handler.onConnect?.((err) => handler.onError(err), null);
			handler.onHeaders?.(statusCode, responseHeaders, resume$1, getStatusText(statusCode));
			handler.onData?.(Buffer.from(responseData));
			handler.onComplete?.(responseTrailers);
			deleteMockDispatch(mockDispatches, key);
		}
		function resume$1() {}
		return true;
	}
	function buildMockDispatch$2() {
		const agent = this[kMockAgent$2];
		const origin = this[kOrigin$2];
		const originalDispatch = this[kOriginalDispatch$2];
		return function dispatch(opts, handler) {
			if (agent.isMockActive) try {
				mockDispatch.call(this, opts, handler);
			} catch (error) {
				if (error instanceof MockNotMatchedError) {
					const netConnect = agent[kGetNetConnect$1]();
					if (netConnect === false) throw new MockNotMatchedError(`${error.message}: subsequent request to origin ${origin} was not allowed (net.connect disabled)`);
					if (checkNetConnect(netConnect, origin)) originalDispatch.call(this, opts, handler);
					else throw new MockNotMatchedError(`${error.message}: subsequent request to origin ${origin} was not allowed (net.connect is not enabled for this origin)`);
				} else throw error;
			}
			else originalDispatch.call(this, opts, handler);
		};
	}
	function checkNetConnect(netConnect, origin) {
		const url = new URL(origin);
		if (netConnect === true) return true;
		else if (Array.isArray(netConnect) && netConnect.some((matcher) => matchValue$1(matcher, url.host))) return true;
		return false;
	}
	function buildAndValidateMockOptions$1(opts) {
		if (opts) {
			const { agent,...mockOptions } = opts;
			if ("enableCallHistory" in mockOptions && typeof mockOptions.enableCallHistory !== "boolean") throw new InvalidArgumentError$13("options.enableCallHistory must to be a boolean");
			if ("acceptNonStandardSearchParameters" in mockOptions && typeof mockOptions.acceptNonStandardSearchParameters !== "boolean") throw new InvalidArgumentError$13("options.acceptNonStandardSearchParameters must to be a boolean");
			return mockOptions;
		}
	}
	module.exports = {
		getResponseData: getResponseData$1,
		getMockDispatch,
		addMockDispatch: addMockDispatch$1,
		deleteMockDispatch,
		buildKey: buildKey$1,
		generateKeyValues,
		matchValue: matchValue$1,
		getResponse,
		getStatusText,
		mockDispatch,
		buildMockDispatch: buildMockDispatch$2,
		checkNetConnect,
		buildAndValidateMockOptions: buildAndValidateMockOptions$1,
		getHeaderByName,
		buildHeadersFromArray,
		normalizeSearchParams: normalizeSearchParams$1
	};
}) });

//#endregion
//#region node_modules/undici/lib/mock/mock-interceptor.js
var require_mock_interceptor = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/mock/mock-interceptor.js": ((exports, module) => {
	const { getResponseData, buildKey, addMockDispatch } = require_mock_utils();
	const { kDispatches: kDispatches$3, kDispatchKey, kDefaultHeaders, kDefaultTrailers, kContentLength, kMockDispatch, kIgnoreTrailingSlash: kIgnoreTrailingSlash$3 } = require_mock_symbols();
	const { InvalidArgumentError: InvalidArgumentError$12 } = require_errors();
	const { serializePathWithQuery: serializePathWithQuery$1 } = require_util$5();
	/**
	* Defines the scope API for an interceptor reply
	*/
	var MockScope = class {
		constructor(mockDispatch$1) {
			this[kMockDispatch] = mockDispatch$1;
		}
		/**
		* Delay a reply by a set amount in ms.
		*/
		delay(waitInMs) {
			if (typeof waitInMs !== "number" || !Number.isInteger(waitInMs) || waitInMs <= 0) throw new InvalidArgumentError$12("waitInMs must be a valid integer > 0");
			this[kMockDispatch].delay = waitInMs;
			return this;
		}
		/**
		* For a defined reply, never mark as consumed.
		*/
		persist() {
			this[kMockDispatch].persist = true;
			return this;
		}
		/**
		* Allow one to define a reply for a set amount of matching requests.
		*/
		times(repeatTimes) {
			if (typeof repeatTimes !== "number" || !Number.isInteger(repeatTimes) || repeatTimes <= 0) throw new InvalidArgumentError$12("repeatTimes must be a valid integer > 0");
			this[kMockDispatch].times = repeatTimes;
			return this;
		}
	};
	/**
	* Defines an interceptor for a Mock
	*/
	var MockInterceptor$2 = class {
		constructor(opts, mockDispatches) {
			if (typeof opts !== "object") throw new InvalidArgumentError$12("opts must be an object");
			if (typeof opts.path === "undefined") throw new InvalidArgumentError$12("opts.path must be defined");
			if (typeof opts.method === "undefined") opts.method = "GET";
			if (typeof opts.path === "string") if (opts.query) opts.path = serializePathWithQuery$1(opts.path, opts.query);
			else {
				const parsedURL = new URL(opts.path, "data://");
				opts.path = parsedURL.pathname + parsedURL.search;
			}
			if (typeof opts.method === "string") opts.method = opts.method.toUpperCase();
			this[kDispatchKey] = buildKey(opts);
			this[kDispatches$3] = mockDispatches;
			this[kIgnoreTrailingSlash$3] = opts.ignoreTrailingSlash ?? false;
			this[kDefaultHeaders] = {};
			this[kDefaultTrailers] = {};
			this[kContentLength] = false;
		}
		createMockScopeDispatchData({ statusCode, data, responseOptions }) {
			const responseData = getResponseData(data);
			const contentLength = this[kContentLength] ? { "content-length": responseData.length } : {};
			const headers = {
				...this[kDefaultHeaders],
				...contentLength,
				...responseOptions.headers
			};
			const trailers = {
				...this[kDefaultTrailers],
				...responseOptions.trailers
			};
			return {
				statusCode,
				data,
				headers,
				trailers
			};
		}
		validateReplyParameters(replyParameters) {
			if (typeof replyParameters.statusCode === "undefined") throw new InvalidArgumentError$12("statusCode must be defined");
			if (typeof replyParameters.responseOptions !== "object" || replyParameters.responseOptions === null) throw new InvalidArgumentError$12("responseOptions must be an object");
		}
		/**
		* Mock an undici request with a defined reply.
		*/
		reply(replyOptionsCallbackOrStatusCode) {
			if (typeof replyOptionsCallbackOrStatusCode === "function") {
				const wrappedDefaultsCallback = (opts) => {
					const resolvedData = replyOptionsCallbackOrStatusCode(opts);
					if (typeof resolvedData !== "object" || resolvedData === null) throw new InvalidArgumentError$12("reply options callback must return an object");
					const replyParameters$1 = {
						data: "",
						responseOptions: {},
						...resolvedData
					};
					this.validateReplyParameters(replyParameters$1);
					return { ...this.createMockScopeDispatchData(replyParameters$1) };
				};
				const newMockDispatch$1 = addMockDispatch(this[kDispatches$3], this[kDispatchKey], wrappedDefaultsCallback, { ignoreTrailingSlash: this[kIgnoreTrailingSlash$3] });
				return new MockScope(newMockDispatch$1);
			}
			const replyParameters = {
				statusCode: replyOptionsCallbackOrStatusCode,
				data: arguments[1] === void 0 ? "" : arguments[1],
				responseOptions: arguments[2] === void 0 ? {} : arguments[2]
			};
			this.validateReplyParameters(replyParameters);
			const dispatchData = this.createMockScopeDispatchData(replyParameters);
			const newMockDispatch = addMockDispatch(this[kDispatches$3], this[kDispatchKey], dispatchData, { ignoreTrailingSlash: this[kIgnoreTrailingSlash$3] });
			return new MockScope(newMockDispatch);
		}
		/**
		* Mock an undici request with a defined error.
		*/
		replyWithError(error) {
			if (typeof error === "undefined") throw new InvalidArgumentError$12("error must be defined");
			const newMockDispatch = addMockDispatch(this[kDispatches$3], this[kDispatchKey], { error }, { ignoreTrailingSlash: this[kIgnoreTrailingSlash$3] });
			return new MockScope(newMockDispatch);
		}
		/**
		* Set default reply headers on the interceptor for subsequent replies
		*/
		defaultReplyHeaders(headers) {
			if (typeof headers === "undefined") throw new InvalidArgumentError$12("headers must be defined");
			this[kDefaultHeaders] = headers;
			return this;
		}
		/**
		* Set default reply trailers on the interceptor for subsequent replies
		*/
		defaultReplyTrailers(trailers) {
			if (typeof trailers === "undefined") throw new InvalidArgumentError$12("trailers must be defined");
			this[kDefaultTrailers] = trailers;
			return this;
		}
		/**
		* Set reply content length header for replies on the interceptor
		*/
		replyContentLength() {
			this[kContentLength] = true;
			return this;
		}
	};
	module.exports.MockInterceptor = MockInterceptor$2;
	module.exports.MockScope = MockScope;
}) });

//#endregion
//#region node_modules/undici/lib/mock/mock-client.js
var require_mock_client = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/mock/mock-client.js": ((exports, module) => {
	const { promisify: promisify$2 } = __require("node:util");
	const Client$1 = require_client();
	const { buildMockDispatch: buildMockDispatch$1 } = require_mock_utils();
	const { kDispatches: kDispatches$2, kMockAgent: kMockAgent$1, kClose: kClose$1, kOriginalClose: kOriginalClose$1, kOrigin: kOrigin$1, kOriginalDispatch: kOriginalDispatch$1, kConnected: kConnected$1, kIgnoreTrailingSlash: kIgnoreTrailingSlash$2 } = require_mock_symbols();
	const { MockInterceptor: MockInterceptor$1 } = require_mock_interceptor();
	const Symbols$1 = require_symbols();
	const { InvalidArgumentError: InvalidArgumentError$11 } = require_errors();
	/**
	* MockClient provides an API that extends the Client to influence the mockDispatches.
	*/
	var MockClient$2 = class extends Client$1 {
		constructor(origin, opts) {
			if (!opts || !opts.agent || typeof opts.agent.dispatch !== "function") throw new InvalidArgumentError$11("Argument opts.agent must implement Agent");
			super(origin, opts);
			this[kMockAgent$1] = opts.agent;
			this[kOrigin$1] = origin;
			this[kIgnoreTrailingSlash$2] = opts.ignoreTrailingSlash ?? false;
			this[kDispatches$2] = [];
			this[kConnected$1] = 1;
			this[kOriginalDispatch$1] = this.dispatch;
			this[kOriginalClose$1] = this.close.bind(this);
			this.dispatch = buildMockDispatch$1.call(this);
			this.close = this[kClose$1];
		}
		get [Symbols$1.kConnected]() {
			return this[kConnected$1];
		}
		/**
		* Sets up the base interceptor for mocking replies from undici.
		*/
		intercept(opts) {
			return new MockInterceptor$1(opts && {
				ignoreTrailingSlash: this[kIgnoreTrailingSlash$2],
				...opts
			}, this[kDispatches$2]);
		}
		cleanMocks() {
			this[kDispatches$2] = [];
		}
		async [kClose$1]() {
			await promisify$2(this[kOriginalClose$1])();
			this[kConnected$1] = 0;
			this[kMockAgent$1][Symbols$1.kClients].delete(this[kOrigin$1]);
		}
	};
	module.exports = MockClient$2;
}) });

//#endregion
//#region node_modules/undici/lib/mock/mock-call-history.js
var require_mock_call_history = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/mock/mock-call-history.js": ((exports, module) => {
	const { kMockCallHistoryAddLog: kMockCallHistoryAddLog$1 } = require_mock_symbols();
	const { InvalidArgumentError: InvalidArgumentError$10 } = require_errors();
	function handleFilterCallsWithOptions(criteria, options, handler, store) {
		switch (options.operator) {
			case "OR":
				store.push(...handler(criteria));
				return store;
			case "AND": return handler.call({ logs: store }, criteria);
			default: throw new InvalidArgumentError$10("options.operator must to be a case insensitive string equal to 'OR' or 'AND'");
		}
	}
	function buildAndValidateFilterCallsOptions(options = {}) {
		const finalOptions = {};
		if ("operator" in options) {
			if (typeof options.operator !== "string" || options.operator.toUpperCase() !== "OR" && options.operator.toUpperCase() !== "AND") throw new InvalidArgumentError$10("options.operator must to be a case insensitive string equal to 'OR' or 'AND'");
			return {
				...finalOptions,
				operator: options.operator.toUpperCase()
			};
		}
		return finalOptions;
	}
	function makeFilterCalls(parameterName) {
		return (parameterValue) => {
			if (typeof parameterValue === "string" || parameterValue == null) return this.logs.filter((log) => {
				return log[parameterName] === parameterValue;
			});
			if (parameterValue instanceof RegExp) return this.logs.filter((log) => {
				return parameterValue.test(log[parameterName]);
			});
			throw new InvalidArgumentError$10(`${parameterName} parameter should be one of string, regexp, undefined or null`);
		};
	}
	function computeUrlWithMaybeSearchParameters(requestInit) {
		try {
			const url = new URL(requestInit.path, requestInit.origin);
			if (url.search.length !== 0) return url;
			url.search = new URLSearchParams(requestInit.query).toString();
			return url;
		} catch (error) {
			throw new InvalidArgumentError$10("An error occurred when computing MockCallHistoryLog.url", { cause: error });
		}
	}
	var MockCallHistoryLog$1 = class {
		constructor(requestInit = {}) {
			this.body = requestInit.body;
			this.headers = requestInit.headers;
			this.method = requestInit.method;
			const url = computeUrlWithMaybeSearchParameters(requestInit);
			this.fullUrl = url.toString();
			this.origin = url.origin;
			this.path = url.pathname;
			this.searchParams = Object.fromEntries(url.searchParams);
			this.protocol = url.protocol;
			this.host = url.host;
			this.port = url.port;
			this.hash = url.hash;
		}
		toMap() {
			return new Map([
				["protocol", this.protocol],
				["host", this.host],
				["port", this.port],
				["origin", this.origin],
				["path", this.path],
				["hash", this.hash],
				["searchParams", this.searchParams],
				["fullUrl", this.fullUrl],
				["method", this.method],
				["body", this.body],
				["headers", this.headers]
			]);
		}
		toString() {
			const options = {
				betweenKeyValueSeparator: "->",
				betweenPairSeparator: "|"
			};
			let result = "";
			this.toMap().forEach((value, key) => {
				if (typeof value === "string" || value === void 0 || value === null) result = `${result}${key}${options.betweenKeyValueSeparator}${value}${options.betweenPairSeparator}`;
				if (typeof value === "object" && value !== null || Array.isArray(value)) result = `${result}${key}${options.betweenKeyValueSeparator}${JSON.stringify(value)}${options.betweenPairSeparator}`;
			});
			return result.slice(0, -1);
		}
	};
	var MockCallHistory$2 = class {
		logs = [];
		calls() {
			return this.logs;
		}
		firstCall() {
			return this.logs.at(0);
		}
		lastCall() {
			return this.logs.at(-1);
		}
		nthCall(number) {
			if (typeof number !== "number") throw new InvalidArgumentError$10("nthCall must be called with a number");
			if (!Number.isInteger(number)) throw new InvalidArgumentError$10("nthCall must be called with an integer");
			if (Math.sign(number) !== 1) throw new InvalidArgumentError$10("nthCall must be called with a positive value. use firstCall or lastCall instead");
			return this.logs.at(number - 1);
		}
		filterCalls(criteria, options) {
			if (this.logs.length === 0) return this.logs;
			if (typeof criteria === "function") return this.logs.filter(criteria);
			if (criteria instanceof RegExp) return this.logs.filter((log) => {
				return criteria.test(log.toString());
			});
			if (typeof criteria === "object" && criteria !== null) {
				if (Object.keys(criteria).length === 0) return this.logs;
				const finalOptions = {
					operator: "OR",
					...buildAndValidateFilterCallsOptions(options)
				};
				let maybeDuplicatedLogsFiltered = [];
				if ("protocol" in criteria) maybeDuplicatedLogsFiltered = handleFilterCallsWithOptions(criteria.protocol, finalOptions, this.filterCallsByProtocol, maybeDuplicatedLogsFiltered);
				if ("host" in criteria) maybeDuplicatedLogsFiltered = handleFilterCallsWithOptions(criteria.host, finalOptions, this.filterCallsByHost, maybeDuplicatedLogsFiltered);
				if ("port" in criteria) maybeDuplicatedLogsFiltered = handleFilterCallsWithOptions(criteria.port, finalOptions, this.filterCallsByPort, maybeDuplicatedLogsFiltered);
				if ("origin" in criteria) maybeDuplicatedLogsFiltered = handleFilterCallsWithOptions(criteria.origin, finalOptions, this.filterCallsByOrigin, maybeDuplicatedLogsFiltered);
				if ("path" in criteria) maybeDuplicatedLogsFiltered = handleFilterCallsWithOptions(criteria.path, finalOptions, this.filterCallsByPath, maybeDuplicatedLogsFiltered);
				if ("hash" in criteria) maybeDuplicatedLogsFiltered = handleFilterCallsWithOptions(criteria.hash, finalOptions, this.filterCallsByHash, maybeDuplicatedLogsFiltered);
				if ("fullUrl" in criteria) maybeDuplicatedLogsFiltered = handleFilterCallsWithOptions(criteria.fullUrl, finalOptions, this.filterCallsByFullUrl, maybeDuplicatedLogsFiltered);
				if ("method" in criteria) maybeDuplicatedLogsFiltered = handleFilterCallsWithOptions(criteria.method, finalOptions, this.filterCallsByMethod, maybeDuplicatedLogsFiltered);
				return [...new Set(maybeDuplicatedLogsFiltered)];
			}
			throw new InvalidArgumentError$10("criteria parameter should be one of function, regexp, or object");
		}
		filterCallsByProtocol = makeFilterCalls.call(this, "protocol");
		filterCallsByHost = makeFilterCalls.call(this, "host");
		filterCallsByPort = makeFilterCalls.call(this, "port");
		filterCallsByOrigin = makeFilterCalls.call(this, "origin");
		filterCallsByPath = makeFilterCalls.call(this, "path");
		filterCallsByHash = makeFilterCalls.call(this, "hash");
		filterCallsByFullUrl = makeFilterCalls.call(this, "fullUrl");
		filterCallsByMethod = makeFilterCalls.call(this, "method");
		clear() {
			this.logs = [];
		}
		[kMockCallHistoryAddLog$1](requestInit) {
			const log = new MockCallHistoryLog$1(requestInit);
			this.logs.push(log);
			return log;
		}
		*[Symbol.iterator]() {
			for (const log of this.calls()) yield log;
		}
	};
	module.exports.MockCallHistory = MockCallHistory$2;
	module.exports.MockCallHistoryLog = MockCallHistoryLog$1;
}) });

//#endregion
//#region node_modules/undici/lib/mock/mock-pool.js
var require_mock_pool = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/mock/mock-pool.js": ((exports, module) => {
	const { promisify: promisify$1 } = __require("node:util");
	const Pool$1 = require_pool();
	const { buildMockDispatch } = require_mock_utils();
	const { kDispatches: kDispatches$1, kMockAgent, kClose, kOriginalClose, kOrigin, kOriginalDispatch, kConnected, kIgnoreTrailingSlash: kIgnoreTrailingSlash$1 } = require_mock_symbols();
	const { MockInterceptor } = require_mock_interceptor();
	const Symbols = require_symbols();
	const { InvalidArgumentError: InvalidArgumentError$9 } = require_errors();
	/**
	* MockPool provides an API that extends the Pool to influence the mockDispatches.
	*/
	var MockPool$2 = class extends Pool$1 {
		constructor(origin, opts) {
			if (!opts || !opts.agent || typeof opts.agent.dispatch !== "function") throw new InvalidArgumentError$9("Argument opts.agent must implement Agent");
			super(origin, opts);
			this[kMockAgent] = opts.agent;
			this[kOrigin] = origin;
			this[kIgnoreTrailingSlash$1] = opts.ignoreTrailingSlash ?? false;
			this[kDispatches$1] = [];
			this[kConnected] = 1;
			this[kOriginalDispatch] = this.dispatch;
			this[kOriginalClose] = this.close.bind(this);
			this.dispatch = buildMockDispatch.call(this);
			this.close = this[kClose];
		}
		get [Symbols.kConnected]() {
			return this[kConnected];
		}
		/**
		* Sets up the base interceptor for mocking replies from undici.
		*/
		intercept(opts) {
			return new MockInterceptor(opts && {
				ignoreTrailingSlash: this[kIgnoreTrailingSlash$1],
				...opts
			}, this[kDispatches$1]);
		}
		cleanMocks() {
			this[kDispatches$1] = [];
		}
		async [kClose]() {
			await promisify$1(this[kOriginalClose])();
			this[kConnected] = 0;
			this[kMockAgent][Symbols.kClients].delete(this[kOrigin]);
		}
	};
	module.exports = MockPool$2;
}) });

//#endregion
//#region node_modules/undici/lib/mock/pending-interceptors-formatter.js
var require_pending_interceptors_formatter = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/mock/pending-interceptors-formatter.js": ((exports, module) => {
	const { Transform: Transform$1 } = __require("node:stream");
	const { Console } = __require("node:console");
	const PERSISTENT = process.versions.icu ? "✅" : "Y ";
	const NOT_PERSISTENT = process.versions.icu ? "❌" : "N ";
	/**
	* Gets the output of `console.table(…)` as a string.
	*/
	module.exports = class PendingInterceptorsFormatter$1 {
		constructor({ disableColors } = {}) {
			this.transform = new Transform$1({ transform(chunk, _enc, cb) {
				cb(null, chunk);
			} });
			this.logger = new Console({
				stdout: this.transform,
				inspectOptions: { colors: !disableColors && !process.env.CI }
			});
		}
		format(pendingInterceptors) {
			const withPrettyHeaders = pendingInterceptors.map(({ method, path: path$1, data: { statusCode }, persist, times, timesInvoked, origin }) => ({
				Method: method,
				Origin: origin,
				Path: path$1,
				"Status code": statusCode,
				Persistent: persist ? PERSISTENT : NOT_PERSISTENT,
				Invocations: timesInvoked,
				Remaining: persist ? Infinity : times - timesInvoked
			}));
			this.logger.table(withPrettyHeaders);
			return this.transform.read().toString();
		}
	};
}) });

//#endregion
//#region node_modules/undici/lib/mock/mock-agent.js
var require_mock_agent = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/mock/mock-agent.js": ((exports, module) => {
	const { kClients } = require_symbols();
	const Agent$3 = require_agent();
	const { kAgent, kMockAgentSet, kMockAgentGet, kDispatches, kIsMockActive, kNetConnect, kGetNetConnect, kOptions, kFactory, kMockAgentRegisterCallHistory, kMockAgentIsCallHistoryEnabled, kMockAgentAddCallHistoryLog, kMockAgentMockCallHistoryInstance, kMockAgentAcceptsNonStandardSearchParameters, kMockCallHistoryAddLog, kIgnoreTrailingSlash } = require_mock_symbols();
	const MockClient$1 = require_mock_client();
	const MockPool$1 = require_mock_pool();
	const { matchValue, normalizeSearchParams, buildAndValidateMockOptions } = require_mock_utils();
	const { InvalidArgumentError: InvalidArgumentError$8, UndiciError: UndiciError$2 } = require_errors();
	const Dispatcher$1 = require_dispatcher();
	const PendingInterceptorsFormatter = require_pending_interceptors_formatter();
	const { MockCallHistory: MockCallHistory$1 } = require_mock_call_history();
	var MockAgent$2 = class extends Dispatcher$1 {
		constructor(opts) {
			super(opts);
			const mockOptions = buildAndValidateMockOptions(opts);
			this[kNetConnect] = true;
			this[kIsMockActive] = true;
			this[kMockAgentIsCallHistoryEnabled] = mockOptions?.enableCallHistory ?? false;
			this[kMockAgentAcceptsNonStandardSearchParameters] = mockOptions?.acceptNonStandardSearchParameters ?? false;
			this[kIgnoreTrailingSlash] = mockOptions?.ignoreTrailingSlash ?? false;
			if (opts?.agent && typeof opts.agent.dispatch !== "function") throw new InvalidArgumentError$8("Argument opts.agent must implement Agent");
			const agent = opts?.agent ? opts.agent : new Agent$3(opts);
			this[kAgent] = agent;
			this[kClients] = agent[kClients];
			this[kOptions] = mockOptions;
			if (this[kMockAgentIsCallHistoryEnabled]) this[kMockAgentRegisterCallHistory]();
		}
		get(origin) {
			const originKey = this[kIgnoreTrailingSlash] ? origin.replace(/\/$/, "") : origin;
			let dispatcher = this[kMockAgentGet](originKey);
			if (!dispatcher) {
				dispatcher = this[kFactory](originKey);
				this[kMockAgentSet](originKey, dispatcher);
			}
			return dispatcher;
		}
		dispatch(opts, handler) {
			this.get(opts.origin);
			this[kMockAgentAddCallHistoryLog](opts);
			const acceptNonStandardSearchParameters = this[kMockAgentAcceptsNonStandardSearchParameters];
			const dispatchOpts = { ...opts };
			if (acceptNonStandardSearchParameters && dispatchOpts.path) {
				const [path$1, searchParams] = dispatchOpts.path.split("?");
				const normalizedSearchParams = normalizeSearchParams(searchParams, acceptNonStandardSearchParameters);
				dispatchOpts.path = `${path$1}?${normalizedSearchParams}`;
			}
			return this[kAgent].dispatch(dispatchOpts, handler);
		}
		async close() {
			this.clearCallHistory();
			await this[kAgent].close();
			this[kClients].clear();
		}
		deactivate() {
			this[kIsMockActive] = false;
		}
		activate() {
			this[kIsMockActive] = true;
		}
		enableNetConnect(matcher) {
			if (typeof matcher === "string" || typeof matcher === "function" || matcher instanceof RegExp) if (Array.isArray(this[kNetConnect])) this[kNetConnect].push(matcher);
			else this[kNetConnect] = [matcher];
			else if (typeof matcher === "undefined") this[kNetConnect] = true;
			else throw new InvalidArgumentError$8("Unsupported matcher. Must be one of String|Function|RegExp.");
		}
		disableNetConnect() {
			this[kNetConnect] = false;
		}
		enableCallHistory() {
			this[kMockAgentIsCallHistoryEnabled] = true;
			return this;
		}
		disableCallHistory() {
			this[kMockAgentIsCallHistoryEnabled] = false;
			return this;
		}
		getCallHistory() {
			return this[kMockAgentMockCallHistoryInstance];
		}
		clearCallHistory() {
			if (this[kMockAgentMockCallHistoryInstance] !== void 0) this[kMockAgentMockCallHistoryInstance].clear();
		}
		get isMockActive() {
			return this[kIsMockActive];
		}
		[kMockAgentRegisterCallHistory]() {
			if (this[kMockAgentMockCallHistoryInstance] === void 0) this[kMockAgentMockCallHistoryInstance] = new MockCallHistory$1();
		}
		[kMockAgentAddCallHistoryLog](opts) {
			if (this[kMockAgentIsCallHistoryEnabled]) {
				this[kMockAgentRegisterCallHistory]();
				this[kMockAgentMockCallHistoryInstance][kMockCallHistoryAddLog](opts);
			}
		}
		[kMockAgentSet](origin, dispatcher) {
			this[kClients].set(origin, {
				count: 0,
				dispatcher
			});
		}
		[kFactory](origin) {
			const mockOptions = Object.assign({ agent: this }, this[kOptions]);
			return this[kOptions] && this[kOptions].connections === 1 ? new MockClient$1(origin, mockOptions) : new MockPool$1(origin, mockOptions);
		}
		[kMockAgentGet](origin) {
			const result = this[kClients].get(origin);
			if (result?.dispatcher) return result.dispatcher;
			if (typeof origin !== "string") {
				const dispatcher = this[kFactory]("http://localhost:9999");
				this[kMockAgentSet](origin, dispatcher);
				return dispatcher;
			}
			for (const [keyMatcher, result$1] of Array.from(this[kClients])) if (result$1 && typeof keyMatcher !== "string" && matchValue(keyMatcher, origin)) {
				const dispatcher = this[kFactory](origin);
				this[kMockAgentSet](origin, dispatcher);
				dispatcher[kDispatches] = result$1.dispatcher[kDispatches];
				return dispatcher;
			}
		}
		[kGetNetConnect]() {
			return this[kNetConnect];
		}
		pendingInterceptors() {
			const mockAgentClients = this[kClients];
			return Array.from(mockAgentClients.entries()).flatMap(([origin, result]) => result.dispatcher[kDispatches].map((dispatch) => ({
				...dispatch,
				origin
			}))).filter(({ pending }) => pending);
		}
		assertNoPendingInterceptors({ pendingInterceptorsFormatter = new PendingInterceptorsFormatter() } = {}) {
			const pending = this.pendingInterceptors();
			if (pending.length === 0) return;
			throw new UndiciError$2(pending.length === 1 ? `1 interceptor is pending:\n\n${pendingInterceptorsFormatter.format(pending)}`.trim() : `${pending.length} interceptors are pending:\n\n${pendingInterceptorsFormatter.format(pending)}`.trim());
		}
	};
	module.exports = MockAgent$2;
}) });

//#endregion
//#region node_modules/undici/lib/mock/snapshot-utils.js
var require_snapshot_utils = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/mock/snapshot-utils.js": ((exports, module) => {
	const { InvalidArgumentError: InvalidArgumentError$7 } = require_errors();
	/**
	* @typedef {Object} HeaderFilters
	* @property {Set<string>} ignore - Set of headers to ignore for matching
	* @property {Set<string>} exclude - Set of headers to exclude from matching
	* @property {Set<string>} match - Set of headers to match (empty means match
	*/
	/**
	* Creates cached header sets for performance
	*
	* @param {import('./snapshot-recorder').SnapshotRecorderMatchOptions} matchOptions - Matching options for headers
	* @returns {HeaderFilters} - Cached sets for ignore, exclude, and match headers
	*/
	function createHeaderFilters$1(matchOptions = {}) {
		const { ignoreHeaders = [], excludeHeaders = [], matchHeaders: matchHeaders$1 = [], caseSensitive = false } = matchOptions;
		return {
			ignore: new Set(ignoreHeaders.map((header) => caseSensitive ? header : header.toLowerCase())),
			exclude: new Set(excludeHeaders.map((header) => caseSensitive ? header : header.toLowerCase())),
			match: new Set(matchHeaders$1.map((header) => caseSensitive ? header : header.toLowerCase()))
		};
	}
	let crypto$3;
	try {
		crypto$3 = __require("node:crypto");
	} catch {}
	/**
	* @callback HashIdFunction
	* @param {string} value - The value to hash
	* @returns {string} - The base64url encoded hash of the value
	*/
	/**
	* Generates a hash for a given value
	* @type {HashIdFunction}
	*/
	const hashId$1 = crypto$3?.hash ? (value) => crypto$3.hash("sha256", value, "base64url") : (value) => Buffer.from(value).toString("base64url");
	/**
	* @typedef {(url: string) => boolean} IsUrlExcluded Checks if a URL matches any of the exclude patterns
	*/
	/** @typedef {{[key: Lowercase<string>]: string}} NormalizedHeaders */
	/** @typedef {Array<string>} UndiciHeaders */
	/** @typedef {Record<string, string|string[]>} Headers */
	/**
	* @param {*} headers
	* @returns {headers is UndiciHeaders}
	*/
	function isUndiciHeaders(headers) {
		return Array.isArray(headers) && (headers.length & 1) === 0;
	}
	/**
	* Factory function to create a URL exclusion checker
	* @param {Array<string| RegExp>} [excludePatterns=[]] - Array of patterns to exclude
	* @returns {IsUrlExcluded} - A function that checks if a URL matches any of the exclude patterns
	*/
	function isUrlExcludedFactory$1(excludePatterns = []) {
		if (excludePatterns.length === 0) return () => false;
		return function isUrlExcluded(url) {
			let urlLowerCased;
			for (const pattern of excludePatterns) if (typeof pattern === "string") {
				if (!urlLowerCased) urlLowerCased = url.toLowerCase();
				if (urlLowerCased.includes(pattern.toLowerCase())) return true;
			} else if (pattern instanceof RegExp) {
				if (pattern.test(url)) return true;
			}
			return false;
		};
	}
	/**
	* Normalizes headers for consistent comparison
	*
	* @param {Object|UndiciHeaders} headers - Headers to normalize
	* @returns {NormalizedHeaders} - Normalized headers as a lowercase object
	*/
	function normalizeHeaders$3(headers) {
		/** @type {NormalizedHeaders} */
		const normalizedHeaders = {};
		if (!headers) return normalizedHeaders;
		if (isUndiciHeaders(headers)) {
			for (let i = 0; i < headers.length; i += 2) {
				const key = headers[i];
				const value = headers[i + 1];
				if (key && value !== void 0) {
					const keyStr = Buffer.isBuffer(key) ? key.toString() : key;
					const valueStr = Buffer.isBuffer(value) ? value.toString() : value;
					normalizedHeaders[keyStr.toLowerCase()] = valueStr;
				}
			}
			return normalizedHeaders;
		}
		if (headers && typeof headers === "object") {
			for (const [key, value] of Object.entries(headers)) if (key && typeof key === "string") normalizedHeaders[key.toLowerCase()] = Array.isArray(value) ? value.join(", ") : String(value);
		}
		return normalizedHeaders;
	}
	const validSnapshotModes = [
		"record",
		"playback",
		"update"
	];
	/** @typedef {typeof validSnapshotModes[number]} SnapshotMode */
	/**
	* @param {*} mode - The snapshot mode to validate
	* @returns {asserts mode is SnapshotMode}
	*/
	function validateSnapshotMode$1(mode) {
		if (!validSnapshotModes.includes(mode)) throw new InvalidArgumentError$7(`Invalid snapshot mode: ${mode}. Must be one of: ${validSnapshotModes.join(", ")}`);
	}
	module.exports = {
		createHeaderFilters: createHeaderFilters$1,
		hashId: hashId$1,
		isUndiciHeaders,
		normalizeHeaders: normalizeHeaders$3,
		isUrlExcludedFactory: isUrlExcludedFactory$1,
		validateSnapshotMode: validateSnapshotMode$1
	};
}) });

//#endregion
//#region node_modules/undici/lib/mock/snapshot-recorder.js
var require_snapshot_recorder = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/mock/snapshot-recorder.js": ((exports, module) => {
	const { writeFile, readFile, mkdir } = __require("node:fs/promises");
	const { dirname, resolve } = __require("node:path");
	const { setTimeout: setTimeout$1, clearTimeout: clearTimeout$1 } = __require("node:timers");
	const { InvalidArgumentError: InvalidArgumentError$6, UndiciError: UndiciError$1 } = require_errors();
	const { hashId, isUrlExcludedFactory, normalizeHeaders: normalizeHeaders$2, createHeaderFilters } = require_snapshot_utils();
	/**
	* @typedef {Object} SnapshotRequestOptions
	* @property {string} method - HTTP method (e.g. 'GET', 'POST', etc.)
	* @property {string} path - Request path
	* @property {string} origin - Request origin (base URL)
	* @property {import('./snapshot-utils').Headers|import('./snapshot-utils').UndiciHeaders} headers - Request headers
	* @property {import('./snapshot-utils').NormalizedHeaders} _normalizedHeaders - Request headers as a lowercase object
	* @property {string|Buffer} [body] - Request body (optional)
	*/
	/**
	* @typedef {Object} SnapshotEntryRequest
	* @property {string} method - HTTP method (e.g. 'GET', 'POST', etc.)
	* @property {string} url - Full URL of the request
	* @property {import('./snapshot-utils').NormalizedHeaders} headers - Normalized headers as a lowercase object
	* @property {string|Buffer} [body] - Request body (optional)
	*/
	/**
	* @typedef {Object} SnapshotEntryResponse
	* @property {number} statusCode - HTTP status code of the response
	* @property {import('./snapshot-utils').NormalizedHeaders} headers - Normalized response headers as a lowercase object
	* @property {string} body - Response body as a base64url encoded string
	* @property {Object} [trailers] - Optional response trailers
	*/
	/**
	* @typedef {Object} SnapshotEntry
	* @property {SnapshotEntryRequest} request - The request object
	* @property {Array<SnapshotEntryResponse>} responses - Array of response objects
	* @property {number} callCount - Number of times this snapshot has been called
	* @property {string} timestamp - ISO timestamp of when the snapshot was created
	*/
	/**
	* @typedef {Object} SnapshotRecorderMatchOptions
	* @property {Array<string>} [matchHeaders=[]] - Headers to match (empty array means match all headers)
	* @property {Array<string>} [ignoreHeaders=[]] - Headers to ignore for matching
	* @property {Array<string>} [excludeHeaders=[]] - Headers to exclude from matching
	* @property {boolean} [matchBody=true] - Whether to match request body
	* @property {boolean} [matchQuery=true] - Whether to match query properties
	* @property {boolean} [caseSensitive=false] - Whether header matching is case-sensitive
	*/
	/**
	* @typedef {Object} SnapshotRecorderOptions
	* @property {string} [snapshotPath] - Path to save/load snapshots
	* @property {import('./snapshot-utils').SnapshotMode} [mode='record'] - Mode: 'record' or 'playback'
	* @property {number} [maxSnapshots=Infinity] - Maximum number of snapshots to keep
	* @property {boolean} [autoFlush=false] - Whether to automatically flush snapshots to disk
	* @property {number} [flushInterval=30000] - Auto-flush interval in milliseconds (default: 30 seconds)
	* @property {Array<string|RegExp>} [excludeUrls=[]] - URLs to exclude from recording
	* @property {function} [shouldRecord=null] - Function to filter requests for recording
	* @property {function} [shouldPlayback=null] - Function to filter requests
	*/
	/**
	* @typedef {Object} SnapshotFormattedRequest
	* @property {string} method - HTTP method (e.g. 'GET', 'POST', etc.)
	* @property {string} url - Full URL of the request (with query parameters if matchQuery is true)
	* @property {import('./snapshot-utils').NormalizedHeaders} headers - Normalized headers as a lowercase object
	* @property {string} body - Request body (optional, only if matchBody is true)
	*/
	/**
	* @typedef {Object} SnapshotInfo
	* @property {string} hash - Hash key for the snapshot
	* @property {SnapshotEntryRequest} request - The request object
	* @property {number} responseCount - Number of responses recorded for this request
	* @property {number} callCount - Number of times this snapshot has been called
	* @property {string} timestamp - ISO timestamp of when the snapshot was created
	*/
	/**
	* Formats a request for consistent snapshot storage
	* Caches normalized headers to avoid repeated processing
	*
	* @param {SnapshotRequestOptions} opts - Request options
	* @param {import('./snapshot-utils').HeaderFilters} headerFilters - Cached header sets for performance
	* @param {SnapshotRecorderMatchOptions} [matchOptions] - Matching options for headers and body
	* @returns {SnapshotFormattedRequest} - Formatted request object
	*/
	function formatRequestKey(opts, headerFilters, matchOptions = {}) {
		const url = new URL(opts.path, opts.origin);
		const normalized = opts._normalizedHeaders || normalizeHeaders$2(opts.headers);
		if (!opts._normalizedHeaders) opts._normalizedHeaders = normalized;
		return {
			method: opts.method || "GET",
			url: matchOptions.matchQuery !== false ? url.toString() : `${url.origin}${url.pathname}`,
			headers: filterHeadersForMatching(normalized, headerFilters, matchOptions),
			body: matchOptions.matchBody !== false && opts.body ? String(opts.body) : ""
		};
	}
	/**
	* Filters headers based on matching configuration
	*
	* @param {import('./snapshot-utils').Headers} headers - Headers to filter
	* @param {import('./snapshot-utils').HeaderFilters} headerFilters - Cached sets for ignore, exclude, and match headers
	* @param {SnapshotRecorderMatchOptions} [matchOptions] - Matching options for headers
	*/
	function filterHeadersForMatching(headers, headerFilters, matchOptions = {}) {
		if (!headers || typeof headers !== "object") return {};
		const { caseSensitive = false } = matchOptions;
		const filtered = {};
		const { ignore, exclude, match } = headerFilters;
		for (const [key, value] of Object.entries(headers)) {
			const headerKey = caseSensitive ? key : key.toLowerCase();
			if (exclude.has(headerKey)) continue;
			if (ignore.has(headerKey)) continue;
			if (match.size !== 0) {
				if (!match.has(headerKey)) continue;
			}
			filtered[headerKey] = value;
		}
		return filtered;
	}
	/**
	* Filters headers for storage (only excludes sensitive headers)
	*
	* @param {import('./snapshot-utils').Headers} headers - Headers to filter
	* @param {import('./snapshot-utils').HeaderFilters} headerFilters - Cached sets for ignore, exclude, and match headers
	* @param {SnapshotRecorderMatchOptions} [matchOptions] - Matching options for headers
	*/
	function filterHeadersForStorage(headers, headerFilters, matchOptions = {}) {
		if (!headers || typeof headers !== "object") return {};
		const { caseSensitive = false } = matchOptions;
		const filtered = {};
		const { exclude: excludeSet } = headerFilters;
		for (const [key, value] of Object.entries(headers)) {
			const headerKey = caseSensitive ? key : key.toLowerCase();
			if (excludeSet.has(headerKey)) continue;
			filtered[headerKey] = value;
		}
		return filtered;
	}
	/**
	* Creates a hash key for request matching
	* Properly orders headers to avoid conflicts and uses crypto hashing when available
	*
	* @param {SnapshotFormattedRequest} formattedRequest - Request object
	* @returns {string} - Base64url encoded hash of the request
	*/
	function createRequestHash(formattedRequest) {
		const parts = [formattedRequest.method, formattedRequest.url];
		if (formattedRequest.headers && typeof formattedRequest.headers === "object") {
			const headerKeys = Object.keys(formattedRequest.headers).sort();
			for (const key of headerKeys) {
				const values = Array.isArray(formattedRequest.headers[key]) ? formattedRequest.headers[key] : [formattedRequest.headers[key]];
				parts.push(key);
				for (const value of values.sort()) parts.push(String(value));
			}
		}
		parts.push(formattedRequest.body);
		const content = parts.join("|");
		return hashId(content);
	}
	var SnapshotRecorder$1 = class {
		/** @type {NodeJS.Timeout | null} */
		#flushTimeout;
		/** @type {import('./snapshot-utils').IsUrlExcluded} */
		#isUrlExcluded;
		/** @type {Map<string, SnapshotEntry>} */
		#snapshots = /* @__PURE__ */ new Map();
		/** @type {string|undefined} */
		#snapshotPath;
		/** @type {number} */
		#maxSnapshots = Infinity;
		/** @type {boolean} */
		#autoFlush = false;
		/** @type {import('./snapshot-utils').HeaderFilters} */
		#headerFilters;
		/**
		* Creates a new SnapshotRecorder instance
		* @param {SnapshotRecorderOptions&SnapshotRecorderMatchOptions} [options={}] - Configuration options for the recorder
		*/
		constructor(options = {}) {
			this.#snapshotPath = options.snapshotPath;
			this.#maxSnapshots = options.maxSnapshots || Infinity;
			this.#autoFlush = options.autoFlush || false;
			this.flushInterval = options.flushInterval || 3e4;
			this._flushTimer = null;
			/** @type {Required<SnapshotRecorderMatchOptions>} */
			this.matchOptions = {
				matchHeaders: options.matchHeaders || [],
				ignoreHeaders: options.ignoreHeaders || [],
				excludeHeaders: options.excludeHeaders || [],
				matchBody: options.matchBody !== false,
				matchQuery: options.matchQuery !== false,
				caseSensitive: options.caseSensitive || false
			};
			this.#headerFilters = createHeaderFilters(this.matchOptions);
			this.shouldRecord = options.shouldRecord || (() => true);
			this.shouldPlayback = options.shouldPlayback || (() => true);
			this.#isUrlExcluded = isUrlExcludedFactory(options.excludeUrls);
			if (this.#autoFlush && this.#snapshotPath) this.#startAutoFlush();
		}
		/**
		* Records a request-response interaction
		* @param {SnapshotRequestOptions} requestOpts - Request options
		* @param {SnapshotEntryResponse} response - Response data to record
		* @return {Promise<void>} - Resolves when the recording is complete
		*/
		async record(requestOpts, response) {
			if (!this.shouldRecord(requestOpts)) return;
			const url = new URL(requestOpts.path, requestOpts.origin).toString();
			if (this.#isUrlExcluded(url)) return;
			const request$2 = formatRequestKey(requestOpts, this.#headerFilters, this.matchOptions);
			const hash = createRequestHash(request$2);
			const normalizedHeaders = normalizeHeaders$2(response.headers);
			/** @type {SnapshotEntryResponse} */
			const responseData = {
				statusCode: response.statusCode,
				headers: filterHeadersForStorage(normalizedHeaders, this.#headerFilters, this.matchOptions),
				body: Buffer.isBuffer(response.body) ? response.body.toString("base64") : Buffer.from(String(response.body || "")).toString("base64"),
				trailers: response.trailers
			};
			if (this.#snapshots.size >= this.#maxSnapshots && !this.#snapshots.has(hash)) {
				const oldestKey = this.#snapshots.keys().next().value;
				this.#snapshots.delete(oldestKey);
			}
			const existingSnapshot = this.#snapshots.get(hash);
			if (existingSnapshot && existingSnapshot.responses) {
				existingSnapshot.responses.push(responseData);
				existingSnapshot.timestamp = (/* @__PURE__ */ new Date()).toISOString();
			} else this.#snapshots.set(hash, {
				request: request$2,
				responses: [responseData],
				callCount: 0,
				timestamp: (/* @__PURE__ */ new Date()).toISOString()
			});
			if (this.#autoFlush && this.#snapshotPath) this.#scheduleFlush();
		}
		/**
		* Finds a matching snapshot for the given request
		* Returns the appropriate response based on call count for sequential responses
		*
		* @param {SnapshotRequestOptions} requestOpts - Request options to match
		* @returns {SnapshotEntry&Record<'response', SnapshotEntryResponse>|undefined} - Matching snapshot response or undefined if not found
		*/
		findSnapshot(requestOpts) {
			if (!this.shouldPlayback(requestOpts)) return;
			const url = new URL(requestOpts.path, requestOpts.origin).toString();
			if (this.#isUrlExcluded(url)) return;
			const request$2 = formatRequestKey(requestOpts, this.#headerFilters, this.matchOptions);
			const hash = createRequestHash(request$2);
			const snapshot = this.#snapshots.get(hash);
			if (!snapshot) return void 0;
			const currentCallCount = snapshot.callCount || 0;
			const responseIndex = Math.min(currentCallCount, snapshot.responses.length - 1);
			snapshot.callCount = currentCallCount + 1;
			return {
				...snapshot,
				response: snapshot.responses[responseIndex]
			};
		}
		/**
		* Loads snapshots from file
		* @param {string} [filePath] - Optional file path to load snapshots from
		* @return {Promise<void>} - Resolves when snapshots are loaded
		*/
		async loadSnapshots(filePath) {
			const path$1 = filePath || this.#snapshotPath;
			if (!path$1) throw new InvalidArgumentError$6("Snapshot path is required");
			try {
				const data = await readFile(resolve(path$1), "utf8");
				const parsed = JSON.parse(data);
				if (Array.isArray(parsed)) {
					this.#snapshots.clear();
					for (const { hash, snapshot } of parsed) this.#snapshots.set(hash, snapshot);
				} else this.#snapshots = new Map(Object.entries(parsed));
			} catch (error) {
				if (error.code === "ENOENT") this.#snapshots.clear();
				else throw new UndiciError$1(`Failed to load snapshots from ${path$1}`, { cause: error });
			}
		}
		/**
		* Saves snapshots to file
		*
		* @param {string} [filePath] - Optional file path to save snapshots
		* @returns {Promise<void>} - Resolves when snapshots are saved
		*/
		async saveSnapshots(filePath) {
			const path$1 = filePath || this.#snapshotPath;
			if (!path$1) throw new InvalidArgumentError$6("Snapshot path is required");
			const resolvedPath = resolve(path$1);
			await mkdir(dirname(resolvedPath), { recursive: true });
			const data = Array.from(this.#snapshots.entries()).map(([hash, snapshot]) => ({
				hash,
				snapshot
			}));
			await writeFile(resolvedPath, JSON.stringify(data, null, 2), { flush: true });
		}
		/**
		* Clears all recorded snapshots
		* @returns {void}
		*/
		clear() {
			this.#snapshots.clear();
		}
		/**
		* Gets all recorded snapshots
		* @return {Array<SnapshotEntry>} - Array of all recorded snapshots
		*/
		getSnapshots() {
			return Array.from(this.#snapshots.values());
		}
		/**
		* Gets snapshot count
		* @return {number} - Number of recorded snapshots
		*/
		size() {
			return this.#snapshots.size;
		}
		/**
		* Resets call counts for all snapshots (useful for test cleanup)
		* @returns {void}
		*/
		resetCallCounts() {
			for (const snapshot of this.#snapshots.values()) snapshot.callCount = 0;
		}
		/**
		* Deletes a specific snapshot by request options
		* @param {SnapshotRequestOptions} requestOpts - Request options to match
		* @returns {boolean} - True if snapshot was deleted, false if not found
		*/
		deleteSnapshot(requestOpts) {
			const request$2 = formatRequestKey(requestOpts, this.#headerFilters, this.matchOptions);
			const hash = createRequestHash(request$2);
			return this.#snapshots.delete(hash);
		}
		/**
		* Gets information about a specific snapshot
		* @param {SnapshotRequestOptions} requestOpts - Request options to match
		* @returns {SnapshotInfo|null} - Snapshot information or null if not found
		*/
		getSnapshotInfo(requestOpts) {
			const request$2 = formatRequestKey(requestOpts, this.#headerFilters, this.matchOptions);
			const hash = createRequestHash(request$2);
			const snapshot = this.#snapshots.get(hash);
			if (!snapshot) return null;
			return {
				hash,
				request: snapshot.request,
				responseCount: snapshot.responses ? snapshot.responses.length : snapshot.response ? 1 : 0,
				callCount: snapshot.callCount || 0,
				timestamp: snapshot.timestamp
			};
		}
		/**
		* Replaces all snapshots with new data (full replacement)
		* @param {Array<{hash: string; snapshot: SnapshotEntry}>|Record<string, SnapshotEntry>} snapshotData - New snapshot data to replace existing ones
		* @returns {void}
		*/
		replaceSnapshots(snapshotData) {
			this.#snapshots.clear();
			if (Array.isArray(snapshotData)) for (const { hash, snapshot } of snapshotData) this.#snapshots.set(hash, snapshot);
			else if (snapshotData && typeof snapshotData === "object") this.#snapshots = new Map(Object.entries(snapshotData));
		}
		/**
		* Starts the auto-flush timer
		* @returns {void}
		*/
		#startAutoFlush() {
			return this.#scheduleFlush();
		}
		/**
		* Stops the auto-flush timer
		* @returns {void}
		*/
		#stopAutoFlush() {
			if (this.#flushTimeout) {
				clearTimeout$1(this.#flushTimeout);
				this.saveSnapshots().catch(() => {});
				this.#flushTimeout = null;
			}
		}
		/**
		* Schedules a flush (debounced to avoid excessive writes)
		*/
		#scheduleFlush() {
			this.#flushTimeout = setTimeout$1(() => {
				this.saveSnapshots().catch(() => {});
				if (this.#autoFlush) this.#flushTimeout?.refresh();
				else this.#flushTimeout = null;
			}, 1e3);
		}
		/**
		* Cleanup method to stop timers
		* @returns {void}
		*/
		destroy() {
			this.#stopAutoFlush();
			if (this.#flushTimeout) {
				clearTimeout$1(this.#flushTimeout);
				this.#flushTimeout = null;
			}
		}
		/**
		* Async close method that saves all recordings and performs cleanup
		* @returns {Promise<void>}
		*/
		async close() {
			if (this.#snapshotPath && this.#snapshots.size !== 0) await this.saveSnapshots();
			this.destroy();
		}
	};
	module.exports = {
		SnapshotRecorder: SnapshotRecorder$1,
		formatRequestKey,
		createRequestHash,
		filterHeadersForMatching,
		filterHeadersForStorage,
		createHeaderFilters
	};
}) });

//#endregion
//#region node_modules/undici/lib/mock/snapshot-agent.js
var require_snapshot_agent = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/mock/snapshot-agent.js": ((exports, module) => {
	const Agent$2 = require_agent();
	const MockAgent$1 = require_mock_agent();
	const { SnapshotRecorder } = require_snapshot_recorder();
	const WrapHandler$1 = require_wrap_handler();
	const { InvalidArgumentError: InvalidArgumentError$5, UndiciError } = require_errors();
	const { validateSnapshotMode } = require_snapshot_utils();
	const kSnapshotRecorder = Symbol("kSnapshotRecorder");
	const kSnapshotMode = Symbol("kSnapshotMode");
	const kSnapshotPath = Symbol("kSnapshotPath");
	const kSnapshotLoaded = Symbol("kSnapshotLoaded");
	const kRealAgent = Symbol("kRealAgent");
	let warningEmitted$1 = false;
	var SnapshotAgent$1 = class extends MockAgent$1 {
		constructor(opts = {}) {
			if (!warningEmitted$1) {
				process.emitWarning("SnapshotAgent is experimental and subject to change", "ExperimentalWarning");
				warningEmitted$1 = true;
			}
			const { mode = "record", snapshotPath = null,...mockAgentOpts } = opts;
			super(mockAgentOpts);
			validateSnapshotMode(mode);
			if ((mode === "playback" || mode === "update") && !snapshotPath) throw new InvalidArgumentError$5(`snapshotPath is required when mode is '${mode}'`);
			this[kSnapshotMode] = mode;
			this[kSnapshotPath] = snapshotPath;
			this[kSnapshotRecorder] = new SnapshotRecorder({
				snapshotPath: this[kSnapshotPath],
				mode: this[kSnapshotMode],
				maxSnapshots: opts.maxSnapshots,
				autoFlush: opts.autoFlush,
				flushInterval: opts.flushInterval,
				matchHeaders: opts.matchHeaders,
				ignoreHeaders: opts.ignoreHeaders,
				excludeHeaders: opts.excludeHeaders,
				matchBody: opts.matchBody,
				matchQuery: opts.matchQuery,
				caseSensitive: opts.caseSensitive,
				shouldRecord: opts.shouldRecord,
				shouldPlayback: opts.shouldPlayback,
				excludeUrls: opts.excludeUrls
			});
			this[kSnapshotLoaded] = false;
			if (this[kSnapshotMode] === "record" || this[kSnapshotMode] === "update") this[kRealAgent] = new Agent$2(opts);
			if ((this[kSnapshotMode] === "playback" || this[kSnapshotMode] === "update") && this[kSnapshotPath]) this.loadSnapshots().catch(() => {});
		}
		dispatch(opts, handler) {
			handler = WrapHandler$1.wrap(handler);
			const mode = this[kSnapshotMode];
			if (mode === "playback" || mode === "update") {
				if (!this[kSnapshotLoaded]) return this.#asyncDispatch(opts, handler);
				const snapshot = this[kSnapshotRecorder].findSnapshot(opts);
				if (snapshot) return this.#replaySnapshot(snapshot, handler);
				else if (mode === "update") return this.#recordAndReplay(opts, handler);
				else {
					const error = new UndiciError(`No snapshot found for ${opts.method || "GET"} ${opts.path}`);
					if (handler.onError) {
						handler.onError(error);
						return;
					}
					throw error;
				}
			} else if (mode === "record") return this.#recordAndReplay(opts, handler);
		}
		/**
		* Async version of dispatch for when we need to load snapshots first
		*/
		async #asyncDispatch(opts, handler) {
			await this.loadSnapshots();
			return this.dispatch(opts, handler);
		}
		/**
		* Records a real request and replays the response
		*/
		#recordAndReplay(opts, handler) {
			const responseData = {
				statusCode: null,
				headers: {},
				trailers: {},
				body: []
			};
			const self = this;
			return this[kRealAgent].dispatch(opts, {
				onRequestStart(controller, context) {
					return handler.onRequestStart(controller, {
						...context,
						history: this.history
					});
				},
				onRequestUpgrade(controller, statusCode, headers, socket) {
					return handler.onRequestUpgrade(controller, statusCode, headers, socket);
				},
				onResponseStart(controller, statusCode, headers, statusMessage) {
					responseData.statusCode = statusCode;
					responseData.headers = headers;
					return handler.onResponseStart(controller, statusCode, headers, statusMessage);
				},
				onResponseData(controller, chunk) {
					responseData.body.push(chunk);
					return handler.onResponseData(controller, chunk);
				},
				onResponseEnd(controller, trailers) {
					responseData.trailers = trailers;
					const responseBody = Buffer.concat(responseData.body);
					self[kSnapshotRecorder].record(opts, {
						statusCode: responseData.statusCode,
						headers: responseData.headers,
						body: responseBody,
						trailers: responseData.trailers
					}).then(() => {
						handler.onResponseEnd(controller, trailers);
					}).catch((error) => {
						handler.onResponseError(controller, error);
					});
				}
			});
		}
		/**
		* Replays a recorded response
		*
		* @param {Object} snapshot - The recorded snapshot to replay.
		* @param {Object} handler - The handler to call with the response data.
		* @returns {void}
		*/
		#replaySnapshot(snapshot, handler) {
			try {
				const { response } = snapshot;
				const controller = {
					pause() {},
					resume() {},
					abort(reason) {
						this.aborted = true;
						this.reason = reason;
					},
					aborted: false,
					paused: false
				};
				handler.onRequestStart(controller);
				handler.onResponseStart(controller, response.statusCode, response.headers);
				const body = Buffer.from(response.body, "base64");
				handler.onResponseData(controller, body);
				handler.onResponseEnd(controller, response.trailers);
			} catch (error) {
				handler.onError?.(error);
			}
		}
		/**
		* Loads snapshots from file
		*
		* @param {string} [filePath] - Optional file path to load snapshots from.
		* @returns {Promise<void>} - Resolves when snapshots are loaded.
		*/
		async loadSnapshots(filePath) {
			await this[kSnapshotRecorder].loadSnapshots(filePath || this[kSnapshotPath]);
			this[kSnapshotLoaded] = true;
			if (this[kSnapshotMode] === "playback") this.#setupMockInterceptors();
		}
		/**
		* Saves snapshots to file
		*
		* @param {string} [filePath] - Optional file path to save snapshots to.
		* @returns {Promise<void>} - Resolves when snapshots are saved.
		*/
		async saveSnapshots(filePath) {
			return this[kSnapshotRecorder].saveSnapshots(filePath || this[kSnapshotPath]);
		}
		/**
		* Sets up MockAgent interceptors based on recorded snapshots.
		*
		* This method creates MockAgent interceptors for each recorded snapshot,
		* allowing the SnapshotAgent to fall back to MockAgent's standard intercept
		* mechanism in playback mode. Each interceptor is configured to persist
		* (remain active for multiple requests) and responds with the recorded
		* response data.
		*
		* Called automatically when loading snapshots in playback mode.
		*
		* @returns {void}
		*/
		#setupMockInterceptors() {
			for (const snapshot of this[kSnapshotRecorder].getSnapshots()) {
				const { request: request$2, responses, response } = snapshot;
				const url = new URL(request$2.url);
				const mockPool = this.get(url.origin);
				const responseData = responses ? responses[0] : response;
				if (!responseData) continue;
				mockPool.intercept({
					path: url.pathname + url.search,
					method: request$2.method,
					headers: request$2.headers,
					body: request$2.body
				}).reply(responseData.statusCode, responseData.body, {
					headers: responseData.headers,
					trailers: responseData.trailers
				}).persist();
			}
		}
		/**
		* Gets the snapshot recorder
		* @return {SnapshotRecorder} - The snapshot recorder instance
		*/
		getRecorder() {
			return this[kSnapshotRecorder];
		}
		/**
		* Gets the current mode
		* @return {import('./snapshot-utils').SnapshotMode} - The current snapshot mode
		*/
		getMode() {
			return this[kSnapshotMode];
		}
		/**
		* Clears all snapshots
		* @returns {void}
		*/
		clearSnapshots() {
			this[kSnapshotRecorder].clear();
		}
		/**
		* Resets call counts for all snapshots (useful for test cleanup)
		* @returns {void}
		*/
		resetCallCounts() {
			this[kSnapshotRecorder].resetCallCounts();
		}
		/**
		* Deletes a specific snapshot by request options
		* @param {import('./snapshot-recorder').SnapshotRequestOptions} requestOpts - Request options to identify the snapshot
		* @return {Promise<boolean>} - Returns true if the snapshot was deleted, false if not found
		*/
		deleteSnapshot(requestOpts) {
			return this[kSnapshotRecorder].deleteSnapshot(requestOpts);
		}
		/**
		* Gets information about a specific snapshot
		* @returns {import('./snapshot-recorder').SnapshotInfo|null} - Snapshot information or null if not found
		*/
		getSnapshotInfo(requestOpts) {
			return this[kSnapshotRecorder].getSnapshotInfo(requestOpts);
		}
		/**
		* Replaces all snapshots with new data (full replacement)
		* @param {Array<{hash: string; snapshot: import('./snapshot-recorder').SnapshotEntryshotEntry}>|Record<string, import('./snapshot-recorder').SnapshotEntry>} snapshotData - New snapshot data to replace existing snapshots
		* @returns {void}
		*/
		replaceSnapshots(snapshotData) {
			this[kSnapshotRecorder].replaceSnapshots(snapshotData);
		}
		/**
		* Closes the agent, saving snapshots and cleaning up resources.
		*
		* @returns {Promise<void>}
		*/
		async close() {
			await this[kSnapshotRecorder].close();
			await this[kRealAgent]?.close();
			await super.close();
		}
	};
	module.exports = SnapshotAgent$1;
}) });

//#endregion
//#region node_modules/undici/lib/global.js
var require_global = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/global.js": ((exports, module) => {
	const globalDispatcher = Symbol.for("undici.globalDispatcher.1");
	const { InvalidArgumentError: InvalidArgumentError$4 } = require_errors();
	const Agent$1 = require_agent();
	if (getGlobalDispatcher$3() === void 0) setGlobalDispatcher$1(new Agent$1());
	function setGlobalDispatcher$1(agent) {
		if (!agent || typeof agent.dispatch !== "function") throw new InvalidArgumentError$4("Argument agent must implement Agent");
		Object.defineProperty(globalThis, globalDispatcher, {
			value: agent,
			writable: true,
			enumerable: false,
			configurable: false
		});
	}
	function getGlobalDispatcher$3() {
		return globalThis[globalDispatcher];
	}
	module.exports = {
		setGlobalDispatcher: setGlobalDispatcher$1,
		getGlobalDispatcher: getGlobalDispatcher$3
	};
}) });

//#endregion
//#region node_modules/undici/lib/handler/decorator-handler.js
var require_decorator_handler = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/handler/decorator-handler.js": ((exports, module) => {
	const assert$13 = __require("node:assert");
	const WrapHandler = require_wrap_handler();
	/**
	* @deprecated
	*/
	module.exports = class DecoratorHandler$5 {
		#handler;
		#onCompleteCalled = false;
		#onErrorCalled = false;
		#onResponseStartCalled = false;
		constructor(handler) {
			if (typeof handler !== "object" || handler === null) throw new TypeError("handler must be an object");
			this.#handler = WrapHandler.wrap(handler);
		}
		onRequestStart(...args) {
			this.#handler.onRequestStart?.(...args);
		}
		onRequestUpgrade(...args) {
			assert$13(!this.#onCompleteCalled);
			assert$13(!this.#onErrorCalled);
			return this.#handler.onRequestUpgrade?.(...args);
		}
		onResponseStart(...args) {
			assert$13(!this.#onCompleteCalled);
			assert$13(!this.#onErrorCalled);
			assert$13(!this.#onResponseStartCalled);
			this.#onResponseStartCalled = true;
			return this.#handler.onResponseStart?.(...args);
		}
		onResponseData(...args) {
			assert$13(!this.#onCompleteCalled);
			assert$13(!this.#onErrorCalled);
			return this.#handler.onResponseData?.(...args);
		}
		onResponseEnd(...args) {
			assert$13(!this.#onCompleteCalled);
			assert$13(!this.#onErrorCalled);
			this.#onCompleteCalled = true;
			return this.#handler.onResponseEnd?.(...args);
		}
		onResponseError(...args) {
			this.#onErrorCalled = true;
			return this.#handler.onResponseError?.(...args);
		}
		/**
		* @deprecated
		*/
		onBodySent() {}
	};
}) });

//#endregion
//#region node_modules/undici/lib/handler/redirect-handler.js
var require_redirect_handler = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/handler/redirect-handler.js": ((exports, module) => {
	const util$6 = require_util$5();
	const { kBodyUsed } = require_symbols();
	const assert$12 = __require("node:assert");
	const { InvalidArgumentError: InvalidArgumentError$3 } = require_errors();
	const EE$1 = __require("node:events");
	const redirectableStatusCodes = [
		300,
		301,
		302,
		303,
		307,
		308
	];
	const kBody = Symbol("body");
	const noop$1 = () => {};
	var BodyAsyncIterable = class {
		constructor(body) {
			this[kBody] = body;
			this[kBodyUsed] = false;
		}
		async *[Symbol.asyncIterator]() {
			assert$12(!this[kBodyUsed], "disturbed");
			this[kBodyUsed] = true;
			yield* this[kBody];
		}
	};
	var RedirectHandler$2 = class RedirectHandler$2 {
		static buildDispatch(dispatcher, maxRedirections) {
			if (maxRedirections != null && (!Number.isInteger(maxRedirections) || maxRedirections < 0)) throw new InvalidArgumentError$3("maxRedirections must be a positive number");
			const dispatch = dispatcher.dispatch.bind(dispatcher);
			return (opts, originalHandler) => dispatch(opts, new RedirectHandler$2(dispatch, maxRedirections, opts, originalHandler));
		}
		constructor(dispatch, maxRedirections, opts, handler) {
			if (maxRedirections != null && (!Number.isInteger(maxRedirections) || maxRedirections < 0)) throw new InvalidArgumentError$3("maxRedirections must be a positive number");
			this.dispatch = dispatch;
			this.location = null;
			const { maxRedirections: _,...cleanOpts } = opts;
			this.opts = cleanOpts;
			this.maxRedirections = maxRedirections;
			this.handler = handler;
			this.history = [];
			if (util$6.isStream(this.opts.body)) {
				if (util$6.bodyLength(this.opts.body) === 0) this.opts.body.on("data", function() {
					assert$12(false);
				});
				if (typeof this.opts.body.readableDidRead !== "boolean") {
					this.opts.body[kBodyUsed] = false;
					EE$1.prototype.on.call(this.opts.body, "data", function() {
						this[kBodyUsed] = true;
					});
				}
			} else if (this.opts.body && typeof this.opts.body.pipeTo === "function") this.opts.body = new BodyAsyncIterable(this.opts.body);
			else if (this.opts.body && typeof this.opts.body !== "string" && !ArrayBuffer.isView(this.opts.body) && util$6.isIterable(this.opts.body) && !util$6.isFormDataLike(this.opts.body)) this.opts.body = new BodyAsyncIterable(this.opts.body);
		}
		onRequestStart(controller, context) {
			this.handler.onRequestStart?.(controller, {
				...context,
				history: this.history
			});
		}
		onRequestUpgrade(controller, statusCode, headers, socket) {
			this.handler.onRequestUpgrade?.(controller, statusCode, headers, socket);
		}
		onResponseStart(controller, statusCode, headers, statusMessage) {
			if (this.opts.throwOnMaxRedirect && this.history.length >= this.maxRedirections) throw new Error("max redirects");
			if ((statusCode === 301 || statusCode === 302) && this.opts.method === "POST") {
				this.opts.method = "GET";
				if (util$6.isStream(this.opts.body)) util$6.destroy(this.opts.body.on("error", noop$1));
				this.opts.body = null;
			}
			if (statusCode === 303 && this.opts.method !== "HEAD") {
				this.opts.method = "GET";
				if (util$6.isStream(this.opts.body)) util$6.destroy(this.opts.body.on("error", noop$1));
				this.opts.body = null;
			}
			this.location = this.history.length >= this.maxRedirections || util$6.isDisturbed(this.opts.body) || redirectableStatusCodes.indexOf(statusCode) === -1 ? null : headers.location;
			if (this.opts.origin) this.history.push(new URL(this.opts.path, this.opts.origin));
			if (!this.location) {
				this.handler.onResponseStart?.(controller, statusCode, headers, statusMessage);
				return;
			}
			const { origin, pathname, search } = util$6.parseURL(new URL(this.location, this.opts.origin && new URL(this.opts.path, this.opts.origin)));
			const path$1 = search ? `${pathname}${search}` : pathname;
			const redirectUrlString = `${origin}${path$1}`;
			for (const historyUrl of this.history) if (historyUrl.toString() === redirectUrlString) throw new InvalidArgumentError$3(`Redirect loop detected. Cannot redirect to ${origin}. This typically happens when using a Client or Pool with cross-origin redirects. Use an Agent for cross-origin redirects.`);
			this.opts.headers = cleanRequestHeaders(this.opts.headers, statusCode === 303, this.opts.origin !== origin);
			this.opts.path = path$1;
			this.opts.origin = origin;
			this.opts.query = null;
		}
		onResponseData(controller, chunk) {
			if (this.location) {} else this.handler.onResponseData?.(controller, chunk);
		}
		onResponseEnd(controller, trailers) {
			if (this.location) this.dispatch(this.opts, this);
			else this.handler.onResponseEnd(controller, trailers);
		}
		onResponseError(controller, error) {
			this.handler.onResponseError?.(controller, error);
		}
	};
	function shouldRemoveHeader(header, removeContent, unknownOrigin) {
		if (header.length === 4) return util$6.headerNameToString(header) === "host";
		if (removeContent && util$6.headerNameToString(header).startsWith("content-")) return true;
		if (unknownOrigin && (header.length === 13 || header.length === 6 || header.length === 19)) {
			const name = util$6.headerNameToString(header);
			return name === "authorization" || name === "cookie" || name === "proxy-authorization";
		}
		return false;
	}
	function cleanRequestHeaders(headers, removeContent, unknownOrigin) {
		const ret = [];
		if (Array.isArray(headers)) {
			for (let i = 0; i < headers.length; i += 2) if (!shouldRemoveHeader(headers[i], removeContent, unknownOrigin)) ret.push(headers[i], headers[i + 1]);
		} else if (headers && typeof headers === "object") {
			const entries = typeof headers[Symbol.iterator] === "function" ? headers : Object.entries(headers);
			for (const [key, value] of entries) if (!shouldRemoveHeader(key, removeContent, unknownOrigin)) ret.push(key, value);
		} else assert$12(headers == null, "headers must be an object or an array");
		return ret;
	}
	module.exports = RedirectHandler$2;
}) });

//#endregion
//#region node_modules/undici/lib/interceptor/redirect.js
var require_redirect = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/interceptor/redirect.js": ((exports, module) => {
	const RedirectHandler$1 = require_redirect_handler();
	function createRedirectInterceptor({ maxRedirections: defaultMaxRedirections } = {}) {
		return (dispatch) => {
			return function Intercept(opts, handler) {
				const { maxRedirections = defaultMaxRedirections,...rest } = opts;
				if (maxRedirections == null || maxRedirections === 0) return dispatch(opts, handler);
				const dispatchOpts = { ...rest };
				const redirectHandler = new RedirectHandler$1(dispatch, maxRedirections, dispatchOpts, handler);
				return dispatch(dispatchOpts, redirectHandler);
			};
		};
	}
	module.exports = createRedirectInterceptor;
}) });

//#endregion
//#region node_modules/undici/lib/interceptor/response-error.js
var require_response_error = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/interceptor/response-error.js": ((exports, module) => {
	const DecoratorHandler$4 = require_decorator_handler();
	const { ResponseError } = require_errors();
	var ResponseErrorHandler = class extends DecoratorHandler$4 {
		#statusCode;
		#contentType;
		#decoder;
		#headers;
		#body;
		constructor(_opts, { handler }) {
			super(handler);
		}
		#checkContentType(contentType) {
			return (this.#contentType ?? "").indexOf(contentType) === 0;
		}
		onRequestStart(controller, context) {
			this.#statusCode = 0;
			this.#contentType = null;
			this.#decoder = null;
			this.#headers = null;
			this.#body = "";
			return super.onRequestStart(controller, context);
		}
		onResponseStart(controller, statusCode, headers, statusMessage) {
			this.#statusCode = statusCode;
			this.#headers = headers;
			this.#contentType = headers["content-type"];
			if (this.#statusCode < 400) return super.onResponseStart(controller, statusCode, headers, statusMessage);
			if (this.#checkContentType("application/json") || this.#checkContentType("text/plain")) this.#decoder = new TextDecoder("utf-8");
		}
		onResponseData(controller, chunk) {
			if (this.#statusCode < 400) return super.onResponseData(controller, chunk);
			this.#body += this.#decoder?.decode(chunk, { stream: true }) ?? "";
		}
		onResponseEnd(controller, trailers) {
			if (this.#statusCode >= 400) {
				this.#body += this.#decoder?.decode(void 0, { stream: false }) ?? "";
				if (this.#checkContentType("application/json")) try {
					this.#body = JSON.parse(this.#body);
				} catch {}
				let err;
				const stackTraceLimit = Error.stackTraceLimit;
				Error.stackTraceLimit = 0;
				try {
					err = new ResponseError("Response Error", this.#statusCode, {
						body: this.#body,
						headers: this.#headers
					});
				} finally {
					Error.stackTraceLimit = stackTraceLimit;
				}
				super.onResponseError(controller, err);
			} else super.onResponseEnd(controller, trailers);
		}
		onResponseError(controller, err) {
			super.onResponseError(controller, err);
		}
	};
	module.exports = () => {
		return (dispatch) => {
			return function Intercept(opts, handler) {
				return dispatch(opts, new ResponseErrorHandler(opts, { handler }));
			};
		};
	};
}) });

//#endregion
//#region node_modules/undici/lib/interceptor/retry.js
var require_retry = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/interceptor/retry.js": ((exports, module) => {
	const RetryHandler$1 = require_retry_handler();
	module.exports = (globalOpts) => {
		return (dispatch) => {
			return function retryInterceptor(opts, handler) {
				return dispatch(opts, new RetryHandler$1({
					...opts,
					retryOptions: {
						...globalOpts,
						...opts.retryOptions
					}
				}, {
					handler,
					dispatch
				}));
			};
		};
	};
}) });

//#endregion
//#region node_modules/undici/lib/interceptor/dump.js
var require_dump = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/interceptor/dump.js": ((exports, module) => {
	const { InvalidArgumentError: InvalidArgumentError$2, RequestAbortedError } = require_errors();
	const DecoratorHandler$3 = require_decorator_handler();
	var DumpHandler = class extends DecoratorHandler$3 {
		#maxSize = 1024 * 1024;
		#dumped = false;
		#size = 0;
		#controller = null;
		aborted = false;
		reason = false;
		constructor({ maxSize, signal }, handler) {
			if (maxSize != null && (!Number.isFinite(maxSize) || maxSize < 1)) throw new InvalidArgumentError$2("maxSize must be a number greater than 0");
			super(handler);
			this.#maxSize = maxSize ?? this.#maxSize;
		}
		#abort(reason) {
			this.aborted = true;
			this.reason = reason;
		}
		onRequestStart(controller, context) {
			controller.abort = this.#abort.bind(this);
			this.#controller = controller;
			return super.onRequestStart(controller, context);
		}
		onResponseStart(controller, statusCode, headers, statusMessage) {
			const contentLength = headers["content-length"];
			if (contentLength != null && contentLength > this.#maxSize) throw new RequestAbortedError(`Response size (${contentLength}) larger than maxSize (${this.#maxSize})`);
			if (this.aborted === true) return true;
			return super.onResponseStart(controller, statusCode, headers, statusMessage);
		}
		onResponseError(controller, err) {
			if (this.#dumped) return;
			err = this.#controller?.reason ?? err;
			super.onResponseError(controller, err);
		}
		onResponseData(controller, chunk) {
			this.#size = this.#size + chunk.length;
			if (this.#size >= this.#maxSize) {
				this.#dumped = true;
				if (this.aborted === true) super.onResponseError(controller, this.reason);
				else super.onResponseEnd(controller, {});
			}
			return true;
		}
		onResponseEnd(controller, trailers) {
			if (this.#dumped) return;
			if (this.#controller.aborted === true) {
				super.onResponseError(controller, this.reason);
				return;
			}
			super.onResponseEnd(controller, trailers);
		}
	};
	function createDumpInterceptor({ maxSize: defaultMaxSize } = { maxSize: 1024 * 1024 }) {
		return (dispatch) => {
			return function Intercept(opts, handler) {
				const { dumpMaxSize = defaultMaxSize } = opts;
				const dumpHandler = new DumpHandler({
					maxSize: dumpMaxSize,
					signal: opts.signal
				}, handler);
				return dispatch(opts, dumpHandler);
			};
		};
	}
	module.exports = createDumpInterceptor;
}) });

//#endregion
//#region node_modules/undici/lib/interceptor/dns.js
var require_dns = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/interceptor/dns.js": ((exports, module) => {
	const { isIP } = __require("node:net");
	const { lookup } = __require("node:dns");
	const DecoratorHandler$2 = require_decorator_handler();
	const { InvalidArgumentError: InvalidArgumentError$1, InformationalError } = require_errors();
	const maxInt = Math.pow(2, 31) - 1;
	var DNSInstance = class {
		#maxTTL = 0;
		#maxItems = 0;
		#records = /* @__PURE__ */ new Map();
		dualStack = true;
		affinity = null;
		lookup = null;
		pick = null;
		constructor(opts) {
			this.#maxTTL = opts.maxTTL;
			this.#maxItems = opts.maxItems;
			this.dualStack = opts.dualStack;
			this.affinity = opts.affinity;
			this.lookup = opts.lookup ?? this.#defaultLookup;
			this.pick = opts.pick ?? this.#defaultPick;
		}
		get full() {
			return this.#records.size === this.#maxItems;
		}
		runLookup(origin, opts, cb) {
			const ips = this.#records.get(origin.hostname);
			if (ips == null && this.full) {
				cb(null, origin);
				return;
			}
			const newOpts = {
				affinity: this.affinity,
				dualStack: this.dualStack,
				lookup: this.lookup,
				pick: this.pick,
				...opts.dns,
				maxTTL: this.#maxTTL,
				maxItems: this.#maxItems
			};
			if (ips == null) this.lookup(origin, newOpts, (err, addresses) => {
				if (err || addresses == null || addresses.length === 0) {
					cb(err ?? new InformationalError("No DNS entries found"));
					return;
				}
				this.setRecords(origin, addresses);
				const records = this.#records.get(origin.hostname);
				const ip = this.pick(origin, records, newOpts.affinity);
				let port$1;
				if (typeof ip.port === "number") port$1 = `:${ip.port}`;
				else if (origin.port !== "") port$1 = `:${origin.port}`;
				else port$1 = "";
				cb(null, new URL(`${origin.protocol}//${ip.family === 6 ? `[${ip.address}]` : ip.address}${port$1}`));
			});
			else {
				const ip = this.pick(origin, ips, newOpts.affinity);
				if (ip == null) {
					this.#records.delete(origin.hostname);
					this.runLookup(origin, opts, cb);
					return;
				}
				let port$1;
				if (typeof ip.port === "number") port$1 = `:${ip.port}`;
				else if (origin.port !== "") port$1 = `:${origin.port}`;
				else port$1 = "";
				cb(null, new URL(`${origin.protocol}//${ip.family === 6 ? `[${ip.address}]` : ip.address}${port$1}`));
			}
		}
		#defaultLookup(origin, opts, cb) {
			lookup(origin.hostname, {
				all: true,
				family: this.dualStack === false ? this.affinity : 0,
				order: "ipv4first"
			}, (err, addresses) => {
				if (err) return cb(err);
				const results = /* @__PURE__ */ new Map();
				for (const addr of addresses) results.set(`${addr.address}:${addr.family}`, addr);
				cb(null, results.values());
			});
		}
		#defaultPick(origin, hostnameRecords, affinity) {
			let ip = null;
			const { records, offset } = hostnameRecords;
			let family;
			if (this.dualStack) {
				if (affinity == null) if (offset == null || offset === maxInt) {
					hostnameRecords.offset = 0;
					affinity = 4;
				} else {
					hostnameRecords.offset++;
					affinity = (hostnameRecords.offset & 1) === 1 ? 6 : 4;
				}
				if (records[affinity] != null && records[affinity].ips.length > 0) family = records[affinity];
				else family = records[affinity === 4 ? 6 : 4];
			} else family = records[affinity];
			if (family == null || family.ips.length === 0) return ip;
			if (family.offset == null || family.offset === maxInt) family.offset = 0;
			else family.offset++;
			const position = family.offset % family.ips.length;
			ip = family.ips[position] ?? null;
			if (ip == null) return ip;
			if (Date.now() - ip.timestamp > ip.ttl) {
				family.ips.splice(position, 1);
				return this.pick(origin, hostnameRecords, affinity);
			}
			return ip;
		}
		pickFamily(origin, ipFamily) {
			const records = this.#records.get(origin.hostname)?.records;
			if (!records) return null;
			const family = records[ipFamily];
			if (!family) return null;
			if (family.offset == null || family.offset === maxInt) family.offset = 0;
			else family.offset++;
			const position = family.offset % family.ips.length;
			const ip = family.ips[position] ?? null;
			if (ip == null) return ip;
			if (Date.now() - ip.timestamp > ip.ttl) family.ips.splice(position, 1);
			return ip;
		}
		setRecords(origin, addresses) {
			const timestamp = Date.now();
			const records = { records: {
				4: null,
				6: null
			} };
			for (const record of addresses) {
				record.timestamp = timestamp;
				if (typeof record.ttl === "number") record.ttl = Math.min(record.ttl, this.#maxTTL);
				else record.ttl = this.#maxTTL;
				const familyRecords = records.records[record.family] ?? { ips: [] };
				familyRecords.ips.push(record);
				records.records[record.family] = familyRecords;
			}
			this.#records.set(origin.hostname, records);
		}
		deleteRecords(origin) {
			this.#records.delete(origin.hostname);
		}
		getHandler(meta, opts) {
			return new DNSDispatchHandler(this, meta, opts);
		}
	};
	var DNSDispatchHandler = class extends DecoratorHandler$2 {
		#state = null;
		#opts = null;
		#dispatch = null;
		#origin = null;
		#controller = null;
		#newOrigin = null;
		#firstTry = true;
		constructor(state, { origin, handler, dispatch, newOrigin }, opts) {
			super(handler);
			this.#origin = origin;
			this.#newOrigin = newOrigin;
			this.#opts = { ...opts };
			this.#state = state;
			this.#dispatch = dispatch;
		}
		onResponseError(controller, err) {
			switch (err.code) {
				case "ETIMEDOUT":
				case "ECONNREFUSED":
					if (this.#state.dualStack) {
						if (!this.#firstTry) {
							super.onResponseError(controller, err);
							return;
						}
						this.#firstTry = false;
						const otherFamily = this.#newOrigin.hostname[0] === "[" ? 4 : 6;
						const ip = this.#state.pickFamily(this.#origin, otherFamily);
						if (ip == null) {
							super.onResponseError(controller, err);
							return;
						}
						let port$1;
						if (typeof ip.port === "number") port$1 = `:${ip.port}`;
						else if (this.#origin.port !== "") port$1 = `:${this.#origin.port}`;
						else port$1 = "";
						const dispatchOpts = {
							...this.#opts,
							origin: `${this.#origin.protocol}//${ip.family === 6 ? `[${ip.address}]` : ip.address}${port$1}`
						};
						this.#dispatch(dispatchOpts, this);
						return;
					}
					super.onResponseError(controller, err);
					break;
				case "ENOTFOUND":
					this.#state.deleteRecords(this.#origin);
					super.onResponseError(controller, err);
					break;
				default:
					super.onResponseError(controller, err);
					break;
			}
		}
	};
	module.exports = (interceptorOpts) => {
		if (interceptorOpts?.maxTTL != null && (typeof interceptorOpts?.maxTTL !== "number" || interceptorOpts?.maxTTL < 0)) throw new InvalidArgumentError$1("Invalid maxTTL. Must be a positive number");
		if (interceptorOpts?.maxItems != null && (typeof interceptorOpts?.maxItems !== "number" || interceptorOpts?.maxItems < 1)) throw new InvalidArgumentError$1("Invalid maxItems. Must be a positive number and greater than zero");
		if (interceptorOpts?.affinity != null && interceptorOpts?.affinity !== 4 && interceptorOpts?.affinity !== 6) throw new InvalidArgumentError$1("Invalid affinity. Must be either 4 or 6");
		if (interceptorOpts?.dualStack != null && typeof interceptorOpts?.dualStack !== "boolean") throw new InvalidArgumentError$1("Invalid dualStack. Must be a boolean");
		if (interceptorOpts?.lookup != null && typeof interceptorOpts?.lookup !== "function") throw new InvalidArgumentError$1("Invalid lookup. Must be a function");
		if (interceptorOpts?.pick != null && typeof interceptorOpts?.pick !== "function") throw new InvalidArgumentError$1("Invalid pick. Must be a function");
		const dualStack = interceptorOpts?.dualStack ?? true;
		let affinity;
		if (dualStack) affinity = interceptorOpts?.affinity ?? null;
		else affinity = interceptorOpts?.affinity ?? 4;
		const opts = {
			maxTTL: interceptorOpts?.maxTTL ?? 1e4,
			lookup: interceptorOpts?.lookup ?? null,
			pick: interceptorOpts?.pick ?? null,
			dualStack,
			affinity,
			maxItems: interceptorOpts?.maxItems ?? Infinity
		};
		const instance = new DNSInstance(opts);
		return (dispatch) => {
			return function dnsInterceptor(origDispatchOpts, handler) {
				const origin = origDispatchOpts.origin.constructor === URL ? origDispatchOpts.origin : new URL(origDispatchOpts.origin);
				if (isIP(origin.hostname) !== 0) return dispatch(origDispatchOpts, handler);
				instance.runLookup(origin, origDispatchOpts, (err, newOrigin) => {
					if (err) return handler.onResponseError(null, err);
					const dispatchOpts = {
						...origDispatchOpts,
						servername: origin.hostname,
						origin: newOrigin.origin,
						headers: {
							host: origin.host,
							...origDispatchOpts.headers
						}
					};
					dispatch(dispatchOpts, instance.getHandler({
						origin,
						dispatch,
						handler,
						newOrigin
					}, origDispatchOpts));
				});
				return true;
			};
		};
	};
}) });

//#endregion
//#region node_modules/undici/lib/util/cache.js
var require_cache$2 = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/util/cache.js": ((exports, module) => {
	const { safeHTTPMethods, pathHasQueryOrFragment } = require_util$5();
	const { serializePathWithQuery } = require_util$5();
	/**
	* @param {import('../../types/dispatcher.d.ts').default.DispatchOptions} opts
	*/
	function makeCacheKey$1(opts) {
		if (!opts.origin) throw new Error("opts.origin is undefined");
		let fullPath = opts.path || "/";
		if (opts.query && !pathHasQueryOrFragment(opts.path)) fullPath = serializePathWithQuery(fullPath, opts.query);
		return {
			origin: opts.origin.toString(),
			method: opts.method,
			path: fullPath,
			headers: opts.headers
		};
	}
	/**
	* @param {Record<string, string[] | string>}
	* @returns {Record<string, string[] | string>}
	*/
	function normalizeHeaders$1(opts) {
		let headers;
		if (opts.headers == null) headers = {};
		else if (typeof opts.headers[Symbol.iterator] === "function") {
			headers = {};
			for (const x of opts.headers) {
				if (!Array.isArray(x)) throw new Error("opts.headers is not a valid header map");
				const [key, val] = x;
				if (typeof key !== "string" || typeof val !== "string") throw new Error("opts.headers is not a valid header map");
				headers[key.toLowerCase()] = val;
			}
		} else if (typeof opts.headers === "object") {
			headers = {};
			for (const key of Object.keys(opts.headers)) headers[key.toLowerCase()] = opts.headers[key];
		} else throw new Error("opts.headers is not an object");
		return headers;
	}
	/**
	* @param {any} key
	*/
	function assertCacheKey$2(key) {
		if (typeof key !== "object") throw new TypeError(`expected key to be object, got ${typeof key}`);
		for (const property of [
			"origin",
			"method",
			"path"
		]) if (typeof key[property] !== "string") throw new TypeError(`expected key.${property} to be string, got ${typeof key[property]}`);
		if (key.headers !== void 0 && typeof key.headers !== "object") throw new TypeError(`expected headers to be object, got ${typeof key}`);
	}
	/**
	* @param {any} value
	*/
	function assertCacheValue$2(value) {
		if (typeof value !== "object") throw new TypeError(`expected value to be object, got ${typeof value}`);
		for (const property of [
			"statusCode",
			"cachedAt",
			"staleAt",
			"deleteAt"
		]) if (typeof value[property] !== "number") throw new TypeError(`expected value.${property} to be number, got ${typeof value[property]}`);
		if (typeof value.statusMessage !== "string") throw new TypeError(`expected value.statusMessage to be string, got ${typeof value.statusMessage}`);
		if (value.headers != null && typeof value.headers !== "object") throw new TypeError(`expected value.rawHeaders to be object, got ${typeof value.headers}`);
		if (value.vary !== void 0 && typeof value.vary !== "object") throw new TypeError(`expected value.vary to be object, got ${typeof value.vary}`);
		if (value.etag !== void 0 && typeof value.etag !== "string") throw new TypeError(`expected value.etag to be string, got ${typeof value.etag}`);
	}
	/**
	* @see https://www.rfc-editor.org/rfc/rfc9111.html#name-cache-control
	* @see https://www.iana.org/assignments/http-cache-directives/http-cache-directives.xhtml
	
	* @param {string | string[]} header
	* @returns {import('../../types/cache-interceptor.d.ts').default.CacheControlDirectives}
	*/
	function parseCacheControlHeader$2(header) {
		/**
		* @type {import('../../types/cache-interceptor.d.ts').default.CacheControlDirectives}
		*/
		const output = {};
		let directives;
		if (Array.isArray(header)) {
			directives = [];
			for (const directive of header) directives.push(...directive.split(","));
		} else directives = header.split(",");
		for (let i = 0; i < directives.length; i++) {
			const directive = directives[i].toLowerCase();
			const keyValueDelimiter = directive.indexOf("=");
			let key;
			let value;
			if (keyValueDelimiter !== -1) {
				key = directive.substring(0, keyValueDelimiter).trimStart();
				value = directive.substring(keyValueDelimiter + 1);
			} else key = directive.trim();
			switch (key) {
				case "min-fresh":
				case "max-stale":
				case "max-age":
				case "s-maxage":
				case "stale-while-revalidate":
				case "stale-if-error": {
					if (value === void 0 || value[0] === " ") continue;
					if (value.length >= 2 && value[0] === "\"" && value[value.length - 1] === "\"") value = value.substring(1, value.length - 1);
					const parsedValue = parseInt(value, 10);
					if (parsedValue !== parsedValue) continue;
					if (key === "max-age" && key in output && output[key] >= parsedValue) continue;
					output[key] = parsedValue;
					break;
				}
				case "private":
				case "no-cache": if (value) {
					if (value[0] === "\"") {
						const headers = [value.substring(1)];
						let foundEndingQuote = value[value.length - 1] === "\"";
						if (!foundEndingQuote) for (let j = i + 1; j < directives.length; j++) {
							const nextPart = directives[j];
							const nextPartLength = nextPart.length;
							headers.push(nextPart.trim());
							if (nextPartLength !== 0 && nextPart[nextPartLength - 1] === "\"") {
								foundEndingQuote = true;
								break;
							}
						}
						if (foundEndingQuote) {
							let lastHeader = headers[headers.length - 1];
							if (lastHeader[lastHeader.length - 1] === "\"") {
								lastHeader = lastHeader.substring(0, lastHeader.length - 1);
								headers[headers.length - 1] = lastHeader;
							}
							if (key in output) output[key] = output[key].concat(headers);
							else output[key] = headers;
						}
					} else if (key in output) output[key] = output[key].concat(value);
					else output[key] = [value];
					break;
				}
				case "public":
				case "no-store":
				case "must-revalidate":
				case "proxy-revalidate":
				case "immutable":
				case "no-transform":
				case "must-understand":
				case "only-if-cached":
					if (value) continue;
					output[key] = true;
					break;
				default: continue;
			}
		}
		return output;
	}
	/**
	* @param {string | string[]} varyHeader Vary header from the server
	* @param {Record<string, string | string[]>} headers Request headers
	* @returns {Record<string, string | string[]>}
	*/
	function parseVaryHeader$1(varyHeader, headers) {
		if (typeof varyHeader === "string" && varyHeader.includes("*")) return headers;
		const output = {};
		const varyingHeaders = typeof varyHeader === "string" ? varyHeader.split(",") : varyHeader;
		for (const header of varyingHeaders) {
			const trimmedHeader = header.trim().toLowerCase();
			output[trimmedHeader] = headers[trimmedHeader] ?? null;
		}
		return output;
	}
	/**
	* Note: this deviates from the spec a little. Empty etags ("", W/"") are valid,
	*  however, including them in cached resposnes serves little to no purpose.
	*
	* @see https://www.rfc-editor.org/rfc/rfc9110.html#name-etag
	*
	* @param {string} etag
	* @returns {boolean}
	*/
	function isEtagUsable$1(etag) {
		if (etag.length <= 2) return false;
		if (etag[0] === "\"" && etag[etag.length - 1] === "\"") return !(etag[1] === "\"" || etag.startsWith("\"W/"));
		if (etag.startsWith("W/\"") && etag[etag.length - 1] === "\"") return etag.length !== 4;
		return false;
	}
	/**
	* @param {unknown} store
	* @returns {asserts store is import('../../types/cache-interceptor.d.ts').default.CacheStore}
	*/
	function assertCacheStore$1(store, name = "CacheStore") {
		if (typeof store !== "object" || store === null) throw new TypeError(`expected type of ${name} to be a CacheStore, got ${store === null ? "null" : typeof store}`);
		for (const fn of [
			"get",
			"createWriteStream",
			"delete"
		]) if (typeof store[fn] !== "function") throw new TypeError(`${name} needs to have a \`${fn}()\` function`);
	}
	/**
	* @param {unknown} methods
	* @returns {asserts methods is import('../../types/cache-interceptor.d.ts').default.CacheMethods[]}
	*/
	function assertCacheMethods$1(methods, name = "CacheMethods") {
		if (!Array.isArray(methods)) throw new TypeError(`expected type of ${name} needs to be an array, got ${methods === null ? "null" : typeof methods}`);
		if (methods.length === 0) throw new TypeError(`${name} needs to have at least one method`);
		for (const method of methods) if (!safeHTTPMethods.includes(method)) throw new TypeError(`element of ${name}-array needs to be one of following values: ${safeHTTPMethods.join(", ")}, got ${method}`);
	}
	module.exports = {
		makeCacheKey: makeCacheKey$1,
		normalizeHeaders: normalizeHeaders$1,
		assertCacheKey: assertCacheKey$2,
		assertCacheValue: assertCacheValue$2,
		parseCacheControlHeader: parseCacheControlHeader$2,
		parseVaryHeader: parseVaryHeader$1,
		isEtagUsable: isEtagUsable$1,
		assertCacheMethods: assertCacheMethods$1,
		assertCacheStore: assertCacheStore$1
	};
}) });

//#endregion
//#region node_modules/undici/lib/util/date.js
var require_date = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/util/date.js": ((exports, module) => {
	const IMF_DAYS = [
		"mon",
		"tue",
		"wed",
		"thu",
		"fri",
		"sat",
		"sun"
	];
	const IMF_SPACES = [
		4,
		7,
		11,
		16,
		25
	];
	const IMF_MONTHS = [
		"jan",
		"feb",
		"mar",
		"apr",
		"may",
		"jun",
		"jul",
		"aug",
		"sep",
		"oct",
		"nov",
		"dec"
	];
	const IMF_COLONS = [19, 22];
	const ASCTIME_SPACES = [
		3,
		7,
		10,
		19
	];
	const RFC850_DAYS = [
		"monday",
		"tuesday",
		"wednesday",
		"thursday",
		"friday",
		"saturday",
		"sunday"
	];
	/**
	* @see https://www.rfc-editor.org/rfc/rfc9110.html#name-date-time-formats
	*
	* @param {string} date
	* @param {Date} [now]
	* @returns {Date | undefined}
	*/
	function parseHttpDate$1(date, now) {
		date = date.toLowerCase();
		switch (date[3]) {
			case ",": return parseImfDate(date);
			case " ": return parseAscTimeDate(date);
			default: return parseRfc850Date(date, now);
		}
	}
	/**
	* @see https://httpwg.org/specs/rfc9110.html#preferred.date.format
	*
	* @param {string} date
	* @returns {Date | undefined}
	*/
	function parseImfDate(date) {
		if (date.length !== 29) return;
		if (!date.endsWith("gmt")) return;
		for (const spaceInx of IMF_SPACES) if (date[spaceInx] !== " ") return;
		for (const colonIdx of IMF_COLONS) if (date[colonIdx] !== ":") return;
		const dayName = date.substring(0, 3);
		if (!IMF_DAYS.includes(dayName)) return;
		const dayString = date.substring(5, 7);
		const day = Number.parseInt(dayString);
		if (isNaN(day) || day < 10 && dayString[0] !== "0") return;
		const month = date.substring(8, 11);
		const monthIdx = IMF_MONTHS.indexOf(month);
		if (monthIdx === -1) return;
		const year = Number.parseInt(date.substring(12, 16));
		if (isNaN(year)) return;
		const hourString = date.substring(17, 19);
		const hour = Number.parseInt(hourString);
		if (isNaN(hour) || hour < 10 && hourString[0] !== "0") return;
		const minuteString = date.substring(20, 22);
		const minute = Number.parseInt(minuteString);
		if (isNaN(minute) || minute < 10 && minuteString[0] !== "0") return;
		const secondString = date.substring(23, 25);
		const second = Number.parseInt(secondString);
		if (isNaN(second) || second < 10 && secondString[0] !== "0") return;
		return new Date(Date.UTC(year, monthIdx, day, hour, minute, second));
	}
	/**
	* @see https://httpwg.org/specs/rfc9110.html#obsolete.date.formats
	*
	* @param {string} date
	* @returns {Date | undefined}
	*/
	function parseAscTimeDate(date) {
		if (date.length !== 24) return;
		for (const spaceIdx of ASCTIME_SPACES) if (date[spaceIdx] !== " ") return;
		const dayName = date.substring(0, 3);
		if (!IMF_DAYS.includes(dayName)) return;
		const month = date.substring(4, 7);
		const monthIdx = IMF_MONTHS.indexOf(month);
		if (monthIdx === -1) return;
		const dayString = date.substring(8, 10);
		const day = Number.parseInt(dayString);
		if (isNaN(day) || day < 10 && dayString[0] !== " ") return;
		const hourString = date.substring(11, 13);
		const hour = Number.parseInt(hourString);
		if (isNaN(hour) || hour < 10 && hourString[0] !== "0") return;
		const minuteString = date.substring(14, 16);
		const minute = Number.parseInt(minuteString);
		if (isNaN(minute) || minute < 10 && minuteString[0] !== "0") return;
		const secondString = date.substring(17, 19);
		const second = Number.parseInt(secondString);
		if (isNaN(second) || second < 10 && secondString[0] !== "0") return;
		const year = Number.parseInt(date.substring(20, 24));
		if (isNaN(year)) return;
		return new Date(Date.UTC(year, monthIdx, day, hour, minute, second));
	}
	/**
	* @see https://httpwg.org/specs/rfc9110.html#obsolete.date.formats
	*
	* @param {string} date
	* @param {Date} [now]
	* @returns {Date | undefined}
	*/
	function parseRfc850Date(date, now = /* @__PURE__ */ new Date()) {
		if (!date.endsWith("gmt")) return;
		const commaIndex = date.indexOf(",");
		if (commaIndex === -1) return;
		if (date.length - commaIndex - 1 !== 23) return;
		const dayName = date.substring(0, commaIndex);
		if (!RFC850_DAYS.includes(dayName)) return;
		if (date[commaIndex + 1] !== " " || date[commaIndex + 4] !== "-" || date[commaIndex + 8] !== "-" || date[commaIndex + 11] !== " " || date[commaIndex + 14] !== ":" || date[commaIndex + 17] !== ":" || date[commaIndex + 20] !== " ") return;
		const dayString = date.substring(commaIndex + 2, commaIndex + 4);
		const day = Number.parseInt(dayString);
		if (isNaN(day) || day < 10 && dayString[0] !== "0") return;
		const month = date.substring(commaIndex + 5, commaIndex + 8);
		const monthIdx = IMF_MONTHS.indexOf(month);
		if (monthIdx === -1) return;
		let year = Number.parseInt(date.substring(commaIndex + 9, commaIndex + 11));
		if (isNaN(year)) return;
		const currentYear = now.getUTCFullYear();
		const currentDecade = currentYear % 100;
		const currentCentury = Math.floor(currentYear / 100);
		if (year > currentDecade && year - currentDecade >= 50) year += (currentCentury - 1) * 100;
		else year += currentCentury * 100;
		const hourString = date.substring(commaIndex + 12, commaIndex + 14);
		const hour = Number.parseInt(hourString);
		if (isNaN(hour) || hour < 10 && hourString[0] !== "0") return;
		const minuteString = date.substring(commaIndex + 15, commaIndex + 17);
		const minute = Number.parseInt(minuteString);
		if (isNaN(minute) || minute < 10 && minuteString[0] !== "0") return;
		const secondString = date.substring(commaIndex + 18, commaIndex + 20);
		const second = Number.parseInt(secondString);
		if (isNaN(second) || second < 10 && secondString[0] !== "0") return;
		return new Date(Date.UTC(year, monthIdx, day, hour, minute, second));
	}
	module.exports = { parseHttpDate: parseHttpDate$1 };
}) });

//#endregion
//#region node_modules/undici/lib/handler/cache-handler.js
var require_cache_handler = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/handler/cache-handler.js": ((exports, module) => {
	const util$5 = require_util$5();
	const { parseCacheControlHeader: parseCacheControlHeader$1, parseVaryHeader, isEtagUsable } = require_cache$2();
	const { parseHttpDate } = require_date();
	function noop() {}
	const HEURISTICALLY_CACHEABLE_STATUS_CODES = [
		200,
		203,
		204,
		206,
		300,
		301,
		308,
		404,
		405,
		410,
		414,
		501
	];
	const NOT_UNDERSTOOD_STATUS_CODES = [206, 304];
	const MAX_RESPONSE_AGE = 2147483647e3;
	/**
	* @typedef {import('../../types/dispatcher.d.ts').default.DispatchHandler} DispatchHandler
	*
	* @implements {DispatchHandler}
	*/
	var CacheHandler$1 = class {
		/**
		* @type {import('../../types/cache-interceptor.d.ts').default.CacheKey}
		*/
		#cacheKey;
		/**
		* @type {import('../../types/cache-interceptor.d.ts').default.CacheHandlerOptions['type']}
		*/
		#cacheType;
		/**
		* @type {number | undefined}
		*/
		#cacheByDefault;
		/**
		* @type {import('../../types/cache-interceptor.d.ts').default.CacheStore}
		*/
		#store;
		/**
		* @type {import('../../types/dispatcher.d.ts').default.DispatchHandler}
		*/
		#handler;
		/**
		* @type {import('node:stream').Writable | undefined}
		*/
		#writeStream;
		/**
		* @param {import('../../types/cache-interceptor.d.ts').default.CacheHandlerOptions} opts
		* @param {import('../../types/cache-interceptor.d.ts').default.CacheKey} cacheKey
		* @param {import('../../types/dispatcher.d.ts').default.DispatchHandler} handler
		*/
		constructor({ store, type, cacheByDefault }, cacheKey, handler) {
			this.#store = store;
			this.#cacheType = type;
			this.#cacheByDefault = cacheByDefault;
			this.#cacheKey = cacheKey;
			this.#handler = handler;
		}
		onRequestStart(controller, context) {
			this.#writeStream?.destroy();
			this.#writeStream = void 0;
			this.#handler.onRequestStart?.(controller, context);
		}
		onRequestUpgrade(controller, statusCode, headers, socket) {
			this.#handler.onRequestUpgrade?.(controller, statusCode, headers, socket);
		}
		/**
		* @param {import('../../types/dispatcher.d.ts').default.DispatchController} controller
		* @param {number} statusCode
		* @param {import('../../types/header.d.ts').IncomingHttpHeaders} resHeaders
		* @param {string} statusMessage
		*/
		onResponseStart(controller, statusCode, resHeaders, statusMessage) {
			const downstreamOnHeaders = () => this.#handler.onResponseStart?.(controller, statusCode, resHeaders, statusMessage);
			if (!util$5.safeHTTPMethods.includes(this.#cacheKey.method) && statusCode >= 200 && statusCode <= 399) {
				try {
					this.#store.delete(this.#cacheKey)?.catch?.(noop);
				} catch {}
				return downstreamOnHeaders();
			}
			const cacheControlHeader = resHeaders["cache-control"];
			const heuristicallyCacheable = resHeaders["last-modified"] && HEURISTICALLY_CACHEABLE_STATUS_CODES.includes(statusCode);
			if (!cacheControlHeader && !resHeaders["expires"] && !heuristicallyCacheable && !this.#cacheByDefault) return downstreamOnHeaders();
			const cacheControlDirectives = cacheControlHeader ? parseCacheControlHeader$1(cacheControlHeader) : {};
			if (!canCacheResponse(this.#cacheType, statusCode, resHeaders, cacheControlDirectives)) return downstreamOnHeaders();
			const now = Date.now();
			const resAge = resHeaders.age ? getAge(resHeaders.age) : void 0;
			if (resAge && resAge >= MAX_RESPONSE_AGE) return downstreamOnHeaders();
			const resDate = typeof resHeaders.date === "string" ? parseHttpDate(resHeaders.date) : void 0;
			const staleAt = determineStaleAt(this.#cacheType, now, resAge, resHeaders, resDate, cacheControlDirectives) ?? this.#cacheByDefault;
			if (staleAt === void 0 || resAge && resAge > staleAt) return downstreamOnHeaders();
			const baseTime = resDate ? resDate.getTime() : now;
			const absoluteStaleAt = staleAt + baseTime;
			if (now >= absoluteStaleAt) return downstreamOnHeaders();
			let varyDirectives;
			if (this.#cacheKey.headers && resHeaders.vary) {
				varyDirectives = parseVaryHeader(resHeaders.vary, this.#cacheKey.headers);
				if (!varyDirectives) return downstreamOnHeaders();
			}
			const deleteAt = determineDeleteAt(baseTime, cacheControlDirectives, absoluteStaleAt);
			const strippedHeaders = stripNecessaryHeaders(resHeaders, cacheControlDirectives);
			/**
			* @type {import('../../types/cache-interceptor.d.ts').default.CacheValue}
			*/
			const value = {
				statusCode,
				statusMessage,
				headers: strippedHeaders,
				vary: varyDirectives,
				cacheControlDirectives,
				cachedAt: resAge ? now - resAge : now,
				staleAt: absoluteStaleAt,
				deleteAt
			};
			if (typeof resHeaders.etag === "string" && isEtagUsable(resHeaders.etag)) value.etag = resHeaders.etag;
			this.#writeStream = this.#store.createWriteStream(this.#cacheKey, value);
			if (!this.#writeStream) return downstreamOnHeaders();
			const handler = this;
			this.#writeStream.on("drain", () => controller.resume()).on("error", function() {
				handler.#writeStream = void 0;
				handler.#store.delete(handler.#cacheKey);
			}).on("close", function() {
				if (handler.#writeStream === this) handler.#writeStream = void 0;
				controller.resume();
			});
			return downstreamOnHeaders();
		}
		onResponseData(controller, chunk) {
			if (this.#writeStream?.write(chunk) === false) controller.pause();
			this.#handler.onResponseData?.(controller, chunk);
		}
		onResponseEnd(controller, trailers) {
			this.#writeStream?.end();
			this.#handler.onResponseEnd?.(controller, trailers);
		}
		onResponseError(controller, err) {
			this.#writeStream?.destroy(err);
			this.#writeStream = void 0;
			this.#handler.onResponseError?.(controller, err);
		}
	};
	/**
	* @see https://www.rfc-editor.org/rfc/rfc9111.html#name-storing-responses-to-authen
	*
	* @param {import('../../types/cache-interceptor.d.ts').default.CacheOptions['type']} cacheType
	* @param {number} statusCode
	* @param {import('../../types/header.d.ts').IncomingHttpHeaders} resHeaders
	* @param {import('../../types/cache-interceptor.d.ts').default.CacheControlDirectives} cacheControlDirectives
	*/
	function canCacheResponse(cacheType, statusCode, resHeaders, cacheControlDirectives) {
		if (statusCode < 200 || NOT_UNDERSTOOD_STATUS_CODES.includes(statusCode)) return false;
		if (!HEURISTICALLY_CACHEABLE_STATUS_CODES.includes(statusCode) && !resHeaders["expires"] && !cacheControlDirectives.public && cacheControlDirectives["max-age"] === void 0 && !(cacheControlDirectives.private && cacheType === "private") && !(cacheControlDirectives["s-maxage"] !== void 0 && cacheType === "shared")) return false;
		if (cacheControlDirectives["no-store"]) return false;
		if (cacheType === "shared" && cacheControlDirectives.private === true) return false;
		if (resHeaders.vary?.includes("*")) return false;
		if (resHeaders.authorization) {
			if (!cacheControlDirectives.public || typeof resHeaders.authorization !== "string") return false;
			if (Array.isArray(cacheControlDirectives["no-cache"]) && cacheControlDirectives["no-cache"].includes("authorization")) return false;
			if (Array.isArray(cacheControlDirectives["private"]) && cacheControlDirectives["private"].includes("authorization")) return false;
		}
		return true;
	}
	/**
	* @param {string | string[]} ageHeader
	* @returns {number | undefined}
	*/
	function getAge(ageHeader) {
		const age = parseInt(Array.isArray(ageHeader) ? ageHeader[0] : ageHeader);
		return isNaN(age) ? void 0 : age * 1e3;
	}
	/**
	* @param {import('../../types/cache-interceptor.d.ts').default.CacheOptions['type']} cacheType
	* @param {number} now
	* @param {number | undefined} age
	* @param {import('../../types/header.d.ts').IncomingHttpHeaders} resHeaders
	* @param {Date | undefined} responseDate
	* @param {import('../../types/cache-interceptor.d.ts').default.CacheControlDirectives} cacheControlDirectives
	*
	* @returns {number | undefined} time that the value is stale at in seconds or undefined if it shouldn't be cached
	*/
	function determineStaleAt(cacheType, now, age, resHeaders, responseDate, cacheControlDirectives) {
		if (cacheType === "shared") {
			const sMaxAge = cacheControlDirectives["s-maxage"];
			if (sMaxAge !== void 0) return sMaxAge > 0 ? sMaxAge * 1e3 : void 0;
		}
		const maxAge = cacheControlDirectives["max-age"];
		if (maxAge !== void 0) return maxAge > 0 ? maxAge * 1e3 : void 0;
		if (typeof resHeaders.expires === "string") {
			const expiresDate = parseHttpDate(resHeaders.expires);
			if (expiresDate) {
				if (now >= expiresDate.getTime()) return;
				if (responseDate) {
					if (responseDate >= expiresDate) return;
					if (age !== void 0 && age > expiresDate - responseDate) return;
				}
				return expiresDate.getTime() - now;
			}
		}
		if (typeof resHeaders["last-modified"] === "string") {
			const lastModified = new Date(resHeaders["last-modified"]);
			if (isValidDate(lastModified)) {
				if (lastModified.getTime() >= now) return;
				return (now - lastModified.getTime()) * .1;
			}
		}
		if (cacheControlDirectives.immutable) return 31536e3;
	}
	/**
	* @param {number} now
	* @param {import('../../types/cache-interceptor.d.ts').default.CacheControlDirectives} cacheControlDirectives
	* @param {number} staleAt
	*/
	function determineDeleteAt(now, cacheControlDirectives, staleAt) {
		let staleWhileRevalidate = -Infinity;
		let staleIfError = -Infinity;
		let immutable = -Infinity;
		if (cacheControlDirectives["stale-while-revalidate"]) staleWhileRevalidate = staleAt + cacheControlDirectives["stale-while-revalidate"] * 1e3;
		if (cacheControlDirectives["stale-if-error"]) staleIfError = staleAt + cacheControlDirectives["stale-if-error"] * 1e3;
		if (staleWhileRevalidate === -Infinity && staleIfError === -Infinity) immutable = now + 31536e6;
		return Math.max(staleAt, staleWhileRevalidate, staleIfError, immutable);
	}
	/**
	* Strips headers required to be removed in cached responses
	* @param {import('../../types/header.d.ts').IncomingHttpHeaders} resHeaders
	* @param {import('../../types/cache-interceptor.d.ts').default.CacheControlDirectives} cacheControlDirectives
	* @returns {Record<string, string | string []>}
	*/
	function stripNecessaryHeaders(resHeaders, cacheControlDirectives) {
		const headersToRemove = [
			"connection",
			"proxy-authenticate",
			"proxy-authentication-info",
			"proxy-authorization",
			"proxy-connection",
			"te",
			"transfer-encoding",
			"upgrade",
			"age"
		];
		if (resHeaders["connection"]) if (Array.isArray(resHeaders["connection"])) headersToRemove.push(...resHeaders["connection"].map((header) => header.trim()));
		else headersToRemove.push(...resHeaders["connection"].split(",").map((header) => header.trim()));
		if (Array.isArray(cacheControlDirectives["no-cache"])) headersToRemove.push(...cacheControlDirectives["no-cache"]);
		if (Array.isArray(cacheControlDirectives["private"])) headersToRemove.push(...cacheControlDirectives["private"]);
		let strippedHeaders;
		for (const headerName of headersToRemove) if (resHeaders[headerName]) {
			strippedHeaders ??= { ...resHeaders };
			delete strippedHeaders[headerName];
		}
		return strippedHeaders ?? resHeaders;
	}
	/**
	* @param {Date} date
	* @returns {boolean}
	*/
	function isValidDate(date) {
		return date instanceof Date && Number.isFinite(date.valueOf());
	}
	module.exports = CacheHandler$1;
}) });

//#endregion
//#region node_modules/undici/lib/cache/memory-cache-store.js
var require_memory_cache_store = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/cache/memory-cache-store.js": ((exports, module) => {
	const { Writable: Writable$2 } = __require("node:stream");
	const { EventEmitter } = __require("node:events");
	const { assertCacheKey: assertCacheKey$1, assertCacheValue: assertCacheValue$1 } = require_cache$2();
	/**
	* @typedef {import('../../types/cache-interceptor.d.ts').default.CacheKey} CacheKey
	* @typedef {import('../../types/cache-interceptor.d.ts').default.CacheValue} CacheValue
	* @typedef {import('../../types/cache-interceptor.d.ts').default.CacheStore} CacheStore
	* @typedef {import('../../types/cache-interceptor.d.ts').default.GetResult} GetResult
	*/
	/**
	* @implements {CacheStore}
	* @extends {EventEmitter}
	*/
	var MemoryCacheStore$1 = class extends EventEmitter {
		#maxCount = 1024;
		#maxSize = 104857600;
		#maxEntrySize = 5242880;
		#size = 0;
		#count = 0;
		#entries = /* @__PURE__ */ new Map();
		#hasEmittedMaxSizeEvent = false;
		/**
		* @param {import('../../types/cache-interceptor.d.ts').default.MemoryCacheStoreOpts | undefined} [opts]
		*/
		constructor(opts) {
			super();
			if (opts) {
				if (typeof opts !== "object") throw new TypeError("MemoryCacheStore options must be an object");
				if (opts.maxCount !== void 0) {
					if (typeof opts.maxCount !== "number" || !Number.isInteger(opts.maxCount) || opts.maxCount < 0) throw new TypeError("MemoryCacheStore options.maxCount must be a non-negative integer");
					this.#maxCount = opts.maxCount;
				}
				if (opts.maxSize !== void 0) {
					if (typeof opts.maxSize !== "number" || !Number.isInteger(opts.maxSize) || opts.maxSize < 0) throw new TypeError("MemoryCacheStore options.maxSize must be a non-negative integer");
					this.#maxSize = opts.maxSize;
				}
				if (opts.maxEntrySize !== void 0) {
					if (typeof opts.maxEntrySize !== "number" || !Number.isInteger(opts.maxEntrySize) || opts.maxEntrySize < 0) throw new TypeError("MemoryCacheStore options.maxEntrySize must be a non-negative integer");
					this.#maxEntrySize = opts.maxEntrySize;
				}
			}
		}
		/**
		* Get the current size of the cache in bytes
		* @returns {number} The current size of the cache in bytes
		*/
		get size() {
			return this.#size;
		}
		/**
		* Check if the cache is full (either max size or max count reached)
		* @returns {boolean} True if the cache is full, false otherwise
		*/
		isFull() {
			return this.#size >= this.#maxSize || this.#count >= this.#maxCount;
		}
		/**
		* @param {import('../../types/cache-interceptor.d.ts').default.CacheKey} req
		* @returns {import('../../types/cache-interceptor.d.ts').default.GetResult | undefined}
		*/
		get(key) {
			assertCacheKey$1(key);
			const topLevelKey = `${key.origin}:${key.path}`;
			const now = Date.now();
			const entries = this.#entries.get(topLevelKey);
			const entry = entries ? findEntry(key, entries, now) : null;
			return entry == null ? void 0 : {
				statusMessage: entry.statusMessage,
				statusCode: entry.statusCode,
				headers: entry.headers,
				body: entry.body,
				vary: entry.vary ? entry.vary : void 0,
				etag: entry.etag,
				cacheControlDirectives: entry.cacheControlDirectives,
				cachedAt: entry.cachedAt,
				staleAt: entry.staleAt,
				deleteAt: entry.deleteAt
			};
		}
		/**
		* @param {import('../../types/cache-interceptor.d.ts').default.CacheKey} key
		* @param {import('../../types/cache-interceptor.d.ts').default.CacheValue} val
		* @returns {Writable | undefined}
		*/
		createWriteStream(key, val) {
			assertCacheKey$1(key);
			assertCacheValue$1(val);
			const topLevelKey = `${key.origin}:${key.path}`;
			const store = this;
			const entry = {
				...key,
				...val,
				body: [],
				size: 0
			};
			return new Writable$2({
				write(chunk, encoding, callback) {
					if (typeof chunk === "string") chunk = Buffer.from(chunk, encoding);
					entry.size += chunk.byteLength;
					if (entry.size >= store.#maxEntrySize) this.destroy();
					else entry.body.push(chunk);
					callback(null);
				},
				final(callback) {
					let entries = store.#entries.get(topLevelKey);
					if (!entries) {
						entries = [];
						store.#entries.set(topLevelKey, entries);
					}
					const previousEntry = findEntry(key, entries, Date.now());
					if (previousEntry) {
						const index = entries.indexOf(previousEntry);
						entries.splice(index, 1, entry);
						store.#size -= previousEntry.size;
					} else {
						entries.push(entry);
						store.#count += 1;
					}
					store.#size += entry.size;
					if (store.#size > store.#maxSize || store.#count > store.#maxCount) {
						if (!store.#hasEmittedMaxSizeEvent) {
							store.emit("maxSizeExceeded", {
								size: store.#size,
								maxSize: store.#maxSize,
								count: store.#count,
								maxCount: store.#maxCount
							});
							store.#hasEmittedMaxSizeEvent = true;
						}
						for (const [key$1, entries$1] of store.#entries) {
							for (const entry$1 of entries$1.splice(0, entries$1.length / 2)) {
								store.#size -= entry$1.size;
								store.#count -= 1;
							}
							if (entries$1.length === 0) store.#entries.delete(key$1);
						}
						if (store.#size < store.#maxSize && store.#count < store.#maxCount) store.#hasEmittedMaxSizeEvent = false;
					}
					callback(null);
				}
			});
		}
		/**
		* @param {CacheKey} key
		*/
		delete(key) {
			if (typeof key !== "object") throw new TypeError(`expected key to be object, got ${typeof key}`);
			const topLevelKey = `${key.origin}:${key.path}`;
			for (const entry of this.#entries.get(topLevelKey) ?? []) {
				this.#size -= entry.size;
				this.#count -= 1;
			}
			this.#entries.delete(topLevelKey);
		}
	};
	function findEntry(key, entries, now) {
		return entries.find((entry) => entry.deleteAt > now && entry.method === key.method && (entry.vary == null || Object.keys(entry.vary).every((headerName) => {
			if (entry.vary[headerName] === null) return key.headers[headerName] === void 0;
			return entry.vary[headerName] === key.headers[headerName];
		})));
	}
	module.exports = MemoryCacheStore$1;
}) });

//#endregion
//#region node_modules/undici/lib/handler/cache-revalidation-handler.js
var require_cache_revalidation_handler = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/handler/cache-revalidation-handler.js": ((exports, module) => {
	const assert$11 = __require("node:assert");
	/**
	* This takes care of revalidation requests we send to the origin. If we get
	*  a response indicating that what we have is cached (via a HTTP 304), we can
	*  continue using the cached value. Otherwise, we'll receive the new response
	*  here, which we then just pass on to the next handler (most likely a
	*  CacheHandler). Note that this assumes the proper headers were already
	*  included in the request to tell the origin that we want to revalidate the
	*  response (i.e. if-modified-since or if-none-match).
	*
	* @see https://www.rfc-editor.org/rfc/rfc9111.html#name-validation
	*
	* @implements {import('../../types/dispatcher.d.ts').default.DispatchHandler}
	*/
	var CacheRevalidationHandler$1 = class {
		#successful = false;
		/**
		* @type {((boolean, any) => void) | null}
		*/
		#callback;
		/**
		* @type {(import('../../types/dispatcher.d.ts').default.DispatchHandler)}
		*/
		#handler;
		#context;
		/**
		* @type {boolean}
		*/
		#allowErrorStatusCodes;
		/**
		* @param {(boolean) => void} callback Function to call if the cached value is valid
		* @param {import('../../types/dispatcher.d.ts').default.DispatchHandlers} handler
		* @param {boolean} allowErrorStatusCodes
		*/
		constructor(callback, handler, allowErrorStatusCodes) {
			if (typeof callback !== "function") throw new TypeError("callback must be a function");
			this.#callback = callback;
			this.#handler = handler;
			this.#allowErrorStatusCodes = allowErrorStatusCodes;
		}
		onRequestStart(_, context) {
			this.#successful = false;
			this.#context = context;
		}
		onRequestUpgrade(controller, statusCode, headers, socket) {
			this.#handler.onRequestUpgrade?.(controller, statusCode, headers, socket);
		}
		onResponseStart(controller, statusCode, headers, statusMessage) {
			assert$11(this.#callback != null);
			this.#successful = statusCode === 304 || this.#allowErrorStatusCodes && statusCode >= 500 && statusCode <= 504;
			this.#callback(this.#successful, this.#context);
			this.#callback = null;
			if (this.#successful) return true;
			this.#handler.onRequestStart?.(controller, this.#context);
			this.#handler.onResponseStart?.(controller, statusCode, headers, statusMessage);
		}
		onResponseData(controller, chunk) {
			if (this.#successful) return;
			return this.#handler.onResponseData?.(controller, chunk);
		}
		onResponseEnd(controller, trailers) {
			if (this.#successful) return;
			this.#handler.onResponseEnd?.(controller, trailers);
		}
		onResponseError(controller, err) {
			if (this.#successful) return;
			if (this.#callback) {
				this.#callback(false);
				this.#callback = null;
			}
			if (typeof this.#handler.onResponseError === "function") this.#handler.onResponseError(controller, err);
			else throw err;
		}
	};
	module.exports = CacheRevalidationHandler$1;
}) });

//#endregion
//#region node_modules/undici/lib/interceptor/cache.js
var require_cache$1 = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/interceptor/cache.js": ((exports, module) => {
	const assert$10 = __require("node:assert");
	const { Readable: Readable$1 } = __require("node:stream");
	const util$4 = require_util$5();
	const CacheHandler = require_cache_handler();
	const MemoryCacheStore = require_memory_cache_store();
	const CacheRevalidationHandler = require_cache_revalidation_handler();
	const { assertCacheStore, assertCacheMethods, makeCacheKey, normalizeHeaders, parseCacheControlHeader } = require_cache$2();
	const { AbortError } = require_errors();
	/**
	* @typedef {(options: import('../../types/dispatcher.d.ts').default.DispatchOptions, handler: import('../../types/dispatcher.d.ts').default.DispatchHandler) => void} DispatchFn
	*/
	/**
	* @param {import('../../types/cache-interceptor.d.ts').default.GetResult} result
	* @param {import('../../types/cache-interceptor.d.ts').default.CacheControlDirectives | undefined} cacheControlDirectives
	* @returns {boolean}
	*/
	function needsRevalidation(result, cacheControlDirectives) {
		if (cacheControlDirectives?.["no-cache"]) return true;
		if (result.cacheControlDirectives?.["no-cache"] && !Array.isArray(result.cacheControlDirectives["no-cache"])) return true;
		const now = Date.now();
		if (now > result.staleAt) {
			if (cacheControlDirectives?.["max-stale"]) {
				const gracePeriod = result.staleAt + cacheControlDirectives["max-stale"] * 1e3;
				return now > gracePeriod;
			}
			return true;
		}
		if (cacheControlDirectives?.["min-fresh"]) {
			const timeLeftTillStale = result.staleAt - now;
			const threshold = cacheControlDirectives["min-fresh"] * 1e3;
			return timeLeftTillStale <= threshold;
		}
		return false;
	}
	/**
	* @param {DispatchFn} dispatch
	* @param {import('../../types/cache-interceptor.d.ts').default.CacheHandlerOptions} globalOpts
	* @param {import('../../types/cache-interceptor.d.ts').default.CacheKey} cacheKey
	* @param {import('../../types/dispatcher.d.ts').default.DispatchHandler} handler
	* @param {import('../../types/dispatcher.d.ts').default.RequestOptions} opts
	* @param {import('../../types/cache-interceptor.d.ts').default.CacheControlDirectives | undefined} reqCacheControl
	*/
	function handleUncachedResponse(dispatch, globalOpts, cacheKey, handler, opts, reqCacheControl) {
		if (reqCacheControl?.["only-if-cached"]) {
			let aborted = false;
			try {
				if (typeof handler.onConnect === "function") {
					handler.onConnect(() => {
						aborted = true;
					});
					if (aborted) return;
				}
				if (typeof handler.onHeaders === "function") {
					handler.onHeaders(504, [], () => {}, "Gateway Timeout");
					if (aborted) return;
				}
				if (typeof handler.onComplete === "function") handler.onComplete([]);
			} catch (err) {
				if (typeof handler.onError === "function") handler.onError(err);
			}
			return true;
		}
		return dispatch(opts, new CacheHandler(globalOpts, cacheKey, handler));
	}
	/**
	* @param {import('../../types/dispatcher.d.ts').default.DispatchHandler} handler
	* @param {import('../../types/dispatcher.d.ts').default.RequestOptions} opts
	* @param {import('../../types/cache-interceptor.d.ts').default.GetResult} result
	* @param {number} age
	* @param {any} context
	* @param {boolean} isStale
	*/
	function sendCachedValue(handler, opts, result, age, context, isStale) {
		const stream$2 = util$4.isStream(result.body) ? result.body : Readable$1.from(result.body ?? []);
		assert$10(!stream$2.destroyed, "stream should not be destroyed");
		assert$10(!stream$2.readableDidRead, "stream should not be readableDidRead");
		const controller = {
			resume() {
				stream$2.resume();
			},
			pause() {
				stream$2.pause();
			},
			get paused() {
				return stream$2.isPaused();
			},
			get aborted() {
				return stream$2.destroyed;
			},
			get reason() {
				return stream$2.errored;
			},
			abort(reason) {
				stream$2.destroy(reason ?? new AbortError());
			}
		};
		stream$2.on("error", function(err) {
			if (!this.readableEnded) if (typeof handler.onResponseError === "function") handler.onResponseError(controller, err);
			else throw err;
		}).on("close", function() {
			if (!this.errored) handler.onResponseEnd?.(controller, {});
		});
		handler.onRequestStart?.(controller, context);
		if (stream$2.destroyed) return;
		const headers = {
			...result.headers,
			age: String(age)
		};
		if (isStale) headers.warning = "110 - \"response is stale\"";
		handler.onResponseStart?.(controller, result.statusCode, headers, result.statusMessage);
		if (opts.method === "HEAD") stream$2.destroy();
		else stream$2.on("data", function(chunk) {
			handler.onResponseData?.(controller, chunk);
		});
	}
	/**
	* @param {DispatchFn} dispatch
	* @param {import('../../types/cache-interceptor.d.ts').default.CacheHandlerOptions} globalOpts
	* @param {import('../../types/cache-interceptor.d.ts').default.CacheKey} cacheKey
	* @param {import('../../types/dispatcher.d.ts').default.DispatchHandler} handler
	* @param {import('../../types/dispatcher.d.ts').default.RequestOptions} opts
	* @param {import('../../types/cache-interceptor.d.ts').default.CacheControlDirectives | undefined} reqCacheControl
	* @param {import('../../types/cache-interceptor.d.ts').default.GetResult | undefined} result
	*/
	function handleResult(dispatch, globalOpts, cacheKey, handler, opts, reqCacheControl, result) {
		if (!result) return handleUncachedResponse(dispatch, globalOpts, cacheKey, handler, opts, reqCacheControl);
		const now = Date.now();
		if (now > result.deleteAt) return dispatch(opts, new CacheHandler(globalOpts, cacheKey, handler));
		const age = Math.round((now - result.cachedAt) / 1e3);
		if (reqCacheControl?.["max-age"] && age >= reqCacheControl["max-age"]) return dispatch(opts, handler);
		if (needsRevalidation(result, reqCacheControl)) {
			if (util$4.isStream(opts.body) && util$4.bodyLength(opts.body) !== 0) return dispatch(opts, new CacheHandler(globalOpts, cacheKey, handler));
			let withinStaleIfErrorThreshold = false;
			const staleIfErrorExpiry = result.cacheControlDirectives["stale-if-error"] ?? reqCacheControl?.["stale-if-error"];
			if (staleIfErrorExpiry) withinStaleIfErrorThreshold = now < result.staleAt + staleIfErrorExpiry * 1e3;
			let headers = {
				...opts.headers,
				"if-modified-since": new Date(result.cachedAt).toUTCString()
			};
			if (result.etag) headers["if-none-match"] = result.etag;
			if (result.vary) headers = {
				...headers,
				...result.vary
			};
			return dispatch({
				...opts,
				headers
			}, new CacheRevalidationHandler((success, context) => {
				if (success) sendCachedValue(handler, opts, result, age, context, true);
				else if (util$4.isStream(result.body)) result.body.on("error", () => {}).destroy();
			}, new CacheHandler(globalOpts, cacheKey, handler), withinStaleIfErrorThreshold));
		}
		if (util$4.isStream(opts.body)) opts.body.on("error", () => {}).destroy();
		sendCachedValue(handler, opts, result, age, null, false);
	}
	/**
	* @param {import('../../types/cache-interceptor.d.ts').default.CacheOptions} [opts]
	* @returns {import('../../types/dispatcher.d.ts').default.DispatcherComposeInterceptor}
	*/
	module.exports = (opts = {}) => {
		const { store = new MemoryCacheStore(), methods = ["GET"], cacheByDefault = void 0, type = "shared" } = opts;
		if (typeof opts !== "object" || opts === null) throw new TypeError(`expected type of opts to be an Object, got ${opts === null ? "null" : typeof opts}`);
		assertCacheStore(store, "opts.store");
		assertCacheMethods(methods, "opts.methods");
		if (typeof cacheByDefault !== "undefined" && typeof cacheByDefault !== "number") throw new TypeError(`expected opts.cacheByDefault to be number or undefined, got ${typeof cacheByDefault}`);
		if (typeof type !== "undefined" && type !== "shared" && type !== "private") throw new TypeError(`expected opts.type to be shared, private, or undefined, got ${typeof type}`);
		const globalOpts = {
			store,
			methods,
			cacheByDefault,
			type
		};
		const safeMethodsToNotCache = util$4.safeHTTPMethods.filter((method) => methods.includes(method) === false);
		return (dispatch) => {
			return (opts$1, handler) => {
				if (!opts$1.origin || safeMethodsToNotCache.includes(opts$1.method)) return dispatch(opts$1, handler);
				opts$1 = {
					...opts$1,
					headers: normalizeHeaders(opts$1)
				};
				const reqCacheControl = opts$1.headers?.["cache-control"] ? parseCacheControlHeader(opts$1.headers["cache-control"]) : void 0;
				if (reqCacheControl?.["no-store"]) return dispatch(opts$1, handler);
				/**
				* @type {import('../../types/cache-interceptor.d.ts').default.CacheKey}
				*/
				const cacheKey = makeCacheKey(opts$1);
				const result = store.get(cacheKey);
				if (result && typeof result.then === "function") result.then((result$1) => {
					handleResult(dispatch, globalOpts, cacheKey, handler, opts$1, reqCacheControl, result$1);
				});
				else handleResult(dispatch, globalOpts, cacheKey, handler, opts$1, reqCacheControl, result);
				return true;
			};
		};
	};
}) });

//#endregion
//#region node_modules/undici/lib/interceptor/decompress.js
var require_decompress = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/interceptor/decompress.js": ((exports, module) => {
	const { createInflate: createInflate$1, createGunzip, createBrotliDecompress, createZstdDecompress } = __require("node:zlib");
	const { pipeline: pipeline$2 } = __require("node:stream");
	const DecoratorHandler$1 = require_decorator_handler();
	/** @typedef {import('node:stream').Transform} Transform */
	/** @typedef {import('node:stream').Transform} Controller */
	/** @typedef {Transform&import('node:zlib').Zlib} DecompressorStream */
	/** @type {Record<string, () => DecompressorStream>} */
	const supportedEncodings = {
		gzip: createGunzip,
		"x-gzip": createGunzip,
		br: createBrotliDecompress,
		deflate: createInflate$1,
		compress: createInflate$1,
		"x-compress": createInflate$1,
		...createZstdDecompress ? { zstd: createZstdDecompress } : {}
	};
	const defaultSkipStatusCodes = [204, 304];
	let warningEmitted = false;
	/**
	* @typedef {Object} DecompressHandlerOptions
	* @property {number[]|Readonly<number[]>} [skipStatusCodes=[204, 304]] - List of status codes to skip decompression for
	* @property {boolean} [skipErrorResponses] - Whether to skip decompression for error responses (status codes >= 400)
	*/
	var DecompressHandler = class extends DecoratorHandler$1 {
		/** @type {Transform[]} */
		#decompressors = [];
		/** @type {NodeJS.WritableStream&NodeJS.ReadableStream|null} */
		#pipelineStream;
		/** @type {Readonly<number[]>} */
		#skipStatusCodes;
		/** @type {boolean} */
		#skipErrorResponses;
		constructor(handler, { skipStatusCodes = defaultSkipStatusCodes, skipErrorResponses = true } = {}) {
			super(handler);
			this.#skipStatusCodes = skipStatusCodes;
			this.#skipErrorResponses = skipErrorResponses;
		}
		/**
		* Determines if decompression should be skipped based on encoding and status code
		* @param {string} contentEncoding - Content-Encoding header value
		* @param {number} statusCode - HTTP status code of the response
		* @returns {boolean} - True if decompression should be skipped
		*/
		#shouldSkipDecompression(contentEncoding, statusCode) {
			if (!contentEncoding || statusCode < 200) return true;
			if (this.#skipStatusCodes.includes(statusCode)) return true;
			if (this.#skipErrorResponses && statusCode >= 400) return true;
			return false;
		}
		/**
		* Creates a chain of decompressors for multiple content encodings
		*
		* @param {string} encodings - Comma-separated list of content encodings
		* @returns {Array<DecompressorStream>} - Array of decompressor streams
		*/
		#createDecompressionChain(encodings) {
			const parts = encodings.split(",");
			/** @type {DecompressorStream[]} */
			const decompressors = [];
			for (let i = parts.length - 1; i >= 0; i--) {
				const encoding = parts[i].trim();
				if (!encoding) continue;
				if (!supportedEncodings[encoding]) {
					decompressors.length = 0;
					return decompressors;
				}
				decompressors.push(supportedEncodings[encoding]());
			}
			return decompressors;
		}
		/**
		* Sets up event handlers for a decompressor stream using readable events
		* @param {DecompressorStream} decompressor - The decompressor stream
		* @param {Controller} controller - The controller to coordinate with
		* @returns {void}
		*/
		#setupDecompressorEvents(decompressor, controller) {
			decompressor.on("readable", () => {
				let chunk;
				while ((chunk = decompressor.read()) !== null) if (super.onResponseData(controller, chunk) === false) break;
			});
			decompressor.on("error", (error) => {
				super.onResponseError(controller, error);
			});
		}
		/**
		* Sets up event handling for a single decompressor
		* @param {Controller} controller - The controller to handle events
		* @returns {void}
		*/
		#setupSingleDecompressor(controller) {
			const decompressor = this.#decompressors[0];
			this.#setupDecompressorEvents(decompressor, controller);
			decompressor.on("end", () => {
				super.onResponseEnd(controller, {});
			});
		}
		/**
		* Sets up event handling for multiple chained decompressors using pipeline
		* @param {Controller} controller - The controller to handle events
		* @returns {void}
		*/
		#setupMultipleDecompressors(controller) {
			const lastDecompressor = this.#decompressors[this.#decompressors.length - 1];
			this.#setupDecompressorEvents(lastDecompressor, controller);
			this.#pipelineStream = pipeline$2(this.#decompressors, (err) => {
				if (err) {
					super.onResponseError(controller, err);
					return;
				}
				super.onResponseEnd(controller, {});
			});
		}
		/**
		* Cleans up decompressor references to prevent memory leaks
		* @returns {void}
		*/
		#cleanupDecompressors() {
			this.#decompressors.length = 0;
			this.#pipelineStream = null;
		}
		/**
		* @param {Controller} controller
		* @param {number} statusCode
		* @param {Record<string, string | string[] | undefined>} headers
		* @param {string} statusMessage
		* @returns {void}
		*/
		onResponseStart(controller, statusCode, headers, statusMessage) {
			const contentEncoding = headers["content-encoding"];
			if (this.#shouldSkipDecompression(contentEncoding, statusCode)) return super.onResponseStart(controller, statusCode, headers, statusMessage);
			const decompressors = this.#createDecompressionChain(contentEncoding.toLowerCase());
			if (decompressors.length === 0) {
				this.#cleanupDecompressors();
				return super.onResponseStart(controller, statusCode, headers, statusMessage);
			}
			this.#decompressors = decompressors;
			const { "content-encoding": _, "content-length": __,...newHeaders } = headers;
			if (this.#decompressors.length === 1) this.#setupSingleDecompressor(controller);
			else this.#setupMultipleDecompressors(controller);
			super.onResponseStart(controller, statusCode, newHeaders, statusMessage);
		}
		/**
		* @param {Controller} controller
		* @param {Buffer} chunk
		* @returns {void}
		*/
		onResponseData(controller, chunk) {
			if (this.#decompressors.length > 0) {
				this.#decompressors[0].write(chunk);
				return;
			}
			super.onResponseData(controller, chunk);
		}
		/**
		* @param {Controller} controller
		* @param {Record<string, string | string[]> | undefined} trailers
		* @returns {void}
		*/
		onResponseEnd(controller, trailers) {
			if (this.#decompressors.length > 0) {
				this.#decompressors[0].end();
				this.#cleanupDecompressors();
				return;
			}
			super.onResponseEnd(controller, trailers);
		}
		/**
		* @param {Controller} controller
		* @param {Error} err
		* @returns {void}
		*/
		onResponseError(controller, err) {
			if (this.#decompressors.length > 0) {
				for (const decompressor of this.#decompressors) decompressor.destroy(err);
				this.#cleanupDecompressors();
			}
			super.onResponseError(controller, err);
		}
	};
	/**
	* Creates a decompression interceptor for HTTP responses
	* @param {DecompressHandlerOptions} [options] - Options for the interceptor
	* @returns {Function} - Interceptor function
	*/
	function createDecompressInterceptor(options = {}) {
		if (!warningEmitted) {
			process.emitWarning("DecompressInterceptor is experimental and subject to change", "ExperimentalWarning");
			warningEmitted = true;
		}
		return (dispatch) => {
			return (opts, handler) => {
				const decompressHandler = new DecompressHandler(handler, options);
				return dispatch(opts, decompressHandler);
			};
		};
	}
	module.exports = createDecompressInterceptor;
}) });

//#endregion
//#region node_modules/undici/lib/cache/sqlite-cache-store.js
var require_sqlite_cache_store = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/cache/sqlite-cache-store.js": ((exports, module) => {
	const { Writable: Writable$1 } = __require("node:stream");
	const { assertCacheKey, assertCacheValue } = require_cache$2();
	let DatabaseSync;
	const VERSION = 3;
	const MAX_ENTRY_SIZE = 2 * 1e3 * 1e3 * 1e3;
	/**
	* @typedef {import('../../types/cache-interceptor.d.ts').default.CacheStore} CacheStore
	* @implements {CacheStore}
	*
	* @typedef {{
	*  id: Readonly<number>,
	*  body?: Uint8Array
	*  statusCode: number
	*  statusMessage: string
	*  headers?: string
	*  vary?: string
	*  etag?: string
	*  cacheControlDirectives?: string
	*  cachedAt: number
	*  staleAt: number
	*  deleteAt: number
	* }} SqliteStoreValue
	*/
	module.exports = class SqliteCacheStore$1 {
		#maxEntrySize = MAX_ENTRY_SIZE;
		#maxCount = Infinity;
		/**
		* @type {import('node:sqlite').DatabaseSync}
		*/
		#db;
		/**
		* @type {import('node:sqlite').StatementSync}
		*/
		#getValuesQuery;
		/**
		* @type {import('node:sqlite').StatementSync}
		*/
		#updateValueQuery;
		/**
		* @type {import('node:sqlite').StatementSync}
		*/
		#insertValueQuery;
		/**
		* @type {import('node:sqlite').StatementSync}
		*/
		#deleteExpiredValuesQuery;
		/**
		* @type {import('node:sqlite').StatementSync}
		*/
		#deleteByUrlQuery;
		/**
		* @type {import('node:sqlite').StatementSync}
		*/
		#countEntriesQuery;
		/**
		* @type {import('node:sqlite').StatementSync | null}
		*/
		#deleteOldValuesQuery;
		/**
		* @param {import('../../types/cache-interceptor.d.ts').default.SqliteCacheStoreOpts | undefined} opts
		*/
		constructor(opts) {
			if (opts) {
				if (typeof opts !== "object") throw new TypeError("SqliteCacheStore options must be an object");
				if (opts.maxEntrySize !== void 0) {
					if (typeof opts.maxEntrySize !== "number" || !Number.isInteger(opts.maxEntrySize) || opts.maxEntrySize < 0) throw new TypeError("SqliteCacheStore options.maxEntrySize must be a non-negative integer");
					if (opts.maxEntrySize > MAX_ENTRY_SIZE) throw new TypeError("SqliteCacheStore options.maxEntrySize must be less than 2gb");
					this.#maxEntrySize = opts.maxEntrySize;
				}
				if (opts.maxCount !== void 0) {
					if (typeof opts.maxCount !== "number" || !Number.isInteger(opts.maxCount) || opts.maxCount < 0) throw new TypeError("SqliteCacheStore options.maxCount must be a non-negative integer");
					this.#maxCount = opts.maxCount;
				}
			}
			if (!DatabaseSync) DatabaseSync = __require("node:sqlite").DatabaseSync;
			this.#db = new DatabaseSync(opts?.location ?? ":memory:");
			this.#db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      PRAGMA temp_store = memory;
      PRAGMA optimize;

      CREATE TABLE IF NOT EXISTS cacheInterceptorV${VERSION} (
        -- Data specific to us
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT NOT NULL,
        method TEXT NOT NULL,

        -- Data returned to the interceptor
        body BUF NULL,
        deleteAt INTEGER NOT NULL,
        statusCode INTEGER NOT NULL,
        statusMessage TEXT NOT NULL,
        headers TEXT NULL,
        cacheControlDirectives TEXT NULL,
        etag TEXT NULL,
        vary TEXT NULL,
        cachedAt INTEGER NOT NULL,
        staleAt INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_cacheInterceptorV${VERSION}_getValuesQuery ON cacheInterceptorV${VERSION}(url, method, deleteAt);
      CREATE INDEX IF NOT EXISTS idx_cacheInterceptorV${VERSION}_deleteByUrlQuery ON cacheInterceptorV${VERSION}(deleteAt);
    `);
			this.#getValuesQuery = this.#db.prepare(`
      SELECT
        id,
        body,
        deleteAt,
        statusCode,
        statusMessage,
        headers,
        etag,
        cacheControlDirectives,
        vary,
        cachedAt,
        staleAt
      FROM cacheInterceptorV${VERSION}
      WHERE
        url = ?
        AND method = ?
      ORDER BY
        deleteAt ASC
    `);
			this.#updateValueQuery = this.#db.prepare(`
      UPDATE cacheInterceptorV${VERSION} SET
        body = ?,
        deleteAt = ?,
        statusCode = ?,
        statusMessage = ?,
        headers = ?,
        etag = ?,
        cacheControlDirectives = ?,
        cachedAt = ?,
        staleAt = ?
      WHERE
        id = ?
    `);
			this.#insertValueQuery = this.#db.prepare(`
      INSERT INTO cacheInterceptorV${VERSION} (
        url,
        method,
        body,
        deleteAt,
        statusCode,
        statusMessage,
        headers,
        etag,
        cacheControlDirectives,
        vary,
        cachedAt,
        staleAt
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
			this.#deleteByUrlQuery = this.#db.prepare(`DELETE FROM cacheInterceptorV${VERSION} WHERE url = ?`);
			this.#countEntriesQuery = this.#db.prepare(`SELECT COUNT(*) AS total FROM cacheInterceptorV${VERSION}`);
			this.#deleteExpiredValuesQuery = this.#db.prepare(`DELETE FROM cacheInterceptorV${VERSION} WHERE deleteAt <= ?`);
			this.#deleteOldValuesQuery = this.#maxCount === Infinity ? null : this.#db.prepare(`
        DELETE FROM cacheInterceptorV${VERSION}
        WHERE id IN (
          SELECT
            id
          FROM cacheInterceptorV${VERSION}
          ORDER BY cachedAt DESC
          LIMIT ?
        )
      `);
		}
		close() {
			this.#db.close();
		}
		/**
		* @param {import('../../types/cache-interceptor.d.ts').default.CacheKey} key
		* @returns {(import('../../types/cache-interceptor.d.ts').default.GetResult & { body?: Buffer }) | undefined}
		*/
		get(key) {
			assertCacheKey(key);
			const value = this.#findValue(key);
			return value ? {
				body: value.body ? Buffer.from(value.body.buffer, value.body.byteOffset, value.body.byteLength) : void 0,
				statusCode: value.statusCode,
				statusMessage: value.statusMessage,
				headers: value.headers ? JSON.parse(value.headers) : void 0,
				etag: value.etag ? value.etag : void 0,
				vary: value.vary ? JSON.parse(value.vary) : void 0,
				cacheControlDirectives: value.cacheControlDirectives ? JSON.parse(value.cacheControlDirectives) : void 0,
				cachedAt: value.cachedAt,
				staleAt: value.staleAt,
				deleteAt: value.deleteAt
			} : void 0;
		}
		/**
		* @param {import('../../types/cache-interceptor.d.ts').default.CacheKey} key
		* @param {import('../../types/cache-interceptor.d.ts').default.CacheValue & { body: null | Buffer | Array<Buffer>}} value
		*/
		set(key, value) {
			assertCacheKey(key);
			const url = this.#makeValueUrl(key);
			const body = Array.isArray(value.body) ? Buffer.concat(value.body) : value.body;
			const size = body?.byteLength;
			if (size && size > this.#maxEntrySize) return;
			const existingValue = this.#findValue(key, true);
			if (existingValue) this.#updateValueQuery.run(body, value.deleteAt, value.statusCode, value.statusMessage, value.headers ? JSON.stringify(value.headers) : null, value.etag ? value.etag : null, value.cacheControlDirectives ? JSON.stringify(value.cacheControlDirectives) : null, value.cachedAt, value.staleAt, existingValue.id);
			else {
				this.#prune();
				this.#insertValueQuery.run(url, key.method, body, value.deleteAt, value.statusCode, value.statusMessage, value.headers ? JSON.stringify(value.headers) : null, value.etag ? value.etag : null, value.cacheControlDirectives ? JSON.stringify(value.cacheControlDirectives) : null, value.vary ? JSON.stringify(value.vary) : null, value.cachedAt, value.staleAt);
			}
		}
		/**
		* @param {import('../../types/cache-interceptor.d.ts').default.CacheKey} key
		* @param {import('../../types/cache-interceptor.d.ts').default.CacheValue} value
		* @returns {Writable | undefined}
		*/
		createWriteStream(key, value) {
			assertCacheKey(key);
			assertCacheValue(value);
			let size = 0;
			/**
			* @type {Buffer[] | null}
			*/
			const body = [];
			const store = this;
			return new Writable$1({
				decodeStrings: true,
				write(chunk, encoding, callback) {
					size += chunk.byteLength;
					if (size < store.#maxEntrySize) body.push(chunk);
					else this.destroy();
					callback();
				},
				final(callback) {
					store.set(key, {
						...value,
						body
					});
					callback();
				}
			});
		}
		/**
		* @param {import('../../types/cache-interceptor.d.ts').default.CacheKey} key
		*/
		delete(key) {
			if (typeof key !== "object") throw new TypeError(`expected key to be object, got ${typeof key}`);
			this.#deleteByUrlQuery.run(this.#makeValueUrl(key));
		}
		#prune() {
			if (Number.isFinite(this.#maxCount) && this.size <= this.#maxCount) return 0;
			{
				const removed = this.#deleteExpiredValuesQuery.run(Date.now()).changes;
				if (removed) return removed;
			}
			{
				const removed = this.#deleteOldValuesQuery?.run(Math.max(Math.floor(this.#maxCount * .1), 1)).changes;
				if (removed) return removed;
			}
			return 0;
		}
		/**
		* Counts the number of rows in the cache
		* @returns {Number}
		*/
		get size() {
			const { total } = this.#countEntriesQuery.get();
			return total;
		}
		/**
		* @param {import('../../types/cache-interceptor.d.ts').default.CacheKey} key
		* @returns {string}
		*/
		#makeValueUrl(key) {
			return `${key.origin}/${key.path}`;
		}
		/**
		* @param {import('../../types/cache-interceptor.d.ts').default.CacheKey} key
		* @param {boolean} [canBeExpired=false]
		* @returns {SqliteStoreValue | undefined}
		*/
		#findValue(key, canBeExpired = false) {
			const url = this.#makeValueUrl(key);
			const { headers, method } = key;
			/**
			* @type {SqliteStoreValue[]}
			*/
			const values = this.#getValuesQuery.all(url, method);
			if (values.length === 0) return;
			const now = Date.now();
			for (const value of values) {
				if (now >= value.deleteAt && !canBeExpired) return;
				let matches = true;
				if (value.vary) {
					const vary = JSON.parse(value.vary);
					for (const header in vary) if (!headerValueEquals(headers[header], vary[header])) {
						matches = false;
						break;
					}
				}
				if (matches) return value;
			}
		}
	};
	/**
	* @param {string|string[]|null|undefined} lhs
	* @param {string|string[]|null|undefined} rhs
	* @returns {boolean}
	*/
	function headerValueEquals(lhs, rhs) {
		if (lhs == null && rhs == null) return true;
		if (lhs == null && rhs != null || lhs != null && rhs == null) return false;
		if (Array.isArray(lhs) && Array.isArray(rhs)) {
			if (lhs.length !== rhs.length) return false;
			return lhs.every((x, i) => x === rhs[i]);
		}
		return lhs === rhs;
	}
}) });

//#endregion
//#region node_modules/undici/lib/web/fetch/headers.js
var require_headers = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/fetch/headers.js": ((exports, module) => {
	const { kConstruct: kConstruct$7 } = require_symbols();
	const { kEnumerableProperty: kEnumerableProperty$9 } = require_util$5();
	const { iteratorMixin, isValidHeaderName: isValidHeaderName$1, isValidHeaderValue } = require_util$4();
	const { webidl: webidl$11 } = require_webidl();
	const assert$9 = __require("node:assert");
	const util$3 = __require("node:util");
	/**
	* @param {number} code
	* @returns {code is (0x0a | 0x0d | 0x09 | 0x20)}
	*/
	function isHTTPWhiteSpaceCharCode(code) {
		return code === 10 || code === 13 || code === 9 || code === 32;
	}
	/**
	* @see https://fetch.spec.whatwg.org/#concept-header-value-normalize
	* @param {string} potentialValue
	* @returns {string}
	*/
	function headerValueNormalize(potentialValue) {
		let i = 0;
		let j = potentialValue.length;
		while (j > i && isHTTPWhiteSpaceCharCode(potentialValue.charCodeAt(j - 1))) --j;
		while (j > i && isHTTPWhiteSpaceCharCode(potentialValue.charCodeAt(i))) ++i;
		return i === 0 && j === potentialValue.length ? potentialValue : potentialValue.substring(i, j);
	}
	/**
	* @param {Headers} headers
	* @param {Array|Object} object
	*/
	function fill$1(headers, object) {
		if (Array.isArray(object)) for (let i = 0; i < object.length; ++i) {
			const header = object[i];
			if (header.length !== 2) throw webidl$11.errors.exception({
				header: "Headers constructor",
				message: `expected name/value pair to be length 2, found ${header.length}.`
			});
			appendHeader(headers, header[0], header[1]);
		}
		else if (typeof object === "object" && object !== null) {
			const keys = Object.keys(object);
			for (let i = 0; i < keys.length; ++i) appendHeader(headers, keys[i], object[keys[i]]);
		} else throw webidl$11.errors.conversionFailed({
			prefix: "Headers constructor",
			argument: "Argument 1",
			types: ["sequence<sequence<ByteString>>", "record<ByteString, ByteString>"]
		});
	}
	/**
	* @see https://fetch.spec.whatwg.org/#concept-headers-append
	* @param {Headers} headers
	* @param {string} name
	* @param {string} value
	*/
	function appendHeader(headers, name, value) {
		value = headerValueNormalize(value);
		if (!isValidHeaderName$1(name)) throw webidl$11.errors.invalidArgument({
			prefix: "Headers.append",
			value: name,
			type: "header name"
		});
		else if (!isValidHeaderValue(value)) throw webidl$11.errors.invalidArgument({
			prefix: "Headers.append",
			value,
			type: "header value"
		});
		if (getHeadersGuard$2(headers) === "immutable") throw new TypeError("immutable");
		return getHeadersList$2(headers).append(name, value, false);
	}
	/**
	* @param {Headers} target
	*/
	function headersListSortAndCombine(target) {
		const headersList = getHeadersList$2(target);
		if (!headersList) return [];
		if (headersList.sortedMap) return headersList.sortedMap;
		const headers = [];
		const names = headersList.toSortedArray();
		const cookies = headersList.cookies;
		if (cookies === null || cookies.length === 1) return headersList.sortedMap = names;
		for (let i = 0; i < names.length; ++i) {
			const { 0: name, 1: value } = names[i];
			if (name === "set-cookie") for (let j = 0; j < cookies.length; ++j) headers.push([name, cookies[j]]);
			else headers.push([name, value]);
		}
		return headersList.sortedMap = headers;
	}
	function compareHeaderName(a, b) {
		return a[0] < b[0] ? -1 : 1;
	}
	var HeadersList$3 = class HeadersList$3 {
		/** @type {[string, string][]|null} */
		cookies = null;
		sortedMap;
		headersMap;
		constructor(init) {
			if (init instanceof HeadersList$3) {
				this.headersMap = new Map(init.headersMap);
				this.sortedMap = init.sortedMap;
				this.cookies = init.cookies === null ? null : [...init.cookies];
			} else {
				this.headersMap = new Map(init);
				this.sortedMap = null;
			}
		}
		/**
		* @see https://fetch.spec.whatwg.org/#header-list-contains
		* @param {string} name
		* @param {boolean} isLowerCase
		*/
		contains(name, isLowerCase) {
			return this.headersMap.has(isLowerCase ? name : name.toLowerCase());
		}
		clear() {
			this.headersMap.clear();
			this.sortedMap = null;
			this.cookies = null;
		}
		/**
		* @see https://fetch.spec.whatwg.org/#concept-header-list-append
		* @param {string} name
		* @param {string} value
		* @param {boolean} isLowerCase
		*/
		append(name, value, isLowerCase) {
			this.sortedMap = null;
			const lowercaseName = isLowerCase ? name : name.toLowerCase();
			const exists = this.headersMap.get(lowercaseName);
			if (exists) {
				const delimiter = lowercaseName === "cookie" ? "; " : ", ";
				this.headersMap.set(lowercaseName, {
					name: exists.name,
					value: `${exists.value}${delimiter}${value}`
				});
			} else this.headersMap.set(lowercaseName, {
				name,
				value
			});
			if (lowercaseName === "set-cookie") (this.cookies ??= []).push(value);
		}
		/**
		* @see https://fetch.spec.whatwg.org/#concept-header-list-set
		* @param {string} name
		* @param {string} value
		* @param {boolean} isLowerCase
		*/
		set(name, value, isLowerCase) {
			this.sortedMap = null;
			const lowercaseName = isLowerCase ? name : name.toLowerCase();
			if (lowercaseName === "set-cookie") this.cookies = [value];
			this.headersMap.set(lowercaseName, {
				name,
				value
			});
		}
		/**
		* @see https://fetch.spec.whatwg.org/#concept-header-list-delete
		* @param {string} name
		* @param {boolean} isLowerCase
		*/
		delete(name, isLowerCase) {
			this.sortedMap = null;
			if (!isLowerCase) name = name.toLowerCase();
			if (name === "set-cookie") this.cookies = null;
			this.headersMap.delete(name);
		}
		/**
		* @see https://fetch.spec.whatwg.org/#concept-header-list-get
		* @param {string} name
		* @param {boolean} isLowerCase
		* @returns {string | null}
		*/
		get(name, isLowerCase) {
			return this.headersMap.get(isLowerCase ? name : name.toLowerCase())?.value ?? null;
		}
		*[Symbol.iterator]() {
			for (const { 0: name, 1: { value } } of this.headersMap) yield [name, value];
		}
		get entries() {
			const headers = {};
			if (this.headersMap.size !== 0) for (const { name, value } of this.headersMap.values()) headers[name] = value;
			return headers;
		}
		rawValues() {
			return this.headersMap.values();
		}
		get entriesList() {
			const headers = [];
			if (this.headersMap.size !== 0) for (const { 0: lowerName, 1: { name, value } } of this.headersMap) if (lowerName === "set-cookie") for (const cookie of this.cookies) headers.push([name, cookie]);
			else headers.push([name, value]);
			return headers;
		}
		toSortedArray() {
			const size = this.headersMap.size;
			const array = new Array(size);
			if (size <= 32) {
				if (size === 0) return array;
				const iterator = this.headersMap[Symbol.iterator]();
				const firstValue = iterator.next().value;
				array[0] = [firstValue[0], firstValue[1].value];
				assert$9(firstValue[1].value !== null);
				for (let i = 1, j = 0, right = 0, left = 0, pivot = 0, x, value; i < size; ++i) {
					value = iterator.next().value;
					x = array[i] = [value[0], value[1].value];
					assert$9(x[1] !== null);
					left = 0;
					right = i;
					while (left < right) {
						pivot = left + (right - left >> 1);
						if (array[pivot][0] <= x[0]) left = pivot + 1;
						else right = pivot;
					}
					if (i !== pivot) {
						j = i;
						while (j > left) array[j] = array[--j];
						array[left] = x;
					}
				}
				/* c8 ignore next 4 */
				if (!iterator.next().done) throw new TypeError("Unreachable");
				return array;
			} else {
				let i = 0;
				for (const { 0: name, 1: { value } } of this.headersMap) {
					array[i++] = [name, value];
					assert$9(value !== null);
				}
				return array.sort(compareHeaderName);
			}
		}
	};
	var Headers$4 = class Headers$4 {
		#guard;
		/**
		* @type {HeadersList}
		*/
		#headersList;
		/**
		* @param {HeadersInit|Symbol} [init]
		* @returns
		*/
		constructor(init = void 0) {
			webidl$11.util.markAsUncloneable(this);
			if (init === kConstruct$7) return;
			this.#headersList = new HeadersList$3();
			this.#guard = "none";
			if (init !== void 0) {
				init = webidl$11.converters.HeadersInit(init, "Headers constructor", "init");
				fill$1(this, init);
			}
		}
		append(name, value) {
			webidl$11.brandCheck(this, Headers$4);
			webidl$11.argumentLengthCheck(arguments, 2, "Headers.append");
			const prefix = "Headers.append";
			name = webidl$11.converters.ByteString(name, prefix, "name");
			value = webidl$11.converters.ByteString(value, prefix, "value");
			return appendHeader(this, name, value);
		}
		delete(name) {
			webidl$11.brandCheck(this, Headers$4);
			webidl$11.argumentLengthCheck(arguments, 1, "Headers.delete");
			name = webidl$11.converters.ByteString(name, "Headers.delete", "name");
			if (!isValidHeaderName$1(name)) throw webidl$11.errors.invalidArgument({
				prefix: "Headers.delete",
				value: name,
				type: "header name"
			});
			if (this.#guard === "immutable") throw new TypeError("immutable");
			if (!this.#headersList.contains(name, false)) return;
			this.#headersList.delete(name, false);
		}
		get(name) {
			webidl$11.brandCheck(this, Headers$4);
			webidl$11.argumentLengthCheck(arguments, 1, "Headers.get");
			const prefix = "Headers.get";
			name = webidl$11.converters.ByteString(name, prefix, "name");
			if (!isValidHeaderName$1(name)) throw webidl$11.errors.invalidArgument({
				prefix,
				value: name,
				type: "header name"
			});
			return this.#headersList.get(name, false);
		}
		has(name) {
			webidl$11.brandCheck(this, Headers$4);
			webidl$11.argumentLengthCheck(arguments, 1, "Headers.has");
			const prefix = "Headers.has";
			name = webidl$11.converters.ByteString(name, prefix, "name");
			if (!isValidHeaderName$1(name)) throw webidl$11.errors.invalidArgument({
				prefix,
				value: name,
				type: "header name"
			});
			return this.#headersList.contains(name, false);
		}
		set(name, value) {
			webidl$11.brandCheck(this, Headers$4);
			webidl$11.argumentLengthCheck(arguments, 2, "Headers.set");
			const prefix = "Headers.set";
			name = webidl$11.converters.ByteString(name, prefix, "name");
			value = webidl$11.converters.ByteString(value, prefix, "value");
			value = headerValueNormalize(value);
			if (!isValidHeaderName$1(name)) throw webidl$11.errors.invalidArgument({
				prefix,
				value: name,
				type: "header name"
			});
			else if (!isValidHeaderValue(value)) throw webidl$11.errors.invalidArgument({
				prefix,
				value,
				type: "header value"
			});
			if (this.#guard === "immutable") throw new TypeError("immutable");
			this.#headersList.set(name, value, false);
		}
		getSetCookie() {
			webidl$11.brandCheck(this, Headers$4);
			const list = this.#headersList.cookies;
			if (list) return [...list];
			return [];
		}
		[util$3.inspect.custom](depth, options) {
			options.depth ??= depth;
			return `Headers ${util$3.formatWithOptions(options, this.#headersList.entries)}`;
		}
		static getHeadersGuard(o) {
			return o.#guard;
		}
		static setHeadersGuard(o, guard) {
			o.#guard = guard;
		}
		/**
		* @param {Headers} o
		*/
		static getHeadersList(o) {
			return o.#headersList;
		}
		/**
		* @param {Headers} target
		* @param {HeadersList} list
		*/
		static setHeadersList(target, list) {
			target.#headersList = list;
		}
	};
	const { getHeadersGuard: getHeadersGuard$2, setHeadersGuard: setHeadersGuard$2, getHeadersList: getHeadersList$2, setHeadersList: setHeadersList$2 } = Headers$4;
	Reflect.deleteProperty(Headers$4, "getHeadersGuard");
	Reflect.deleteProperty(Headers$4, "setHeadersGuard");
	Reflect.deleteProperty(Headers$4, "getHeadersList");
	Reflect.deleteProperty(Headers$4, "setHeadersList");
	iteratorMixin("Headers", Headers$4, headersListSortAndCombine, 0, 1);
	Object.defineProperties(Headers$4.prototype, {
		append: kEnumerableProperty$9,
		delete: kEnumerableProperty$9,
		get: kEnumerableProperty$9,
		has: kEnumerableProperty$9,
		set: kEnumerableProperty$9,
		getSetCookie: kEnumerableProperty$9,
		[Symbol.toStringTag]: {
			value: "Headers",
			configurable: true
		},
		[util$3.inspect.custom]: { enumerable: false }
	});
	webidl$11.converters.HeadersInit = function(V, prefix, argument) {
		if (webidl$11.util.Type(V) === webidl$11.util.Types.OBJECT) {
			const iterator = Reflect.get(V, Symbol.iterator);
			if (!util$3.types.isProxy(V) && iterator === Headers$4.prototype.entries) try {
				return getHeadersList$2(V).entriesList;
			} catch {}
			if (typeof iterator === "function") return webidl$11.converters["sequence<sequence<ByteString>>"](V, prefix, argument, iterator.bind(V));
			return webidl$11.converters["record<ByteString, ByteString>"](V, prefix, argument);
		}
		throw webidl$11.errors.conversionFailed({
			prefix: "Headers constructor",
			argument: "Argument 1",
			types: ["sequence<sequence<ByteString>>", "record<ByteString, ByteString>"]
		});
	};
	module.exports = {
		fill: fill$1,
		compareHeaderName,
		Headers: Headers$4,
		HeadersList: HeadersList$3,
		getHeadersGuard: getHeadersGuard$2,
		setHeadersGuard: setHeadersGuard$2,
		setHeadersList: setHeadersList$2,
		getHeadersList: getHeadersList$2
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/fetch/response.js
var require_response = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/fetch/response.js": ((exports, module) => {
	const { Headers: Headers$3, HeadersList: HeadersList$2, fill, getHeadersGuard: getHeadersGuard$1, setHeadersGuard: setHeadersGuard$1, setHeadersList: setHeadersList$1 } = require_headers();
	const { extractBody: extractBody$2, cloneBody: cloneBody$1, mixinBody: mixinBody$1, streamRegistry, bodyUnusable: bodyUnusable$1 } = require_body();
	const util$2 = require_util$5();
	const nodeUtil$1 = __require("node:util");
	const { kEnumerableProperty: kEnumerableProperty$8 } = util$2;
	const { isValidReasonPhrase, isCancelled: isCancelled$1, isAborted: isAborted$1, serializeJavascriptValueToJSONString, isErrorLike: isErrorLike$1, isomorphicEncode: isomorphicEncode$1, environmentSettingsObject: relevantRealm } = require_util$4();
	const { redirectStatusSet: redirectStatusSet$1, nullBodyStatus: nullBodyStatus$1 } = require_constants$2();
	const { webidl: webidl$10 } = require_webidl();
	const { URLSerializer: URLSerializer$3 } = require_data_url();
	const { kConstruct: kConstruct$6 } = require_symbols();
	const assert$8 = __require("node:assert");
	const { isArrayBuffer: isArrayBuffer$2 } = nodeUtil$1.types;
	const textEncoder = new TextEncoder("utf-8");
	var Response = class Response {
		/** @type {Headers} */
		#headers;
		#state;
		static error() {
			return fromInnerResponse$2(makeNetworkError$1(), "immutable");
		}
		static json(data, init = void 0) {
			webidl$10.argumentLengthCheck(arguments, 1, "Response.json");
			if (init !== null) init = webidl$10.converters.ResponseInit(init);
			const bytes = textEncoder.encode(serializeJavascriptValueToJSONString(data));
			const body = extractBody$2(bytes);
			const responseObject = fromInnerResponse$2(makeResponse$1({}), "response");
			initializeResponse(responseObject, init, {
				body: body[0],
				type: "application/json"
			});
			return responseObject;
		}
		static redirect(url, status = 302) {
			webidl$10.argumentLengthCheck(arguments, 1, "Response.redirect");
			url = webidl$10.converters.USVString(url);
			status = webidl$10.converters["unsigned short"](status);
			let parsedURL;
			try {
				parsedURL = new URL(url, relevantRealm.settingsObject.baseUrl);
			} catch (err) {
				throw new TypeError(`Failed to parse URL from ${url}`, { cause: err });
			}
			if (!redirectStatusSet$1.has(status)) throw new RangeError(`Invalid status code ${status}`);
			const responseObject = fromInnerResponse$2(makeResponse$1({}), "immutable");
			responseObject.#state.status = status;
			const value = isomorphicEncode$1(URLSerializer$3(parsedURL));
			responseObject.#state.headersList.append("location", value, true);
			return responseObject;
		}
		constructor(body = null, init = void 0) {
			webidl$10.util.markAsUncloneable(this);
			if (body === kConstruct$6) return;
			if (body !== null) body = webidl$10.converters.BodyInit(body);
			init = webidl$10.converters.ResponseInit(init);
			this.#state = makeResponse$1({});
			this.#headers = new Headers$3(kConstruct$6);
			setHeadersGuard$1(this.#headers, "response");
			setHeadersList$1(this.#headers, this.#state.headersList);
			let bodyWithType = null;
			if (body != null) {
				const [extractedBody, type] = extractBody$2(body);
				bodyWithType = {
					body: extractedBody,
					type
				};
			}
			initializeResponse(this, init, bodyWithType);
		}
		get type() {
			webidl$10.brandCheck(this, Response);
			return this.#state.type;
		}
		get url() {
			webidl$10.brandCheck(this, Response);
			const urlList = this.#state.urlList;
			const url = urlList[urlList.length - 1] ?? null;
			if (url === null) return "";
			return URLSerializer$3(url, true);
		}
		get redirected() {
			webidl$10.brandCheck(this, Response);
			return this.#state.urlList.length > 1;
		}
		get status() {
			webidl$10.brandCheck(this, Response);
			return this.#state.status;
		}
		get ok() {
			webidl$10.brandCheck(this, Response);
			return this.#state.status >= 200 && this.#state.status <= 299;
		}
		get statusText() {
			webidl$10.brandCheck(this, Response);
			return this.#state.statusText;
		}
		get headers() {
			webidl$10.brandCheck(this, Response);
			return this.#headers;
		}
		get body() {
			webidl$10.brandCheck(this, Response);
			return this.#state.body ? this.#state.body.stream : null;
		}
		get bodyUsed() {
			webidl$10.brandCheck(this, Response);
			return !!this.#state.body && util$2.isDisturbed(this.#state.body.stream);
		}
		clone() {
			webidl$10.brandCheck(this, Response);
			if (bodyUnusable$1(this.#state)) throw webidl$10.errors.exception({
				header: "Response.clone",
				message: "Body has already been consumed."
			});
			const clonedResponse = cloneResponse$1(this.#state);
			if (this.#state.body?.stream) streamRegistry.register(this, new WeakRef(this.#state.body.stream));
			return fromInnerResponse$2(clonedResponse, getHeadersGuard$1(this.#headers));
		}
		[nodeUtil$1.inspect.custom](depth, options) {
			if (options.depth === null) options.depth = 2;
			options.colors ??= true;
			const properties = {
				status: this.status,
				statusText: this.statusText,
				headers: this.headers,
				body: this.body,
				bodyUsed: this.bodyUsed,
				ok: this.ok,
				redirected: this.redirected,
				type: this.type,
				url: this.url
			};
			return `Response ${nodeUtil$1.formatWithOptions(options, properties)}`;
		}
		/**
		* @param {Response} response
		*/
		static getResponseHeaders(response) {
			return response.#headers;
		}
		/**
		* @param {Response} response
		* @param {Headers} newHeaders
		*/
		static setResponseHeaders(response, newHeaders) {
			response.#headers = newHeaders;
		}
		/**
		* @param {Response} response
		*/
		static getResponseState(response) {
			return response.#state;
		}
		/**
		* @param {Response} response
		* @param {any} newState
		*/
		static setResponseState(response, newState) {
			response.#state = newState;
		}
	};
	const { getResponseHeaders, setResponseHeaders, getResponseState: getResponseState$2, setResponseState } = Response;
	Reflect.deleteProperty(Response, "getResponseHeaders");
	Reflect.deleteProperty(Response, "setResponseHeaders");
	Reflect.deleteProperty(Response, "getResponseState");
	Reflect.deleteProperty(Response, "setResponseState");
	mixinBody$1(Response, getResponseState$2);
	Object.defineProperties(Response.prototype, {
		type: kEnumerableProperty$8,
		url: kEnumerableProperty$8,
		status: kEnumerableProperty$8,
		ok: kEnumerableProperty$8,
		redirected: kEnumerableProperty$8,
		statusText: kEnumerableProperty$8,
		headers: kEnumerableProperty$8,
		clone: kEnumerableProperty$8,
		body: kEnumerableProperty$8,
		bodyUsed: kEnumerableProperty$8,
		[Symbol.toStringTag]: {
			value: "Response",
			configurable: true
		}
	});
	Object.defineProperties(Response, {
		json: kEnumerableProperty$8,
		redirect: kEnumerableProperty$8,
		error: kEnumerableProperty$8
	});
	function cloneResponse$1(response) {
		if (response.internalResponse) return filterResponse$1(cloneResponse$1(response.internalResponse), response.type);
		const newResponse = makeResponse$1({
			...response,
			body: null
		});
		if (response.body != null) newResponse.body = cloneBody$1(response.body);
		return newResponse;
	}
	function makeResponse$1(init) {
		return {
			aborted: false,
			rangeRequested: false,
			timingAllowPassed: false,
			requestIncludesCredentials: false,
			type: "default",
			status: 200,
			timingInfo: null,
			cacheState: "",
			statusText: "",
			...init,
			headersList: init?.headersList ? new HeadersList$2(init?.headersList) : new HeadersList$2(),
			urlList: init?.urlList ? [...init.urlList] : []
		};
	}
	function makeNetworkError$1(reason) {
		const isError = isErrorLike$1(reason);
		return makeResponse$1({
			type: "error",
			status: 0,
			error: isError ? reason : new Error(reason ? String(reason) : reason),
			aborted: reason && reason.name === "AbortError"
		});
	}
	function isNetworkError$1(response) {
		return response.type === "error" && response.status === 0;
	}
	function makeFilteredResponse(response, state) {
		state = {
			internalResponse: response,
			...state
		};
		return new Proxy(response, {
			get(target, p) {
				return p in state ? state[p] : target[p];
			},
			set(target, p, value) {
				assert$8(!(p in state));
				target[p] = value;
				return true;
			}
		});
	}
	function filterResponse$1(response, type) {
		if (type === "basic") return makeFilteredResponse(response, {
			type: "basic",
			headersList: response.headersList
		});
		else if (type === "cors") return makeFilteredResponse(response, {
			type: "cors",
			headersList: response.headersList
		});
		else if (type === "opaque") return makeFilteredResponse(response, {
			type: "opaque",
			urlList: Object.freeze([]),
			status: 0,
			statusText: "",
			body: null
		});
		else if (type === "opaqueredirect") return makeFilteredResponse(response, {
			type: "opaqueredirect",
			status: 0,
			statusText: "",
			headersList: [],
			body: null
		});
		else assert$8(false);
	}
	function makeAppropriateNetworkError$1(fetchParams, err = null) {
		assert$8(isCancelled$1(fetchParams));
		return isAborted$1(fetchParams) ? makeNetworkError$1(Object.assign(new DOMException("The operation was aborted.", "AbortError"), { cause: err })) : makeNetworkError$1(Object.assign(new DOMException("Request was cancelled."), { cause: err }));
	}
	function initializeResponse(response, init, body) {
		if (init.status !== null && (init.status < 200 || init.status > 599)) throw new RangeError("init[\"status\"] must be in the range of 200 to 599, inclusive.");
		if ("statusText" in init && init.statusText != null) {
			if (!isValidReasonPhrase(String(init.statusText))) throw new TypeError("Invalid statusText");
		}
		if ("status" in init && init.status != null) getResponseState$2(response).status = init.status;
		if ("statusText" in init && init.statusText != null) getResponseState$2(response).statusText = init.statusText;
		if ("headers" in init && init.headers != null) fill(getResponseHeaders(response), init.headers);
		if (body) {
			if (nullBodyStatus$1.includes(response.status)) throw webidl$10.errors.exception({
				header: "Response constructor",
				message: `Invalid response status code ${response.status}`
			});
			getResponseState$2(response).body = body.body;
			if (body.type != null && !getResponseState$2(response).headersList.contains("content-type", true)) getResponseState$2(response).headersList.append("content-type", body.type, true);
		}
	}
	/**
	* @see https://fetch.spec.whatwg.org/#response-create
	* @param {any} innerResponse
	* @param {'request' | 'immutable' | 'request-no-cors' | 'response' | 'none'} guard
	* @returns {Response}
	*/
	function fromInnerResponse$2(innerResponse, guard) {
		const response = new Response(kConstruct$6);
		setResponseState(response, innerResponse);
		const headers = new Headers$3(kConstruct$6);
		setResponseHeaders(response, headers);
		setHeadersList$1(headers, innerResponse.headersList);
		setHeadersGuard$1(headers, guard);
		if (innerResponse.body?.stream) streamRegistry.register(response, new WeakRef(innerResponse.body.stream));
		return response;
	}
	webidl$10.converters.XMLHttpRequestBodyInit = function(V, prefix, name) {
		if (typeof V === "string") return webidl$10.converters.USVString(V, prefix, name);
		if (webidl$10.is.Blob(V)) return V;
		if (ArrayBuffer.isView(V) || isArrayBuffer$2(V)) return V;
		if (webidl$10.is.FormData(V)) return V;
		if (webidl$10.is.URLSearchParams(V)) return V;
		return webidl$10.converters.DOMString(V, prefix, name);
	};
	webidl$10.converters.BodyInit = function(V, prefix, argument) {
		if (webidl$10.is.ReadableStream(V)) return V;
		if (V?.[Symbol.asyncIterator]) return V;
		return webidl$10.converters.XMLHttpRequestBodyInit(V, prefix, argument);
	};
	webidl$10.converters.ResponseInit = webidl$10.dictionaryConverter([
		{
			key: "status",
			converter: webidl$10.converters["unsigned short"],
			defaultValue: () => 200
		},
		{
			key: "statusText",
			converter: webidl$10.converters.ByteString,
			defaultValue: () => ""
		},
		{
			key: "headers",
			converter: webidl$10.converters.HeadersInit
		}
	]);
	webidl$10.is.Response = webidl$10.util.MakeTypeAssertion(Response);
	module.exports = {
		isNetworkError: isNetworkError$1,
		makeNetworkError: makeNetworkError$1,
		makeResponse: makeResponse$1,
		makeAppropriateNetworkError: makeAppropriateNetworkError$1,
		filterResponse: filterResponse$1,
		Response,
		cloneResponse: cloneResponse$1,
		fromInnerResponse: fromInnerResponse$2,
		getResponseState: getResponseState$2
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/fetch/request.js
var require_request = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/fetch/request.js": ((exports, module) => {
	const { extractBody: extractBody$1, mixinBody, cloneBody, bodyUnusable } = require_body();
	const { Headers: Headers$2, fill: fillHeaders, HeadersList: HeadersList$1, setHeadersGuard, getHeadersGuard, setHeadersList, getHeadersList: getHeadersList$1 } = require_headers();
	const util$1 = require_util$5();
	const nodeUtil = __require("node:util");
	const { isValidHTTPToken, sameOrigin: sameOrigin$1, environmentSettingsObject: environmentSettingsObject$3 } = require_util$4();
	const { forbiddenMethodsSet, corsSafeListedMethodsSet, referrerPolicy, requestRedirect, requestMode, requestCredentials, requestCache, requestDuplex } = require_constants$2();
	const { kEnumerableProperty: kEnumerableProperty$7, normalizedMethodRecordsBase, normalizedMethodRecords } = util$1;
	const { webidl: webidl$9 } = require_webidl();
	const { URLSerializer: URLSerializer$2 } = require_data_url();
	const { kConstruct: kConstruct$5 } = require_symbols();
	const assert$7 = __require("node:assert");
	const { getMaxListeners, setMaxListeners, defaultMaxListeners } = __require("node:events");
	const kAbortController = Symbol("abortController");
	const requestFinalizer = new FinalizationRegistry(({ signal, abort: abort$1 }) => {
		signal.removeEventListener("abort", abort$1);
	});
	const dependentControllerMap = /* @__PURE__ */ new WeakMap();
	let abortSignalHasEventHandlerLeakWarning;
	try {
		abortSignalHasEventHandlerLeakWarning = getMaxListeners(new AbortController().signal) > 0;
	} catch {
		abortSignalHasEventHandlerLeakWarning = false;
	}
	function buildAbort(acRef) {
		return abort$1;
		function abort$1() {
			const ac = acRef.deref();
			if (ac !== void 0) {
				requestFinalizer.unregister(abort$1);
				this.removeEventListener("abort", abort$1);
				ac.abort(this.reason);
				const controllerList = dependentControllerMap.get(ac.signal);
				if (controllerList !== void 0) {
					if (controllerList.size !== 0) {
						for (const ref of controllerList) {
							const ctrl = ref.deref();
							if (ctrl !== void 0) ctrl.abort(this.reason);
						}
						controllerList.clear();
					}
					dependentControllerMap.delete(ac.signal);
				}
			}
		}
	}
	let patchMethodWarning = false;
	var Request$2 = class Request$2 {
		/** @type {AbortSignal} */
		#signal;
		/** @type {import('../../dispatcher/dispatcher')} */
		#dispatcher;
		/** @type {Headers} */
		#headers;
		#state;
		constructor(input, init = void 0) {
			webidl$9.util.markAsUncloneable(this);
			if (input === kConstruct$5) return;
			webidl$9.argumentLengthCheck(arguments, 1, "Request constructor");
			input = webidl$9.converters.RequestInfo(input);
			init = webidl$9.converters.RequestInit(init);
			let request$2 = null;
			let fallbackMode = null;
			const baseUrl = environmentSettingsObject$3.settingsObject.baseUrl;
			let signal = null;
			if (typeof input === "string") {
				this.#dispatcher = init.dispatcher;
				let parsedURL;
				try {
					parsedURL = new URL(input, baseUrl);
				} catch (err) {
					throw new TypeError("Failed to parse URL from " + input, { cause: err });
				}
				if (parsedURL.username || parsedURL.password) throw new TypeError("Request cannot be constructed from a URL that includes credentials: " + input);
				request$2 = makeRequest$2({ urlList: [parsedURL] });
				fallbackMode = "cors";
			} else {
				assert$7(webidl$9.is.Request(input));
				request$2 = input.#state;
				signal = input.#signal;
				this.#dispatcher = init.dispatcher || input.#dispatcher;
			}
			const origin = environmentSettingsObject$3.settingsObject.origin;
			let window = "client";
			if (request$2.window?.constructor?.name === "EnvironmentSettingsObject" && sameOrigin$1(request$2.window, origin)) window = request$2.window;
			if (init.window != null) throw new TypeError(`'window' option '${window}' must be null`);
			if ("window" in init) window = "no-window";
			request$2 = makeRequest$2({
				method: request$2.method,
				headersList: request$2.headersList,
				unsafeRequest: request$2.unsafeRequest,
				client: environmentSettingsObject$3.settingsObject,
				window,
				priority: request$2.priority,
				origin: request$2.origin,
				referrer: request$2.referrer,
				referrerPolicy: request$2.referrerPolicy,
				mode: request$2.mode,
				credentials: request$2.credentials,
				cache: request$2.cache,
				redirect: request$2.redirect,
				integrity: request$2.integrity,
				keepalive: request$2.keepalive,
				reloadNavigation: request$2.reloadNavigation,
				historyNavigation: request$2.historyNavigation,
				urlList: [...request$2.urlList]
			});
			const initHasKey = Object.keys(init).length !== 0;
			if (initHasKey) {
				if (request$2.mode === "navigate") request$2.mode = "same-origin";
				request$2.reloadNavigation = false;
				request$2.historyNavigation = false;
				request$2.origin = "client";
				request$2.referrer = "client";
				request$2.referrerPolicy = "";
				request$2.url = request$2.urlList[request$2.urlList.length - 1];
				request$2.urlList = [request$2.url];
			}
			if (init.referrer !== void 0) {
				const referrer = init.referrer;
				if (referrer === "") request$2.referrer = "no-referrer";
				else {
					let parsedReferrer;
					try {
						parsedReferrer = new URL(referrer, baseUrl);
					} catch (err) {
						throw new TypeError(`Referrer "${referrer}" is not a valid URL.`, { cause: err });
					}
					if (parsedReferrer.protocol === "about:" && parsedReferrer.hostname === "client" || origin && !sameOrigin$1(parsedReferrer, environmentSettingsObject$3.settingsObject.baseUrl)) request$2.referrer = "client";
					else request$2.referrer = parsedReferrer;
				}
			}
			if (init.referrerPolicy !== void 0) request$2.referrerPolicy = init.referrerPolicy;
			let mode;
			if (init.mode !== void 0) mode = init.mode;
			else mode = fallbackMode;
			if (mode === "navigate") throw webidl$9.errors.exception({
				header: "Request constructor",
				message: "invalid request mode navigate."
			});
			if (mode != null) request$2.mode = mode;
			if (init.credentials !== void 0) request$2.credentials = init.credentials;
			if (init.cache !== void 0) request$2.cache = init.cache;
			if (request$2.cache === "only-if-cached" && request$2.mode !== "same-origin") throw new TypeError("'only-if-cached' can be set only with 'same-origin' mode");
			if (init.redirect !== void 0) request$2.redirect = init.redirect;
			if (init.integrity != null) request$2.integrity = String(init.integrity);
			if (init.keepalive !== void 0) request$2.keepalive = Boolean(init.keepalive);
			if (init.method !== void 0) {
				let method = init.method;
				const mayBeNormalized = normalizedMethodRecords[method];
				if (mayBeNormalized !== void 0) request$2.method = mayBeNormalized;
				else {
					if (!isValidHTTPToken(method)) throw new TypeError(`'${method}' is not a valid HTTP method.`);
					const upperCase = method.toUpperCase();
					if (forbiddenMethodsSet.has(upperCase)) throw new TypeError(`'${method}' HTTP method is unsupported.`);
					method = normalizedMethodRecordsBase[upperCase] ?? method;
					request$2.method = method;
				}
				if (!patchMethodWarning && request$2.method === "patch") {
					process.emitWarning("Using `patch` is highly likely to result in a `405 Method Not Allowed`. `PATCH` is much more likely to succeed.", { code: "UNDICI-FETCH-patch" });
					patchMethodWarning = true;
				}
			}
			if (init.signal !== void 0) signal = init.signal;
			this.#state = request$2;
			const ac = new AbortController();
			this.#signal = ac.signal;
			if (signal != null) if (signal.aborted) ac.abort(signal.reason);
			else {
				this[kAbortController] = ac;
				const acRef = new WeakRef(ac);
				const abort$1 = buildAbort(acRef);
				if (abortSignalHasEventHandlerLeakWarning && getMaxListeners(signal) === defaultMaxListeners) setMaxListeners(1500, signal);
				util$1.addAbortListener(signal, abort$1);
				requestFinalizer.register(ac, {
					signal,
					abort: abort$1
				}, abort$1);
			}
			this.#headers = new Headers$2(kConstruct$5);
			setHeadersList(this.#headers, request$2.headersList);
			setHeadersGuard(this.#headers, "request");
			if (mode === "no-cors") {
				if (!corsSafeListedMethodsSet.has(request$2.method)) throw new TypeError(`'${request$2.method} is unsupported in no-cors mode.`);
				setHeadersGuard(this.#headers, "request-no-cors");
			}
			if (initHasKey) {
				/** @type {HeadersList} */
				const headersList = getHeadersList$1(this.#headers);
				const headers = init.headers !== void 0 ? init.headers : new HeadersList$1(headersList);
				headersList.clear();
				if (headers instanceof HeadersList$1) {
					for (const { name, value } of headers.rawValues()) headersList.append(name, value, false);
					headersList.cookies = headers.cookies;
				} else fillHeaders(this.#headers, headers);
			}
			const inputBody = webidl$9.is.Request(input) ? input.#state.body : null;
			if ((init.body != null || inputBody != null) && (request$2.method === "GET" || request$2.method === "HEAD")) throw new TypeError("Request with GET/HEAD method cannot have body.");
			let initBody = null;
			if (init.body != null) {
				const [extractedBody, contentType] = extractBody$1(init.body, request$2.keepalive);
				initBody = extractedBody;
				if (contentType && !getHeadersList$1(this.#headers).contains("content-type", true)) this.#headers.append("content-type", contentType, true);
			}
			const inputOrInitBody = initBody ?? inputBody;
			if (inputOrInitBody != null && inputOrInitBody.source == null) {
				if (initBody != null && init.duplex == null) throw new TypeError("RequestInit: duplex option is required when sending a body.");
				if (request$2.mode !== "same-origin" && request$2.mode !== "cors") throw new TypeError("If request is made from ReadableStream, mode should be \"same-origin\" or \"cors\"");
				request$2.useCORSPreflightFlag = true;
			}
			let finalBody = inputOrInitBody;
			if (initBody == null && inputBody != null) {
				if (bodyUnusable(input.#state)) throw new TypeError("Cannot construct a Request with a Request object that has already been used.");
				const identityTransform = new TransformStream();
				inputBody.stream.pipeThrough(identityTransform);
				finalBody = {
					source: inputBody.source,
					length: inputBody.length,
					stream: identityTransform.readable
				};
			}
			this.#state.body = finalBody;
		}
		get method() {
			webidl$9.brandCheck(this, Request$2);
			return this.#state.method;
		}
		get url() {
			webidl$9.brandCheck(this, Request$2);
			return URLSerializer$2(this.#state.url);
		}
		get headers() {
			webidl$9.brandCheck(this, Request$2);
			return this.#headers;
		}
		get destination() {
			webidl$9.brandCheck(this, Request$2);
			return this.#state.destination;
		}
		get referrer() {
			webidl$9.brandCheck(this, Request$2);
			if (this.#state.referrer === "no-referrer") return "";
			if (this.#state.referrer === "client") return "about:client";
			return this.#state.referrer.toString();
		}
		get referrerPolicy() {
			webidl$9.brandCheck(this, Request$2);
			return this.#state.referrerPolicy;
		}
		get mode() {
			webidl$9.brandCheck(this, Request$2);
			return this.#state.mode;
		}
		get credentials() {
			webidl$9.brandCheck(this, Request$2);
			return this.#state.credentials;
		}
		get cache() {
			webidl$9.brandCheck(this, Request$2);
			return this.#state.cache;
		}
		get redirect() {
			webidl$9.brandCheck(this, Request$2);
			return this.#state.redirect;
		}
		get integrity() {
			webidl$9.brandCheck(this, Request$2);
			return this.#state.integrity;
		}
		get keepalive() {
			webidl$9.brandCheck(this, Request$2);
			return this.#state.keepalive;
		}
		get isReloadNavigation() {
			webidl$9.brandCheck(this, Request$2);
			return this.#state.reloadNavigation;
		}
		get isHistoryNavigation() {
			webidl$9.brandCheck(this, Request$2);
			return this.#state.historyNavigation;
		}
		get signal() {
			webidl$9.brandCheck(this, Request$2);
			return this.#signal;
		}
		get body() {
			webidl$9.brandCheck(this, Request$2);
			return this.#state.body ? this.#state.body.stream : null;
		}
		get bodyUsed() {
			webidl$9.brandCheck(this, Request$2);
			return !!this.#state.body && util$1.isDisturbed(this.#state.body.stream);
		}
		get duplex() {
			webidl$9.brandCheck(this, Request$2);
			return "half";
		}
		clone() {
			webidl$9.brandCheck(this, Request$2);
			if (bodyUnusable(this.#state)) throw new TypeError("unusable");
			const clonedRequest = cloneRequest$1(this.#state);
			const ac = new AbortController();
			if (this.signal.aborted) ac.abort(this.signal.reason);
			else {
				let list = dependentControllerMap.get(this.signal);
				if (list === void 0) {
					list = /* @__PURE__ */ new Set();
					dependentControllerMap.set(this.signal, list);
				}
				const acRef = new WeakRef(ac);
				list.add(acRef);
				util$1.addAbortListener(ac.signal, buildAbort(acRef));
			}
			return fromInnerRequest$1(clonedRequest, this.#dispatcher, ac.signal, getHeadersGuard(this.#headers));
		}
		[nodeUtil.inspect.custom](depth, options) {
			if (options.depth === null) options.depth = 2;
			options.colors ??= true;
			const properties = {
				method: this.method,
				url: this.url,
				headers: this.headers,
				destination: this.destination,
				referrer: this.referrer,
				referrerPolicy: this.referrerPolicy,
				mode: this.mode,
				credentials: this.credentials,
				cache: this.cache,
				redirect: this.redirect,
				integrity: this.integrity,
				keepalive: this.keepalive,
				isReloadNavigation: this.isReloadNavigation,
				isHistoryNavigation: this.isHistoryNavigation,
				signal: this.signal
			};
			return `Request ${nodeUtil.formatWithOptions(options, properties)}`;
		}
		/**
		* @param {Request} request
		* @param {AbortSignal} newSignal
		*/
		static setRequestSignal(request$2, newSignal) {
			request$2.#signal = newSignal;
			return request$2;
		}
		/**
		* @param {Request} request
		*/
		static getRequestDispatcher(request$2) {
			return request$2.#dispatcher;
		}
		/**
		* @param {Request} request
		* @param {import('../../dispatcher/dispatcher')} newDispatcher
		*/
		static setRequestDispatcher(request$2, newDispatcher) {
			request$2.#dispatcher = newDispatcher;
		}
		/**
		* @param {Request} request
		* @param {Headers} newHeaders
		*/
		static setRequestHeaders(request$2, newHeaders) {
			request$2.#headers = newHeaders;
		}
		/**
		* @param {Request} request
		*/
		static getRequestState(request$2) {
			return request$2.#state;
		}
		/**
		* @param {Request} request
		* @param {any} newState
		*/
		static setRequestState(request$2, newState) {
			request$2.#state = newState;
		}
	};
	const { setRequestSignal, getRequestDispatcher: getRequestDispatcher$1, setRequestDispatcher, setRequestHeaders, getRequestState: getRequestState$2, setRequestState } = Request$2;
	Reflect.deleteProperty(Request$2, "setRequestSignal");
	Reflect.deleteProperty(Request$2, "getRequestDispatcher");
	Reflect.deleteProperty(Request$2, "setRequestDispatcher");
	Reflect.deleteProperty(Request$2, "setRequestHeaders");
	Reflect.deleteProperty(Request$2, "getRequestState");
	Reflect.deleteProperty(Request$2, "setRequestState");
	mixinBody(Request$2, getRequestState$2);
	function makeRequest$2(init) {
		return {
			method: init.method ?? "GET",
			localURLsOnly: init.localURLsOnly ?? false,
			unsafeRequest: init.unsafeRequest ?? false,
			body: init.body ?? null,
			client: init.client ?? null,
			reservedClient: init.reservedClient ?? null,
			replacesClientId: init.replacesClientId ?? "",
			window: init.window ?? "client",
			keepalive: init.keepalive ?? false,
			serviceWorkers: init.serviceWorkers ?? "all",
			initiator: init.initiator ?? "",
			destination: init.destination ?? "",
			priority: init.priority ?? null,
			origin: init.origin ?? "client",
			policyContainer: init.policyContainer ?? "client",
			referrer: init.referrer ?? "client",
			referrerPolicy: init.referrerPolicy ?? "",
			mode: init.mode ?? "no-cors",
			useCORSPreflightFlag: init.useCORSPreflightFlag ?? false,
			credentials: init.credentials ?? "same-origin",
			useCredentials: init.useCredentials ?? false,
			cache: init.cache ?? "default",
			redirect: init.redirect ?? "follow",
			integrity: init.integrity ?? "",
			cryptoGraphicsNonceMetadata: init.cryptoGraphicsNonceMetadata ?? "",
			parserMetadata: init.parserMetadata ?? "",
			reloadNavigation: init.reloadNavigation ?? false,
			historyNavigation: init.historyNavigation ?? false,
			userActivation: init.userActivation ?? false,
			taintedOrigin: init.taintedOrigin ?? false,
			redirectCount: init.redirectCount ?? 0,
			responseTainting: init.responseTainting ?? "basic",
			preventNoCacheCacheControlHeaderModification: init.preventNoCacheCacheControlHeaderModification ?? false,
			done: init.done ?? false,
			timingAllowFailed: init.timingAllowFailed ?? false,
			urlList: init.urlList,
			url: init.urlList[0],
			headersList: init.headersList ? new HeadersList$1(init.headersList) : new HeadersList$1()
		};
	}
	function cloneRequest$1(request$2) {
		const newRequest = makeRequest$2({
			...request$2,
			body: null
		});
		if (request$2.body != null) newRequest.body = cloneBody(request$2.body);
		return newRequest;
	}
	/**
	* @see https://fetch.spec.whatwg.org/#request-create
	* @param {any} innerRequest
	* @param {import('../../dispatcher/agent')} dispatcher
	* @param {AbortSignal} signal
	* @param {'request' | 'immutable' | 'request-no-cors' | 'response' | 'none'} guard
	* @returns {Request}
	*/
	function fromInnerRequest$1(innerRequest, dispatcher, signal, guard) {
		const request$2 = new Request$2(kConstruct$5);
		setRequestState(request$2, innerRequest);
		setRequestDispatcher(request$2, dispatcher);
		setRequestSignal(request$2, signal);
		const headers = new Headers$2(kConstruct$5);
		setRequestHeaders(request$2, headers);
		setHeadersList(headers, innerRequest.headersList);
		setHeadersGuard(headers, guard);
		return request$2;
	}
	Object.defineProperties(Request$2.prototype, {
		method: kEnumerableProperty$7,
		url: kEnumerableProperty$7,
		headers: kEnumerableProperty$7,
		redirect: kEnumerableProperty$7,
		clone: kEnumerableProperty$7,
		signal: kEnumerableProperty$7,
		duplex: kEnumerableProperty$7,
		destination: kEnumerableProperty$7,
		body: kEnumerableProperty$7,
		bodyUsed: kEnumerableProperty$7,
		isHistoryNavigation: kEnumerableProperty$7,
		isReloadNavigation: kEnumerableProperty$7,
		keepalive: kEnumerableProperty$7,
		integrity: kEnumerableProperty$7,
		cache: kEnumerableProperty$7,
		credentials: kEnumerableProperty$7,
		attribute: kEnumerableProperty$7,
		referrerPolicy: kEnumerableProperty$7,
		referrer: kEnumerableProperty$7,
		mode: kEnumerableProperty$7,
		[Symbol.toStringTag]: {
			value: "Request",
			configurable: true
		}
	});
	webidl$9.is.Request = webidl$9.util.MakeTypeAssertion(Request$2);
	/**
	* @param {*} V
	* @returns {import('../../../types/fetch').Request|string}
	*
	* @see https://fetch.spec.whatwg.org/#requestinfo
	*/
	webidl$9.converters.RequestInfo = function(V) {
		if (typeof V === "string") return webidl$9.converters.USVString(V);
		if (webidl$9.is.Request(V)) return V;
		return webidl$9.converters.USVString(V);
	};
	/**
	* @param {*} V
	* @returns {import('../../../types/fetch').RequestInit}
	* @see https://fetch.spec.whatwg.org/#requestinit
	*/
	webidl$9.converters.RequestInit = webidl$9.dictionaryConverter([
		{
			key: "method",
			converter: webidl$9.converters.ByteString
		},
		{
			key: "headers",
			converter: webidl$9.converters.HeadersInit
		},
		{
			key: "body",
			converter: webidl$9.nullableConverter(webidl$9.converters.BodyInit)
		},
		{
			key: "referrer",
			converter: webidl$9.converters.USVString
		},
		{
			key: "referrerPolicy",
			converter: webidl$9.converters.DOMString,
			allowedValues: referrerPolicy
		},
		{
			key: "mode",
			converter: webidl$9.converters.DOMString,
			allowedValues: requestMode
		},
		{
			key: "credentials",
			converter: webidl$9.converters.DOMString,
			allowedValues: requestCredentials
		},
		{
			key: "cache",
			converter: webidl$9.converters.DOMString,
			allowedValues: requestCache
		},
		{
			key: "redirect",
			converter: webidl$9.converters.DOMString,
			allowedValues: requestRedirect
		},
		{
			key: "integrity",
			converter: webidl$9.converters.DOMString
		},
		{
			key: "keepalive",
			converter: webidl$9.converters.boolean
		},
		{
			key: "signal",
			converter: webidl$9.nullableConverter((signal) => webidl$9.converters.AbortSignal(signal, "RequestInit", "signal"))
		},
		{
			key: "window",
			converter: webidl$9.converters.any
		},
		{
			key: "duplex",
			converter: webidl$9.converters.DOMString,
			allowedValues: requestDuplex
		},
		{
			key: "dispatcher",
			converter: webidl$9.converters.any
		}
	]);
	module.exports = {
		Request: Request$2,
		makeRequest: makeRequest$2,
		fromInnerRequest: fromInnerRequest$1,
		cloneRequest: cloneRequest$1,
		getRequestDispatcher: getRequestDispatcher$1,
		getRequestState: getRequestState$2
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/subresource-integrity/subresource-integrity.js
var require_subresource_integrity = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/subresource-integrity/subresource-integrity.js": ((exports, module) => {
	const assert$6 = __require("node:assert");
	/**
	* @typedef {object} Metadata
	* @property {SRIHashAlgorithm} alg - The algorithm used for the hash.
	* @property {string} val - The base64-encoded hash value.
	*/
	/**
	* @typedef {Metadata[]} MetadataList
	*/
	/**
	* @typedef {('sha256' | 'sha384' | 'sha512')} SRIHashAlgorithm
	*/
	/**
	* @type {Map<SRIHashAlgorithm, number>}
	*
	* The valid SRI hash algorithm token set is the ordered set « "sha256",
	* "sha384", "sha512" » (corresponding to SHA-256, SHA-384, and SHA-512
	* respectively). The ordering of this set is meaningful, with stronger
	* algorithms appearing later in the set.
	*
	* @see https://w3c.github.io/webappsec-subresource-integrity/#valid-sri-hash-algorithm-token-set
	*/
	const validSRIHashAlgorithmTokenSet = new Map([
		["sha256", 0],
		["sha384", 1],
		["sha512", 2]
	]);
	/** @type {import('crypto')} */
	let crypto$2;
	try {
		crypto$2 = __require("node:crypto");
		const cryptoHashes = crypto$2.getHashes();
		if (cryptoHashes.length === 0) validSRIHashAlgorithmTokenSet.clear();
		for (const algorithm of validSRIHashAlgorithmTokenSet.keys()) if (cryptoHashes.includes(algorithm) === false) validSRIHashAlgorithmTokenSet.delete(algorithm);
	} catch {
		validSRIHashAlgorithmTokenSet.clear();
	}
	/**
	* @typedef GetSRIHashAlgorithmIndex
	* @type {(algorithm: SRIHashAlgorithm) => number}
	* @param {SRIHashAlgorithm} algorithm
	* @returns {number} The index of the algorithm in the valid SRI hash algorithm
	* token set.
	*/
	const getSRIHashAlgorithmIndex = Map.prototype.get.bind(validSRIHashAlgorithmTokenSet);
	/**
	* @typedef IsValidSRIHashAlgorithm
	* @type {(algorithm: string) => algorithm is SRIHashAlgorithm}
	* @param {*} algorithm
	* @returns {algorithm is SRIHashAlgorithm}
	*/
	const isValidSRIHashAlgorithm = Map.prototype.has.bind(validSRIHashAlgorithmTokenSet);
	/**
	* @param {Uint8Array} bytes
	* @param {string} metadataList
	* @returns {boolean}
	*
	* @see https://w3c.github.io/webappsec-subresource-integrity/#does-response-match-metadatalist
	*/
	const bytesMatch$1 = crypto$2 === void 0 || validSRIHashAlgorithmTokenSet.size === 0 ? () => true : (bytes, metadataList) => {
		const parsedMetadata = parseMetadata(metadataList);
		if (parsedMetadata.length === 0) return true;
		const metadata = getStrongestMetadata(parsedMetadata);
		for (const item of metadata) {
			const algorithm = item.alg;
			const expectedValue = item.val;
			const actualValue = applyAlgorithmToBytes(algorithm, bytes);
			if (caseSensitiveMatch(actualValue, expectedValue)) return true;
		}
		return false;
	};
	/**
	* @param {MetadataList} metadataList
	* @returns {MetadataList} The strongest hash algorithm from the metadata list.
	*/
	function getStrongestMetadata(metadataList) {
		const result = [];
		/** @type {Metadata|null} */
		let strongest = null;
		for (const item of metadataList) {
			assert$6(isValidSRIHashAlgorithm(item.alg), "Invalid SRI hash algorithm token");
			if (result.length === 0) {
				result.push(item);
				strongest = item;
				continue;
			}
			const currentAlgorithm = strongest.alg;
			const currentAlgorithmIndex = getSRIHashAlgorithmIndex(currentAlgorithm);
			const newAlgorithm = item.alg;
			const newAlgorithmIndex = getSRIHashAlgorithmIndex(newAlgorithm);
			if (newAlgorithmIndex < currentAlgorithmIndex) continue;
			else if (newAlgorithmIndex > currentAlgorithmIndex) {
				strongest = item;
				result[0] = item;
				result.length = 1;
			} else result.push(item);
		}
		return result;
	}
	/**
	* @param {string} metadata
	* @returns {MetadataList}
	*
	* @see https://w3c.github.io/webappsec-subresource-integrity/#parse-metadata
	*/
	function parseMetadata(metadata) {
		/** @type {MetadataList} */
		const result = [];
		for (const item of metadata.split(" ")) {
			const algorithmExpression = item.split("?", 1)[0];
			let base64Value = "";
			const algorithmAndValue = [algorithmExpression.slice(0, 6), algorithmExpression.slice(7)];
			const algorithm = algorithmAndValue[0];
			if (!isValidSRIHashAlgorithm(algorithm)) continue;
			if (algorithmAndValue[1]) base64Value = algorithmAndValue[1];
			const metadata$1 = {
				alg: algorithm,
				val: base64Value
			};
			result.push(metadata$1);
		}
		return result;
	}
	/**
	* Applies the specified hash algorithm to the given bytes
	*
	* @typedef {(algorithm: SRIHashAlgorithm, bytes: Uint8Array) => string} ApplyAlgorithmToBytes
	* @param {SRIHashAlgorithm} algorithm
	* @param {Uint8Array} bytes
	* @returns {string}
	*/
	const applyAlgorithmToBytes = (algorithm, bytes) => {
		return crypto$2.hash(algorithm, bytes, "base64");
	};
	/**
	* Compares two base64 strings, allowing for base64url
	* in the second string.
	*
	* @param {string} actualValue base64 encoded string
	* @param {string} expectedValue base64 or base64url encoded string
	* @returns {boolean}
	*/
	function caseSensitiveMatch(actualValue, expectedValue) {
		let actualValueLength = actualValue.length;
		if (actualValueLength !== 0 && actualValue[actualValueLength - 1] === "=") actualValueLength -= 1;
		if (actualValueLength !== 0 && actualValue[actualValueLength - 1] === "=") actualValueLength -= 1;
		let expectedValueLength = expectedValue.length;
		if (expectedValueLength !== 0 && expectedValue[expectedValueLength - 1] === "=") expectedValueLength -= 1;
		if (expectedValueLength !== 0 && expectedValue[expectedValueLength - 1] === "=") expectedValueLength -= 1;
		if (actualValueLength !== expectedValueLength) return false;
		for (let i = 0; i < actualValueLength; ++i) {
			if (actualValue[i] === expectedValue[i] || actualValue[i] === "+" && expectedValue[i] === "-" || actualValue[i] === "/" && expectedValue[i] === "_") continue;
			return false;
		}
		return true;
	}
	module.exports = {
		applyAlgorithmToBytes,
		bytesMatch: bytesMatch$1,
		caseSensitiveMatch,
		isValidSRIHashAlgorithm,
		getStrongestMetadata,
		parseMetadata
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/fetch/index.js
var require_fetch = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/fetch/index.js": ((exports, module) => {
	const { makeNetworkError, makeAppropriateNetworkError, filterResponse, makeResponse, fromInnerResponse: fromInnerResponse$1, getResponseState: getResponseState$1 } = require_response();
	const { HeadersList } = require_headers();
	const { Request: Request$1, cloneRequest, getRequestDispatcher, getRequestState: getRequestState$1 } = require_request();
	const zlib = __require("node:zlib");
	const { makePolicyContainer, clonePolicyContainer, requestBadPort, TAOCheck, appendRequestOriginHeader, responseLocationURL, requestCurrentURL, setRequestReferrerPolicyOnRedirect, tryUpgradeRequestToAPotentiallyTrustworthyURL, createOpaqueTimingInfo, appendFetchMetadata, corsCheck, crossOriginResourcePolicyCheck, determineRequestsReferrer, coarsenedSharedCurrentTime, sameOrigin, isCancelled, isAborted, isErrorLike, fullyReadBody, readableStreamClose, isomorphicEncode, urlIsLocal, urlIsHttpHttpsScheme: urlIsHttpHttpsScheme$1, urlHasHttpsScheme, clampAndCoarsenConnectionTimingInfo, simpleRangeHeaderValue, buildContentRange, createInflate, extractMimeType } = require_util$4();
	const assert$5 = __require("node:assert");
	const { safelyExtractBody, extractBody } = require_body();
	const { redirectStatusSet, nullBodyStatus, safeMethodsSet, requestBodyHeader, subresourceSet } = require_constants$2();
	const EE = __require("node:events");
	const { Readable, pipeline: pipeline$1, finished, isErrored, isReadable } = __require("node:stream");
	const { addAbortListener, bufferToLowerCasedHeaderName } = require_util$5();
	const { dataURLProcessor, serializeAMimeType: serializeAMimeType$1, minimizeSupportedMimeType } = require_data_url();
	const { getGlobalDispatcher: getGlobalDispatcher$2 } = require_global();
	const { webidl: webidl$8 } = require_webidl();
	const { STATUS_CODES } = __require("node:http");
	const { bytesMatch } = require_subresource_integrity();
	const { createDeferredPromise: createDeferredPromise$2 } = require_promise();
	const GET_OR_HEAD = ["GET", "HEAD"];
	const defaultUserAgent = typeof __UNDICI_IS_NODE__ !== "undefined" || typeof esbuildDetection !== "undefined" ? "node" : "undici";
	/** @type {import('buffer').resolveObjectURL} */
	let resolveObjectURL;
	var Fetch = class extends EE {
		constructor(dispatcher) {
			super();
			this.dispatcher = dispatcher;
			this.connection = null;
			this.dump = false;
			this.state = "ongoing";
		}
		terminate(reason) {
			if (this.state !== "ongoing") return;
			this.state = "terminated";
			this.connection?.destroy(reason);
			this.emit("terminated", reason);
		}
		abort(error) {
			if (this.state !== "ongoing") return;
			this.state = "aborted";
			if (!error) error = new DOMException("The operation was aborted.", "AbortError");
			this.serializedAbortReason = error;
			this.connection?.destroy(error);
			this.emit("terminated", error);
		}
	};
	function handleFetchDone(response) {
		finalizeAndReportTiming(response, "fetch");
	}
	function fetch(input, init = void 0) {
		webidl$8.argumentLengthCheck(arguments, 1, "globalThis.fetch");
		let p = createDeferredPromise$2();
		let requestObject;
		try {
			requestObject = new Request$1(input, init);
		} catch (e) {
			p.reject(e);
			return p.promise;
		}
		const request$2 = getRequestState$1(requestObject);
		if (requestObject.signal.aborted) {
			abortFetch(p, request$2, null, requestObject.signal.reason);
			return p.promise;
		}
		if (request$2.client.globalObject?.constructor?.name === "ServiceWorkerGlobalScope") request$2.serviceWorkers = "none";
		let responseObject = null;
		let locallyAborted = false;
		let controller = null;
		addAbortListener(requestObject.signal, () => {
			locallyAborted = true;
			assert$5(controller != null);
			controller.abort(requestObject.signal.reason);
			const realResponse = responseObject?.deref();
			abortFetch(p, request$2, realResponse, requestObject.signal.reason);
		});
		const processResponse = (response) => {
			if (locallyAborted) return;
			if (response.aborted) {
				abortFetch(p, request$2, responseObject, controller.serializedAbortReason);
				return;
			}
			if (response.type === "error") {
				p.reject(new TypeError("fetch failed", { cause: response.error }));
				return;
			}
			responseObject = new WeakRef(fromInnerResponse$1(response, "immutable"));
			p.resolve(responseObject.deref());
			p = null;
		};
		controller = fetching$3({
			request: request$2,
			processResponseEndOfBody: handleFetchDone,
			processResponse,
			dispatcher: getRequestDispatcher(requestObject)
		});
		return p.promise;
	}
	function finalizeAndReportTiming(response, initiatorType = "other") {
		if (response.type === "error" && response.aborted) return;
		if (!response.urlList?.length) return;
		const originalURL = response.urlList[0];
		let timingInfo = response.timingInfo;
		let cacheState = response.cacheState;
		if (!urlIsHttpHttpsScheme$1(originalURL)) return;
		if (timingInfo === null) return;
		if (!response.timingAllowPassed) {
			timingInfo = createOpaqueTimingInfo({ startTime: timingInfo.startTime });
			cacheState = "";
		}
		timingInfo.endTime = coarsenedSharedCurrentTime();
		response.timingInfo = timingInfo;
		markResourceTiming(timingInfo, originalURL.href, initiatorType, globalThis, cacheState, "", response.status);
	}
	const markResourceTiming = performance.markResourceTiming;
	function abortFetch(p, request$2, responseObject, error) {
		if (p) p.reject(error);
		if (request$2.body?.stream != null && isReadable(request$2.body.stream)) request$2.body.stream.cancel(error).catch((err) => {
			if (err.code === "ERR_INVALID_STATE") return;
			throw err;
		});
		if (responseObject == null) return;
		const response = getResponseState$1(responseObject);
		if (response.body?.stream != null && isReadable(response.body.stream)) response.body.stream.cancel(error).catch((err) => {
			if (err.code === "ERR_INVALID_STATE") return;
			throw err;
		});
	}
	function fetching$3({ request: request$2, processRequestBodyChunkLength, processRequestEndOfBody, processResponse, processResponseEndOfBody, processResponseConsumeBody, useParallelQueue = false, dispatcher = getGlobalDispatcher$2() }) {
		assert$5(dispatcher);
		let taskDestination = null;
		let crossOriginIsolatedCapability = false;
		if (request$2.client != null) {
			taskDestination = request$2.client.globalObject;
			crossOriginIsolatedCapability = request$2.client.crossOriginIsolatedCapability;
		}
		const currentTime = coarsenedSharedCurrentTime(crossOriginIsolatedCapability);
		const timingInfo = createOpaqueTimingInfo({ startTime: currentTime });
		const fetchParams = {
			controller: new Fetch(dispatcher),
			request: request$2,
			timingInfo,
			processRequestBodyChunkLength,
			processRequestEndOfBody,
			processResponse,
			processResponseConsumeBody,
			processResponseEndOfBody,
			taskDestination,
			crossOriginIsolatedCapability
		};
		assert$5(!request$2.body || request$2.body.stream);
		if (request$2.window === "client") request$2.window = request$2.client?.globalObject?.constructor?.name === "Window" ? request$2.client : "no-window";
		if (request$2.origin === "client") request$2.origin = request$2.client.origin;
		if (request$2.policyContainer === "client") if (request$2.client != null) request$2.policyContainer = clonePolicyContainer(request$2.client.policyContainer);
		else request$2.policyContainer = makePolicyContainer();
		if (!request$2.headersList.contains("accept", true)) request$2.headersList.append("accept", "*/*", true);
		if (!request$2.headersList.contains("accept-language", true)) request$2.headersList.append("accept-language", "*", true);
		if (request$2.priority === null) {}
		if (subresourceSet.has(request$2.destination)) {}
		mainFetch(fetchParams, false);
		return fetchParams.controller;
	}
	async function mainFetch(fetchParams, recursive) {
		try {
			const request$2 = fetchParams.request;
			let response = null;
			if (request$2.localURLsOnly && !urlIsLocal(requestCurrentURL(request$2))) response = makeNetworkError("local URLs only");
			tryUpgradeRequestToAPotentiallyTrustworthyURL(request$2);
			if (requestBadPort(request$2) === "blocked") response = makeNetworkError("bad port");
			if (request$2.referrerPolicy === "") request$2.referrerPolicy = request$2.policyContainer.referrerPolicy;
			if (request$2.referrer !== "no-referrer") request$2.referrer = determineRequestsReferrer(request$2);
			if (response === null) {
				const currentURL = requestCurrentURL(request$2);
				if (sameOrigin(currentURL, request$2.url) && request$2.responseTainting === "basic" || currentURL.protocol === "data:" || request$2.mode === "navigate" || request$2.mode === "websocket") {
					request$2.responseTainting = "basic";
					response = await schemeFetch(fetchParams);
				} else if (request$2.mode === "same-origin") response = makeNetworkError("request mode cannot be \"same-origin\"");
				else if (request$2.mode === "no-cors") if (request$2.redirect !== "follow") response = makeNetworkError("redirect mode cannot be \"follow\" for \"no-cors\" request");
				else {
					request$2.responseTainting = "opaque";
					response = await schemeFetch(fetchParams);
				}
				else if (!urlIsHttpHttpsScheme$1(requestCurrentURL(request$2))) response = makeNetworkError("URL scheme must be a HTTP(S) scheme");
				else {
					request$2.responseTainting = "cors";
					response = await httpFetch(fetchParams);
				}
			}
			if (recursive) return response;
			if (response.status !== 0 && !response.internalResponse) {
				if (request$2.responseTainting === "cors") {}
				if (request$2.responseTainting === "basic") response = filterResponse(response, "basic");
				else if (request$2.responseTainting === "cors") response = filterResponse(response, "cors");
				else if (request$2.responseTainting === "opaque") response = filterResponse(response, "opaque");
				else assert$5(false);
			}
			let internalResponse = response.status === 0 ? response : response.internalResponse;
			if (internalResponse.urlList.length === 0) internalResponse.urlList.push(...request$2.urlList);
			if (!request$2.timingAllowFailed) response.timingAllowPassed = true;
			if (response.type === "opaque" && internalResponse.status === 206 && internalResponse.rangeRequested && !request$2.headers.contains("range", true)) response = internalResponse = makeNetworkError();
			if (response.status !== 0 && (request$2.method === "HEAD" || request$2.method === "CONNECT" || nullBodyStatus.includes(internalResponse.status))) {
				internalResponse.body = null;
				fetchParams.controller.dump = true;
			}
			if (request$2.integrity) {
				const processBodyError = (reason) => fetchFinale(fetchParams, makeNetworkError(reason));
				if (request$2.responseTainting === "opaque" || response.body == null) {
					processBodyError(response.error);
					return;
				}
				const processBody = (bytes) => {
					if (!bytesMatch(bytes, request$2.integrity)) {
						processBodyError("integrity mismatch");
						return;
					}
					response.body = safelyExtractBody(bytes)[0];
					fetchFinale(fetchParams, response);
				};
				fullyReadBody(response.body, processBody, processBodyError);
			} else fetchFinale(fetchParams, response);
		} catch (err) {
			fetchParams.controller.terminate(err);
		}
	}
	function schemeFetch(fetchParams) {
		if (isCancelled(fetchParams) && fetchParams.request.redirectCount === 0) return Promise.resolve(makeAppropriateNetworkError(fetchParams));
		const { request: request$2 } = fetchParams;
		const { protocol: scheme } = requestCurrentURL(request$2);
		switch (scheme) {
			case "about:": return Promise.resolve(makeNetworkError("about scheme is not supported"));
			case "blob:": {
				if (!resolveObjectURL) resolveObjectURL = __require("node:buffer").resolveObjectURL;
				const blobURLEntry = requestCurrentURL(request$2);
				if (blobURLEntry.search.length !== 0) return Promise.resolve(makeNetworkError("NetworkError when attempting to fetch resource."));
				const blob = resolveObjectURL(blobURLEntry.toString());
				if (request$2.method !== "GET" || !webidl$8.is.Blob(blob)) return Promise.resolve(makeNetworkError("invalid method"));
				const response = makeResponse();
				const fullLength = blob.size;
				const serializedFullLength = isomorphicEncode(`${fullLength}`);
				const type = blob.type;
				if (!request$2.headersList.contains("range", true)) {
					const bodyWithType = extractBody(blob);
					response.statusText = "OK";
					response.body = bodyWithType[0];
					response.headersList.set("content-length", serializedFullLength, true);
					response.headersList.set("content-type", type, true);
				} else {
					response.rangeRequested = true;
					const rangeHeader = request$2.headersList.get("range", true);
					const rangeValue = simpleRangeHeaderValue(rangeHeader, true);
					if (rangeValue === "failure") return Promise.resolve(makeNetworkError("failed to fetch the data URL"));
					let { rangeStartValue: rangeStart, rangeEndValue: rangeEnd } = rangeValue;
					if (rangeStart === null) {
						rangeStart = fullLength - rangeEnd;
						rangeEnd = rangeStart + rangeEnd - 1;
					} else {
						if (rangeStart >= fullLength) return Promise.resolve(makeNetworkError("Range start is greater than the blob's size."));
						if (rangeEnd === null || rangeEnd >= fullLength) rangeEnd = fullLength - 1;
					}
					const slicedBlob = blob.slice(rangeStart, rangeEnd, type);
					response.body = extractBody(slicedBlob)[0];
					const serializedSlicedLength = isomorphicEncode(`${slicedBlob.size}`);
					const contentRange = buildContentRange(rangeStart, rangeEnd, fullLength);
					response.status = 206;
					response.statusText = "Partial Content";
					response.headersList.set("content-length", serializedSlicedLength, true);
					response.headersList.set("content-type", type, true);
					response.headersList.set("content-range", contentRange, true);
				}
				return Promise.resolve(response);
			}
			case "data:": {
				const currentURL = requestCurrentURL(request$2);
				const dataURLStruct = dataURLProcessor(currentURL);
				if (dataURLStruct === "failure") return Promise.resolve(makeNetworkError("failed to fetch the data URL"));
				const mimeType = serializeAMimeType$1(dataURLStruct.mimeType);
				return Promise.resolve(makeResponse({
					statusText: "OK",
					headersList: [["content-type", {
						name: "Content-Type",
						value: mimeType
					}]],
					body: safelyExtractBody(dataURLStruct.body)[0]
				}));
			}
			case "file:": return Promise.resolve(makeNetworkError("not implemented... yet..."));
			case "http:":
			case "https:": return httpFetch(fetchParams).catch((err) => makeNetworkError(err));
			default: return Promise.resolve(makeNetworkError("unknown scheme"));
		}
	}
	function finalizeResponse(fetchParams, response) {
		fetchParams.request.done = true;
		if (fetchParams.processResponseDone != null) queueMicrotask(() => fetchParams.processResponseDone(response));
	}
	function fetchFinale(fetchParams, response) {
		let timingInfo = fetchParams.timingInfo;
		const processResponseEndOfBody = () => {
			const unsafeEndTime = Date.now();
			if (fetchParams.request.destination === "document") fetchParams.controller.fullTimingInfo = timingInfo;
			fetchParams.controller.reportTimingSteps = () => {
				if (!urlIsHttpHttpsScheme$1(fetchParams.request.url)) return;
				timingInfo.endTime = unsafeEndTime;
				let cacheState = response.cacheState;
				const bodyInfo = response.bodyInfo;
				if (!response.timingAllowPassed) {
					timingInfo = createOpaqueTimingInfo(timingInfo);
					cacheState = "";
				}
				let responseStatus = 0;
				if (fetchParams.request.mode !== "navigator" || !response.hasCrossOriginRedirects) {
					responseStatus = response.status;
					const mimeType = extractMimeType(response.headersList);
					if (mimeType !== "failure") bodyInfo.contentType = minimizeSupportedMimeType(mimeType);
				}
				if (fetchParams.request.initiatorType != null) markResourceTiming(timingInfo, fetchParams.request.url.href, fetchParams.request.initiatorType, globalThis, cacheState, bodyInfo, responseStatus);
			};
			const processResponseEndOfBodyTask = () => {
				fetchParams.request.done = true;
				if (fetchParams.processResponseEndOfBody != null) queueMicrotask(() => fetchParams.processResponseEndOfBody(response));
				if (fetchParams.request.initiatorType != null) fetchParams.controller.reportTimingSteps();
			};
			queueMicrotask(() => processResponseEndOfBodyTask());
		};
		if (fetchParams.processResponse != null) queueMicrotask(() => {
			fetchParams.processResponse(response);
			fetchParams.processResponse = null;
		});
		const internalResponse = response.type === "error" ? response : response.internalResponse ?? response;
		if (internalResponse.body == null) processResponseEndOfBody();
		else finished(internalResponse.body.stream, () => {
			processResponseEndOfBody();
		});
	}
	async function httpFetch(fetchParams) {
		const request$2 = fetchParams.request;
		let response = null;
		let actualResponse = null;
		const timingInfo = fetchParams.timingInfo;
		if (request$2.serviceWorkers === "all") {}
		if (response === null) {
			if (request$2.redirect === "follow") request$2.serviceWorkers = "none";
			actualResponse = response = await httpNetworkOrCacheFetch(fetchParams);
			if (request$2.responseTainting === "cors" && corsCheck(request$2, response) === "failure") return makeNetworkError("cors failure");
			if (TAOCheck(request$2, response) === "failure") request$2.timingAllowFailed = true;
		}
		if ((request$2.responseTainting === "opaque" || response.type === "opaque") && crossOriginResourcePolicyCheck(request$2.origin, request$2.client, request$2.destination, actualResponse) === "blocked") return makeNetworkError("blocked");
		if (redirectStatusSet.has(actualResponse.status)) {
			if (request$2.redirect !== "manual") fetchParams.controller.connection.destroy(void 0, false);
			if (request$2.redirect === "error") response = makeNetworkError("unexpected redirect");
			else if (request$2.redirect === "manual") response = actualResponse;
			else if (request$2.redirect === "follow") response = await httpRedirectFetch(fetchParams, response);
			else assert$5(false);
		}
		response.timingInfo = timingInfo;
		return response;
	}
	function httpRedirectFetch(fetchParams, response) {
		const request$2 = fetchParams.request;
		const actualResponse = response.internalResponse ? response.internalResponse : response;
		let locationURL;
		try {
			locationURL = responseLocationURL(actualResponse, requestCurrentURL(request$2).hash);
			if (locationURL == null) return response;
		} catch (err) {
			return Promise.resolve(makeNetworkError(err));
		}
		if (!urlIsHttpHttpsScheme$1(locationURL)) return Promise.resolve(makeNetworkError("URL scheme must be a HTTP(S) scheme"));
		if (request$2.redirectCount === 20) return Promise.resolve(makeNetworkError("redirect count exceeded"));
		request$2.redirectCount += 1;
		if (request$2.mode === "cors" && (locationURL.username || locationURL.password) && !sameOrigin(request$2, locationURL)) return Promise.resolve(makeNetworkError("cross origin not allowed for request mode \"cors\""));
		if (request$2.responseTainting === "cors" && (locationURL.username || locationURL.password)) return Promise.resolve(makeNetworkError("URL cannot contain credentials for request mode \"cors\""));
		if (actualResponse.status !== 303 && request$2.body != null && request$2.body.source == null) return Promise.resolve(makeNetworkError());
		if ([301, 302].includes(actualResponse.status) && request$2.method === "POST" || actualResponse.status === 303 && !GET_OR_HEAD.includes(request$2.method)) {
			request$2.method = "GET";
			request$2.body = null;
			for (const headerName of requestBodyHeader) request$2.headersList.delete(headerName);
		}
		if (!sameOrigin(requestCurrentURL(request$2), locationURL)) {
			request$2.headersList.delete("authorization", true);
			request$2.headersList.delete("proxy-authorization", true);
			request$2.headersList.delete("cookie", true);
			request$2.headersList.delete("host", true);
		}
		if (request$2.body != null) {
			assert$5(request$2.body.source != null);
			request$2.body = safelyExtractBody(request$2.body.source)[0];
		}
		const timingInfo = fetchParams.timingInfo;
		timingInfo.redirectEndTime = timingInfo.postRedirectStartTime = coarsenedSharedCurrentTime(fetchParams.crossOriginIsolatedCapability);
		if (timingInfo.redirectStartTime === 0) timingInfo.redirectStartTime = timingInfo.startTime;
		request$2.urlList.push(locationURL);
		setRequestReferrerPolicyOnRedirect(request$2, actualResponse);
		return mainFetch(fetchParams, true);
	}
	async function httpNetworkOrCacheFetch(fetchParams, isAuthenticationFetch = false, isNewConnectionFetch = false) {
		const request$2 = fetchParams.request;
		let httpFetchParams = null;
		let httpRequest = null;
		let response = null;
		const httpCache = null;
		if (request$2.window === "no-window" && request$2.redirect === "error") {
			httpFetchParams = fetchParams;
			httpRequest = request$2;
		} else {
			httpRequest = cloneRequest(request$2);
			httpFetchParams = { ...fetchParams };
			httpFetchParams.request = httpRequest;
		}
		const includeCredentials = request$2.credentials === "include" || request$2.credentials === "same-origin" && request$2.responseTainting === "basic";
		const contentLength = httpRequest.body ? httpRequest.body.length : null;
		let contentLengthHeaderValue = null;
		if (httpRequest.body == null && ["POST", "PUT"].includes(httpRequest.method)) contentLengthHeaderValue = "0";
		if (contentLength != null) contentLengthHeaderValue = isomorphicEncode(`${contentLength}`);
		if (contentLengthHeaderValue != null) httpRequest.headersList.append("content-length", contentLengthHeaderValue, true);
		if (contentLength != null && httpRequest.keepalive) {}
		if (webidl$8.is.URL(httpRequest.referrer)) httpRequest.headersList.append("referer", isomorphicEncode(httpRequest.referrer.href), true);
		appendRequestOriginHeader(httpRequest);
		appendFetchMetadata(httpRequest);
		if (!httpRequest.headersList.contains("user-agent", true)) httpRequest.headersList.append("user-agent", defaultUserAgent, true);
		if (httpRequest.cache === "default" && (httpRequest.headersList.contains("if-modified-since", true) || httpRequest.headersList.contains("if-none-match", true) || httpRequest.headersList.contains("if-unmodified-since", true) || httpRequest.headersList.contains("if-match", true) || httpRequest.headersList.contains("if-range", true))) httpRequest.cache = "no-store";
		if (httpRequest.cache === "no-cache" && !httpRequest.preventNoCacheCacheControlHeaderModification && !httpRequest.headersList.contains("cache-control", true)) httpRequest.headersList.append("cache-control", "max-age=0", true);
		if (httpRequest.cache === "no-store" || httpRequest.cache === "reload") {
			if (!httpRequest.headersList.contains("pragma", true)) httpRequest.headersList.append("pragma", "no-cache", true);
			if (!httpRequest.headersList.contains("cache-control", true)) httpRequest.headersList.append("cache-control", "no-cache", true);
		}
		if (httpRequest.headersList.contains("range", true)) httpRequest.headersList.append("accept-encoding", "identity", true);
		if (!httpRequest.headersList.contains("accept-encoding", true)) if (urlHasHttpsScheme(requestCurrentURL(httpRequest))) httpRequest.headersList.append("accept-encoding", "br, gzip, deflate", true);
		else httpRequest.headersList.append("accept-encoding", "gzip, deflate", true);
		httpRequest.headersList.delete("host", true);
		if (includeCredentials) {}
		if (httpCache == null) httpRequest.cache = "no-store";
		if (httpRequest.cache !== "no-store" && httpRequest.cache !== "reload") {}
		if (response == null) {
			if (httpRequest.cache === "only-if-cached") return makeNetworkError("only if cached");
			const forwardResponse = await httpNetworkFetch(httpFetchParams, includeCredentials, isNewConnectionFetch);
			if (!safeMethodsSet.has(httpRequest.method) && forwardResponse.status >= 200 && forwardResponse.status <= 399) {}
			if (response == null) response = forwardResponse;
		}
		response.urlList = [...httpRequest.urlList];
		if (httpRequest.headersList.contains("range", true)) response.rangeRequested = true;
		response.requestIncludesCredentials = includeCredentials;
		if (response.status === 407) {
			if (request$2.window === "no-window") return makeNetworkError();
			if (isCancelled(fetchParams)) return makeAppropriateNetworkError(fetchParams);
			return makeNetworkError("proxy authentication required");
		}
		if (response.status === 421 && !isNewConnectionFetch && (request$2.body == null || request$2.body.source != null)) {
			if (isCancelled(fetchParams)) return makeAppropriateNetworkError(fetchParams);
			fetchParams.controller.connection.destroy();
			response = await httpNetworkOrCacheFetch(fetchParams, isAuthenticationFetch, true);
		}
		if (isAuthenticationFetch) {}
		return response;
	}
	async function httpNetworkFetch(fetchParams, includeCredentials = false, forceNewConnection = false) {
		assert$5(!fetchParams.controller.connection || fetchParams.controller.connection.destroyed);
		fetchParams.controller.connection = {
			abort: null,
			destroyed: false,
			destroy(err, abort$1 = true) {
				if (!this.destroyed) {
					this.destroyed = true;
					if (abort$1) this.abort?.(err ?? new DOMException("The operation was aborted.", "AbortError"));
				}
			}
		};
		const request$2 = fetchParams.request;
		let response = null;
		const timingInfo = fetchParams.timingInfo;
		request$2.cache = "no-store";
		if (request$2.mode === "websocket") {}
		let requestBody = null;
		if (request$2.body == null && fetchParams.processRequestEndOfBody) queueMicrotask(() => fetchParams.processRequestEndOfBody());
		else if (request$2.body != null) {
			const processBodyChunk = async function* (bytes) {
				if (isCancelled(fetchParams)) return;
				yield bytes;
				fetchParams.processRequestBodyChunkLength?.(bytes.byteLength);
			};
			const processEndOfBody = () => {
				if (isCancelled(fetchParams)) return;
				if (fetchParams.processRequestEndOfBody) fetchParams.processRequestEndOfBody();
			};
			const processBodyError = (e) => {
				if (isCancelled(fetchParams)) return;
				if (e.name === "AbortError") fetchParams.controller.abort();
				else fetchParams.controller.terminate(e);
			};
			requestBody = (async function* () {
				try {
					for await (const bytes of request$2.body.stream) yield* processBodyChunk(bytes);
					processEndOfBody();
				} catch (err) {
					processBodyError(err);
				}
			})();
		}
		try {
			const { body, status, statusText, headersList, socket } = await dispatch({ body: requestBody });
			if (socket) response = makeResponse({
				status,
				statusText,
				headersList,
				socket
			});
			else {
				const iterator = body[Symbol.asyncIterator]();
				fetchParams.controller.next = () => iterator.next();
				response = makeResponse({
					status,
					statusText,
					headersList
				});
			}
		} catch (err) {
			if (err.name === "AbortError") {
				fetchParams.controller.connection.destroy();
				return makeAppropriateNetworkError(fetchParams, err);
			}
			return makeNetworkError(err);
		}
		const pullAlgorithm = () => {
			return fetchParams.controller.resume();
		};
		const cancelAlgorithm = (reason) => {
			if (!isCancelled(fetchParams)) fetchParams.controller.abort(reason);
		};
		const stream$2 = new ReadableStream({
			start(controller) {
				fetchParams.controller.controller = controller;
			},
			pull: pullAlgorithm,
			cancel: cancelAlgorithm,
			type: "bytes"
		});
		response.body = {
			stream: stream$2,
			source: null,
			length: null
		};
		if (!fetchParams.controller.resume) fetchParams.controller.on("terminated", onAborted);
		fetchParams.controller.resume = async () => {
			while (true) {
				let bytes;
				let isFailure;
				try {
					const { done, value } = await fetchParams.controller.next();
					if (isAborted(fetchParams)) break;
					bytes = done ? void 0 : value;
				} catch (err) {
					if (fetchParams.controller.ended && !timingInfo.encodedBodySize) bytes = void 0;
					else {
						bytes = err;
						isFailure = true;
					}
				}
				if (bytes === void 0) {
					readableStreamClose(fetchParams.controller.controller);
					finalizeResponse(fetchParams, response);
					return;
				}
				timingInfo.decodedBodySize += bytes?.byteLength ?? 0;
				if (isFailure) {
					fetchParams.controller.terminate(bytes);
					return;
				}
				const buffer$1 = new Uint8Array(bytes);
				if (buffer$1.byteLength) fetchParams.controller.controller.enqueue(buffer$1);
				if (isErrored(stream$2)) {
					fetchParams.controller.terminate();
					return;
				}
				if (fetchParams.controller.controller.desiredSize <= 0) return;
			}
		};
		function onAborted(reason) {
			if (isAborted(fetchParams)) {
				response.aborted = true;
				if (isReadable(stream$2)) fetchParams.controller.controller.error(fetchParams.controller.serializedAbortReason);
			} else if (isReadable(stream$2)) fetchParams.controller.controller.error(new TypeError("terminated", { cause: isErrorLike(reason) ? reason : void 0 }));
			fetchParams.controller.connection.destroy();
		}
		return response;
		function dispatch({ body }) {
			const url = requestCurrentURL(request$2);
			/** @type {import('../../..').Agent} */
			const agent = fetchParams.controller.dispatcher;
			return new Promise((resolve$1, reject) => agent.dispatch({
				path: url.pathname + url.search,
				origin: url.origin,
				method: request$2.method,
				body: agent.isMockActive ? request$2.body && (request$2.body.source || request$2.body.stream) : body,
				headers: request$2.headersList.entries,
				maxRedirections: 0,
				upgrade: request$2.mode === "websocket" ? "websocket" : void 0
			}, {
				body: null,
				abort: null,
				onConnect(abort$1) {
					const { connection } = fetchParams.controller;
					timingInfo.finalConnectionTimingInfo = clampAndCoarsenConnectionTimingInfo(void 0, timingInfo.postRedirectStartTime, fetchParams.crossOriginIsolatedCapability);
					if (connection.destroyed) abort$1(new DOMException("The operation was aborted.", "AbortError"));
					else {
						fetchParams.controller.on("terminated", abort$1);
						this.abort = connection.abort = abort$1;
					}
					timingInfo.finalNetworkRequestStartTime = coarsenedSharedCurrentTime(fetchParams.crossOriginIsolatedCapability);
				},
				onResponseStarted() {
					timingInfo.finalNetworkResponseStartTime = coarsenedSharedCurrentTime(fetchParams.crossOriginIsolatedCapability);
				},
				onHeaders(status, rawHeaders, resume$1, statusText) {
					if (status < 200) return false;
					/** @type {string[]} */
					let codings = [];
					const headersList = new HeadersList();
					for (let i = 0; i < rawHeaders.length; i += 2) headersList.append(bufferToLowerCasedHeaderName(rawHeaders[i]), rawHeaders[i + 1].toString("latin1"), true);
					const contentEncoding = headersList.get("content-encoding", true);
					if (contentEncoding) codings = contentEncoding.toLowerCase().split(",").map((x) => x.trim());
					const location = headersList.get("location", true);
					this.body = new Readable({ read: resume$1 });
					const decoders = [];
					const willFollow = location && request$2.redirect === "follow" && redirectStatusSet.has(status);
					if (codings.length !== 0 && request$2.method !== "HEAD" && request$2.method !== "CONNECT" && !nullBodyStatus.includes(status) && !willFollow) for (let i = codings.length - 1; i >= 0; --i) {
						const coding = codings[i];
						if (coding === "x-gzip" || coding === "gzip") decoders.push(zlib.createGunzip({
							flush: zlib.constants.Z_SYNC_FLUSH,
							finishFlush: zlib.constants.Z_SYNC_FLUSH
						}));
						else if (coding === "deflate") decoders.push(createInflate({
							flush: zlib.constants.Z_SYNC_FLUSH,
							finishFlush: zlib.constants.Z_SYNC_FLUSH
						}));
						else if (coding === "br") decoders.push(zlib.createBrotliDecompress({
							flush: zlib.constants.BROTLI_OPERATION_FLUSH,
							finishFlush: zlib.constants.BROTLI_OPERATION_FLUSH
						}));
						else if (coding === "zstd" && typeof zlib.createZstdDecompress === "function") decoders.push(zlib.createZstdDecompress({
							flush: zlib.constants.ZSTD_e_continue,
							finishFlush: zlib.constants.ZSTD_e_end
						}));
						else {
							decoders.length = 0;
							break;
						}
					}
					const onError$1 = this.onError.bind(this);
					resolve$1({
						status,
						statusText,
						headersList,
						body: decoders.length ? pipeline$1(this.body, ...decoders, (err) => {
							if (err) this.onError(err);
						}).on("error", onError$1) : this.body.on("error", onError$1)
					});
					return true;
				},
				onData(chunk) {
					if (fetchParams.controller.dump) return;
					const bytes = chunk;
					timingInfo.encodedBodySize += bytes.byteLength;
					return this.body.push(bytes);
				},
				onComplete() {
					if (this.abort) fetchParams.controller.off("terminated", this.abort);
					fetchParams.controller.ended = true;
					this.body.push(null);
				},
				onError(error) {
					if (this.abort) fetchParams.controller.off("terminated", this.abort);
					this.body?.destroy(error);
					fetchParams.controller.terminate(error);
					reject(error);
				},
				onUpgrade(status, rawHeaders, socket) {
					if (status !== 101) return;
					const headersList = new HeadersList();
					for (let i = 0; i < rawHeaders.length; i += 2) headersList.append(bufferToLowerCasedHeaderName(rawHeaders[i]), rawHeaders[i + 1].toString("latin1"), true);
					resolve$1({
						status,
						statusText: STATUS_CODES[status],
						headersList,
						socket
					});
					return true;
				}
			}));
		}
	}
	module.exports = {
		fetch,
		Fetch,
		fetching: fetching$3,
		finalizeAndReportTiming
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/cache/util.js
var require_util$3 = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/cache/util.js": ((exports, module) => {
	const assert$4 = __require("node:assert");
	const { URLSerializer: URLSerializer$1 } = require_data_url();
	const { isValidHeaderName } = require_util$4();
	/**
	* @see https://url.spec.whatwg.org/#concept-url-equals
	* @param {URL} A
	* @param {URL} B
	* @param {boolean | undefined} excludeFragment
	* @returns {boolean}
	*/
	function urlEquals$1(A, B, excludeFragment = false) {
		const serializedA = URLSerializer$1(A, excludeFragment);
		const serializedB = URLSerializer$1(B, excludeFragment);
		return serializedA === serializedB;
	}
	/**
	* @see https://github.com/chromium/chromium/blob/694d20d134cb553d8d89e5500b9148012b1ba299/content/browser/cache_storage/cache_storage_cache.cc#L260-L262
	* @param {string} header
	*/
	function getFieldValues$1(header) {
		assert$4(header !== null);
		const values = [];
		for (let value of header.split(",")) {
			value = value.trim();
			if (isValidHeaderName(value)) values.push(value);
		}
		return values;
	}
	module.exports = {
		urlEquals: urlEquals$1,
		getFieldValues: getFieldValues$1
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/cache/cache.js
var require_cache = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/cache/cache.js": ((exports, module) => {
	const assert$3 = __require("node:assert");
	const { kConstruct: kConstruct$4 } = require_symbols();
	const { urlEquals, getFieldValues } = require_util$3();
	const { kEnumerableProperty: kEnumerableProperty$6, isDisturbed } = require_util$5();
	const { webidl: webidl$7 } = require_webidl();
	const { cloneResponse, fromInnerResponse, getResponseState } = require_response();
	const { Request, fromInnerRequest, getRequestState } = require_request();
	const { fetching: fetching$2 } = require_fetch();
	const { urlIsHttpHttpsScheme, readAllBytes } = require_util$4();
	const { createDeferredPromise: createDeferredPromise$1 } = require_promise();
	/**
	* @see https://w3c.github.io/ServiceWorker/#dfn-cache-batch-operation
	* @typedef {Object} CacheBatchOperation
	* @property {'delete' | 'put'} type
	* @property {any} request
	* @property {any} response
	* @property {import('../../../types/cache').CacheQueryOptions} options
	*/
	/**
	* @see https://w3c.github.io/ServiceWorker/#dfn-request-response-list
	* @typedef {[any, any][]} requestResponseList
	*/
	var Cache$1 = class Cache$1 {
		/**
		* @see https://w3c.github.io/ServiceWorker/#dfn-relevant-request-response-list
		* @type {requestResponseList}
		*/
		#relevantRequestResponseList;
		constructor() {
			if (arguments[0] !== kConstruct$4) webidl$7.illegalConstructor();
			webidl$7.util.markAsUncloneable(this);
			this.#relevantRequestResponseList = arguments[1];
		}
		async match(request$2, options = {}) {
			webidl$7.brandCheck(this, Cache$1);
			const prefix = "Cache.match";
			webidl$7.argumentLengthCheck(arguments, 1, prefix);
			request$2 = webidl$7.converters.RequestInfo(request$2);
			options = webidl$7.converters.CacheQueryOptions(options, prefix, "options");
			const p = this.#internalMatchAll(request$2, options, 1);
			if (p.length === 0) return;
			return p[0];
		}
		async matchAll(request$2 = void 0, options = {}) {
			webidl$7.brandCheck(this, Cache$1);
			const prefix = "Cache.matchAll";
			if (request$2 !== void 0) request$2 = webidl$7.converters.RequestInfo(request$2);
			options = webidl$7.converters.CacheQueryOptions(options, prefix, "options");
			return this.#internalMatchAll(request$2, options);
		}
		async add(request$2) {
			webidl$7.brandCheck(this, Cache$1);
			webidl$7.argumentLengthCheck(arguments, 1, "Cache.add");
			request$2 = webidl$7.converters.RequestInfo(request$2);
			const requests = [request$2];
			return await this.addAll(requests);
		}
		async addAll(requests) {
			webidl$7.brandCheck(this, Cache$1);
			const prefix = "Cache.addAll";
			webidl$7.argumentLengthCheck(arguments, 1, prefix);
			const responsePromises = [];
			const requestList = [];
			for (let request$2 of requests) {
				if (request$2 === void 0) throw webidl$7.errors.conversionFailed({
					prefix,
					argument: "Argument 1",
					types: ["undefined is not allowed"]
				});
				request$2 = webidl$7.converters.RequestInfo(request$2);
				if (typeof request$2 === "string") continue;
				const r = getRequestState(request$2);
				if (!urlIsHttpHttpsScheme(r.url) || r.method !== "GET") throw webidl$7.errors.exception({
					header: prefix,
					message: "Expected http/s scheme when method is not GET."
				});
			}
			/** @type {ReturnType<typeof fetching>[]} */
			const fetchControllers = [];
			for (const request$2 of requests) {
				const r = getRequestState(new Request(request$2));
				if (!urlIsHttpHttpsScheme(r.url)) throw webidl$7.errors.exception({
					header: prefix,
					message: "Expected http/s scheme."
				});
				r.initiator = "fetch";
				r.destination = "subresource";
				requestList.push(r);
				const responsePromise = createDeferredPromise$1();
				fetchControllers.push(fetching$2({
					request: r,
					processResponse(response) {
						if (response.type === "error" || response.status === 206 || response.status < 200 || response.status > 299) responsePromise.reject(webidl$7.errors.exception({
							header: "Cache.addAll",
							message: "Received an invalid status code or the request failed."
						}));
						else if (response.headersList.contains("vary")) {
							const fieldValues = getFieldValues(response.headersList.get("vary"));
							for (const fieldValue of fieldValues) if (fieldValue === "*") {
								responsePromise.reject(webidl$7.errors.exception({
									header: "Cache.addAll",
									message: "invalid vary field value"
								}));
								for (const controller of fetchControllers) controller.abort();
								return;
							}
						}
					},
					processResponseEndOfBody(response) {
						if (response.aborted) {
							responsePromise.reject(new DOMException("aborted", "AbortError"));
							return;
						}
						responsePromise.resolve(response);
					}
				}));
				responsePromises.push(responsePromise.promise);
			}
			const responses = await Promise.all(responsePromises);
			const operations = [];
			let index = 0;
			for (const response of responses) {
				/** @type {CacheBatchOperation} */
				const operation = {
					type: "put",
					request: requestList[index],
					response
				};
				operations.push(operation);
				index++;
			}
			const cacheJobPromise = createDeferredPromise$1();
			let errorData = null;
			try {
				this.#batchCacheOperations(operations);
			} catch (e) {
				errorData = e;
			}
			queueMicrotask(() => {
				if (errorData === null) cacheJobPromise.resolve(void 0);
				else cacheJobPromise.reject(errorData);
			});
			return cacheJobPromise.promise;
		}
		async put(request$2, response) {
			webidl$7.brandCheck(this, Cache$1);
			const prefix = "Cache.put";
			webidl$7.argumentLengthCheck(arguments, 2, prefix);
			request$2 = webidl$7.converters.RequestInfo(request$2);
			response = webidl$7.converters.Response(response, prefix, "response");
			let innerRequest = null;
			if (webidl$7.is.Request(request$2)) innerRequest = getRequestState(request$2);
			else innerRequest = getRequestState(new Request(request$2));
			if (!urlIsHttpHttpsScheme(innerRequest.url) || innerRequest.method !== "GET") throw webidl$7.errors.exception({
				header: prefix,
				message: "Expected an http/s scheme when method is not GET"
			});
			const innerResponse = getResponseState(response);
			if (innerResponse.status === 206) throw webidl$7.errors.exception({
				header: prefix,
				message: "Got 206 status"
			});
			if (innerResponse.headersList.contains("vary")) {
				const fieldValues = getFieldValues(innerResponse.headersList.get("vary"));
				for (const fieldValue of fieldValues) if (fieldValue === "*") throw webidl$7.errors.exception({
					header: prefix,
					message: "Got * vary field value"
				});
			}
			if (innerResponse.body && (isDisturbed(innerResponse.body.stream) || innerResponse.body.stream.locked)) throw webidl$7.errors.exception({
				header: prefix,
				message: "Response body is locked or disturbed"
			});
			const clonedResponse = cloneResponse(innerResponse);
			const bodyReadPromise = createDeferredPromise$1();
			if (innerResponse.body != null) {
				const reader = innerResponse.body.stream.getReader();
				readAllBytes(reader, bodyReadPromise.resolve, bodyReadPromise.reject);
			} else bodyReadPromise.resolve(void 0);
			/** @type {CacheBatchOperation[]} */
			const operations = [];
			/** @type {CacheBatchOperation} */
			const operation = {
				type: "put",
				request: innerRequest,
				response: clonedResponse
			};
			operations.push(operation);
			const bytes = await bodyReadPromise.promise;
			if (clonedResponse.body != null) clonedResponse.body.source = bytes;
			const cacheJobPromise = createDeferredPromise$1();
			let errorData = null;
			try {
				this.#batchCacheOperations(operations);
			} catch (e) {
				errorData = e;
			}
			queueMicrotask(() => {
				if (errorData === null) cacheJobPromise.resolve();
				else cacheJobPromise.reject(errorData);
			});
			return cacheJobPromise.promise;
		}
		async delete(request$2, options = {}) {
			webidl$7.brandCheck(this, Cache$1);
			const prefix = "Cache.delete";
			webidl$7.argumentLengthCheck(arguments, 1, prefix);
			request$2 = webidl$7.converters.RequestInfo(request$2);
			options = webidl$7.converters.CacheQueryOptions(options, prefix, "options");
			/**
			* @type {Request}
			*/
			let r = null;
			if (webidl$7.is.Request(request$2)) {
				r = getRequestState(request$2);
				if (r.method !== "GET" && !options.ignoreMethod) return false;
			} else {
				assert$3(typeof request$2 === "string");
				r = getRequestState(new Request(request$2));
			}
			/** @type {CacheBatchOperation[]} */
			const operations = [];
			/** @type {CacheBatchOperation} */
			const operation = {
				type: "delete",
				request: r,
				options
			};
			operations.push(operation);
			const cacheJobPromise = createDeferredPromise$1();
			let errorData = null;
			let requestResponses;
			try {
				requestResponses = this.#batchCacheOperations(operations);
			} catch (e) {
				errorData = e;
			}
			queueMicrotask(() => {
				if (errorData === null) cacheJobPromise.resolve(!!requestResponses?.length);
				else cacheJobPromise.reject(errorData);
			});
			return cacheJobPromise.promise;
		}
		/**
		* @see https://w3c.github.io/ServiceWorker/#dom-cache-keys
		* @param {any} request
		* @param {import('../../../types/cache').CacheQueryOptions} options
		* @returns {Promise<readonly Request[]>}
		*/
		async keys(request$2 = void 0, options = {}) {
			webidl$7.brandCheck(this, Cache$1);
			const prefix = "Cache.keys";
			if (request$2 !== void 0) request$2 = webidl$7.converters.RequestInfo(request$2);
			options = webidl$7.converters.CacheQueryOptions(options, prefix, "options");
			let r = null;
			if (request$2 !== void 0) {
				if (webidl$7.is.Request(request$2)) {
					r = getRequestState(request$2);
					if (r.method !== "GET" && !options.ignoreMethod) return [];
				} else if (typeof request$2 === "string") r = getRequestState(new Request(request$2));
			}
			const promise = createDeferredPromise$1();
			const requests = [];
			if (request$2 === void 0) for (const requestResponse of this.#relevantRequestResponseList) requests.push(requestResponse[0]);
			else {
				const requestResponses = this.#queryCache(r, options);
				for (const requestResponse of requestResponses) requests.push(requestResponse[0]);
			}
			queueMicrotask(() => {
				const requestList = [];
				for (const request$3 of requests) {
					const requestObject = fromInnerRequest(request$3, void 0, new AbortController().signal, "immutable");
					requestList.push(requestObject);
				}
				promise.resolve(Object.freeze(requestList));
			});
			return promise.promise;
		}
		/**
		* @see https://w3c.github.io/ServiceWorker/#batch-cache-operations-algorithm
		* @param {CacheBatchOperation[]} operations
		* @returns {requestResponseList}
		*/
		#batchCacheOperations(operations) {
			const cache = this.#relevantRequestResponseList;
			const backupCache = [...cache];
			const addedItems = [];
			const resultList = [];
			try {
				for (const operation of operations) {
					if (operation.type !== "delete" && operation.type !== "put") throw webidl$7.errors.exception({
						header: "Cache.#batchCacheOperations",
						message: "operation type does not match \"delete\" or \"put\""
					});
					if (operation.type === "delete" && operation.response != null) throw webidl$7.errors.exception({
						header: "Cache.#batchCacheOperations",
						message: "delete operation should not have an associated response"
					});
					if (this.#queryCache(operation.request, operation.options, addedItems).length) throw new DOMException("???", "InvalidStateError");
					let requestResponses;
					if (operation.type === "delete") {
						requestResponses = this.#queryCache(operation.request, operation.options);
						if (requestResponses.length === 0) return [];
						for (const requestResponse of requestResponses) {
							const idx = cache.indexOf(requestResponse);
							assert$3(idx !== -1);
							cache.splice(idx, 1);
						}
					} else if (operation.type === "put") {
						if (operation.response == null) throw webidl$7.errors.exception({
							header: "Cache.#batchCacheOperations",
							message: "put operation should have an associated response"
						});
						const r = operation.request;
						if (!urlIsHttpHttpsScheme(r.url)) throw webidl$7.errors.exception({
							header: "Cache.#batchCacheOperations",
							message: "expected http or https scheme"
						});
						if (r.method !== "GET") throw webidl$7.errors.exception({
							header: "Cache.#batchCacheOperations",
							message: "not get method"
						});
						if (operation.options != null) throw webidl$7.errors.exception({
							header: "Cache.#batchCacheOperations",
							message: "options must not be defined"
						});
						requestResponses = this.#queryCache(operation.request);
						for (const requestResponse of requestResponses) {
							const idx = cache.indexOf(requestResponse);
							assert$3(idx !== -1);
							cache.splice(idx, 1);
						}
						cache.push([operation.request, operation.response]);
						addedItems.push([operation.request, operation.response]);
					}
					resultList.push([operation.request, operation.response]);
				}
				return resultList;
			} catch (e) {
				this.#relevantRequestResponseList.length = 0;
				this.#relevantRequestResponseList = backupCache;
				throw e;
			}
		}
		/**
		* @see https://w3c.github.io/ServiceWorker/#query-cache
		* @param {any} requestQuery
		* @param {import('../../../types/cache').CacheQueryOptions} options
		* @param {requestResponseList} targetStorage
		* @returns {requestResponseList}
		*/
		#queryCache(requestQuery, options, targetStorage) {
			/** @type {requestResponseList} */
			const resultList = [];
			const storage = targetStorage ?? this.#relevantRequestResponseList;
			for (const requestResponse of storage) {
				const [cachedRequest, cachedResponse] = requestResponse;
				if (this.#requestMatchesCachedItem(requestQuery, cachedRequest, cachedResponse, options)) resultList.push(requestResponse);
			}
			return resultList;
		}
		/**
		* @see https://w3c.github.io/ServiceWorker/#request-matches-cached-item-algorithm
		* @param {any} requestQuery
		* @param {any} request
		* @param {any | null} response
		* @param {import('../../../types/cache').CacheQueryOptions | undefined} options
		* @returns {boolean}
		*/
		#requestMatchesCachedItem(requestQuery, request$2, response = null, options) {
			const queryURL = new URL(requestQuery.url);
			const cachedURL = new URL(request$2.url);
			if (options?.ignoreSearch) {
				cachedURL.search = "";
				queryURL.search = "";
			}
			if (!urlEquals(queryURL, cachedURL, true)) return false;
			if (response == null || options?.ignoreVary || !response.headersList.contains("vary")) return true;
			const fieldValues = getFieldValues(response.headersList.get("vary"));
			for (const fieldValue of fieldValues) {
				if (fieldValue === "*") return false;
				const requestValue = request$2.headersList.get(fieldValue);
				const queryValue = requestQuery.headersList.get(fieldValue);
				if (requestValue !== queryValue) return false;
			}
			return true;
		}
		#internalMatchAll(request$2, options, maxResponses = Infinity) {
			let r = null;
			if (request$2 !== void 0) {
				if (webidl$7.is.Request(request$2)) {
					r = getRequestState(request$2);
					if (r.method !== "GET" && !options.ignoreMethod) return [];
				} else if (typeof request$2 === "string") r = getRequestState(new Request(request$2));
			}
			const responses = [];
			if (request$2 === void 0) for (const requestResponse of this.#relevantRequestResponseList) responses.push(requestResponse[1]);
			else {
				const requestResponses = this.#queryCache(r, options);
				for (const requestResponse of requestResponses) responses.push(requestResponse[1]);
			}
			const responseList = [];
			for (const response of responses) {
				const responseObject = fromInnerResponse(response, "immutable");
				responseList.push(responseObject.clone());
				if (responseList.length >= maxResponses) break;
			}
			return Object.freeze(responseList);
		}
	};
	Object.defineProperties(Cache$1.prototype, {
		[Symbol.toStringTag]: {
			value: "Cache",
			configurable: true
		},
		match: kEnumerableProperty$6,
		matchAll: kEnumerableProperty$6,
		add: kEnumerableProperty$6,
		addAll: kEnumerableProperty$6,
		put: kEnumerableProperty$6,
		delete: kEnumerableProperty$6,
		keys: kEnumerableProperty$6
	});
	const cacheQueryOptionConverters = [
		{
			key: "ignoreSearch",
			converter: webidl$7.converters.boolean,
			defaultValue: () => false
		},
		{
			key: "ignoreMethod",
			converter: webidl$7.converters.boolean,
			defaultValue: () => false
		},
		{
			key: "ignoreVary",
			converter: webidl$7.converters.boolean,
			defaultValue: () => false
		}
	];
	webidl$7.converters.CacheQueryOptions = webidl$7.dictionaryConverter(cacheQueryOptionConverters);
	webidl$7.converters.MultiCacheQueryOptions = webidl$7.dictionaryConverter([...cacheQueryOptionConverters, {
		key: "cacheName",
		converter: webidl$7.converters.DOMString
	}]);
	webidl$7.converters.Response = webidl$7.interfaceConverter(webidl$7.is.Response, "Response");
	webidl$7.converters["sequence<RequestInfo>"] = webidl$7.sequenceConverter(webidl$7.converters.RequestInfo);
	module.exports = { Cache: Cache$1 };
}) });

//#endregion
//#region node_modules/undici/lib/web/cache/cachestorage.js
var require_cachestorage = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/cache/cachestorage.js": ((exports, module) => {
	const { Cache } = require_cache();
	const { webidl: webidl$6 } = require_webidl();
	const { kEnumerableProperty: kEnumerableProperty$5 } = require_util$5();
	const { kConstruct: kConstruct$3 } = require_symbols();
	var CacheStorage$1 = class CacheStorage$1 {
		/**
		* @see https://w3c.github.io/ServiceWorker/#dfn-relevant-name-to-cache-map
		* @type {Map<string, import('./cache').requestResponseList}
		*/
		#caches = /* @__PURE__ */ new Map();
		constructor() {
			if (arguments[0] !== kConstruct$3) webidl$6.illegalConstructor();
			webidl$6.util.markAsUncloneable(this);
		}
		async match(request$2, options = {}) {
			webidl$6.brandCheck(this, CacheStorage$1);
			webidl$6.argumentLengthCheck(arguments, 1, "CacheStorage.match");
			request$2 = webidl$6.converters.RequestInfo(request$2);
			options = webidl$6.converters.MultiCacheQueryOptions(options);
			if (options.cacheName != null) {
				if (this.#caches.has(options.cacheName)) {
					const cacheList = this.#caches.get(options.cacheName);
					return await new Cache(kConstruct$3, cacheList).match(request$2, options);
				}
			} else for (const cacheList of this.#caches.values()) {
				const response = await new Cache(kConstruct$3, cacheList).match(request$2, options);
				if (response !== void 0) return response;
			}
		}
		/**
		* @see https://w3c.github.io/ServiceWorker/#cache-storage-has
		* @param {string} cacheName
		* @returns {Promise<boolean>}
		*/
		async has(cacheName) {
			webidl$6.brandCheck(this, CacheStorage$1);
			const prefix = "CacheStorage.has";
			webidl$6.argumentLengthCheck(arguments, 1, prefix);
			cacheName = webidl$6.converters.DOMString(cacheName, prefix, "cacheName");
			return this.#caches.has(cacheName);
		}
		/**
		* @see https://w3c.github.io/ServiceWorker/#dom-cachestorage-open
		* @param {string} cacheName
		* @returns {Promise<Cache>}
		*/
		async open(cacheName) {
			webidl$6.brandCheck(this, CacheStorage$1);
			const prefix = "CacheStorage.open";
			webidl$6.argumentLengthCheck(arguments, 1, prefix);
			cacheName = webidl$6.converters.DOMString(cacheName, prefix, "cacheName");
			if (this.#caches.has(cacheName)) {
				const cache$1 = this.#caches.get(cacheName);
				return new Cache(kConstruct$3, cache$1);
			}
			const cache = [];
			this.#caches.set(cacheName, cache);
			return new Cache(kConstruct$3, cache);
		}
		/**
		* @see https://w3c.github.io/ServiceWorker/#cache-storage-delete
		* @param {string} cacheName
		* @returns {Promise<boolean>}
		*/
		async delete(cacheName) {
			webidl$6.brandCheck(this, CacheStorage$1);
			const prefix = "CacheStorage.delete";
			webidl$6.argumentLengthCheck(arguments, 1, prefix);
			cacheName = webidl$6.converters.DOMString(cacheName, prefix, "cacheName");
			return this.#caches.delete(cacheName);
		}
		/**
		* @see https://w3c.github.io/ServiceWorker/#cache-storage-keys
		* @returns {Promise<string[]>}
		*/
		async keys() {
			webidl$6.brandCheck(this, CacheStorage$1);
			return [...this.#caches.keys()];
		}
	};
	Object.defineProperties(CacheStorage$1.prototype, {
		[Symbol.toStringTag]: {
			value: "CacheStorage",
			configurable: true
		},
		match: kEnumerableProperty$5,
		has: kEnumerableProperty$5,
		open: kEnumerableProperty$5,
		delete: kEnumerableProperty$5,
		keys: kEnumerableProperty$5
	});
	module.exports = { CacheStorage: CacheStorage$1 };
}) });

//#endregion
//#region node_modules/undici/lib/web/cookies/constants.js
var require_constants$1 = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/cookies/constants.js": ((exports, module) => {
	const maxAttributeValueSize$1 = 1024;
	const maxNameValuePairSize$1 = 4096;
	module.exports = {
		maxAttributeValueSize: maxAttributeValueSize$1,
		maxNameValuePairSize: maxNameValuePairSize$1
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/cookies/util.js
var require_util$2 = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/cookies/util.js": ((exports, module) => {
	/**
	* @param {string} value
	* @returns {boolean}
	*/
	function isCTLExcludingHtab$1(value) {
		for (let i = 0; i < value.length; ++i) {
			const code = value.charCodeAt(i);
			if (code >= 0 && code <= 8 || code >= 10 && code <= 31 || code === 127) return true;
		}
		return false;
	}
	/**
	CHAR           = <any US-ASCII character (octets 0 - 127)>
	token          = 1*<any CHAR except CTLs or separators>
	separators     = "(" | ")" | "<" | ">" | "@"
	| "," | ";" | ":" | "\" | <">
	| "/" | "[" | "]" | "?" | "="
	| "{" | "}" | SP | HT
	* @param {string} name
	*/
	function validateCookieName(name) {
		for (let i = 0; i < name.length; ++i) {
			const code = name.charCodeAt(i);
			if (code < 33 || code > 126 || code === 34 || code === 40 || code === 41 || code === 60 || code === 62 || code === 64 || code === 44 || code === 59 || code === 58 || code === 92 || code === 47 || code === 91 || code === 93 || code === 63 || code === 61 || code === 123 || code === 125) throw new Error("Invalid cookie name");
		}
	}
	/**
	cookie-value      = *cookie-octet / ( DQUOTE *cookie-octet DQUOTE )
	cookie-octet      = %x21 / %x23-2B / %x2D-3A / %x3C-5B / %x5D-7E
	; US-ASCII characters excluding CTLs,
	; whitespace DQUOTE, comma, semicolon,
	; and backslash
	* @param {string} value
	*/
	function validateCookieValue(value) {
		let len = value.length;
		let i = 0;
		if (value[0] === "\"") {
			if (len === 1 || value[len - 1] !== "\"") throw new Error("Invalid cookie value");
			--len;
			++i;
		}
		while (i < len) {
			const code = value.charCodeAt(i++);
			if (code < 33 || code > 126 || code === 34 || code === 44 || code === 59 || code === 92) throw new Error("Invalid cookie value");
		}
	}
	/**
	* path-value        = <any CHAR except CTLs or ";">
	* @param {string} path
	*/
	function validateCookiePath(path$1) {
		for (let i = 0; i < path$1.length; ++i) {
			const code = path$1.charCodeAt(i);
			if (code < 32 || code === 127 || code === 59) throw new Error("Invalid cookie path");
		}
	}
	/**
	* I have no idea why these values aren't allowed to be honest,
	* but Deno tests these. - Khafra
	* @param {string} domain
	*/
	function validateCookieDomain(domain) {
		if (domain.startsWith("-") || domain.endsWith(".") || domain.endsWith("-")) throw new Error("Invalid cookie domain");
	}
	const IMFDays = [
		"Sun",
		"Mon",
		"Tue",
		"Wed",
		"Thu",
		"Fri",
		"Sat"
	];
	const IMFMonths = [
		"Jan",
		"Feb",
		"Mar",
		"Apr",
		"May",
		"Jun",
		"Jul",
		"Aug",
		"Sep",
		"Oct",
		"Nov",
		"Dec"
	];
	const IMFPaddedNumbers = Array(61).fill(0).map((_, i) => i.toString().padStart(2, "0"));
	/**
	* @see https://www.rfc-editor.org/rfc/rfc7231#section-7.1.1.1
	* @param {number|Date} date
	IMF-fixdate  = day-name "," SP date1 SP time-of-day SP GMT
	; fixed length/zone/capitalization subset of the format
	; see Section 3.3 of [RFC5322]
	
	day-name     = %x4D.6F.6E ; "Mon", case-sensitive
	/ %x54.75.65 ; "Tue", case-sensitive
	/ %x57.65.64 ; "Wed", case-sensitive
	/ %x54.68.75 ; "Thu", case-sensitive
	/ %x46.72.69 ; "Fri", case-sensitive
	/ %x53.61.74 ; "Sat", case-sensitive
	/ %x53.75.6E ; "Sun", case-sensitive
	date1        = day SP month SP year
	; e.g., 02 Jun 1982
	
	day          = 2DIGIT
	month        = %x4A.61.6E ; "Jan", case-sensitive
	/ %x46.65.62 ; "Feb", case-sensitive
	/ %x4D.61.72 ; "Mar", case-sensitive
	/ %x41.70.72 ; "Apr", case-sensitive
	/ %x4D.61.79 ; "May", case-sensitive
	/ %x4A.75.6E ; "Jun", case-sensitive
	/ %x4A.75.6C ; "Jul", case-sensitive
	/ %x41.75.67 ; "Aug", case-sensitive
	/ %x53.65.70 ; "Sep", case-sensitive
	/ %x4F.63.74 ; "Oct", case-sensitive
	/ %x4E.6F.76 ; "Nov", case-sensitive
	/ %x44.65.63 ; "Dec", case-sensitive
	year         = 4DIGIT
	
	GMT          = %x47.4D.54 ; "GMT", case-sensitive
	
	time-of-day  = hour ":" minute ":" second
	; 00:00:00 - 23:59:60 (leap second)
	
	hour         = 2DIGIT
	minute       = 2DIGIT
	second       = 2DIGIT
	*/
	function toIMFDate(date) {
		if (typeof date === "number") date = new Date(date);
		return `${IMFDays[date.getUTCDay()]}, ${IMFPaddedNumbers[date.getUTCDate()]} ${IMFMonths[date.getUTCMonth()]} ${date.getUTCFullYear()} ${IMFPaddedNumbers[date.getUTCHours()]}:${IMFPaddedNumbers[date.getUTCMinutes()]}:${IMFPaddedNumbers[date.getUTCSeconds()]} GMT`;
	}
	/**
	max-age-av        = "Max-Age=" non-zero-digit *DIGIT
	; In practice, both expires-av and max-age-av
	; are limited to dates representable by the
	; user agent.
	* @param {number} maxAge
	*/
	function validateCookieMaxAge(maxAge) {
		if (maxAge < 0) throw new Error("Invalid cookie max-age");
	}
	/**
	* @see https://www.rfc-editor.org/rfc/rfc6265#section-4.1.1
	* @param {import('./index').Cookie} cookie
	*/
	function stringify$1(cookie) {
		if (cookie.name.length === 0) return null;
		validateCookieName(cookie.name);
		validateCookieValue(cookie.value);
		const out = [`${cookie.name}=${cookie.value}`];
		if (cookie.name.startsWith("__Secure-")) cookie.secure = true;
		if (cookie.name.startsWith("__Host-")) {
			cookie.secure = true;
			cookie.domain = null;
			cookie.path = "/";
		}
		if (cookie.secure) out.push("Secure");
		if (cookie.httpOnly) out.push("HttpOnly");
		if (typeof cookie.maxAge === "number") {
			validateCookieMaxAge(cookie.maxAge);
			out.push(`Max-Age=${cookie.maxAge}`);
		}
		if (cookie.domain) {
			validateCookieDomain(cookie.domain);
			out.push(`Domain=${cookie.domain}`);
		}
		if (cookie.path) {
			validateCookiePath(cookie.path);
			out.push(`Path=${cookie.path}`);
		}
		if (cookie.expires && cookie.expires.toString() !== "Invalid Date") out.push(`Expires=${toIMFDate(cookie.expires)}`);
		if (cookie.sameSite) out.push(`SameSite=${cookie.sameSite}`);
		for (const part of cookie.unparsed) {
			if (!part.includes("=")) throw new Error("Invalid unparsed");
			const [key, ...value] = part.split("=");
			out.push(`${key.trim()}=${value.join("=")}`);
		}
		return out.join("; ");
	}
	module.exports = {
		isCTLExcludingHtab: isCTLExcludingHtab$1,
		validateCookieName,
		validateCookiePath,
		validateCookieValue,
		toIMFDate,
		stringify: stringify$1
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/cookies/parse.js
var require_parse = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/cookies/parse.js": ((exports, module) => {
	const { maxNameValuePairSize, maxAttributeValueSize } = require_constants$1();
	const { isCTLExcludingHtab } = require_util$2();
	const { collectASequenceOfCodePointsFast: collectASequenceOfCodePointsFast$1 } = require_data_url();
	const assert$2 = __require("node:assert");
	const { unescape: qsUnescape } = __require("node:querystring");
	/**
	* @description Parses the field-value attributes of a set-cookie header string.
	* @see https://datatracker.ietf.org/doc/html/draft-ietf-httpbis-rfc6265bis#section-5.4
	* @param {string} header
	* @returns {import('./index').Cookie|null} if the header is invalid, null will be returned
	*/
	function parseSetCookie$1(header) {
		if (isCTLExcludingHtab(header)) return null;
		let nameValuePair = "";
		let unparsedAttributes = "";
		let name = "";
		let value = "";
		if (header.includes(";")) {
			const position = { position: 0 };
			nameValuePair = collectASequenceOfCodePointsFast$1(";", header, position);
			unparsedAttributes = header.slice(position.position);
		} else nameValuePair = header;
		if (!nameValuePair.includes("=")) value = nameValuePair;
		else {
			const position = { position: 0 };
			name = collectASequenceOfCodePointsFast$1("=", nameValuePair, position);
			value = nameValuePair.slice(position.position + 1);
		}
		name = name.trim();
		value = value.trim();
		if (name.length + value.length > maxNameValuePairSize) return null;
		return {
			name,
			value: qsUnescape(value),
			...parseUnparsedAttributes(unparsedAttributes)
		};
	}
	/**
	* Parses the remaining attributes of a set-cookie header
	* @see https://datatracker.ietf.org/doc/html/draft-ietf-httpbis-rfc6265bis#section-5.4
	* @param {string} unparsedAttributes
	* @param {Object.<string, unknown>} [cookieAttributeList={}]
	*/
	function parseUnparsedAttributes(unparsedAttributes, cookieAttributeList = {}) {
		if (unparsedAttributes.length === 0) return cookieAttributeList;
		assert$2(unparsedAttributes[0] === ";");
		unparsedAttributes = unparsedAttributes.slice(1);
		let cookieAv = "";
		if (unparsedAttributes.includes(";")) {
			cookieAv = collectASequenceOfCodePointsFast$1(";", unparsedAttributes, { position: 0 });
			unparsedAttributes = unparsedAttributes.slice(cookieAv.length);
		} else {
			cookieAv = unparsedAttributes;
			unparsedAttributes = "";
		}
		let attributeName = "";
		let attributeValue = "";
		if (cookieAv.includes("=")) {
			const position = { position: 0 };
			attributeName = collectASequenceOfCodePointsFast$1("=", cookieAv, position);
			attributeValue = cookieAv.slice(position.position + 1);
		} else attributeName = cookieAv;
		attributeName = attributeName.trim();
		attributeValue = attributeValue.trim();
		if (attributeValue.length > maxAttributeValueSize) return parseUnparsedAttributes(unparsedAttributes, cookieAttributeList);
		const attributeNameLowercase = attributeName.toLowerCase();
		if (attributeNameLowercase === "expires") cookieAttributeList.expires = new Date(attributeValue);
		else if (attributeNameLowercase === "max-age") {
			const charCode = attributeValue.charCodeAt(0);
			if ((charCode < 48 || charCode > 57) && attributeValue[0] !== "-") return parseUnparsedAttributes(unparsedAttributes, cookieAttributeList);
			if (!/^\d+$/.test(attributeValue)) return parseUnparsedAttributes(unparsedAttributes, cookieAttributeList);
			cookieAttributeList.maxAge = Number(attributeValue);
		} else if (attributeNameLowercase === "domain") {
			let cookieDomain = attributeValue;
			if (cookieDomain[0] === ".") cookieDomain = cookieDomain.slice(1);
			cookieDomain = cookieDomain.toLowerCase();
			cookieAttributeList.domain = cookieDomain;
		} else if (attributeNameLowercase === "path") {
			let cookiePath = "";
			if (attributeValue.length === 0 || attributeValue[0] !== "/") cookiePath = "/";
			else cookiePath = attributeValue;
			cookieAttributeList.path = cookiePath;
		} else if (attributeNameLowercase === "secure") cookieAttributeList.secure = true;
		else if (attributeNameLowercase === "httponly") cookieAttributeList.httpOnly = true;
		else if (attributeNameLowercase === "samesite") {
			let enforcement = "Default";
			const attributeValueLowercase = attributeValue.toLowerCase();
			if (attributeValueLowercase.includes("none")) enforcement = "None";
			if (attributeValueLowercase.includes("strict")) enforcement = "Strict";
			if (attributeValueLowercase.includes("lax")) enforcement = "Lax";
			cookieAttributeList.sameSite = enforcement;
		} else {
			cookieAttributeList.unparsed ??= [];
			cookieAttributeList.unparsed.push(`${attributeName}=${attributeValue}`);
		}
		return parseUnparsedAttributes(unparsedAttributes, cookieAttributeList);
	}
	module.exports = {
		parseSetCookie: parseSetCookie$1,
		parseUnparsedAttributes
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/cookies/index.js
var require_cookies = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/cookies/index.js": ((exports, module) => {
	const { parseSetCookie } = require_parse();
	const { stringify } = require_util$2();
	const { webidl: webidl$5 } = require_webidl();
	const { Headers: Headers$1 } = require_headers();
	const brandChecks = webidl$5.brandCheckMultiple([Headers$1, globalThis.Headers].filter(Boolean));
	/**
	* @typedef {Object} Cookie
	* @property {string} name
	* @property {string} value
	* @property {Date|number} [expires]
	* @property {number} [maxAge]
	* @property {string} [domain]
	* @property {string} [path]
	* @property {boolean} [secure]
	* @property {boolean} [httpOnly]
	* @property {'Strict'|'Lax'|'None'} [sameSite]
	* @property {string[]} [unparsed]
	*/
	/**
	* @param {Headers} headers
	* @returns {Record<string, string>}
	*/
	function getCookies$1(headers) {
		webidl$5.argumentLengthCheck(arguments, 1, "getCookies");
		brandChecks(headers);
		const cookie = headers.get("cookie");
		/** @type {Record<string, string>} */
		const out = {};
		if (!cookie) return out;
		for (const piece of cookie.split(";")) {
			const [name, ...value] = piece.split("=");
			out[name.trim()] = value.join("=");
		}
		return out;
	}
	/**
	* @param {Headers} headers
	* @param {string} name
	* @param {{ path?: string, domain?: string }|undefined} attributes
	* @returns {void}
	*/
	function deleteCookie$1(headers, name, attributes) {
		brandChecks(headers);
		const prefix = "deleteCookie";
		webidl$5.argumentLengthCheck(arguments, 2, prefix);
		name = webidl$5.converters.DOMString(name, prefix, "name");
		attributes = webidl$5.converters.DeleteCookieAttributes(attributes);
		setCookie$1(headers, {
			name,
			value: "",
			expires: /* @__PURE__ */ new Date(0),
			...attributes
		});
	}
	/**
	* @param {Headers} headers
	* @returns {Cookie[]}
	*/
	function getSetCookies$1(headers) {
		webidl$5.argumentLengthCheck(arguments, 1, "getSetCookies");
		brandChecks(headers);
		const cookies = headers.getSetCookie();
		if (!cookies) return [];
		return cookies.map((pair) => parseSetCookie(pair));
	}
	/**
	* Parses a cookie string
	* @param {string} cookie
	*/
	function parseCookie$1(cookie) {
		cookie = webidl$5.converters.DOMString(cookie);
		return parseSetCookie(cookie);
	}
	/**
	* @param {Headers} headers
	* @param {Cookie} cookie
	* @returns {void}
	*/
	function setCookie$1(headers, cookie) {
		webidl$5.argumentLengthCheck(arguments, 2, "setCookie");
		brandChecks(headers);
		cookie = webidl$5.converters.Cookie(cookie);
		const str = stringify(cookie);
		if (str) headers.append("set-cookie", str, true);
	}
	webidl$5.converters.DeleteCookieAttributes = webidl$5.dictionaryConverter([{
		converter: webidl$5.nullableConverter(webidl$5.converters.DOMString),
		key: "path",
		defaultValue: () => null
	}, {
		converter: webidl$5.nullableConverter(webidl$5.converters.DOMString),
		key: "domain",
		defaultValue: () => null
	}]);
	webidl$5.converters.Cookie = webidl$5.dictionaryConverter([
		{
			converter: webidl$5.converters.DOMString,
			key: "name"
		},
		{
			converter: webidl$5.converters.DOMString,
			key: "value"
		},
		{
			converter: webidl$5.nullableConverter((value) => {
				if (typeof value === "number") return webidl$5.converters["unsigned long long"](value);
				return new Date(value);
			}),
			key: "expires",
			defaultValue: () => null
		},
		{
			converter: webidl$5.nullableConverter(webidl$5.converters["long long"]),
			key: "maxAge",
			defaultValue: () => null
		},
		{
			converter: webidl$5.nullableConverter(webidl$5.converters.DOMString),
			key: "domain",
			defaultValue: () => null
		},
		{
			converter: webidl$5.nullableConverter(webidl$5.converters.DOMString),
			key: "path",
			defaultValue: () => null
		},
		{
			converter: webidl$5.nullableConverter(webidl$5.converters.boolean),
			key: "secure",
			defaultValue: () => null
		},
		{
			converter: webidl$5.nullableConverter(webidl$5.converters.boolean),
			key: "httpOnly",
			defaultValue: () => null
		},
		{
			converter: webidl$5.converters.USVString,
			key: "sameSite",
			allowedValues: [
				"Strict",
				"Lax",
				"None"
			]
		},
		{
			converter: webidl$5.sequenceConverter(webidl$5.converters.DOMString),
			key: "unparsed",
			defaultValue: () => new Array(0)
		}
	]);
	module.exports = {
		getCookies: getCookies$1,
		deleteCookie: deleteCookie$1,
		getSetCookies: getSetCookies$1,
		setCookie: setCookie$1,
		parseCookie: parseCookie$1
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/websocket/events.js
var require_events = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/websocket/events.js": ((exports, module) => {
	const { webidl: webidl$4 } = require_webidl();
	const { kEnumerableProperty: kEnumerableProperty$4 } = require_util$5();
	const { kConstruct: kConstruct$2 } = require_symbols();
	/**
	* @see https://html.spec.whatwg.org/multipage/comms.html#messageevent
	*/
	var MessageEvent$1 = class MessageEvent$1 extends Event {
		#eventInit;
		constructor(type, eventInitDict = {}) {
			if (type === kConstruct$2) {
				super(arguments[1], arguments[2]);
				webidl$4.util.markAsUncloneable(this);
				return;
			}
			const prefix = "MessageEvent constructor";
			webidl$4.argumentLengthCheck(arguments, 1, prefix);
			type = webidl$4.converters.DOMString(type, prefix, "type");
			eventInitDict = webidl$4.converters.MessageEventInit(eventInitDict, prefix, "eventInitDict");
			super(type, eventInitDict);
			this.#eventInit = eventInitDict;
			webidl$4.util.markAsUncloneable(this);
		}
		get data() {
			webidl$4.brandCheck(this, MessageEvent$1);
			return this.#eventInit.data;
		}
		get origin() {
			webidl$4.brandCheck(this, MessageEvent$1);
			return this.#eventInit.origin;
		}
		get lastEventId() {
			webidl$4.brandCheck(this, MessageEvent$1);
			return this.#eventInit.lastEventId;
		}
		get source() {
			webidl$4.brandCheck(this, MessageEvent$1);
			return this.#eventInit.source;
		}
		get ports() {
			webidl$4.brandCheck(this, MessageEvent$1);
			if (!Object.isFrozen(this.#eventInit.ports)) Object.freeze(this.#eventInit.ports);
			return this.#eventInit.ports;
		}
		initMessageEvent(type, bubbles = false, cancelable = false, data = null, origin = "", lastEventId = "", source = null, ports = []) {
			webidl$4.brandCheck(this, MessageEvent$1);
			webidl$4.argumentLengthCheck(arguments, 1, "MessageEvent.initMessageEvent");
			return new MessageEvent$1(type, {
				bubbles,
				cancelable,
				data,
				origin,
				lastEventId,
				source,
				ports
			});
		}
		static createFastMessageEvent(type, init) {
			const messageEvent = new MessageEvent$1(kConstruct$2, type, init);
			messageEvent.#eventInit = init;
			messageEvent.#eventInit.data ??= null;
			messageEvent.#eventInit.origin ??= "";
			messageEvent.#eventInit.lastEventId ??= "";
			messageEvent.#eventInit.source ??= null;
			messageEvent.#eventInit.ports ??= [];
			return messageEvent;
		}
	};
	const { createFastMessageEvent: createFastMessageEvent$2 } = MessageEvent$1;
	delete MessageEvent$1.createFastMessageEvent;
	/**
	* @see https://websockets.spec.whatwg.org/#the-closeevent-interface
	*/
	var CloseEvent$2 = class CloseEvent$2 extends Event {
		#eventInit;
		constructor(type, eventInitDict = {}) {
			const prefix = "CloseEvent constructor";
			webidl$4.argumentLengthCheck(arguments, 1, prefix);
			type = webidl$4.converters.DOMString(type, prefix, "type");
			eventInitDict = webidl$4.converters.CloseEventInit(eventInitDict);
			super(type, eventInitDict);
			this.#eventInit = eventInitDict;
			webidl$4.util.markAsUncloneable(this);
		}
		get wasClean() {
			webidl$4.brandCheck(this, CloseEvent$2);
			return this.#eventInit.wasClean;
		}
		get code() {
			webidl$4.brandCheck(this, CloseEvent$2);
			return this.#eventInit.code;
		}
		get reason() {
			webidl$4.brandCheck(this, CloseEvent$2);
			return this.#eventInit.reason;
		}
	};
	var ErrorEvent$2 = class ErrorEvent$2 extends Event {
		#eventInit;
		constructor(type, eventInitDict) {
			const prefix = "ErrorEvent constructor";
			webidl$4.argumentLengthCheck(arguments, 1, prefix);
			super(type, eventInitDict);
			webidl$4.util.markAsUncloneable(this);
			type = webidl$4.converters.DOMString(type, prefix, "type");
			eventInitDict = webidl$4.converters.ErrorEventInit(eventInitDict ?? {});
			this.#eventInit = eventInitDict;
		}
		get message() {
			webidl$4.brandCheck(this, ErrorEvent$2);
			return this.#eventInit.message;
		}
		get filename() {
			webidl$4.brandCheck(this, ErrorEvent$2);
			return this.#eventInit.filename;
		}
		get lineno() {
			webidl$4.brandCheck(this, ErrorEvent$2);
			return this.#eventInit.lineno;
		}
		get colno() {
			webidl$4.brandCheck(this, ErrorEvent$2);
			return this.#eventInit.colno;
		}
		get error() {
			webidl$4.brandCheck(this, ErrorEvent$2);
			return this.#eventInit.error;
		}
	};
	Object.defineProperties(MessageEvent$1.prototype, {
		[Symbol.toStringTag]: {
			value: "MessageEvent",
			configurable: true
		},
		data: kEnumerableProperty$4,
		origin: kEnumerableProperty$4,
		lastEventId: kEnumerableProperty$4,
		source: kEnumerableProperty$4,
		ports: kEnumerableProperty$4,
		initMessageEvent: kEnumerableProperty$4
	});
	Object.defineProperties(CloseEvent$2.prototype, {
		[Symbol.toStringTag]: {
			value: "CloseEvent",
			configurable: true
		},
		reason: kEnumerableProperty$4,
		code: kEnumerableProperty$4,
		wasClean: kEnumerableProperty$4
	});
	Object.defineProperties(ErrorEvent$2.prototype, {
		[Symbol.toStringTag]: {
			value: "ErrorEvent",
			configurable: true
		},
		message: kEnumerableProperty$4,
		filename: kEnumerableProperty$4,
		lineno: kEnumerableProperty$4,
		colno: kEnumerableProperty$4,
		error: kEnumerableProperty$4
	});
	webidl$4.converters.MessagePort = webidl$4.interfaceConverter(webidl$4.is.MessagePort, "MessagePort");
	webidl$4.converters["sequence<MessagePort>"] = webidl$4.sequenceConverter(webidl$4.converters.MessagePort);
	const eventInit = [
		{
			key: "bubbles",
			converter: webidl$4.converters.boolean,
			defaultValue: () => false
		},
		{
			key: "cancelable",
			converter: webidl$4.converters.boolean,
			defaultValue: () => false
		},
		{
			key: "composed",
			converter: webidl$4.converters.boolean,
			defaultValue: () => false
		}
	];
	webidl$4.converters.MessageEventInit = webidl$4.dictionaryConverter([
		...eventInit,
		{
			key: "data",
			converter: webidl$4.converters.any,
			defaultValue: () => null
		},
		{
			key: "origin",
			converter: webidl$4.converters.USVString,
			defaultValue: () => ""
		},
		{
			key: "lastEventId",
			converter: webidl$4.converters.DOMString,
			defaultValue: () => ""
		},
		{
			key: "source",
			converter: webidl$4.nullableConverter(webidl$4.converters.MessagePort),
			defaultValue: () => null
		},
		{
			key: "ports",
			converter: webidl$4.converters["sequence<MessagePort>"],
			defaultValue: () => new Array(0)
		}
	]);
	webidl$4.converters.CloseEventInit = webidl$4.dictionaryConverter([
		...eventInit,
		{
			key: "wasClean",
			converter: webidl$4.converters.boolean,
			defaultValue: () => false
		},
		{
			key: "code",
			converter: webidl$4.converters["unsigned short"],
			defaultValue: () => 0
		},
		{
			key: "reason",
			converter: webidl$4.converters.USVString,
			defaultValue: () => ""
		}
	]);
	webidl$4.converters.ErrorEventInit = webidl$4.dictionaryConverter([
		...eventInit,
		{
			key: "message",
			converter: webidl$4.converters.DOMString,
			defaultValue: () => ""
		},
		{
			key: "filename",
			converter: webidl$4.converters.USVString,
			defaultValue: () => ""
		},
		{
			key: "lineno",
			converter: webidl$4.converters["unsigned long"],
			defaultValue: () => 0
		},
		{
			key: "colno",
			converter: webidl$4.converters["unsigned long"],
			defaultValue: () => 0
		},
		{
			key: "error",
			converter: webidl$4.converters.any
		}
	]);
	module.exports = {
		MessageEvent: MessageEvent$1,
		CloseEvent: CloseEvent$2,
		ErrorEvent: ErrorEvent$2,
		createFastMessageEvent: createFastMessageEvent$2
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/websocket/constants.js
var require_constants = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/websocket/constants.js": ((exports, module) => {
	/**
	* This is a Globally Unique Identifier unique used to validate that the
	* endpoint accepts websocket connections.
	* @see https://www.rfc-editor.org/rfc/rfc6455.html#section-1.3
	* @type {'258EAFA5-E914-47DA-95CA-C5AB0DC85B11'}
	*/
	const uid$1 = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
	/**
	* @type {PropertyDescriptor}
	*/
	const staticPropertyDescriptors$1 = {
		enumerable: true,
		writable: false,
		configurable: false
	};
	/**
	* The states of the WebSocket connection.
	*
	* @readonly
	* @enum
	* @property {0} CONNECTING
	* @property {1} OPEN
	* @property {2} CLOSING
	* @property {3} CLOSED
	*/
	const states$5 = {
		CONNECTING: 0,
		OPEN: 1,
		CLOSING: 2,
		CLOSED: 3
	};
	/**
	* @readonly
	* @enum
	* @property {0} NOT_SENT
	* @property {1} PROCESSING
	* @property {2} SENT
	*/
	const sentCloseFrameState$4 = {
		SENT: 1,
		RECEIVED: 2
	};
	/**
	* The WebSocket opcodes.
	*
	* @readonly
	* @enum
	* @property {0x0} CONTINUATION
	* @property {0x1} TEXT
	* @property {0x2} BINARY
	* @property {0x8} CLOSE
	* @property {0x9} PING
	* @property {0xA} PONG
	* @see https://datatracker.ietf.org/doc/html/rfc6455#section-5.2
	*/
	const opcodes$7 = {
		CONTINUATION: 0,
		TEXT: 1,
		BINARY: 2,
		CLOSE: 8,
		PING: 9,
		PONG: 10
	};
	/**
	* The maximum value for an unsigned 16-bit integer.
	*
	* @type {65535} 2 ** 16 - 1
	*/
	const maxUnsigned16Bit$1 = 65535;
	/**
	* The states of the parser.
	*
	* @readonly
	* @enum
	* @property {0} INFO
	* @property {2} PAYLOADLENGTH_16
	* @property {3} PAYLOADLENGTH_64
	* @property {4} READ_DATA
	*/
	const parserStates$1 = {
		INFO: 0,
		PAYLOADLENGTH_16: 2,
		PAYLOADLENGTH_64: 3,
		READ_DATA: 4
	};
	/**
	* An empty buffer.
	*
	* @type {Buffer}
	*/
	const emptyBuffer$2 = Buffer.allocUnsafe(0);
	/**
	* @readonly
	* @property {1} text
	* @property {2} typedArray
	* @property {3} arrayBuffer
	* @property {4} blob
	*/
	const sendHints$2 = {
		text: 1,
		typedArray: 2,
		arrayBuffer: 3,
		blob: 4
	};
	module.exports = {
		uid: uid$1,
		sentCloseFrameState: sentCloseFrameState$4,
		staticPropertyDescriptors: staticPropertyDescriptors$1,
		states: states$5,
		opcodes: opcodes$7,
		maxUnsigned16Bit: maxUnsigned16Bit$1,
		parserStates: parserStates$1,
		emptyBuffer: emptyBuffer$2,
		sendHints: sendHints$2
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/websocket/util.js
var require_util$1 = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/websocket/util.js": ((exports, module) => {
	const { states: states$4, opcodes: opcodes$6 } = require_constants();
	const { isUtf8 } = __require("node:buffer");
	const { collectASequenceOfCodePointsFast, removeHTTPWhitespace } = require_data_url();
	/**
	* @param {number} readyState
	* @returns {boolean}
	*/
	function isConnecting$1(readyState) {
		return readyState === states$4.CONNECTING;
	}
	/**
	* @param {number} readyState
	* @returns {boolean}
	*/
	function isEstablished$3(readyState) {
		return readyState === states$4.OPEN;
	}
	/**
	* @param {number} readyState
	* @returns {boolean}
	*/
	function isClosing$2(readyState) {
		return readyState === states$4.CLOSING;
	}
	/**
	* @param {number} readyState
	* @returns {boolean}
	*/
	function isClosed$2(readyState) {
		return readyState === states$4.CLOSED;
	}
	/**
	* @see https://dom.spec.whatwg.org/#concept-event-fire
	* @param {string} e
	* @param {EventTarget} target
	* @param {(...args: ConstructorParameters<typeof Event>) => Event} eventFactory
	* @param {EventInit | undefined} eventInitDict
	* @returns {void}
	*/
	function fireEvent$1(e, target, eventFactory = (type, init) => new Event(type, init), eventInitDict = {}) {
		const event = eventFactory(e, eventInitDict);
		target.dispatchEvent(event);
	}
	/**
	* @see https://websockets.spec.whatwg.org/#feedback-from-the-protocol
	* @param {import('./websocket').Handler} handler
	* @param {number} type Opcode
	* @param {Buffer} data application data
	* @returns {void}
	*/
	function websocketMessageReceived$1(handler, type, data) {
		handler.onMessage(type, data);
	}
	/**
	* @param {Buffer} buffer
	* @returns {ArrayBuffer}
	*/
	function toArrayBuffer$1(buffer$1) {
		if (buffer$1.byteLength === buffer$1.buffer.byteLength) return buffer$1.buffer;
		return new Uint8Array(buffer$1).buffer;
	}
	/**
	* @see https://datatracker.ietf.org/doc/html/rfc6455
	* @see https://datatracker.ietf.org/doc/html/rfc2616
	* @see https://bugs.chromium.org/p/chromium/issues/detail?id=398407
	* @param {string} protocol
	* @returns {boolean}
	*/
	function isValidSubprotocol$2(protocol) {
		if (protocol.length === 0) return false;
		for (let i = 0; i < protocol.length; ++i) {
			const code = protocol.charCodeAt(i);
			if (code < 33 || code > 126 || code === 34 || code === 40 || code === 41 || code === 44 || code === 47 || code === 58 || code === 59 || code === 60 || code === 61 || code === 62 || code === 63 || code === 64 || code === 91 || code === 92 || code === 93 || code === 123 || code === 125) return false;
		}
		return true;
	}
	/**
	* @see https://datatracker.ietf.org/doc/html/rfc6455#section-7-4
	* @param {number} code
	* @returns {boolean}
	*/
	function isValidStatusCode$1(code) {
		if (code >= 1e3 && code < 1015) return code !== 1004 && code !== 1005 && code !== 1006;
		return code >= 3e3 && code <= 4999;
	}
	/**
	* @see https://datatracker.ietf.org/doc/html/rfc6455#section-5.5
	* @param {number} opcode
	* @returns {boolean}
	*/
	function isControlFrame$1(opcode) {
		return opcode === opcodes$6.CLOSE || opcode === opcodes$6.PING || opcode === opcodes$6.PONG;
	}
	/**
	* @param {number} opcode
	* @returns {boolean}
	*/
	function isContinuationFrame$1(opcode) {
		return opcode === opcodes$6.CONTINUATION;
	}
	/**
	* @param {number} opcode
	* @returns {boolean}
	*/
	function isTextBinaryFrame$1(opcode) {
		return opcode === opcodes$6.TEXT || opcode === opcodes$6.BINARY;
	}
	/**
	*
	* @param {number} opcode
	* @returns {boolean}
	*/
	function isValidOpcode$1(opcode) {
		return isTextBinaryFrame$1(opcode) || isContinuationFrame$1(opcode) || isControlFrame$1(opcode);
	}
	/**
	* Parses a Sec-WebSocket-Extensions header value.
	* @param {string} extensions
	* @returns {Map<string, string>}
	*/
	function parseExtensions$1(extensions) {
		const position = { position: 0 };
		const extensionList = /* @__PURE__ */ new Map();
		while (position.position < extensions.length) {
			const [name, value = ""] = collectASequenceOfCodePointsFast(";", extensions, position).split("=", 2);
			extensionList.set(removeHTTPWhitespace(name, true, false), removeHTTPWhitespace(value, false, true));
			position.position++;
		}
		return extensionList;
	}
	/**
	* @see https://www.rfc-editor.org/rfc/rfc7692#section-7.1.2.2
	* @description "client-max-window-bits = 1*DIGIT"
	* @param {string} value
	* @returns {boolean}
	*/
	function isValidClientWindowBits$1(value) {
		for (let i = 0; i < value.length; i++) {
			const byte = value.charCodeAt(i);
			if (byte < 48 || byte > 57) return false;
		}
		return true;
	}
	/**
	* @see https://whatpr.org/websockets/48/7b748d3...d5570f3.html#get-a-url-record
	* @param {string} url
	* @param {string} [baseURL]
	*/
	function getURLRecord$2(url, baseURL) {
		let urlRecord;
		try {
			urlRecord = new URL(url, baseURL);
		} catch (e) {
			throw new DOMException(e, "SyntaxError");
		}
		if (urlRecord.protocol === "http:") urlRecord.protocol = "ws:";
		else if (urlRecord.protocol === "https:") urlRecord.protocol = "wss:";
		if (urlRecord.protocol !== "ws:" && urlRecord.protocol !== "wss:") throw new DOMException("expected a ws: or wss: url", "SyntaxError");
		if (urlRecord.hash.length || urlRecord.href.endsWith("#")) throw new DOMException("hash", "SyntaxError");
		return urlRecord;
	}
	function validateCloseCodeAndReason$2(code, reason) {
		if (code !== null) {
			if (code !== 1e3 && (code < 3e3 || code > 4999)) throw new DOMException("invalid code", "InvalidAccessError");
		}
		if (reason !== null) {
			const reasonBytesLength = Buffer.byteLength(reason);
			if (reasonBytesLength > 123) throw new DOMException(`Reason must be less than 123 bytes; received ${reasonBytesLength}`, "SyntaxError");
		}
	}
	/**
	* Converts a Buffer to utf-8, even on platforms without icu.
	* @type {(buffer: Buffer) => string}
	*/
	const utf8Decode$3 = (() => {
		if (typeof process.versions.icu === "string") {
			const fatalDecoder = new TextDecoder("utf-8", { fatal: true });
			return fatalDecoder.decode.bind(fatalDecoder);
		}
		return function(buffer$1) {
			if (isUtf8(buffer$1)) return buffer$1.toString("utf-8");
			throw new TypeError("Invalid utf-8 received.");
		};
	})();
	module.exports = {
		isConnecting: isConnecting$1,
		isEstablished: isEstablished$3,
		isClosing: isClosing$2,
		isClosed: isClosed$2,
		fireEvent: fireEvent$1,
		isValidSubprotocol: isValidSubprotocol$2,
		isValidStatusCode: isValidStatusCode$1,
		websocketMessageReceived: websocketMessageReceived$1,
		utf8Decode: utf8Decode$3,
		isControlFrame: isControlFrame$1,
		isContinuationFrame: isContinuationFrame$1,
		isTextBinaryFrame: isTextBinaryFrame$1,
		isValidOpcode: isValidOpcode$1,
		parseExtensions: parseExtensions$1,
		isValidClientWindowBits: isValidClientWindowBits$1,
		toArrayBuffer: toArrayBuffer$1,
		getURLRecord: getURLRecord$2,
		validateCloseCodeAndReason: validateCloseCodeAndReason$2
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/websocket/frame.js
var require_frame = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/websocket/frame.js": ((exports, module) => {
	const { maxUnsigned16Bit, opcodes: opcodes$5 } = require_constants();
	const BUFFER_SIZE = 8 * 1024;
	/** @type {import('crypto')} */
	let crypto$1;
	let buffer = null;
	let bufIdx = BUFFER_SIZE;
	try {
		crypto$1 = __require("node:crypto");
	} catch {
		crypto$1 = { randomFillSync: function randomFillSync(buffer$1, _offset, _size) {
			for (let i = 0; i < buffer$1.length; ++i) buffer$1[i] = Math.random() * 255 | 0;
			return buffer$1;
		} };
	}
	function generateMask() {
		if (bufIdx === BUFFER_SIZE) {
			bufIdx = 0;
			crypto$1.randomFillSync(buffer ??= Buffer.allocUnsafeSlow(BUFFER_SIZE), 0, BUFFER_SIZE);
		}
		return [
			buffer[bufIdx++],
			buffer[bufIdx++],
			buffer[bufIdx++],
			buffer[bufIdx++]
		];
	}
	var WebsocketFrameSend$5 = class {
		/**
		* @param {Buffer|undefined} data
		*/
		constructor(data) {
			this.frameData = data;
		}
		createFrame(opcode) {
			const frameData = this.frameData;
			const maskKey = generateMask();
			const bodyLength$1 = frameData?.byteLength ?? 0;
			/** @type {number} */
			let payloadLength = bodyLength$1;
			let offset = 6;
			if (bodyLength$1 > maxUnsigned16Bit) {
				offset += 8;
				payloadLength = 127;
			} else if (bodyLength$1 > 125) {
				offset += 2;
				payloadLength = 126;
			}
			const buffer$1 = Buffer.allocUnsafe(bodyLength$1 + offset);
			buffer$1[0] = buffer$1[1] = 0;
			buffer$1[0] |= 128;
			buffer$1[0] = (buffer$1[0] & 240) + opcode;
			/*! ws. MIT License. Einar Otto Stangvik <einaros@gmail.com> */
			buffer$1[offset - 4] = maskKey[0];
			buffer$1[offset - 3] = maskKey[1];
			buffer$1[offset - 2] = maskKey[2];
			buffer$1[offset - 1] = maskKey[3];
			buffer$1[1] = payloadLength;
			if (payloadLength === 126) buffer$1.writeUInt16BE(bodyLength$1, 2);
			else if (payloadLength === 127) {
				buffer$1[2] = buffer$1[3] = 0;
				buffer$1.writeUIntBE(bodyLength$1, 4, 6);
			}
			buffer$1[1] |= 128;
			for (let i = 0; i < bodyLength$1; ++i) buffer$1[offset + i] = frameData[i] ^ maskKey[i & 3];
			return buffer$1;
		}
		/**
		* @param {Uint8Array} buffer
		*/
		static createFastTextFrame(buffer$1) {
			const maskKey = generateMask();
			const bodyLength$1 = buffer$1.length;
			for (let i = 0; i < bodyLength$1; ++i) buffer$1[i] ^= maskKey[i & 3];
			let payloadLength = bodyLength$1;
			let offset = 6;
			if (bodyLength$1 > maxUnsigned16Bit) {
				offset += 8;
				payloadLength = 127;
			} else if (bodyLength$1 > 125) {
				offset += 2;
				payloadLength = 126;
			}
			const head = Buffer.allocUnsafeSlow(offset);
			head[0] = 128 | opcodes$5.TEXT;
			head[1] = payloadLength | 128;
			head[offset - 4] = maskKey[0];
			head[offset - 3] = maskKey[1];
			head[offset - 2] = maskKey[2];
			head[offset - 1] = maskKey[3];
			if (payloadLength === 126) head.writeUInt16BE(bodyLength$1, 2);
			else if (payloadLength === 127) {
				head[2] = head[3] = 0;
				head.writeUIntBE(bodyLength$1, 4, 6);
			}
			return [head, buffer$1];
		}
	};
	module.exports = {
		WebsocketFrameSend: WebsocketFrameSend$5,
		generateMask
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/websocket/connection.js
var require_connection = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/websocket/connection.js": ((exports, module) => {
	const { uid, states: states$3, sentCloseFrameState: sentCloseFrameState$3, emptyBuffer: emptyBuffer$1, opcodes: opcodes$4 } = require_constants();
	const { parseExtensions, isClosed: isClosed$1, isClosing: isClosing$1, isEstablished: isEstablished$2, validateCloseCodeAndReason: validateCloseCodeAndReason$1 } = require_util$1();
	const { makeRequest: makeRequest$1 } = require_request();
	const { fetching: fetching$1 } = require_fetch();
	const { Headers, getHeadersList } = require_headers();
	const { getDecodeSplit } = require_util$4();
	const { WebsocketFrameSend: WebsocketFrameSend$4 } = require_frame();
	const assert$1 = __require("node:assert");
	/** @type {import('crypto')} */
	let crypto;
	try {
		crypto = __require("node:crypto");
	} catch {}
	/**
	* @see https://websockets.spec.whatwg.org/#concept-websocket-establish
	* @param {URL} url
	* @param {string|string[]} protocols
	* @param {import('./websocket').Handler} handler
	* @param {Partial<import('../../../types/websocket').WebSocketInit>} options
	*/
	function establishWebSocketConnection$2(url, protocols, client, handler, options) {
		const requestURL = url;
		requestURL.protocol = url.protocol === "ws:" ? "http:" : "https:";
		const request$2 = makeRequest$1({
			urlList: [requestURL],
			client,
			serviceWorkers: "none",
			referrer: "no-referrer",
			mode: "websocket",
			credentials: "include",
			cache: "no-store",
			redirect: "error"
		});
		if (options.headers) request$2.headersList = getHeadersList(new Headers(options.headers));
		const keyValue = crypto.randomBytes(16).toString("base64");
		request$2.headersList.append("sec-websocket-key", keyValue, true);
		request$2.headersList.append("sec-websocket-version", "13", true);
		for (const protocol of protocols) request$2.headersList.append("sec-websocket-protocol", protocol, true);
		request$2.headersList.append("sec-websocket-extensions", "permessage-deflate; client_max_window_bits", true);
		return fetching$1({
			request: request$2,
			useParallelQueue: true,
			dispatcher: options.dispatcher,
			processResponse(response) {
				if (response.type === "error") handler.readyState = states$3.CLOSED;
				if (response.type === "error" || response.status !== 101) {
					failWebsocketConnection$3(handler, 1002, "Received network error or non-101 status code.", response.error);
					return;
				}
				if (protocols.length !== 0 && !response.headersList.get("Sec-WebSocket-Protocol")) {
					failWebsocketConnection$3(handler, 1002, "Server did not respond with sent protocols.");
					return;
				}
				if (response.headersList.get("Upgrade")?.toLowerCase() !== "websocket") {
					failWebsocketConnection$3(handler, 1002, "Server did not set Upgrade header to \"websocket\".");
					return;
				}
				if (response.headersList.get("Connection")?.toLowerCase() !== "upgrade") {
					failWebsocketConnection$3(handler, 1002, "Server did not set Connection header to \"upgrade\".");
					return;
				}
				const secWSAccept = response.headersList.get("Sec-WebSocket-Accept");
				const digest = crypto.createHash("sha1").update(keyValue + uid).digest("base64");
				if (secWSAccept !== digest) {
					failWebsocketConnection$3(handler, 1002, "Incorrect hash received in Sec-WebSocket-Accept header.");
					return;
				}
				const secExtension = response.headersList.get("Sec-WebSocket-Extensions");
				let extensions;
				if (secExtension !== null) {
					extensions = parseExtensions(secExtension);
					if (!extensions.has("permessage-deflate")) {
						failWebsocketConnection$3(handler, 1002, "Sec-WebSocket-Extensions header does not match.");
						return;
					}
				}
				const secProtocol = response.headersList.get("Sec-WebSocket-Protocol");
				if (secProtocol !== null) {
					if (!getDecodeSplit("sec-websocket-protocol", request$2.headersList).includes(secProtocol)) {
						failWebsocketConnection$3(handler, 1002, "Protocol was not set in the opening handshake.");
						return;
					}
				}
				response.socket.on("data", handler.onSocketData);
				response.socket.on("close", handler.onSocketClose);
				response.socket.on("error", handler.onSocketError);
				handler.wasEverConnected = true;
				handler.onConnectionEstablished(response, extensions);
			}
		});
	}
	/**
	* @see https://whatpr.org/websockets/48.html#close-the-websocket
	* @param {import('./websocket').Handler} object
	* @param {number} [code=null]
	* @param {string} [reason='']
	*/
	function closeWebSocketConnection$2(object, code, reason, validate = false) {
		code ??= null;
		reason ??= "";
		if (validate) validateCloseCodeAndReason$1(code, reason);
		if (isClosed$1(object.readyState) || isClosing$1(object.readyState)) {} else if (!isEstablished$2(object.readyState)) {
			failWebsocketConnection$3(object);
			object.readyState = states$3.CLOSING;
		} else if (!object.closeState.has(sentCloseFrameState$3.SENT) && !object.closeState.has(sentCloseFrameState$3.RECEIVED)) {
			const frame = new WebsocketFrameSend$4();
			if (reason.length !== 0 && code === null) code = 1e3;
			assert$1(code === null || Number.isInteger(code));
			if (code === null && reason.length === 0) frame.frameData = emptyBuffer$1;
			else if (code !== null && reason === null) {
				frame.frameData = Buffer.allocUnsafe(2);
				frame.frameData.writeUInt16BE(code, 0);
			} else if (code !== null && reason !== null) {
				frame.frameData = Buffer.allocUnsafe(2 + Buffer.byteLength(reason));
				frame.frameData.writeUInt16BE(code, 0);
				frame.frameData.write(reason, 2, "utf-8");
			} else frame.frameData = emptyBuffer$1;
			object.socket.write(frame.createFrame(opcodes$4.CLOSE));
			object.closeState.add(sentCloseFrameState$3.SENT);
			object.readyState = states$3.CLOSING;
		} else object.readyState = states$3.CLOSING;
	}
	/**
	* @param {import('./websocket').Handler} handler
	* @param {number} code
	* @param {string|undefined} reason
	* @param {unknown} cause
	* @returns {void}
	*/
	function failWebsocketConnection$3(handler, code, reason, cause) {
		if (isEstablished$2(handler.readyState)) closeWebSocketConnection$2(handler, code, reason, false);
		handler.controller.abort();
		if (handler.socket?.destroyed === false) handler.socket.destroy();
		handler.onFail(code, reason, cause);
	}
	module.exports = {
		establishWebSocketConnection: establishWebSocketConnection$2,
		failWebsocketConnection: failWebsocketConnection$3,
		closeWebSocketConnection: closeWebSocketConnection$2
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/websocket/permessage-deflate.js
var require_permessage_deflate = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/websocket/permessage-deflate.js": ((exports, module) => {
	const { createInflateRaw, Z_DEFAULT_WINDOWBITS } = __require("node:zlib");
	const { isValidClientWindowBits } = require_util$1();
	const tail = Buffer.from([
		0,
		0,
		255,
		255
	]);
	const kBuffer = Symbol("kBuffer");
	const kLength = Symbol("kLength");
	var PerMessageDeflate$1 = class {
		/** @type {import('node:zlib').InflateRaw} */
		#inflate;
		#options = {};
		constructor(extensions) {
			this.#options.serverNoContextTakeover = extensions.has("server_no_context_takeover");
			this.#options.serverMaxWindowBits = extensions.get("server_max_window_bits");
		}
		decompress(chunk, fin, callback) {
			if (!this.#inflate) {
				let windowBits = Z_DEFAULT_WINDOWBITS;
				if (this.#options.serverMaxWindowBits) {
					if (!isValidClientWindowBits(this.#options.serverMaxWindowBits)) {
						callback(/* @__PURE__ */ new Error("Invalid server_max_window_bits"));
						return;
					}
					windowBits = Number.parseInt(this.#options.serverMaxWindowBits);
				}
				this.#inflate = createInflateRaw({ windowBits });
				this.#inflate[kBuffer] = [];
				this.#inflate[kLength] = 0;
				this.#inflate.on("data", (data) => {
					this.#inflate[kBuffer].push(data);
					this.#inflate[kLength] += data.length;
				});
				this.#inflate.on("error", (err) => {
					this.#inflate = null;
					callback(err);
				});
			}
			this.#inflate.write(chunk);
			if (fin) this.#inflate.write(tail);
			this.#inflate.flush(() => {
				const full = Buffer.concat(this.#inflate[kBuffer], this.#inflate[kLength]);
				this.#inflate[kBuffer].length = 0;
				this.#inflate[kLength] = 0;
				callback(null, full);
			});
		}
	};
	module.exports = { PerMessageDeflate: PerMessageDeflate$1 };
}) });

//#endregion
//#region node_modules/undici/lib/web/websocket/receiver.js
var require_receiver = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/websocket/receiver.js": ((exports, module) => {
	const { Writable } = __require("node:stream");
	const assert = __require("node:assert");
	const { parserStates, opcodes: opcodes$3, states: states$2, emptyBuffer, sentCloseFrameState: sentCloseFrameState$2 } = require_constants();
	const { isValidStatusCode, isValidOpcode, websocketMessageReceived, utf8Decode: utf8Decode$2, isControlFrame, isTextBinaryFrame, isContinuationFrame } = require_util$1();
	const { failWebsocketConnection: failWebsocketConnection$2 } = require_connection();
	const { WebsocketFrameSend: WebsocketFrameSend$3 } = require_frame();
	const { PerMessageDeflate } = require_permessage_deflate();
	var ByteParser$2 = class extends Writable {
		#buffers = [];
		#fragmentsBytes = 0;
		#byteOffset = 0;
		#loop = false;
		#state = parserStates.INFO;
		#info = {};
		#fragments = [];
		/** @type {Map<string, PerMessageDeflate>} */
		#extensions;
		/** @type {import('./websocket').Handler} */
		#handler;
		constructor(handler, extensions) {
			super();
			this.#handler = handler;
			this.#extensions = extensions == null ? /* @__PURE__ */ new Map() : extensions;
			if (this.#extensions.has("permessage-deflate")) this.#extensions.set("permessage-deflate", new PerMessageDeflate(extensions));
		}
		/**
		* @param {Buffer} chunk
		* @param {() => void} callback
		*/
		_write(chunk, _, callback) {
			this.#buffers.push(chunk);
			this.#byteOffset += chunk.length;
			this.#loop = true;
			this.run(callback);
		}
		/**
		* Runs whenever a new chunk is received.
		* Callback is called whenever there are no more chunks buffering,
		* or not enough bytes are buffered to parse.
		*/
		run(callback) {
			while (this.#loop) if (this.#state === parserStates.INFO) {
				if (this.#byteOffset < 2) return callback();
				const buffer$1 = this.consume(2);
				const fin = (buffer$1[0] & 128) !== 0;
				const opcode = buffer$1[0] & 15;
				const masked = (buffer$1[1] & 128) === 128;
				const fragmented = !fin && opcode !== opcodes$3.CONTINUATION;
				const payloadLength = buffer$1[1] & 127;
				const rsv1 = buffer$1[0] & 64;
				const rsv2 = buffer$1[0] & 32;
				const rsv3 = buffer$1[0] & 16;
				if (!isValidOpcode(opcode)) {
					failWebsocketConnection$2(this.#handler, 1002, "Invalid opcode received");
					return callback();
				}
				if (masked) {
					failWebsocketConnection$2(this.#handler, 1002, "Frame cannot be masked");
					return callback();
				}
				if (rsv1 !== 0 && !this.#extensions.has("permessage-deflate")) {
					failWebsocketConnection$2(this.#handler, 1002, "Expected RSV1 to be clear.");
					return;
				}
				if (rsv2 !== 0 || rsv3 !== 0) {
					failWebsocketConnection$2(this.#handler, 1002, "RSV1, RSV2, RSV3 must be clear");
					return;
				}
				if (fragmented && !isTextBinaryFrame(opcode)) {
					failWebsocketConnection$2(this.#handler, 1002, "Invalid frame type was fragmented.");
					return;
				}
				if (isTextBinaryFrame(opcode) && this.#fragments.length > 0) {
					failWebsocketConnection$2(this.#handler, 1002, "Expected continuation frame");
					return;
				}
				if (this.#info.fragmented && fragmented) {
					failWebsocketConnection$2(this.#handler, 1002, "Fragmented frame exceeded 125 bytes.");
					return;
				}
				if ((payloadLength > 125 || fragmented) && isControlFrame(opcode)) {
					failWebsocketConnection$2(this.#handler, 1002, "Control frame either too large or fragmented");
					return;
				}
				if (isContinuationFrame(opcode) && this.#fragments.length === 0 && !this.#info.compressed) {
					failWebsocketConnection$2(this.#handler, 1002, "Unexpected continuation frame");
					return;
				}
				if (payloadLength <= 125) {
					this.#info.payloadLength = payloadLength;
					this.#state = parserStates.READ_DATA;
				} else if (payloadLength === 126) this.#state = parserStates.PAYLOADLENGTH_16;
				else if (payloadLength === 127) this.#state = parserStates.PAYLOADLENGTH_64;
				if (isTextBinaryFrame(opcode)) {
					this.#info.binaryType = opcode;
					this.#info.compressed = rsv1 !== 0;
				}
				this.#info.opcode = opcode;
				this.#info.masked = masked;
				this.#info.fin = fin;
				this.#info.fragmented = fragmented;
			} else if (this.#state === parserStates.PAYLOADLENGTH_16) {
				if (this.#byteOffset < 2) return callback();
				const buffer$1 = this.consume(2);
				this.#info.payloadLength = buffer$1.readUInt16BE(0);
				this.#state = parserStates.READ_DATA;
			} else if (this.#state === parserStates.PAYLOADLENGTH_64) {
				if (this.#byteOffset < 8) return callback();
				const buffer$1 = this.consume(8);
				const upper = buffer$1.readUInt32BE(0);
				if (upper > 2 ** 31 - 1) {
					failWebsocketConnection$2(this.#handler, 1009, "Received payload length > 2^31 bytes.");
					return;
				}
				const lower = buffer$1.readUInt32BE(4);
				this.#info.payloadLength = (upper << 8) + lower;
				this.#state = parserStates.READ_DATA;
			} else if (this.#state === parserStates.READ_DATA) {
				if (this.#byteOffset < this.#info.payloadLength) return callback();
				const body = this.consume(this.#info.payloadLength);
				if (isControlFrame(this.#info.opcode)) {
					this.#loop = this.parseControlFrame(body);
					this.#state = parserStates.INFO;
				} else if (!this.#info.compressed) {
					this.writeFragments(body);
					if (!this.#info.fragmented && this.#info.fin) websocketMessageReceived(this.#handler, this.#info.binaryType, this.consumeFragments());
					this.#state = parserStates.INFO;
				} else {
					this.#extensions.get("permessage-deflate").decompress(body, this.#info.fin, (error, data) => {
						if (error) {
							failWebsocketConnection$2(this.#handler, 1007, error.message);
							return;
						}
						this.writeFragments(data);
						if (!this.#info.fin) {
							this.#state = parserStates.INFO;
							this.#loop = true;
							this.run(callback);
							return;
						}
						websocketMessageReceived(this.#handler, this.#info.binaryType, this.consumeFragments());
						this.#loop = true;
						this.#state = parserStates.INFO;
						this.run(callback);
					});
					this.#loop = false;
					break;
				}
			}
		}
		/**
		* Take n bytes from the buffered Buffers
		* @param {number} n
		* @returns {Buffer}
		*/
		consume(n) {
			if (n > this.#byteOffset) throw new Error("Called consume() before buffers satiated.");
			else if (n === 0) return emptyBuffer;
			this.#byteOffset -= n;
			const first = this.#buffers[0];
			if (first.length > n) {
				this.#buffers[0] = first.subarray(n, first.length);
				return first.subarray(0, n);
			} else if (first.length === n) return this.#buffers.shift();
			else {
				let offset = 0;
				const buffer$1 = Buffer.allocUnsafeSlow(n);
				while (offset !== n) {
					const next = this.#buffers[0];
					const length = next.length;
					if (length + offset === n) {
						buffer$1.set(this.#buffers.shift(), offset);
						break;
					} else if (length + offset > n) {
						buffer$1.set(next.subarray(0, n - offset), offset);
						this.#buffers[0] = next.subarray(n - offset);
						break;
					} else {
						buffer$1.set(this.#buffers.shift(), offset);
						offset += length;
					}
				}
				return buffer$1;
			}
		}
		writeFragments(fragment) {
			this.#fragmentsBytes += fragment.length;
			this.#fragments.push(fragment);
		}
		consumeFragments() {
			const fragments = this.#fragments;
			if (fragments.length === 1) {
				this.#fragmentsBytes = 0;
				return fragments.shift();
			}
			let offset = 0;
			const output = Buffer.allocUnsafeSlow(this.#fragmentsBytes);
			for (let i = 0; i < fragments.length; ++i) {
				const buffer$1 = fragments[i];
				output.set(buffer$1, offset);
				offset += buffer$1.length;
			}
			this.#fragments = [];
			this.#fragmentsBytes = 0;
			return output;
		}
		parseCloseBody(data) {
			assert(data.length !== 1);
			/** @type {number|undefined} */
			let code;
			if (data.length >= 2) code = data.readUInt16BE(0);
			if (code !== void 0 && !isValidStatusCode(code)) return {
				code: 1002,
				reason: "Invalid status code",
				error: true
			};
			/** @type {Buffer} */
			let reason = data.subarray(2);
			if (reason[0] === 239 && reason[1] === 187 && reason[2] === 191) reason = reason.subarray(3);
			try {
				reason = utf8Decode$2(reason);
			} catch {
				return {
					code: 1007,
					reason: "Invalid UTF-8",
					error: true
				};
			}
			return {
				code,
				reason,
				error: false
			};
		}
		/**
		* Parses control frames.
		* @param {Buffer} body
		*/
		parseControlFrame(body) {
			const { opcode, payloadLength } = this.#info;
			if (opcode === opcodes$3.CLOSE) {
				if (payloadLength === 1) {
					failWebsocketConnection$2(this.#handler, 1002, "Received close frame with a 1-byte body.");
					return false;
				}
				this.#info.closeInfo = this.parseCloseBody(body);
				if (this.#info.closeInfo.error) {
					const { code, reason } = this.#info.closeInfo;
					failWebsocketConnection$2(this.#handler, code, reason);
					return false;
				}
				if (!this.#handler.closeState.has(sentCloseFrameState$2.SENT) && !this.#handler.closeState.has(sentCloseFrameState$2.RECEIVED)) {
					let body$1 = emptyBuffer;
					if (this.#info.closeInfo.code) {
						body$1 = Buffer.allocUnsafe(2);
						body$1.writeUInt16BE(this.#info.closeInfo.code, 0);
					}
					const closeFrame = new WebsocketFrameSend$3(body$1);
					this.#handler.socket.write(closeFrame.createFrame(opcodes$3.CLOSE));
					this.#handler.closeState.add(sentCloseFrameState$2.SENT);
				}
				this.#handler.readyState = states$2.CLOSING;
				this.#handler.closeState.add(sentCloseFrameState$2.RECEIVED);
				return false;
			} else if (opcode === opcodes$3.PING) {
				if (!this.#handler.closeState.has(sentCloseFrameState$2.RECEIVED)) {
					const frame = new WebsocketFrameSend$3(body);
					this.#handler.socket.write(frame.createFrame(opcodes$3.PONG));
					this.#handler.onPing(body);
				}
			} else if (opcode === opcodes$3.PONG) this.#handler.onPong(body);
			return true;
		}
		get closingInfo() {
			return this.#info.closeInfo;
		}
	};
	module.exports = { ByteParser: ByteParser$2 };
}) });

//#endregion
//#region node_modules/undici/lib/web/websocket/sender.js
var require_sender = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/websocket/sender.js": ((exports, module) => {
	const { WebsocketFrameSend: WebsocketFrameSend$2 } = require_frame();
	const { opcodes: opcodes$2, sendHints: sendHints$1 } = require_constants();
	const FixedQueue = require_fixed_queue();
	/**
	* @typedef {object} SendQueueNode
	* @property {Promise<void> | null} promise
	* @property {((...args: any[]) => any)} callback
	* @property {Buffer | null} frame
	*/
	var SendQueue$1 = class {
		/**
		* @type {FixedQueue}
		*/
		#queue = new FixedQueue();
		/**
		* @type {boolean}
		*/
		#running = false;
		/** @type {import('node:net').Socket} */
		#socket;
		constructor(socket) {
			this.#socket = socket;
		}
		add(item, cb, hint) {
			if (hint !== sendHints$1.blob) {
				if (!this.#running) if (hint === sendHints$1.text) {
					const { 0: head, 1: body } = WebsocketFrameSend$2.createFastTextFrame(item);
					this.#socket.cork();
					this.#socket.write(head);
					this.#socket.write(body, cb);
					this.#socket.uncork();
				} else this.#socket.write(createFrame(item, hint), cb);
				else {
					/** @type {SendQueueNode} */
					const node$1 = {
						promise: null,
						callback: cb,
						frame: createFrame(item, hint)
					};
					this.#queue.push(node$1);
				}
				return;
			}
			/** @type {SendQueueNode} */
			const node = {
				promise: item.arrayBuffer().then((ab) => {
					node.promise = null;
					node.frame = createFrame(ab, hint);
				}),
				callback: cb,
				frame: null
			};
			this.#queue.push(node);
			if (!this.#running) this.#run();
		}
		async #run() {
			this.#running = true;
			const queue = this.#queue;
			while (!queue.isEmpty()) {
				const node = queue.shift();
				if (node.promise !== null) await node.promise;
				this.#socket.write(node.frame, node.callback);
				node.callback = node.frame = null;
			}
			this.#running = false;
		}
	};
	function createFrame(data, hint) {
		return new WebsocketFrameSend$2(toBuffer(data, hint)).createFrame(hint === sendHints$1.text ? opcodes$2.TEXT : opcodes$2.BINARY);
	}
	function toBuffer(data, hint) {
		switch (hint) {
			case sendHints$1.text:
			case sendHints$1.typedArray: return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
			case sendHints$1.arrayBuffer:
			case sendHints$1.blob: return new Uint8Array(data);
		}
	}
	module.exports = { SendQueue: SendQueue$1 };
}) });

//#endregion
//#region node_modules/undici/lib/web/websocket/websocket.js
var require_websocket = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/websocket/websocket.js": ((exports, module) => {
	const { isArrayBuffer: isArrayBuffer$1 } = __require("node:util/types");
	const { webidl: webidl$3 } = require_webidl();
	const { URLSerializer } = require_data_url();
	const { environmentSettingsObject: environmentSettingsObject$2 } = require_util$4();
	const { staticPropertyDescriptors, states: states$1, sentCloseFrameState: sentCloseFrameState$1, sendHints, opcodes: opcodes$1 } = require_constants();
	const { isConnecting, isEstablished: isEstablished$1, isClosing, isClosed, isValidSubprotocol: isValidSubprotocol$1, fireEvent, utf8Decode: utf8Decode$1, toArrayBuffer, getURLRecord: getURLRecord$1 } = require_util$1();
	const { establishWebSocketConnection: establishWebSocketConnection$1, closeWebSocketConnection: closeWebSocketConnection$1, failWebsocketConnection: failWebsocketConnection$1 } = require_connection();
	const { ByteParser: ByteParser$1 } = require_receiver();
	const { kEnumerableProperty: kEnumerableProperty$3 } = require_util$5();
	const { getGlobalDispatcher: getGlobalDispatcher$1 } = require_global();
	const { ErrorEvent: ErrorEvent$1, CloseEvent: CloseEvent$1, createFastMessageEvent: createFastMessageEvent$1 } = require_events();
	const { SendQueue } = require_sender();
	const { WebsocketFrameSend: WebsocketFrameSend$1 } = require_frame();
	const { channels: channels$1 } = require_diagnostics();
	/**
	* @typedef {object} Handler
	* @property {(response: any, extensions?: string[]) => void} onConnectionEstablished
	* @property {(code: number, reason: any) => void} onFail
	* @property {(opcode: number, data: Buffer) => void} onMessage
	* @property {(error: Error) => void} onParserError
	* @property {() => void} onParserDrain
	* @property {(chunk: Buffer) => void} onSocketData
	* @property {(err: Error) => void} onSocketError
	* @property {() => void} onSocketClose
	* @property {(body: Buffer) => void} onPing
	* @property {(body: Buffer) => void} onPong
	*
	* @property {number} readyState
	* @property {import('stream').Duplex} socket
	* @property {Set<number>} closeState
	* @property {import('../fetch/index').Fetch} controller
	* @property {boolean} [wasEverConnected=false]
	*/
	var WebSocket$1 = class WebSocket$1 extends EventTarget {
		#events = {
			open: null,
			error: null,
			close: null,
			message: null
		};
		#bufferedAmount = 0;
		#protocol = "";
		#extensions = "";
		/** @type {SendQueue} */
		#sendQueue;
		/** @type {Handler} */
		#handler = {
			onConnectionEstablished: (response, extensions) => this.#onConnectionEstablished(response, extensions),
			onFail: (code, reason, cause) => this.#onFail(code, reason, cause),
			onMessage: (opcode, data) => this.#onMessage(opcode, data),
			onParserError: (err) => failWebsocketConnection$1(this.#handler, null, err.message),
			onParserDrain: () => this.#onParserDrain(),
			onSocketData: (chunk) => {
				if (!this.#parser.write(chunk)) this.#handler.socket.pause();
			},
			onSocketError: (err) => {
				this.#handler.readyState = states$1.CLOSING;
				if (channels$1.socketError.hasSubscribers) channels$1.socketError.publish(err);
				this.#handler.socket.destroy();
			},
			onSocketClose: () => this.#onSocketClose(),
			onPing: (body) => {
				if (channels$1.ping.hasSubscribers) channels$1.ping.publish({
					payload: body,
					websocket: this
				});
			},
			onPong: (body) => {
				if (channels$1.pong.hasSubscribers) channels$1.pong.publish({
					payload: body,
					websocket: this
				});
			},
			readyState: states$1.CONNECTING,
			socket: null,
			closeState: /* @__PURE__ */ new Set(),
			controller: null,
			wasEverConnected: false
		};
		#url;
		#binaryType;
		/** @type {import('./receiver').ByteParser} */
		#parser;
		/**
		* @param {string} url
		* @param {string|string[]} protocols
		*/
		constructor(url, protocols = []) {
			super();
			webidl$3.util.markAsUncloneable(this);
			const prefix = "WebSocket constructor";
			webidl$3.argumentLengthCheck(arguments, 1, prefix);
			const options = webidl$3.converters["DOMString or sequence<DOMString> or WebSocketInit"](protocols, prefix, "options");
			url = webidl$3.converters.USVString(url);
			protocols = options.protocols;
			const baseURL = environmentSettingsObject$2.settingsObject.baseUrl;
			const urlRecord = getURLRecord$1(url, baseURL);
			if (typeof protocols === "string") protocols = [protocols];
			if (protocols.length !== new Set(protocols.map((p) => p.toLowerCase())).size) throw new DOMException("Invalid Sec-WebSocket-Protocol value", "SyntaxError");
			if (protocols.length > 0 && !protocols.every((p) => isValidSubprotocol$1(p))) throw new DOMException("Invalid Sec-WebSocket-Protocol value", "SyntaxError");
			this.#url = new URL(urlRecord.href);
			const client = environmentSettingsObject$2.settingsObject;
			this.#handler.controller = establishWebSocketConnection$1(urlRecord, protocols, client, this.#handler, options);
			this.#handler.readyState = WebSocket$1.CONNECTING;
			this.#binaryType = "blob";
		}
		/**
		* @see https://websockets.spec.whatwg.org/#dom-websocket-close
		* @param {number|undefined} code
		* @param {string|undefined} reason
		*/
		close(code = void 0, reason = void 0) {
			webidl$3.brandCheck(this, WebSocket$1);
			const prefix = "WebSocket.close";
			if (code !== void 0) code = webidl$3.converters["unsigned short"](code, prefix, "code", { clamp: true });
			if (reason !== void 0) reason = webidl$3.converters.USVString(reason);
			code ??= null;
			reason ??= "";
			closeWebSocketConnection$1(this.#handler, code, reason, true);
		}
		/**
		* @see https://websockets.spec.whatwg.org/#dom-websocket-send
		* @param {NodeJS.TypedArray|ArrayBuffer|Blob|string} data
		*/
		send(data) {
			webidl$3.brandCheck(this, WebSocket$1);
			const prefix = "WebSocket.send";
			webidl$3.argumentLengthCheck(arguments, 1, prefix);
			data = webidl$3.converters.WebSocketSendData(data, prefix, "data");
			if (isConnecting(this.#handler.readyState)) throw new DOMException("Sent before connected.", "InvalidStateError");
			if (!isEstablished$1(this.#handler.readyState) || isClosing(this.#handler.readyState)) return;
			if (typeof data === "string") {
				const buffer$1 = Buffer.from(data);
				this.#bufferedAmount += buffer$1.byteLength;
				this.#sendQueue.add(buffer$1, () => {
					this.#bufferedAmount -= buffer$1.byteLength;
				}, sendHints.text);
			} else if (isArrayBuffer$1(data)) {
				this.#bufferedAmount += data.byteLength;
				this.#sendQueue.add(data, () => {
					this.#bufferedAmount -= data.byteLength;
				}, sendHints.arrayBuffer);
			} else if (ArrayBuffer.isView(data)) {
				this.#bufferedAmount += data.byteLength;
				this.#sendQueue.add(data, () => {
					this.#bufferedAmount -= data.byteLength;
				}, sendHints.typedArray);
			} else if (webidl$3.is.Blob(data)) {
				this.#bufferedAmount += data.size;
				this.#sendQueue.add(data, () => {
					this.#bufferedAmount -= data.size;
				}, sendHints.blob);
			}
		}
		get readyState() {
			webidl$3.brandCheck(this, WebSocket$1);
			return this.#handler.readyState;
		}
		get bufferedAmount() {
			webidl$3.brandCheck(this, WebSocket$1);
			return this.#bufferedAmount;
		}
		get url() {
			webidl$3.brandCheck(this, WebSocket$1);
			return URLSerializer(this.#url);
		}
		get extensions() {
			webidl$3.brandCheck(this, WebSocket$1);
			return this.#extensions;
		}
		get protocol() {
			webidl$3.brandCheck(this, WebSocket$1);
			return this.#protocol;
		}
		get onopen() {
			webidl$3.brandCheck(this, WebSocket$1);
			return this.#events.open;
		}
		set onopen(fn) {
			webidl$3.brandCheck(this, WebSocket$1);
			if (this.#events.open) this.removeEventListener("open", this.#events.open);
			if (typeof fn === "function") {
				this.#events.open = fn;
				this.addEventListener("open", fn);
			} else this.#events.open = null;
		}
		get onerror() {
			webidl$3.brandCheck(this, WebSocket$1);
			return this.#events.error;
		}
		set onerror(fn) {
			webidl$3.brandCheck(this, WebSocket$1);
			if (this.#events.error) this.removeEventListener("error", this.#events.error);
			if (typeof fn === "function") {
				this.#events.error = fn;
				this.addEventListener("error", fn);
			} else this.#events.error = null;
		}
		get onclose() {
			webidl$3.brandCheck(this, WebSocket$1);
			return this.#events.close;
		}
		set onclose(fn) {
			webidl$3.brandCheck(this, WebSocket$1);
			if (this.#events.close) this.removeEventListener("close", this.#events.close);
			if (typeof fn === "function") {
				this.#events.close = fn;
				this.addEventListener("close", fn);
			} else this.#events.close = null;
		}
		get onmessage() {
			webidl$3.brandCheck(this, WebSocket$1);
			return this.#events.message;
		}
		set onmessage(fn) {
			webidl$3.brandCheck(this, WebSocket$1);
			if (this.#events.message) this.removeEventListener("message", this.#events.message);
			if (typeof fn === "function") {
				this.#events.message = fn;
				this.addEventListener("message", fn);
			} else this.#events.message = null;
		}
		get binaryType() {
			webidl$3.brandCheck(this, WebSocket$1);
			return this.#binaryType;
		}
		set binaryType(type) {
			webidl$3.brandCheck(this, WebSocket$1);
			if (type !== "blob" && type !== "arraybuffer") this.#binaryType = "blob";
			else this.#binaryType = type;
		}
		/**
		* @see https://websockets.spec.whatwg.org/#feedback-from-the-protocol
		*/
		#onConnectionEstablished(response, parsedExtensions) {
			this.#handler.socket = response.socket;
			const parser = new ByteParser$1(this.#handler, parsedExtensions);
			parser.on("drain", () => this.#handler.onParserDrain());
			parser.on("error", (err) => this.#handler.onParserError(err));
			this.#parser = parser;
			this.#sendQueue = new SendQueue(response.socket);
			this.#handler.readyState = states$1.OPEN;
			const extensions = response.headersList.get("sec-websocket-extensions");
			if (extensions !== null) this.#extensions = extensions;
			const protocol = response.headersList.get("sec-websocket-protocol");
			if (protocol !== null) this.#protocol = protocol;
			fireEvent("open", this);
			if (channels$1.open.hasSubscribers) {
				const headers = response.headersList.entries;
				channels$1.open.publish({
					address: response.socket.address(),
					protocol: this.#protocol,
					extensions: this.#extensions,
					websocket: this,
					handshakeResponse: {
						status: response.status,
						statusText: response.statusText,
						headers
					}
				});
			}
		}
		#onFail(code, reason, cause) {
			if (reason) fireEvent("error", this, (type, init) => new ErrorEvent$1(type, init), {
				error: new Error(reason, cause ? { cause } : void 0),
				message: reason
			});
			if (!this.#handler.wasEverConnected) {
				this.#handler.readyState = states$1.CLOSED;
				fireEvent("close", this, (type, init) => new CloseEvent$1(type, init), {
					wasClean: false,
					code,
					reason
				});
			}
		}
		#onMessage(type, data) {
			if (this.#handler.readyState !== states$1.OPEN) return;
			let dataForEvent;
			if (type === opcodes$1.TEXT) try {
				dataForEvent = utf8Decode$1(data);
			} catch {
				failWebsocketConnection$1(this.#handler, 1007, "Received invalid UTF-8 in text frame.");
				return;
			}
			else if (type === opcodes$1.BINARY) if (this.#binaryType === "blob") dataForEvent = new Blob([data]);
			else dataForEvent = toArrayBuffer(data);
			fireEvent("message", this, createFastMessageEvent$1, {
				origin: this.#url.origin,
				data: dataForEvent
			});
		}
		#onParserDrain() {
			this.#handler.socket.resume();
		}
		/**
		* @see https://websockets.spec.whatwg.org/#feedback-from-the-protocol
		* @see https://datatracker.ietf.org/doc/html/rfc6455#section-7.1.4
		*/
		#onSocketClose() {
			const wasClean = this.#handler.closeState.has(sentCloseFrameState$1.SENT) && this.#handler.closeState.has(sentCloseFrameState$1.RECEIVED);
			let code = 1005;
			let reason = "";
			const result = this.#parser.closingInfo;
			if (result && !result.error) {
				code = result.code ?? 1005;
				reason = result.reason;
			} else if (!this.#handler.closeState.has(sentCloseFrameState$1.RECEIVED)) code = 1006;
			this.#handler.readyState = states$1.CLOSED;
			fireEvent("close", this, (type, init) => new CloseEvent$1(type, init), {
				wasClean,
				code,
				reason
			});
			if (channels$1.close.hasSubscribers) channels$1.close.publish({
				websocket: this,
				code,
				reason
			});
		}
		/**
		* @param {WebSocket} ws
		* @param {Buffer|undefined} buffer
		*/
		static ping(ws, buffer$1) {
			if (Buffer.isBuffer(buffer$1)) {
				if (buffer$1.length > 125) throw new TypeError("A PING frame cannot have a body larger than 125 bytes.");
			} else if (buffer$1 !== void 0) throw new TypeError("Expected buffer payload");
			const readyState = ws.#handler.readyState;
			if (isEstablished$1(readyState) && !isClosing(readyState) && !isClosed(readyState)) {
				const frame = new WebsocketFrameSend$1(buffer$1);
				ws.#handler.socket.write(frame.createFrame(opcodes$1.PING));
			}
		}
	};
	const { ping: ping$1 } = WebSocket$1;
	Reflect.deleteProperty(WebSocket$1, "ping");
	WebSocket$1.CONNECTING = WebSocket$1.prototype.CONNECTING = states$1.CONNECTING;
	WebSocket$1.OPEN = WebSocket$1.prototype.OPEN = states$1.OPEN;
	WebSocket$1.CLOSING = WebSocket$1.prototype.CLOSING = states$1.CLOSING;
	WebSocket$1.CLOSED = WebSocket$1.prototype.CLOSED = states$1.CLOSED;
	Object.defineProperties(WebSocket$1.prototype, {
		CONNECTING: staticPropertyDescriptors,
		OPEN: staticPropertyDescriptors,
		CLOSING: staticPropertyDescriptors,
		CLOSED: staticPropertyDescriptors,
		url: kEnumerableProperty$3,
		readyState: kEnumerableProperty$3,
		bufferedAmount: kEnumerableProperty$3,
		onopen: kEnumerableProperty$3,
		onerror: kEnumerableProperty$3,
		onclose: kEnumerableProperty$3,
		close: kEnumerableProperty$3,
		onmessage: kEnumerableProperty$3,
		binaryType: kEnumerableProperty$3,
		send: kEnumerableProperty$3,
		extensions: kEnumerableProperty$3,
		protocol: kEnumerableProperty$3,
		[Symbol.toStringTag]: {
			value: "WebSocket",
			writable: false,
			enumerable: false,
			configurable: true
		}
	});
	Object.defineProperties(WebSocket$1, {
		CONNECTING: staticPropertyDescriptors,
		OPEN: staticPropertyDescriptors,
		CLOSING: staticPropertyDescriptors,
		CLOSED: staticPropertyDescriptors
	});
	webidl$3.converters["sequence<DOMString>"] = webidl$3.sequenceConverter(webidl$3.converters.DOMString);
	webidl$3.converters["DOMString or sequence<DOMString>"] = function(V, prefix, argument) {
		if (webidl$3.util.Type(V) === webidl$3.util.Types.OBJECT && Symbol.iterator in V) return webidl$3.converters["sequence<DOMString>"](V);
		return webidl$3.converters.DOMString(V, prefix, argument);
	};
	webidl$3.converters.WebSocketInit = webidl$3.dictionaryConverter([
		{
			key: "protocols",
			converter: webidl$3.converters["DOMString or sequence<DOMString>"],
			defaultValue: () => new Array(0)
		},
		{
			key: "dispatcher",
			converter: webidl$3.converters.any,
			defaultValue: () => getGlobalDispatcher$1()
		},
		{
			key: "headers",
			converter: webidl$3.nullableConverter(webidl$3.converters.HeadersInit)
		}
	]);
	webidl$3.converters["DOMString or sequence<DOMString> or WebSocketInit"] = function(V) {
		if (webidl$3.util.Type(V) === webidl$3.util.Types.OBJECT && !(Symbol.iterator in V)) return webidl$3.converters.WebSocketInit(V);
		return { protocols: webidl$3.converters["DOMString or sequence<DOMString>"](V) };
	};
	webidl$3.converters.WebSocketSendData = function(V) {
		if (webidl$3.util.Type(V) === webidl$3.util.Types.OBJECT) {
			if (webidl$3.is.Blob(V)) return V;
			if (ArrayBuffer.isView(V) || isArrayBuffer$1(V)) return V;
		}
		return webidl$3.converters.USVString(V);
	};
	module.exports = {
		WebSocket: WebSocket$1,
		ping: ping$1
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/websocket/stream/websocketerror.js
var require_websocketerror = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/websocket/stream/websocketerror.js": ((exports, module) => {
	const { webidl: webidl$2 } = require_webidl();
	const { validateCloseCodeAndReason } = require_util$1();
	const { kConstruct: kConstruct$1 } = require_symbols();
	const { kEnumerableProperty: kEnumerableProperty$2 } = require_util$5();
	var WebSocketError$1 = class WebSocketError$1 extends DOMException {
		#closeCode;
		#reason;
		constructor(message = "", init = void 0) {
			message = webidl$2.converters.DOMString(message, "WebSocketError", "message");
			super(message, "WebSocketError");
			if (init === kConstruct$1) return;
			else if (init !== null) init = webidl$2.converters.WebSocketCloseInfo(init);
			let code = init.closeCode ?? null;
			const reason = init.reason ?? "";
			validateCloseCodeAndReason(code, reason);
			if (reason.length !== 0 && code === null) code = 1e3;
			this.#closeCode = code;
			this.#reason = reason;
		}
		get closeCode() {
			return this.#closeCode;
		}
		get reason() {
			return this.#reason;
		}
		/**
		* @param {string} message
		* @param {number|null} code
		* @param {string} reason
		*/
		static createUnvalidatedWebSocketError(message, code, reason) {
			const error = new WebSocketError$1(message, kConstruct$1);
			error.#closeCode = code;
			error.#reason = reason;
			return error;
		}
	};
	const { createUnvalidatedWebSocketError: createUnvalidatedWebSocketError$1 } = WebSocketError$1;
	delete WebSocketError$1.createUnvalidatedWebSocketError;
	Object.defineProperties(WebSocketError$1.prototype, {
		closeCode: kEnumerableProperty$2,
		reason: kEnumerableProperty$2,
		[Symbol.toStringTag]: {
			value: "WebSocketError",
			writable: false,
			enumerable: false,
			configurable: true
		}
	});
	webidl$2.is.WebSocketError = webidl$2.util.MakeTypeAssertion(WebSocketError$1);
	module.exports = {
		WebSocketError: WebSocketError$1,
		createUnvalidatedWebSocketError: createUnvalidatedWebSocketError$1
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/websocket/stream/websocketstream.js
var require_websocketstream = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/websocket/stream/websocketstream.js": ((exports, module) => {
	const { createDeferredPromise } = require_promise();
	const { environmentSettingsObject: environmentSettingsObject$1 } = require_util$4();
	const { states, opcodes, sentCloseFrameState } = require_constants();
	const { webidl: webidl$1 } = require_webidl();
	const { getURLRecord, isValidSubprotocol, isEstablished, utf8Decode } = require_util$1();
	const { establishWebSocketConnection, failWebsocketConnection, closeWebSocketConnection } = require_connection();
	const { isArrayBuffer } = __require("node:util/types");
	const { channels } = require_diagnostics();
	const { WebsocketFrameSend } = require_frame();
	const { ByteParser } = require_receiver();
	const { WebSocketError, createUnvalidatedWebSocketError } = require_websocketerror();
	const { utf8DecodeBytes } = require_util$4();
	const { kEnumerableProperty: kEnumerableProperty$1 } = require_util$5();
	let emittedExperimentalWarning = false;
	var WebSocketStream = class {
		/** @type {URL} */
		#url;
		/** @type {import('../../../util/promise').DeferredPromise} */
		#openedPromise;
		/** @type {import('../../../util/promise').DeferredPromise} */
		#closedPromise;
		/** @type {ReadableStream} */
		#readableStream;
		/** @type {ReadableStreamDefaultController} */
		#readableStreamController;
		/** @type {WritableStream} */
		#writableStream;
		#handshakeAborted = false;
		/** @type {import('../websocket').Handler} */
		#handler = {
			onConnectionEstablished: (response, extensions) => this.#onConnectionEstablished(response, extensions),
			onFail: (_code, _reason) => {},
			onMessage: (opcode, data) => this.#onMessage(opcode, data),
			onParserError: (err) => failWebsocketConnection(this.#handler, null, err.message),
			onParserDrain: () => this.#handler.socket.resume(),
			onSocketData: (chunk) => {
				if (!this.#parser.write(chunk)) this.#handler.socket.pause();
			},
			onSocketError: (err) => {
				this.#handler.readyState = states.CLOSING;
				if (channels.socketError.hasSubscribers) channels.socketError.publish(err);
				this.#handler.socket.destroy();
			},
			onSocketClose: () => this.#onSocketClose(),
			onPing: () => {},
			onPong: () => {},
			readyState: states.CONNECTING,
			socket: null,
			closeState: /* @__PURE__ */ new Set(),
			controller: null,
			wasEverConnected: false
		};
		/** @type {import('../receiver').ByteParser} */
		#parser;
		constructor(url, options = void 0) {
			if (!emittedExperimentalWarning) {
				process.emitWarning("WebSocketStream is experimental! Expect it to change at any time.", { code: "UNDICI-WSS" });
				emittedExperimentalWarning = true;
			}
			webidl$1.argumentLengthCheck(arguments, 1, "WebSocket");
			url = webidl$1.converters.USVString(url);
			if (options !== null) options = webidl$1.converters.WebSocketStreamOptions(options);
			const baseURL = environmentSettingsObject$1.settingsObject.baseUrl;
			const urlRecord = getURLRecord(url, baseURL);
			const protocols = options.protocols;
			if (protocols.length !== new Set(protocols.map((p) => p.toLowerCase())).size) throw new DOMException("Invalid Sec-WebSocket-Protocol value", "SyntaxError");
			if (protocols.length > 0 && !protocols.every((p) => isValidSubprotocol(p))) throw new DOMException("Invalid Sec-WebSocket-Protocol value", "SyntaxError");
			this.#url = urlRecord.toString();
			this.#openedPromise = createDeferredPromise();
			this.#closedPromise = createDeferredPromise();
			if (options.signal != null) {
				const signal = options.signal;
				if (signal.aborted) {
					this.#openedPromise.reject(signal.reason);
					this.#closedPromise.reject(signal.reason);
					return;
				}
				signal.addEventListener("abort", () => {
					if (!isEstablished(this.#handler.readyState)) {
						failWebsocketConnection(this.#handler);
						this.#handler.readyState = states.CLOSING;
						this.#openedPromise.reject(signal.reason);
						this.#closedPromise.reject(signal.reason);
						this.#handshakeAborted = true;
					}
				}, { once: true });
			}
			const client = environmentSettingsObject$1.settingsObject;
			this.#handler.controller = establishWebSocketConnection(urlRecord, protocols, client, this.#handler, options);
		}
		get url() {
			return this.#url.toString();
		}
		get opened() {
			return this.#openedPromise.promise;
		}
		get closed() {
			return this.#closedPromise.promise;
		}
		close(closeInfo = void 0) {
			if (closeInfo !== null) closeInfo = webidl$1.converters.WebSocketCloseInfo(closeInfo);
			const code = closeInfo.closeCode ?? null;
			const reason = closeInfo.reason;
			closeWebSocketConnection(this.#handler, code, reason, true);
		}
		#write(chunk) {
			const promise = createDeferredPromise();
			let data = null;
			let opcode = null;
			if (ArrayBuffer.isView(chunk) || isArrayBuffer(chunk)) {
				data = new Uint8Array(ArrayBuffer.isView(chunk) ? new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength) : chunk);
				opcode = opcodes.BINARY;
			} else {
				let string;
				try {
					string = webidl$1.converters.DOMString(chunk);
				} catch (e) {
					promise.reject(e);
					return;
				}
				data = new TextEncoder().encode(string);
				opcode = opcodes.TEXT;
			}
			if (!this.#handler.closeState.has(sentCloseFrameState.SENT) && !this.#handler.closeState.has(sentCloseFrameState.RECEIVED)) {
				const frame = new WebsocketFrameSend(data);
				this.#handler.socket.write(frame.createFrame(opcode), () => {
					promise.resolve(void 0);
				});
			}
			return promise;
		}
		/** @type {import('../websocket').Handler['onConnectionEstablished']} */
		#onConnectionEstablished(response, parsedExtensions) {
			this.#handler.socket = response.socket;
			const parser = new ByteParser(this.#handler, parsedExtensions);
			parser.on("drain", () => this.#handler.onParserDrain());
			parser.on("error", (err) => this.#handler.onParserError(err));
			this.#parser = parser;
			this.#handler.readyState = states.OPEN;
			const extensions = parsedExtensions ?? "";
			const protocol = response.headersList.get("sec-websocket-protocol") ?? "";
			const readable = new ReadableStream({
				start: (controller) => {
					this.#readableStreamController = controller;
				},
				pull(controller) {
					let chunk;
					while (controller.desiredSize > 0 && (chunk = response.socket.read()) !== null) controller.enqueue(chunk);
				},
				cancel: (reason) => this.#cancel(reason)
			});
			const writable = new WritableStream({
				write: (chunk) => this.#write(chunk),
				close: () => closeWebSocketConnection(this.#handler, null, null),
				abort: (reason) => this.#closeUsingReason(reason)
			});
			this.#readableStream = readable;
			this.#writableStream = writable;
			this.#openedPromise.resolve({
				extensions,
				protocol,
				readable,
				writable
			});
		}
		/** @type {import('../websocket').Handler['onMessage']} */
		#onMessage(type, data) {
			if (this.#handler.readyState !== states.OPEN) return;
			let chunk;
			if (type === opcodes.TEXT) try {
				chunk = utf8Decode(data);
			} catch {
				failWebsocketConnection(this.#handler, "Received invalid UTF-8 in text frame.");
				return;
			}
			else if (type === opcodes.BINARY) chunk = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
			this.#readableStreamController.enqueue(chunk);
		}
		/** @type {import('../websocket').Handler['onSocketClose']} */
		#onSocketClose() {
			const wasClean = this.#handler.closeState.has(sentCloseFrameState.SENT) && this.#handler.closeState.has(sentCloseFrameState.RECEIVED);
			this.#handler.readyState = states.CLOSED;
			if (this.#handshakeAborted) return;
			if (!this.#handler.wasEverConnected) this.#openedPromise.reject(new WebSocketError("Socket never opened"));
			const result = this.#parser.closingInfo;
			let code = result?.code ?? 1005;
			if (!this.#handler.closeState.has(sentCloseFrameState.SENT) && !this.#handler.closeState.has(sentCloseFrameState.RECEIVED)) code = 1006;
			const reason = result?.reason == null ? "" : utf8DecodeBytes(Buffer.from(result.reason));
			if (wasClean) {
				this.#readableStreamController.close();
				if (!this.#writableStream.locked) this.#writableStream.abort(new DOMException("A closed WebSocketStream cannot be written to", "InvalidStateError"));
				this.#closedPromise.resolve({
					closeCode: code,
					reason
				});
			} else {
				const error = createUnvalidatedWebSocketError("unclean close", code, reason);
				this.#readableStreamController.error(error);
				this.#writableStream.abort(error);
				this.#closedPromise.reject(error);
			}
		}
		#closeUsingReason(reason) {
			let code = null;
			let reasonString = "";
			if (webidl$1.is.WebSocketError(reason)) {
				code = reason.closeCode;
				reasonString = reason.reason;
			}
			closeWebSocketConnection(this.#handler, code, reasonString);
		}
		#cancel(reason) {
			this.#closeUsingReason(reason);
		}
	};
	Object.defineProperties(WebSocketStream.prototype, {
		url: kEnumerableProperty$1,
		opened: kEnumerableProperty$1,
		closed: kEnumerableProperty$1,
		close: kEnumerableProperty$1,
		[Symbol.toStringTag]: {
			value: "WebSocketStream",
			writable: false,
			enumerable: false,
			configurable: true
		}
	});
	webidl$1.converters.WebSocketStreamOptions = webidl$1.dictionaryConverter([{
		key: "protocols",
		converter: webidl$1.sequenceConverter(webidl$1.converters.USVString),
		defaultValue: () => []
	}, {
		key: "signal",
		converter: webidl$1.nullableConverter(webidl$1.converters.AbortSignal),
		defaultValue: () => null
	}]);
	webidl$1.converters.WebSocketCloseInfo = webidl$1.dictionaryConverter([{
		key: "closeCode",
		converter: (V) => webidl$1.converters["unsigned short"](V, { enforceRange: true })
	}, {
		key: "reason",
		converter: webidl$1.converters.USVString,
		defaultValue: () => ""
	}]);
	module.exports = { WebSocketStream };
}) });

//#endregion
//#region node_modules/undici/lib/web/eventsource/util.js
var require_util = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/eventsource/util.js": ((exports, module) => {
	/**
	* Checks if the given value is a valid LastEventId.
	* @param {string} value
	* @returns {boolean}
	*/
	function isValidLastEventId$1(value) {
		return value.indexOf("\0") === -1;
	}
	/**
	* Checks if the given value is a base 10 digit.
	* @param {string} value
	* @returns {boolean}
	*/
	function isASCIINumber$1(value) {
		if (value.length === 0) return false;
		for (let i = 0; i < value.length; i++) if (value.charCodeAt(i) < 48 || value.charCodeAt(i) > 57) return false;
		return true;
	}
	function delay$1(ms) {
		return new Promise((resolve$1) => {
			setTimeout(resolve$1, ms);
		});
	}
	module.exports = {
		isValidLastEventId: isValidLastEventId$1,
		isASCIINumber: isASCIINumber$1,
		delay: delay$1
	};
}) });

//#endregion
//#region node_modules/undici/lib/web/eventsource/eventsource-stream.js
var require_eventsource_stream = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/eventsource/eventsource-stream.js": ((exports, module) => {
	const { Transform } = __require("node:stream");
	const { isASCIINumber, isValidLastEventId } = require_util();
	/**
	* @type {number[]} BOM
	*/
	const BOM = [
		239,
		187,
		191
	];
	/**
	* @type {10} LF
	*/
	const LF = 10;
	/**
	* @type {13} CR
	*/
	const CR = 13;
	/**
	* @type {58} COLON
	*/
	const COLON = 58;
	/**
	* @type {32} SPACE
	*/
	const SPACE = 32;
	/**
	* @typedef {object} EventSourceStreamEvent
	* @type {object}
	* @property {string} [event] The event type.
	* @property {string} [data] The data of the message.
	* @property {string} [id] A unique ID for the event.
	* @property {string} [retry] The reconnection time, in milliseconds.
	*/
	/**
	* @typedef eventSourceSettings
	* @type {object}
	* @property {string} [lastEventId] The last event ID received from the server.
	* @property {string} [origin] The origin of the event source.
	* @property {number} [reconnectionTime] The reconnection time, in milliseconds.
	*/
	var EventSourceStream$1 = class extends Transform {
		/**
		* @type {eventSourceSettings}
		*/
		state;
		/**
		* Leading byte-order-mark check.
		* @type {boolean}
		*/
		checkBOM = true;
		/**
		* @type {boolean}
		*/
		crlfCheck = false;
		/**
		* @type {boolean}
		*/
		eventEndCheck = false;
		/**
		* @type {Buffer|null}
		*/
		buffer = null;
		pos = 0;
		event = {
			data: void 0,
			event: void 0,
			id: void 0,
			retry: void 0
		};
		/**
		* @param {object} options
		* @param {boolean} [options.readableObjectMode]
		* @param {eventSourceSettings} [options.eventSourceSettings]
		* @param {(chunk: any, encoding?: BufferEncoding | undefined) => boolean} [options.push]
		*/
		constructor(options = {}) {
			options.readableObjectMode = true;
			super(options);
			this.state = options.eventSourceSettings || {};
			if (options.push) this.push = options.push;
		}
		/**
		* @param {Buffer} chunk
		* @param {string} _encoding
		* @param {Function} callback
		* @returns {void}
		*/
		_transform(chunk, _encoding, callback) {
			if (chunk.length === 0) {
				callback();
				return;
			}
			if (this.buffer) this.buffer = Buffer.concat([this.buffer, chunk]);
			else this.buffer = chunk;
			if (this.checkBOM) switch (this.buffer.length) {
				case 1:
					if (this.buffer[0] === BOM[0]) {
						callback();
						return;
					}
					this.checkBOM = false;
					callback();
					return;
				case 2:
					if (this.buffer[0] === BOM[0] && this.buffer[1] === BOM[1]) {
						callback();
						return;
					}
					this.checkBOM = false;
					break;
				case 3:
					if (this.buffer[0] === BOM[0] && this.buffer[1] === BOM[1] && this.buffer[2] === BOM[2]) {
						this.buffer = Buffer.alloc(0);
						this.checkBOM = false;
						callback();
						return;
					}
					this.checkBOM = false;
					break;
				default:
					if (this.buffer[0] === BOM[0] && this.buffer[1] === BOM[1] && this.buffer[2] === BOM[2]) this.buffer = this.buffer.subarray(3);
					this.checkBOM = false;
					break;
			}
			while (this.pos < this.buffer.length) {
				if (this.eventEndCheck) {
					if (this.crlfCheck) {
						if (this.buffer[this.pos] === LF) {
							this.buffer = this.buffer.subarray(this.pos + 1);
							this.pos = 0;
							this.crlfCheck = false;
							continue;
						}
						this.crlfCheck = false;
					}
					if (this.buffer[this.pos] === LF || this.buffer[this.pos] === CR) {
						if (this.buffer[this.pos] === CR) this.crlfCheck = true;
						this.buffer = this.buffer.subarray(this.pos + 1);
						this.pos = 0;
						if (this.event.data !== void 0 || this.event.event || this.event.id || this.event.retry) this.processEvent(this.event);
						this.clearEvent();
						continue;
					}
					this.eventEndCheck = false;
					continue;
				}
				if (this.buffer[this.pos] === LF || this.buffer[this.pos] === CR) {
					if (this.buffer[this.pos] === CR) this.crlfCheck = true;
					this.parseLine(this.buffer.subarray(0, this.pos), this.event);
					this.buffer = this.buffer.subarray(this.pos + 1);
					this.pos = 0;
					this.eventEndCheck = true;
					continue;
				}
				this.pos++;
			}
			callback();
		}
		/**
		* @param {Buffer} line
		* @param {EventSourceStreamEvent} event
		*/
		parseLine(line, event) {
			if (line.length === 0) return;
			const colonPosition = line.indexOf(COLON);
			if (colonPosition === 0) return;
			let field = "";
			let value = "";
			if (colonPosition !== -1) {
				field = line.subarray(0, colonPosition).toString("utf8");
				let valueStart = colonPosition + 1;
				if (line[valueStart] === SPACE) ++valueStart;
				value = line.subarray(valueStart).toString("utf8");
			} else {
				field = line.toString("utf8");
				value = "";
			}
			switch (field) {
				case "data":
					if (event[field] === void 0) event[field] = value;
					else event[field] += `\n${value}`;
					break;
				case "retry":
					if (isASCIINumber(value)) event[field] = value;
					break;
				case "id":
					if (isValidLastEventId(value)) event[field] = value;
					break;
				case "event":
					if (value.length > 0) event[field] = value;
					break;
			}
		}
		/**
		* @param {EventSourceStreamEvent} event
		*/
		processEvent(event) {
			if (event.retry && isASCIINumber(event.retry)) this.state.reconnectionTime = parseInt(event.retry, 10);
			if (event.id && isValidLastEventId(event.id)) this.state.lastEventId = event.id;
			if (event.data !== void 0) this.push({
				type: event.event || "message",
				options: {
					data: event.data,
					lastEventId: this.state.lastEventId,
					origin: this.state.origin
				}
			});
		}
		clearEvent() {
			this.event = {
				data: void 0,
				event: void 0,
				id: void 0,
				retry: void 0
			};
		}
	};
	module.exports = { EventSourceStream: EventSourceStream$1 };
}) });

//#endregion
//#region node_modules/undici/lib/web/eventsource/eventsource.js
var require_eventsource = /* @__PURE__ */ __commonJS({ "node_modules/undici/lib/web/eventsource/eventsource.js": ((exports, module) => {
	const { pipeline } = __require("node:stream");
	const { fetching } = require_fetch();
	const { makeRequest } = require_request();
	const { webidl } = require_webidl();
	const { EventSourceStream } = require_eventsource_stream();
	const { parseMIMEType: parseMIMEType$1 } = require_data_url();
	const { createFastMessageEvent } = require_events();
	const { isNetworkError } = require_response();
	const { delay } = require_util();
	const { kEnumerableProperty } = require_util$5();
	const { environmentSettingsObject } = require_util$4();
	let experimentalWarned = false;
	/**
	* A reconnection time, in milliseconds. This must initially be an implementation-defined value,
	* probably in the region of a few seconds.
	*
	* In Comparison:
	* - Chrome uses 3000ms.
	* - Deno uses 5000ms.
	*
	* @type {3000}
	*/
	const defaultReconnectionTime = 3e3;
	/**
	* The readyState attribute represents the state of the connection.
	* @typedef ReadyState
	* @type {0|1|2}
	* @readonly
	* @see https://html.spec.whatwg.org/multipage/server-sent-events.html#dom-eventsource-readystate-dev
	*/
	/**
	* The connection has not yet been established, or it was closed and the user
	* agent is reconnecting.
	* @type {0}
	*/
	const CONNECTING = 0;
	/**
	* The user agent has an open connection and is dispatching events as it
	* receives them.
	* @type {1}
	*/
	const OPEN = 1;
	/**
	* The connection is not open, and the user agent is not trying to reconnect.
	* @type {2}
	*/
	const CLOSED = 2;
	/**
	* Requests for the element will have their mode set to "cors" and their credentials mode set to "same-origin".
	* @type {'anonymous'}
	*/
	const ANONYMOUS = "anonymous";
	/**
	* Requests for the element will have their mode set to "cors" and their credentials mode set to "include".
	* @type {'use-credentials'}
	*/
	const USE_CREDENTIALS = "use-credentials";
	/**
	* The EventSource interface is used to receive server-sent events. It
	* connects to a server over HTTP and receives events in text/event-stream
	* format without closing the connection.
	* @extends {EventTarget}
	* @see https://html.spec.whatwg.org/multipage/server-sent-events.html#server-sent-events
	* @api public
	*/
	var EventSource$1 = class EventSource$1 extends EventTarget {
		#events = {
			open: null,
			error: null,
			message: null
		};
		#url;
		#withCredentials = false;
		/**
		* @type {ReadyState}
		*/
		#readyState = CONNECTING;
		#request = null;
		#controller = null;
		#dispatcher;
		/**
		* @type {import('./eventsource-stream').eventSourceSettings}
		*/
		#state;
		/**
		* Creates a new EventSource object.
		* @param {string} url
		* @param {EventSourceInit} [eventSourceInitDict={}]
		* @see https://html.spec.whatwg.org/multipage/server-sent-events.html#the-eventsource-interface
		*/
		constructor(url, eventSourceInitDict = {}) {
			super();
			webidl.util.markAsUncloneable(this);
			const prefix = "EventSource constructor";
			webidl.argumentLengthCheck(arguments, 1, prefix);
			if (!experimentalWarned) {
				experimentalWarned = true;
				process.emitWarning("EventSource is experimental, expect them to change at any time.", { code: "UNDICI-ES" });
			}
			url = webidl.converters.USVString(url);
			eventSourceInitDict = webidl.converters.EventSourceInitDict(eventSourceInitDict, prefix, "eventSourceInitDict");
			this.#dispatcher = eventSourceInitDict.node.dispatcher || eventSourceInitDict.dispatcher;
			this.#state = {
				lastEventId: "",
				reconnectionTime: eventSourceInitDict.node.reconnectionTime
			};
			const settings = environmentSettingsObject;
			let urlRecord;
			try {
				urlRecord = new URL(url, settings.settingsObject.baseUrl);
				this.#state.origin = urlRecord.origin;
			} catch (e) {
				throw new DOMException(e, "SyntaxError");
			}
			this.#url = urlRecord.href;
			let corsAttributeState = ANONYMOUS;
			if (eventSourceInitDict.withCredentials === true) {
				corsAttributeState = USE_CREDENTIALS;
				this.#withCredentials = true;
			}
			const initRequest = {
				redirect: "follow",
				keepalive: true,
				mode: "cors",
				credentials: corsAttributeState === "anonymous" ? "same-origin" : "omit",
				referrer: "no-referrer"
			};
			initRequest.client = environmentSettingsObject.settingsObject;
			initRequest.headersList = [["accept", {
				name: "accept",
				value: "text/event-stream"
			}]];
			initRequest.cache = "no-store";
			initRequest.initiator = "other";
			initRequest.urlList = [new URL(this.#url)];
			this.#request = makeRequest(initRequest);
			this.#connect();
		}
		/**
		* Returns the state of this EventSource object's connection. It can have the
		* values described below.
		* @returns {ReadyState}
		* @readonly
		*/
		get readyState() {
			return this.#readyState;
		}
		/**
		* Returns the URL providing the event stream.
		* @readonly
		* @returns {string}
		*/
		get url() {
			return this.#url;
		}
		/**
		* Returns a boolean indicating whether the EventSource object was
		* instantiated with CORS credentials set (true), or not (false, the default).
		*/
		get withCredentials() {
			return this.#withCredentials;
		}
		#connect() {
			if (this.#readyState === CLOSED) return;
			this.#readyState = CONNECTING;
			const fetchParams = {
				request: this.#request,
				dispatcher: this.#dispatcher
			};
			const processEventSourceEndOfBody = (response) => {
				if (!isNetworkError(response)) return this.#reconnect();
			};
			fetchParams.processResponseEndOfBody = processEventSourceEndOfBody;
			fetchParams.processResponse = (response) => {
				if (isNetworkError(response)) if (response.aborted) {
					this.close();
					this.dispatchEvent(new Event("error"));
					return;
				} else {
					this.#reconnect();
					return;
				}
				const contentType = response.headersList.get("content-type", true);
				const mimeType = contentType !== null ? parseMIMEType$1(contentType) : "failure";
				const contentTypeValid = mimeType !== "failure" && mimeType.essence === "text/event-stream";
				if (response.status !== 200 || contentTypeValid === false) {
					this.close();
					this.dispatchEvent(new Event("error"));
					return;
				}
				this.#readyState = OPEN;
				this.dispatchEvent(new Event("open"));
				this.#state.origin = response.urlList[response.urlList.length - 1].origin;
				const eventSourceStream = new EventSourceStream({
					eventSourceSettings: this.#state,
					push: (event) => {
						this.dispatchEvent(createFastMessageEvent(event.type, event.options));
					}
				});
				pipeline(response.body.stream, eventSourceStream, (error) => {
					if (error?.aborted === false) {
						this.close();
						this.dispatchEvent(new Event("error"));
					}
				});
			};
			this.#controller = fetching(fetchParams);
		}
		/**
		* @see https://html.spec.whatwg.org/multipage/server-sent-events.html#sse-processing-model
		* @returns {Promise<void>}
		*/
		async #reconnect() {
			if (this.#readyState === CLOSED) return;
			this.#readyState = CONNECTING;
			this.dispatchEvent(new Event("error"));
			await delay(this.#state.reconnectionTime);
			if (this.#readyState !== CONNECTING) return;
			if (this.#state.lastEventId.length) this.#request.headersList.set("last-event-id", this.#state.lastEventId, true);
			this.#connect();
		}
		/**
		* Closes the connection, if any, and sets the readyState attribute to
		* CLOSED.
		*/
		close() {
			webidl.brandCheck(this, EventSource$1);
			if (this.#readyState === CLOSED) return;
			this.#readyState = CLOSED;
			this.#controller.abort();
			this.#request = null;
		}
		get onopen() {
			return this.#events.open;
		}
		set onopen(fn) {
			if (this.#events.open) this.removeEventListener("open", this.#events.open);
			if (typeof fn === "function") {
				this.#events.open = fn;
				this.addEventListener("open", fn);
			} else this.#events.open = null;
		}
		get onmessage() {
			return this.#events.message;
		}
		set onmessage(fn) {
			if (this.#events.message) this.removeEventListener("message", this.#events.message);
			if (typeof fn === "function") {
				this.#events.message = fn;
				this.addEventListener("message", fn);
			} else this.#events.message = null;
		}
		get onerror() {
			return this.#events.error;
		}
		set onerror(fn) {
			if (this.#events.error) this.removeEventListener("error", this.#events.error);
			if (typeof fn === "function") {
				this.#events.error = fn;
				this.addEventListener("error", fn);
			} else this.#events.error = null;
		}
	};
	const constantsPropertyDescriptors = {
		CONNECTING: {
			__proto__: null,
			configurable: false,
			enumerable: true,
			value: CONNECTING,
			writable: false
		},
		OPEN: {
			__proto__: null,
			configurable: false,
			enumerable: true,
			value: OPEN,
			writable: false
		},
		CLOSED: {
			__proto__: null,
			configurable: false,
			enumerable: true,
			value: CLOSED,
			writable: false
		}
	};
	Object.defineProperties(EventSource$1, constantsPropertyDescriptors);
	Object.defineProperties(EventSource$1.prototype, constantsPropertyDescriptors);
	Object.defineProperties(EventSource$1.prototype, {
		close: kEnumerableProperty,
		onerror: kEnumerableProperty,
		onmessage: kEnumerableProperty,
		onopen: kEnumerableProperty,
		readyState: kEnumerableProperty,
		url: kEnumerableProperty,
		withCredentials: kEnumerableProperty
	});
	webidl.converters.EventSourceInitDict = webidl.dictionaryConverter([
		{
			key: "withCredentials",
			converter: webidl.converters.boolean,
			defaultValue: () => false
		},
		{
			key: "dispatcher",
			converter: webidl.converters.any
		},
		{
			key: "node",
			converter: webidl.dictionaryConverter([{
				key: "reconnectionTime",
				converter: webidl.converters["unsigned long"],
				defaultValue: () => defaultReconnectionTime
			}, {
				key: "dispatcher",
				converter: webidl.converters.any
			}]),
			defaultValue: () => ({})
		}
	]);
	module.exports = {
		EventSource: EventSource$1,
		defaultReconnectionTime
	};
}) });

//#endregion
//#region node_modules/undici/index.js
var require_undici = /* @__PURE__ */ __commonJS({ "node_modules/undici/index.js": ((exports, module) => {
	const Client = require_client();
	const Dispatcher = require_dispatcher();
	const Pool = require_pool();
	const BalancedPool = require_balanced_pool();
	const Agent = require_agent();
	const ProxyAgent = require_proxy_agent();
	const EnvHttpProxyAgent = require_env_http_proxy_agent();
	const RetryAgent = require_retry_agent();
	const H2CClient = require_h2c_client();
	const errors = require_errors();
	const util = require_util$5();
	const { InvalidArgumentError } = errors;
	const api = require_api();
	const buildConnector = require_connect();
	const MockClient = require_mock_client();
	const { MockCallHistory, MockCallHistoryLog } = require_mock_call_history();
	const MockAgent = require_mock_agent();
	const MockPool = require_mock_pool();
	const SnapshotAgent = require_snapshot_agent();
	const mockErrors = require_mock_errors();
	const RetryHandler = require_retry_handler();
	const { getGlobalDispatcher, setGlobalDispatcher } = require_global();
	const DecoratorHandler = require_decorator_handler();
	const RedirectHandler = require_redirect_handler();
	Object.assign(Dispatcher.prototype, api);
	module.exports.Dispatcher = Dispatcher;
	module.exports.Client = Client;
	module.exports.Pool = Pool;
	module.exports.BalancedPool = BalancedPool;
	module.exports.Agent = Agent;
	module.exports.ProxyAgent = ProxyAgent;
	module.exports.EnvHttpProxyAgent = EnvHttpProxyAgent;
	module.exports.RetryAgent = RetryAgent;
	module.exports.H2CClient = H2CClient;
	module.exports.RetryHandler = RetryHandler;
	module.exports.DecoratorHandler = DecoratorHandler;
	module.exports.RedirectHandler = RedirectHandler;
	module.exports.interceptors = {
		redirect: require_redirect(),
		responseError: require_response_error(),
		retry: require_retry(),
		dump: require_dump(),
		dns: require_dns(),
		cache: require_cache$1(),
		decompress: require_decompress()
	};
	module.exports.cacheStores = { MemoryCacheStore: require_memory_cache_store() };
	const SqliteCacheStore = require_sqlite_cache_store();
	module.exports.cacheStores.SqliteCacheStore = SqliteCacheStore;
	module.exports.buildConnector = buildConnector;
	module.exports.errors = errors;
	module.exports.util = {
		parseHeaders: util.parseHeaders,
		headerNameToString: util.headerNameToString
	};
	function makeDispatcher(fn) {
		return (url, opts, handler) => {
			if (typeof opts === "function") {
				handler = opts;
				opts = null;
			}
			if (!url || typeof url !== "string" && typeof url !== "object" && !(url instanceof URL)) throw new InvalidArgumentError("invalid url");
			if (opts != null && typeof opts !== "object") throw new InvalidArgumentError("invalid opts");
			if (opts && opts.path != null) {
				if (typeof opts.path !== "string") throw new InvalidArgumentError("invalid opts.path");
				let path$1 = opts.path;
				if (!opts.path.startsWith("/")) path$1 = `/${path$1}`;
				url = new URL(util.parseOrigin(url).origin + path$1);
			} else {
				if (!opts) opts = typeof url === "object" ? url : {};
				url = util.parseURL(url);
			}
			const { agent, dispatcher = getGlobalDispatcher() } = opts;
			if (agent) throw new InvalidArgumentError("unsupported opts.agent. Did you mean opts.client?");
			return fn.call(dispatcher, {
				...opts,
				origin: url.origin,
				path: url.search ? `${url.pathname}${url.search}` : url.pathname,
				method: opts.method || (opts.body ? "PUT" : "GET")
			}, handler);
		};
	}
	module.exports.setGlobalDispatcher = setGlobalDispatcher;
	module.exports.getGlobalDispatcher = getGlobalDispatcher;
	const fetchImpl = require_fetch().fetch;
	module.exports.fetch = async function fetch$1(init, options = void 0) {
		try {
			return await fetchImpl(init, options);
		} catch (err) {
			if (err && typeof err === "object") Error.captureStackTrace(err);
			throw err;
		}
	};
	module.exports.Headers = require_headers().Headers;
	module.exports.Response = require_response().Response;
	module.exports.Request = require_request().Request;
	module.exports.FormData = require_formdata().FormData;
	const { setGlobalOrigin, getGlobalOrigin } = require_global$1();
	module.exports.setGlobalOrigin = setGlobalOrigin;
	module.exports.getGlobalOrigin = getGlobalOrigin;
	const { CacheStorage } = require_cachestorage();
	const { kConstruct } = require_symbols();
	module.exports.caches = new CacheStorage(kConstruct);
	const { deleteCookie, getCookies, getSetCookies, setCookie, parseCookie } = require_cookies();
	module.exports.deleteCookie = deleteCookie;
	module.exports.getCookies = getCookies;
	module.exports.getSetCookies = getSetCookies;
	module.exports.setCookie = setCookie;
	module.exports.parseCookie = parseCookie;
	const { parseMIMEType, serializeAMimeType } = require_data_url();
	module.exports.parseMIMEType = parseMIMEType;
	module.exports.serializeAMimeType = serializeAMimeType;
	const { CloseEvent, ErrorEvent, MessageEvent } = require_events();
	const { WebSocket, ping } = require_websocket();
	module.exports.WebSocket = WebSocket;
	module.exports.CloseEvent = CloseEvent;
	module.exports.ErrorEvent = ErrorEvent;
	module.exports.MessageEvent = MessageEvent;
	module.exports.ping = ping;
	module.exports.WebSocketStream = require_websocketstream().WebSocketStream;
	module.exports.WebSocketError = require_websocketerror().WebSocketError;
	module.exports.request = makeDispatcher(api.request);
	module.exports.stream = makeDispatcher(api.stream);
	module.exports.pipeline = makeDispatcher(api.pipeline);
	module.exports.connect = makeDispatcher(api.connect);
	module.exports.upgrade = makeDispatcher(api.upgrade);
	module.exports.MockClient = MockClient;
	module.exports.MockCallHistory = MockCallHistory;
	module.exports.MockCallHistoryLog = MockCallHistoryLog;
	module.exports.MockPool = MockPool;
	module.exports.MockAgent = MockAgent;
	module.exports.SnapshotAgent = SnapshotAgent;
	module.exports.mockErrors = mockErrors;
	const { EventSource } = require_eventsource();
	module.exports.EventSource = EventSource;
	function install() {
		globalThis.fetch = module.exports.fetch;
		globalThis.Headers = module.exports.Headers;
		globalThis.Response = module.exports.Response;
		globalThis.Request = module.exports.Request;
		globalThis.FormData = module.exports.FormData;
		globalThis.WebSocket = module.exports.WebSocket;
		globalThis.CloseEvent = module.exports.CloseEvent;
		globalThis.ErrorEvent = module.exports.ErrorEvent;
		globalThis.MessageEvent = module.exports.MessageEvent;
		globalThis.EventSource = module.exports.EventSource;
	}
	module.exports.install = install;
}) });

//#endregion
//#region src/proxy.ts
var import_undici = /* @__PURE__ */ __toESM(require_undici(), 1);
const execPromise = promisify(exec);
const root = "/tmp/nix-cache";
const hostname = "127.0.0.1";
const port = 5001;
const mimeTypes = {
	".nar": "application/x-nix-nar",
	".nar.xz": "application/x-xz",
	".nar.zst": "application/zstd",
	".narinfo": "application/x-nix-narinfo"
};
let substituters = [];
const server = createServer(async (req, res) => {
	try {
		if (!req.url) return;
		switch (req.method) {
			case "HEAD": {
				const localPath = path.join(root, req.url);
				if (existsSync(localPath)) {
					console.log("✓", localPath);
					res.writeHead(200);
					res.end();
					return;
				}
				for (const substituter of substituters) {
					const substituterURL = new URL(req.url, substituter);
					delete req.headers.host;
					delete req.headers.referer;
					const { statusCode, headers } = await (0, import_undici.request)(substituterURL, {
						method: "HEAD",
						headers: req.headers
					});
					if (statusCode >= 300) continue;
					console.log("✓", substituterURL.href);
					res.writeHead(statusCode, headers);
					res.end();
					return;
				}
				console.log("x", req.url);
				res.writeHead(404, { "Content-Type": "text/plain" });
				res.end("not found");
				return;
			}
			case "GET": {
				const localPath = path.join(root, req.url);
				if (existsSync(localPath)) {
					console.log("<-", localPath);
					const ext = path.parse(localPath).ext;
					const contentType = mimeTypes[ext] || "application/octet-stream";
					res.writeHead(200, {
						"Content-Type": contentType,
						"Content-Disposition": `attachment; filename="${path.basename(localPath)}"`
					});
					const fileStream = createReadStream(localPath);
					fileStream.on("error", (err) => {
						console.error("error streaming file:", err);
						res.end("error streaming file");
					});
					fileStream.pipe(res);
					return;
				}
				for (const substituter of substituters) {
					const substituterURL = new URL(req.url, substituter);
					delete req.headers.host;
					delete req.headers.referer;
					const { statusCode, headers, body } = await (0, import_undici.request)(substituterURL, {
						method: "GET",
						headers: req.headers
					});
					if (statusCode >= 300) continue;
					console.log("<-", substituterURL.href);
					res.writeHead(statusCode, headers);
					body.pipe(res, { end: true });
					return;
				}
				console.log("x", req.url);
				res.writeHead(404, { "Content-Type": "text/plain" });
				res.end("not found");
				return;
			}
			case "PUT": {
				const localPath = path.join(root, req.url);
				const dir = path.dirname(localPath);
				if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
				console.log("->", localPath);
				const fileStream = createWriteStream(localPath, {
					flags: "w+",
					encoding: "binary"
				});
				fileStream.on("finish", () => {
					res.writeHead(201, { "Content-Type": "text/plain" });
					res.end("created");
				});
				req.pipe(fileStream);
				return;
			}
			case "POST":
				if (req.url !== "/substituters") {
					res.writeHead(404, { "Content-Type": "text/plain" });
					res.end("not found");
					return;
				}
				substituters = (await execPromise("nix config show substituters")).stdout.split(" ").map((s) => s.trim());
				console.log("substituters:", substituters.join(", "));
				res.writeHead(200, { "Content-Type": "application/json" });
				res.end(JSON.stringify(substituters));
				return;
		}
	} catch (err) {
		console.error("error handling request:", err);
		res.writeHead(500, { "Content-Type": "text/plain" });
		res.end("internal server error");
	}
});
console.log(`starting server at http://${hostname}:${port}`);
server.listen(port, hostname);

//#endregion
export {  };