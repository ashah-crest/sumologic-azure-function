/*jshint esversion: 6 */
/**
 * Created by duc on 6/30/17.
 */
import { request } from 'https';
import { gzip } from 'zlib';
import { parse } from 'url';

import { MessageBucket } from './messagebucket.js';
import { p_retryMax } from './sumoutils.js';

var metadataMap = {
      category: "X-Sumo-Category",
      sourceName: "X-Sumo-Name",
      sourceHost: "X-Sumo-Host",
};

/**
 * Class to receive messages and send to a designated Sumo endpoint. Each client is best used independently with a batch of messages so one can track the number
 * of messages sent successfully to Sumo and develop their own failure handling for those failed to be sent (out of this batch). It is of course
 * totally fine to use a single client for multiple batches for a best effort delivery option.
 * @param options contains information needed for the client including: the endpoint (via the parameter urlString),  max number of retries, a generateBucketKey function (optional)
 * @param context must support method "log".
 * @param flush_failure_callback is a callback function used to handle failures (after all attempts)
 * @param success_callback is a callback function when each batch is sent successfully to Sumo. It should contain some logic to determine all data sent to the client
 * has been attempted to sent to Sumo (either successfully or over max retries).
 * @constructor
 */
export class SumoClient {
    constructor(options, context, flush_failure_callback, success_callback) {
        let myOptions = options || {};
        if (myOptions.urlString) {
            let urlObj = parse(options.urlString);
            myOptions.hostname = urlObj.hostname;
            myOptions.path = urlObj.pathname;
            myOptions.protocol = urlObj.protocol;
        }
        myOptions.method = 'POST';
        this.options = myOptions;
        if (this.options.compress_data == undefined) {
            this.options.compress_data = true;
        }
        // use messagesSent, messagesAttempted and messagedFailed below to keep track of the final delivery status for the overall message batch
        this.messagesReceived = 0;
        this.messagesSent = 0;
        this.messagesAttempted = 0;
        this.messagesFailed = 0;
        this.dataMap = new Map();
        this.context = context || console;
        this.generateBucketKey = options.generateBucketKey || this.generateLogBucketKey;
        this.MaxAttempts = this.options.MaxAttempts || 3;
        this.RetryInterval = this.options.RetryInterval || 3000; // 3 secs
        this.failure_callback = flush_failure_callback;
        this.success_callback = success_callback;
        this._timerID = null;
        this._timerInterval = null;
        if (this.options.timerinterval) {
            this.enableTimer(this.options.timerinterval);
        }
    }
    enableTimer(interval) {
        if (Number(interval) > 0) {
            this.disableTimer();
            this._timerInterval = Number(interval);
            let self = this;
            this._timerID = setInterval(() => {
                self.flushAll();
            }, self._timerInterval);
        }
    }
    disableTimer() {
        if (this._timerID) {
            clearInterval(this._timerID);
            this._timerID = null;
            this._timerInterval = 0;
        }
    }
    /**
     * Default method to generate a headersObj object for the bucket
     * @param message input message
     */
    generateHeaders(message, delete_metadata) {
        let sourceCategory = (this.options.metadata) ? (this.options.metadata.category || '') : '';
        let sourceName = (this.options.metadata) ? (this.options.metadata.name || '') : '';
        let sourceHost = (this.options.metadata) ? (this.options.metadata.host || '') : '';
        let headerObj = {
            "X-Sumo-Name": sourceName,
            "X-Sumo-Category": sourceCategory,
            "X-Sumo-Host": sourceHost,
            "X-Sumo-Client": this.options.clientHeader || "eventhublogs-azure-function",
        };
        if (message.hasOwnProperty('_sumo_metadata')) {
            let metadataOverride = message._sumo_metadata;
            Object.getOwnPropertyNames(metadataOverride).forEach(property => {
                if (metadataMap[property]) {
                    this.targetProperty = metadataMap[property];
                } else { 
                    this.targetProperty = property; 
                }
                headerObj[this.targetProperty] = metadataOverride[property];
            });
            if (typeof delete_metadata === 'undefined' || delete_metadata) {
                delete message._sumo_metadata;
            }
        }
        return headerObj;
    }
    /**
     * Default method to generate the bucket key for the input message. For log messages, we'll use 3 metadata fields as the key
     * @param message input message
     * @return: a string used as the key for the bucket map
     */
    generateLogBucketKey(message) {
        return JSON.stringify(this.generateHeaders(message, false));
    }
    emptyBufferToSumo(metaKey) {
        let targetBuffer = this.dataMap.get(metaKey);
        if (targetBuffer) {
            let message;
            while ((message = targetBuffer.pop())) {
                this.context.log(metaKey + '=' + JSON.stringify(message));
            }
        }
    }
    /**
     * Flush a whole message bucket to sumo, compress data if needed and with up to MaxAttempts
     * @param {string} metaKey - key to identify the buffer from the internal map
     */
    flushBucketToSumo(metaKey) {
        let targetBuffer = this.dataMap.get(metaKey);
        var self = this;
        let curOptions = Object.assign({}, this.options);

        this.context.log("Flush buffer for metaKey:" + metaKey);

        function httpSend(messageArray, data) {
            return new Promise((resolve, reject) => {
                var req = request(curOptions, function (res) {
                    var body = '';
                    res.setEncoding('utf8');
                    res.on('data', function (chunk) {
                        body += chunk; // don't really do anything with body
                    });
                    res.on('end', function () {
                        if (res.statusCode == 200) {
                            self.messagesSent += messageArray.length;
                            self.messagesAttempted += messageArray.length;
                            resolve(body);
                            // TODO: anything here?
                        } else {
                            reject({ error: null, res: res });
                        }
                        // TODO: finalizeContext();
                    });
                });

                req.on('error', function (e) {
                    reject({ error: e, res: null });
                    // TODO: finalizeContext();
                });
                req.write(data);
                req.end();
            });
        }

        if (targetBuffer) {
            curOptions.headers = targetBuffer.getHeadersObject();
            let msgArray = [];
            let message;
            while (targetBuffer.getSize() > 0) {
                message = targetBuffer.remove();
                if (message instanceof Object) {
                    msgArray.push(JSON.stringify(message));
                } else {
                    msgArray.push(message);
                }
            }

            if (curOptions.compress_data) {
                curOptions.headers['Content-Encoding'] = 'gzip';

                gzip(msgArray.join('\n'), function (e, compressed_data) {
                    if (!e) {
                        p_retryMax(httpSend, self.MaxAttempts, self.RetryInterval, [msgArray, compressed_data])
                            .then(() => {
                                //self.context.log("Succesfully sent to Sumo after "+self.MaxAttempts);
                                self.success_callback(self.context);
                            })
                            .catch(() => {
                                //self.context.log("Uh oh, failed to send to Sumo after "+self.MaxAttempts);
                                self.messagesFailed += msgArray.length;
                                self.messagesAttempted += msgArray.length;
                                self.failure_callback(msgArray, self.context);
                            });
                    } else {
                        self.messagesFailed += msgArray.length;
                        self.messagesAttempted += msgArray.length;
                        self.failure_callback(msgArray, self.context);
                    }
                });
            } else {
                //self.context.log('Send raw data to Sumo');
                p_retryMax(httpSend, self.MaxAttempts, self.RetryInterval, [msgArray, msgArray.join('\n')])
                    .then(() => { self.success_callback(self.context); })
                    .catch(() => {
                        self.messagesFailed += msgArray.length;
                        self.messagesAttempted += msgArray.length;
                        self.failure_callback(msgArray, self.context);
                    });
            }
        }
    }
    /**
     * Flush all internal buckets to Sumo
     */
    flushAll() {
        var self = this;
        this.dataMap.forEach((buffer, key, dataMap) => {
            self.flushBucketToSumo(key);
        });
    }
    addData(data) {
        var self = this;

        if (data instanceof Array) {
            data.forEach((item, index, array) => {
                self.messagesReceived += 1;
                self.submitMessage(item);
            });
        } else {
            self.messagesReceived += 1;
            self.submitMessage(data);
        }
    }
    submitMessage(message) {
        let metaKey = this.generateLogBucketKey(message);
        if (!this.dataMap.has(metaKey)) {
            this.dataMap.set(metaKey, new MessageBucket(this.generateHeaders(message)));
        }
        this.dataMap.get(metaKey).add(message);
    }
}


/**
 * Default built-in callback function to handle failures. It simply logs data.
 * @param messageArray is all data failed to be sent to Sumo
 * @param ctx is the context variable that supports a log method.
 * @constructor
 */
export function FlushFailureHandler (messageArray,ctx) {
    if (ctx) {ctx.log("Just log data locally");}
    if (messageArray instanceof Array) ctx.log(messageArray.join('\n')); else ctx.log(messageArray) ;
}

/**
 * Default built-in callback function to handle successful sents. It simply logs a success message
 * @param ctx is a context variable that supports a log method
 * @constructor
 */
export function DefaultSuccessHandler(ctx) {
    ctx.log("Sent to Sumo successfully") ;
}

// export const SumoClient = SumoClient;
// export const FlushFailureHandler = FlushFailureHandler;
// export const DefaultSuccessHandler = DefaultSuccessHandler;

