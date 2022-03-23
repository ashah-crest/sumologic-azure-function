/*jshint esversion: 6 */
/**
 * Created by duc on 6/30/17. This is a client for metric
 */
import { request } from 'https';
import { gzip } from 'zlib';
import url from "url";

import { SumoClient, FlushFailureHandler, DefaultSuccessHandler } from './sumoclient.js';
import { MessageBucket } from './messagebucket.js';
import { p_retryMax } from './sumoutils.js';

/**
 * Class to receive metrics to a designated Sumo endpoint. Similar to the Log client is best used independently with a batch of messages so one can track the number
 * of messages sent successfully to Sumo and develop their own failure handling for those failed to be sent (out of this batch). It is of course
 * totally fine to use a single client for multiple batches for a best effort delivery option.
 * @param options contains information needed for the client including: the endpoint (via the parameter urlString),  max number of retries, a generateBucketKey function (optional)
 * @param context must support method "log".
 * @param flush_failure_callback is a callback function used to handle failures (after all attempts)
 * @param success_callback is a callback function when each batch is sent successfully to Sumo. It should contain some logic to determine all data sent to the client
 * has been attempted to sent to Sumo (either successfully or over max retries).
 * @constructor
 */
export class SumoMetricClient extends SumoClient {
    constructor(options, context, flush_failure_callback, success_callback) {
        this.metric_type_map = { 'GRAPHITE': 'graphite', 'CARBON20': 'carbon20' };
        super(options, context, flush_failure_callback, success_callback);
        if ('metric_type' in options) {
            this.metric_type = options.metric_type;
        } else {
            // default is graphite
            this.metric_type = this.metric_type_map.GRAPHITE;
        }
    }
    /**
     * Default method to generate a headersObj object for the bucket
     * @param message input message
     * @return a header object
     */
    generateHeaders(message, delete_metadata) {
        let headerObj = super.generateHeaders(message, delete_metadata);
        let metricDimensions = (this.options.metadata) ? (this.options.metadata.metricdimension || '') : '';
        let metricMetadata = (this.options.metadata) ? (this.options.metadata.metricmetadata || '') : '';
        headerObj["X-Sumo-Client"] = "eventhubmetrics-azure-function";
        if (metricDimensions != '') {
            headerObj['X-Sumo-Dimensions'] = metricDimensions;
        }
        if (metricMetadata != '') {
            headerObj['X-Sumo-Metadata'] = metricMetadata;
        }

        if (this.options.metric_type == this.metric_type_map.CARBON20) {
            headerObj['Content-Type'] = 'application/vnd.sumologic.carbon2';
        } else {
            headerObj['Content-Type'] = 'application/vnd.sumologic.graphite';
        }

        return headerObj;
    }
    /**
     * For metric data, we need to extract the final string out and submit the message as text so it can be sent in the right metric format
     * @param data
     */
    addData(data) {
        var self = this;

        if (data instanceof Array) {
            data.forEach((item, index, array) => {
                self.messagesReceived += 1;
                submitMessage(item);
            });
        } else {
            self.messagesReceived += 1;
            submitMessage(data);
        }
    }
    submitMessage(message) {
        let metaKey = this.generateLogBucketKey(message);
        if (!this.dataMap.has(metaKey)) {
            this.dataMap.set(metaKey, new MessageBucket(this.generateHeaders(message)));
        }
        this.dataMap.get(metaKey).add(message.metric_string ? message.metric_string : message);
    }
    /**
     * Flush a whole message bucket to sumo, compress data if needed and with up to MaxAttempts
     * @param {string} metaKey - key to identify the buffer from the internal map
     */
    flushBucketToSumo(metaKey) {
        let targetBuffer = this.dataMap.get(metaKey);
        var self = this;
        let curOptions = Object.assign({}, this.options);

        this.context.log("Flush METRIC buffer for metaKey:" + metaKey);

        function httpSend(messageArray, data) {
            return new Promise((resolve, reject) => {
                var req = request(curOptions, function (res) {
                    var body = '';
                    res.setEncoding('utf8');
                    res.on('data', function (chunk) {
                        body += chunk; // don't really do anything with body
                    });
                    res.on('end', function () {
                        console.log('Got response:' + res.statusCode);
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
}