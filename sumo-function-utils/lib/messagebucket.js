/*jshint esversion: 6 */
/**
 * Created by duc on 6/29/17.
 * Class to contain a set of messages that will be sent to Sumo with the same set of headersObj. Headers should be
 * @param headers object contains all the headersObj
 **/

export class MessageBucket {
    constructor(headers) {
        this.headersObj = headers;
        // core queue to store elements
        this.queue = new Array(0);
    }
    getSize() {
        return this.queue.length;
    }
    getHeadersObject() {
        return this.headersObj;
    }
    setHeadersObject(headers) {
        this.headersObj = headers;
    }
    /**
     * Evic and return the first element if buffer is not empty, return null otherwise.
     * @returns {*}
     */
    remove() {
        if (this.queue.length > 0) {
            let element = this.queue.shift();
            return element;
        } else {
            return null;
        }
    }
    // add an element to this bucket
    add(elm) {
        this.queue.push(elm);
    }
}
