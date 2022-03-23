/**
 * Created by duc on 7/12/17.
 */


/**
 * Retry with Promise
 * @param {function} fn - the function to try, should return a Promise
 * @param {int} retry - the number of retries
 * @param {number} interval - the interval in millisecs between retries
 * @param {Array} fnParams - list of params to pass to the function
 * @returns {Promise} - A promise that resolves to the final result
 */
async function retryMax(fn, retry, interval, fnParams){
    return fn.apply(this,fnParams).catch( err => {
        return (retry>1? Promise.wait(interval).then(()=> Promise.retryMax(fn,retry-1,interval, fnParams)):new Error(err));
    });
}

Promise.retryMax = function(fn,retry,interval,fnParams) {
    return fn.apply(this,fnParams).catch( err => {
        return (retry>1? Promise.wait(interval).then(()=> Promise.retryMax(fn,retry-1,interval, fnParams)):Promise.reject(err));
    });
};

/**
 * Promise to run after some delay
 * @param {number} delay - delay in millisecs
 * @returns {Promise}
 */
async function wait(delay){
    let promise = new Promise((fulfill, reject) => {
        setTimeout(() => fulfill, delay);
      });
    await promise;
}

Promise.wait = function(delay) {
    return new Promise((fulfill,reject)=> {
        //console.log(Date.now());
        setTimeout(fulfill,delay||0);
    });
};

/**
 * Retry until timeout
 * @param {function} fn - function to be called, should return a promise
 * @param {number} timeLimit - time limit in millisecs
 * @param {number} interval - interval between function calls, in millisecs
 * @param {Array} fnParams - array of parameters to be passed to the function
 * @returns {Promise} - A promise that resolves to the final result
 */
Promise.retryTillTimeout = function(fn, timeLimit,interval,fnParams) {
    var startTime = Date.now();
    function mainLoop()  {
        return fn.apply(this,fnParams).catch(err => {
            return (Date.now()-startTime <= timeLimit)? Promise.wait(interval).then(() => {return mainLoop();}) : Promise.reject(err);
        });
    }
    return mainLoop();
};

export const p_retryMax = Promise.retryMax;
export const p_wait = Promise.wait;
export const p_retryTillTimeout = Promise.retryTillTimeout;