/**
 * Created by duc on 6/30/17.
 */

import { expect as _expect, should } from 'chai';
var expect = _expect;
import { p_retryMax, p_retryTillTimeout } from '../lib/sumoutils.js';
should();

describe('PromiseTest',function () {

    this.timeout(5000);

    beforeEach(function () {
    });

    it('Retry should work when task fails', function (done) {
        let actualRetry = 0;

        function genTask() {
            return new Promise((fulfill, reject) => {
                actualRetry++;
                console.log("Actual Retry:"+actualRetry);
                reject({message: "fail by design"});
            });
        }

        p_retryMax(genTask, 5, 100).then(() => {
            expect(true).to.equal(false);
        }).catch((err) => {
            console.log("Task failed as expected:" + JSON.stringify(err));
            expect(actualRetry).to.equal(5);
        }).then(() => {done(); });
    });

    it('Retry should work when task succeed', function (done) {
        let actualRetry = 0;

        function genTask() {
            return new Promise((fulfill, reject) => {
                actualRetry++;
                if (actualRetry == 3) fulfill({message: "succeed by design"});
                else reject({message: 'failed: ' + actualRetry});
            });
        }

        p_retryMax(genTask, 5, 100).then(() => {console.log("Succeeded as expected!");
            expect(actualRetry).to.lessThan(5);
        }).catch((err) => {
            console.log('Caught a failure, unexpected!' + JSON.stringify(err));
            expect(false).to.equal(true);
        }).then(() => {done();});
    });


    it('RetryTillTimeout should work when task fails', function (done) {
        let startTime = Date.now();

        function genTask() {
            return new Promise((fulfill, reject) => {
                console.log("Testing function called");
                reject({message:"fail by design"});
            });
        }

        p_retryTillTimeout(genTask, 1000, 100).then(() => {
            expect(true).to.equal(false);
        }).catch((err) => {
            console.log("Task failed as expected:" + JSON.stringify(err));
            expect(Date.now() - startTime).to.greaterThan(1000);
        }).then(() => {done();});
    });

    it('RetryTillTimeout should work when task succeed in time', function (done) {
        let startTime = Date.now();

        function genTask() {
            return new Promise((fulfill, reject) => {
                if (Date.now() - startTime < 500 ) {
                    reject({message: "fail by design"});
                } else fulfill();
            });
        }

        p_retryTillTimeout(genTask, 1000, 100).then(() => {
            console.log("Succeeded after: "+(Date.now()-startTime) + " msecs");
            expect(Date.now() - startTime).to.lessThan(1000);
        }).catch((err) => {
            console.log("Task failed with error:" + JSON.stringify(err));
            expect(true).to.equal(false);
        }).then(()=> {done();});
    });
});


