/**
 * Created by duc on 6/30/17.
 */
import { SumoClient, FlushFailureHandler, Transformer } from '../lib/mainindex.js';
import { expect as _expect, should } from 'chai';
var expect = _expect;
should();

describe('SumoClientTest',function () {
    let sumoEndpoint = process.env.SUMO_ENDPOINT;
    let sumoBadEndpoint = "https://endpoint1.collection.sumologic.com/receiver/v1/http/ZaVnC4dhaV3uTKlva1XrWGAjyNWn7iC07DdLRLiMM05gblqbRUJF_fdTL1Gqq9nstr_rMABh-Tq4b7-Jg8VKCZF8skUe1rFJTnsXAATTbTkjayU_D1cx8A==";
    let testMessageCount = 10;
    let testInputMessages=[];
    let testAzureInputMessages=[];
    let testBlobCount = 3;
    this.timeout(30000);

    beforeEach(function () {
        testInputMessages =[];
        testAzureInputMessages =[];
        for (let j = 0; j<testBlobCount; j++ ) {
            let tmp_buff = [];
            for (let i = 0; i < testMessageCount; i++) {
                let sourceCat = Math.ceil(Math.random() * 10);
                let sourceName = sourceCat +1;
                let sourceHost = sourceName+1 ;
                tmp_buff.push({_sumo_metadata: {category: sourceCat,sourceName:sourceName,sourceHost:sourceHost}, value: i});
                testInputMessages.push({_sumo_metadata: {category: sourceCat,sourceName:sourceName,sourceHost:sourceHost}, value: i});
            }
            testAzureInputMessages.push({records:tmp_buff});
        }
    });

    it('it should send raw data to Sumo', function (done) {
        // test failure
        let options = {
            urlString: sumoEndpoint,
            metadata: {},
            MaxAttempts: 3,
            RetryInterval: 3000,
            compress_data: false
        };

        let sumoClient = new SumoClient(options, console, FlushFailureHandler,validate);
        sumoClient.addData(testInputMessages);
        sumoClient.flushAll();

        function validate() {
            if (sumoClient.messagesAttempted  == testMessageCount*testBlobCount) {
                // data is ready
                expect(sumoClient.messagesFailed).to.equal(0);
                expect(sumoClient.messagesSent).to.equal(testMessageCount*testBlobCount);
                done();
            }
        }
    });

    it('it should send compressed data to Sumo', function (done) {
        // test failure
        let options = {
            urlString: sumoEndpoint,
            metadata: {},
            MaxAttempts: 3,
            RetryInterval: 3000,
            compress_data: true
        };
        let sumoClient = new SumoClient(options, console, FlushFailureHandler,validate);
        sumoClient.addData(testInputMessages);
        sumoClient.flushAll();

        function validate() {
            if (sumoClient.messagesAttempted === testMessageCount*testBlobCount) {
                // data is ready
                expect(sumoClient.messagesFailed).to.equal(0);
                expect(sumoClient.messagesSent).to.equal(testMessageCount*testBlobCount);
                done();
            }
        }
    });

    it('it should fail to send data to a bad Sumo endpoint', function (done) {
        // test failure
        let options = {
            urlString: sumoBadEndpoint,
            metadata: {},
            BufferCapacity: 10,
            MaxAttempts: 3,
            RetryInterval: 3000,
            compress_data: true
        };

        let sumoClient = new SumoClient(options, console, validate,validate);
        for (let i = 0; i < testMessageCount; i++) {
            let sourceCat = Math.ceil(Math.random() * 10);
            sumoClient.addData({_sumo_metadata: {category: sourceCat}, value: i});
        }
        sumoClient.flushAll();

        function validate() {
            console.log("Failed to send data");
            if (sumoClient.messagesAttempted === testMessageCount) {
                expect(sumoClient.messagesSent).to.equal(0);
                expect(sumoClient.messagesFailed).to.equal(testMessageCount);
                done();
            }
        }
    });


    it('Internal timer should work with Azure Function simulation', function (done) {

        let context = {log:console.log,done:done};
        let options ={ urlString:sumoEndpoint,metadata:{}, MaxAttempts:3, RetryInterval:3000,compress_data:true,timerinterval:1000};

        context.log(`JavaScript eventhub trigger function called for message array ${testInputMessages}`);

        let sumoClient = new SumoClient(options,context,failureHandler,successHandler);
        let transformer = new Transformer();
        sumoClient.addData(transformer.azureAudit(testAzureInputMessages));

        // we don't call sumoClient.flushAll() here

        function failureHandler(messageArray,ctx) {
            ctx.log("Failed to send to Sumo");

            ctx.log("Attempted: "+sumoClient.messagesAttempted+ ", Out of total:"+messageArray.length);
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) context.done();
        }
        function successHandler(ctx) {
            ctx.log('Successfully sent to Sumo');
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
                context.done();
            }
        }

    });

    it('Azure Function simulation should work for an array input', function (done) {
        // we simulate an Azure function via the use of a context variable. This test passes if it doesn't time out
        let context = {log:console.log,done:done};

        let options ={ urlString:sumoEndpoint,metadata:{}, MaxAttempts:3, RetryInterval:3000,compress_data:true};

        context.log(`JavaScript eventhub trigger function called for message array ${testInputMessages}`);

        let sumoClient = new SumoClient(options,context,failureHandler,successHandler);
        let transformer = new Transformer();
        sumoClient.addData(transformer.azureAudit(testAzureInputMessages));

        function failureHandler(messageArray,ctx) {
            ctx.log("Failed to send to Sumo");
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) context.done();
        }
        function successHandler(ctx) {
            ctx.log('Successfully sent to Sumo');
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
                context.done();
            }
        }
        context.log("Flushing the rest of the buffers:");
        sumoClient.flushAll();

    });

    it('Azure Function simulation should work for a single message', function (done) {

        // we simulate an Azure function via the use of a context variable. This test passes if it doesn't time out
        let context = {log:console.log,done:done};

        let singleInputMessage = testAzureInputMessages[0];

        let options ={ urlString:sumoEndpoint,metadata:{}, MaxAttempts:3, RetryInterval:3000,compress_data:true};

        let sumoClient = new SumoClient(options,context,failureHandler,successHandler);
        let transformer = new Transformer();
        sumoClient.addData(transformer.azureAudit(singleInputMessage));

        function failureHandler(messageArray,ctx) {
            ctx.log("Failed to send to Sumo");
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) context.done();
        }
        function successHandler(ctx) {
            ctx.log('Successfully sent to Sumo');
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
                context.done();
            }
        }
        context.log("Flushing the rest of the buffers:");
        sumoClient.flushAll();

    });


    it('Azure Function simulating failing to send to Sumo should work', function (done) {

        // we simulate an Azure function via the use of a context variable. This test passes if it doesn't time out
        let context = {log:console.log,done:done};

        let singleInputMessage = testAzureInputMessages[0];

        let options ={ urlString:sumoBadEndpoint,metadata:{}, MaxAttempts:3, RetryInterval:3000,compress_data:true};

        let sumoClient = new SumoClient(options,context,failureHandler,successHandler);
        let transformer = new Transformer();
        sumoClient.addData(transformer.azureAudit(singleInputMessage));

        function failureHandler(messageArray,ctx) {
            ctx.log("Failed to send to Sumo");
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) context.done();
        }
        function successHandler(ctx) {
            ctx.log('Successfully sent to Sumo');
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
                context.done();
            }
        }
        context.log("Flushing the rest of the buffers:");
        sumoClient.flushAll();

    });

});


