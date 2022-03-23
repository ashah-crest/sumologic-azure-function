///////////////////////////////////////////////////////////////////////////////////
//           Function to read from an Azure EventHubs to SumoLogic               //
///////////////////////////////////////////////////////////////////////////////////

import { SumoClient } from './sumoclient';
import { Transformer } from './datatransformer';
var sumoClient;

function setSourceCategory(context, msg) {
    // msg._sumo_metadata = {
    //     "category": "new_source_category"
    // }
}

export default function (context, eventHubMessages) {
    //var options ={ 'urlString':process.env.APPSETTING_SumoSelfEventHubBadEndpoint,'metadata':{}, 'MaxAttempts':3, 'RetryInterval':3000,'compress_data':true};
    var options ={ 'urlString':process.env.APPSETTING_SumoLogsEndpoint,'metadata':{}, 'MaxAttempts':3, 'RetryInterval':3000,'compress_data':true};

    sumoClient = new SumoClient(options,context,failureHandler,successHandler);
    var transformer = new Transformer();
    var messageArray = transformer.azureAudit(eventHubMessages);
    messageArray.forEach( msg => {
        setSourceCategory(context, msg);
        sumoClient.addData(msg);
    });
    context.log("Sending: " + messageArray.length);

    function failureHandler(msgArray,ctx) {
        ctx.log("Failed to send to Sumo, backup to storageaccount now");
        if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
            context.bindings.outputBlob = messageArray.map(function(x) { return JSON.stringify(x);}).join("\n");
            context.done();
        }
    }
    function successHandler(ctx) {
        ctx.log('Successfully sent chunk to Sumo');
        if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
            ctx.log('Sent all data to Sumo. Exit now.');
            context.done();
        }
    }
    context.log("Flushing the rest of the buffers:");
    sumoClient.flushAll();

};

