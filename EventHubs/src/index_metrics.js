
///////////////////////////////////////////////////////////////////////////////////
//           Function to read from an Azure EventHubs to SumoLogic               //
///////////////////////////////////////////////////////////////////////////////////

import { SumoMetricClient } from './sumometricclient';
import { Transformer } from './datatransformer';
var sumoClient;

function isMetricData(msg) {
    //if ((msg['metricName']) && (msg['time']) && ((msg['count']) || (msg['total']) || (msg['average'] in msg) ||(msg['maximum']) || (msg['minimum']) )) return true; else return false;
    return true;
}

/**
 * Function to select a metric message or not.
 * @param metric the metric to be decided
 * @returns {boolean} true if metric is selected, false if not
 */
function selectMetric(metric) {
    //if (['INREQS'].indexOf(metric['metricName'])!=-1) return true; else return false;
    return true;
}

/**
 * Return an array of statistics to be used for a metric
 * @param msg
 * @returns An array of statistic methods to be formed
 */
function selectStatsForMetric(msg) {
    return ["count","total","average","maximum","minimum"]

}

export default function (context, eventHubMessages) {
    //var options ={ 'urlString':process.env.APPSETTING_SumoSelfEventHubBadEndpoint,'metadata':{}, 'MaxAttempts':3, 'RetryInterval':3000,'compress_data':true};
    var options ={ 'urlString':process.env.APPSETTING_SumoLabsMetricEndpoint,'metadata':{}, 'MaxAttempts':3, 'RetryInterval':3000,'compress_data':true, 'metric_type':'carbon20'};

    sumoMetricClient = new SumoMetricClient(options,context,failureHandler,successHandler);
    var transformer = new Transformer();
    var messageArray = transformer.azureAudit(eventHubMessages);
    var azureMetricArray = [];
    var logRawArray = [];

    messageArray.forEach( msg => {
        if (isMetricData(msg)) {
            if (selectMetric(msg)) azureMetricArray.push(msg);
        } else {
            context.log("Not metric data, will ignore");
            //logRawArray.push(msg);
        }
    });

    // generate metric array from the raw Azure metric data
    var metricObjectArray = transformer.generateMetricObjectsFromAzureRawData(azureMetricArray,selectStatsForMetric,'carbon20');
    sumoMetricClient.addData(metricObjectArray);


    context.log("Sending: " + metricObjectArray.length);


    // handlers for success and failures
    function failureHandler(msgArray,ctx) {
        ctx.log("Failed to send metrics to Sumo");
        if (sumoMetricClient.messagesAttempted === sumoMetricClient.messagesReceived) {
            context.bindings.outputBlob = messageArray.map(function(x) { return JSON.stringify(x);}).join("\n");
            context.done();
        }
    }
    function successHandler(ctx) {
        ctx.log('Successfully sent chunk to Sumo');
        if (sumoMetricClient.messagesAttempted === sumoMetricClient.messagesReceived) {
            ctx.log('Sent all metric data to Sumo. Exit now.');
            context.done();
        }
    }

    context.log("Flushing the rest of the buffers:");
    sumoMetricClient.flushAll();
};
