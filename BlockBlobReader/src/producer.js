///////////////////////////////////////////////////////////////////////////////////
//           Function to create tasks using EventGrid Events into Azure EventHubs               //
///////////////////////////////////////////////////////////////////////////////////

import { TableServiceClient } from '@azure/data-tables';
var tableService = TableServiceClient.fromConnectionString(process.env.APPSETTING_AzureWebJobsStorage);

function getRowKey(metadata) {
    let storageName =  metadata.url.split("//").pop().split(".")[0];
    let arr = metadata.url.split('/').slice(3);
    let keyArr = [storageName];
    keyArr.push.apply(keyArr, arr);
    return keyArr.join("-");
}

function getBlobMetadata(message) {
    let url = message.data.url;
    let data = url.split('/');
    let topicArr = message.topic.split('/');

    // '/subscriptions/c088dc46-d692-42ad-a4b6-9a542d28ad2a/resourceGroups/AG-SUMO/providers/Microsoft.Storage/
    //'https://allbloblogs.blob.core.windows.net/webapplogs/AZUREAUDITEVENTHUB/2018/04/26/09/f4f692.log'
    return {
        url: url,
        containerName: data[3],
        blobName: data.slice(4).join('/'),
        storageName: url.split("//").pop().split(".")[0],
        resourceGroupName: topicArr[4],
        subscriptionId: topicArr[2]
    };
}

function getEntity(metadata, endByte, currentEtag) {
     //a single entity group transaction is limited to 100 entities. Also, the entire payload of the transaction may not exceed 4MB
    // RowKey/Partition key cannot contain "/"
    let entity = {
        partitionKey : metadata.containerName,
        rowKey : getRowKey(metadata),
        blobName: metadata.blobName,
        containerName: metadata.containerName,
        storageName: metadata.storageName,
        offset: endByte,
        date: (new Date()).toISOString()
    };
    if (currentEtag) {
        entity['.metadata'] = { etag: currentEtag };
    }
    return entity;
}

function getContentLengthPerBlob(eventHubMessages, allcontentlengths, metadatamap) {
    eventHubMessages.forEach(message => {
        let metadata = getBlobMetadata(message);
        let RowKey = getRowKey(metadata);
        metadatamap[RowKey] = metadata;
        (allcontentlengths[RowKey] || (allcontentlengths[RowKey] = [])).push(message.data.contentLength);
    });
}

function getBlobPointerMap(PartitionKey, RowKey, context) {
    // Todo Add retries for node migration in cases of timeouts(non 400 & 500 errors)
    // tableClient.getEntity(PartitionKey,RowKey)
    return new Promise(function (resolve, reject) {
        tableService.retrieveEntity(process.env.APPSETTING_TABLE_NAME, PartitionKey, RowKey, function (error, result, response) {
          // context.log("inside getBlobPointerMap", response.statusCode);
          if (response.statusCode === 404 || !error) {
            resolve(response);
          } else {
            reject(error);
          }
        });
    });
}

function updateBlobPointerMap(entity, context) {
    return new Promise(function (resolve, reject) {
        let insertOrReplace = ".metadata" in entity ? tableService.replaceEntity.bind(tableService) : tableService.insertEntity.bind(tableService);
        insertOrReplace(process.env.APPSETTING_TABLE_NAME, entity, function (error, result, response) {
            // context.log("inside updateBlobPointerMap", response.statusCode);
            if(!error) {
                resolve(response);
            } else {
                reject(error);
            }
        });
    });
}

function createTasksForBlob(PartitionKey, RowKey, sortedcontentlengths, context, metadata, finalcontext) {
    // context.log("inside createTasksForBlob", PartitionKey, RowKey, sortedcontentlengths, metadata);
    getBlobPointerMap(PartitionKey, RowKey, context).then(function (response) {
        let tasks = [];
        let currentoffset = response.statusCode === 404 ? -1 : Number(response.body.offset);
        let currentEtag = response.statusCode === 404 ? null : response.body['odata.etag'];
        let lastoffset = currentoffset;
        let i, endByte, task;
        for (i = 0; i < sortedcontentlengths.length; i += 1) {
            endByte = sortedcontentlengths[i] - 1;
            if (endByte > lastoffset) {
                // this will remove duplicate contentlengths
                // to specify a range encompassing the first 512 bytes of a blob use x-ms-range: bytes=0-511  contentLength = 512
                // saving in offset: 511 endByte
                task = Object.assign({
                    startByte: lastoffset + 1,
                    endByte: endByte
                }, metadata);
                tasks.push(task);
                lastoffset = endByte;
            }
        }
        if (lastoffset > currentoffset) { // modify offset only when it's been changed
            let entity = getEntity(metadata, lastoffset, currentEtag);
            updateBlobPointerMap(entity, context).then(function (response) {
                context.bindings.tasks = context.bindings.tasks.concat(tasks);
                finalcontext(null, tasks.length + " Tasks added for RowKey: " + RowKey);
            }).catch(function (err) {
                //handle catch with retry when If-match fails else other err
                if (err.code === "UpdateConditionNotSatisfied" && error.statusCode === 412) {
                    context.log("Need to Retry: " + RowKey, entity);
                }
                finalcontext(err, "Unable to Update offset for RowKey: " + RowKey);

            });
        } else {
            finalcontext(null, "No tasks created for RowKey: " + RowKey);
        }

    }).catch(function (err) {
        // unable to retrieve offset
        finalcontext(err, "Unable to Retrieve offset for RowKey: " + RowKey);
    });

}

export default function (context, eventHubMessages) {
    try {
        eventHubMessages = [].concat.apply([], eventHubMessages);
        // context.log("blobtaskproducer message received: ", eventHubMessages.length);
        let metadatamap = {};
        let allcontentlengths = {};
        getContentLengthPerBlob(eventHubMessages, allcontentlengths, metadatamap);
        let processed = 0;
        context.bindings.tasks = [];
        let totalRows = Object.keys(allcontentlengths).length;
        let errArr = [], RowKey;
        for (RowKey in allcontentlengths) {
            let sortedcontentlengths = allcontentlengths[RowKey].sort(); // ensuring increasing order of contentlengths
            let metadata = metadatamap[RowKey];
            let PartitionKey = metadata.containerName;
            createTasksForBlob(PartitionKey, RowKey, sortedcontentlengths, context, metadata, (err, msg) => {
                processed += 1;
                // context.log(RowKey, processed, err, msg);
                if (err) {
                    errArr.push(err);
                }
                if (totalRows === processed) {
                    context.log("Tasks Created: " + JSON.stringify(context.bindings.tasks) + " Blobpaths: " + JSON.stringify(allcontentlengths));
                    if (errArr.length > 0) {
                        context.done(errArr.join('\n'));
                    } else {
                        context.done();
                    }
                }
            });

        }
    } catch (error) {
        context.done(error);
    }
}
