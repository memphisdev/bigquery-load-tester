// Require the framework and instantiate it
const os = require("os");
const cluster = require("cluster");
const { memphis } = require('memphis-dev');
const {BigQuery} = require('@google-cloud/bigquery');

const clusterWorkerSize = os.cpus().length;
let bigqueryClient;
let fillBQconfigRan = false;
let datasetId = "memphisLoadTest";
let tableId = "memphisLoadTest";
let memphisConnection;
const memphis_station = "enriched_data";
const insertOptions = {
    createInsertId: false,
    partialRetries: 0,
    raw: true,
}
const BIG_QUERY_TABLE_FIELDS = [
    { name: 'uuid', type: 'STRING' },
    { name: 'id', type: 'STRING' },
    { name: 'event', type: 'STRING' },
    { name: 'properties', type: 'STRING' },
    { name: 'elements_chain', type: 'STRING' },
    { name: 'person', type: 'STRING' },
    { name: 'elements', type: 'STRING' },
    { name: 'set', type: 'STRING' },
    { name: 'set_once', type: 'STRING' },
    { name: 'distinct_id', type: 'STRING' },
    { name: 'distinct__ids', type: 'STRING' },
    { name: 'team_id', type: 'INT64' },
    { name: 'ip', type: 'STRING' },
    { name: 'site_url', type: 'STRING' },
    { name: 'timestamp', type: 'TIMESTAMP' },
    { name: 'type', type: 'STRING' },
    { name: 'is__identified', type: 'STRING' },
    { name: 'bq_ingested_timestamp', type: 'TIMESTAMP' },
]
const rows = [];
const object = JSON.parse('{"distinct__ids": "1863ac817a022f-03dc9795fbd578-16525635-1d73c0-1863ac817a12306","distinct_id": "1863ac817a022f-03dc9795fbd578-16525635-1d73c0-1863ac817a12306","elements": "16525635-1d73c0-1863ac817a12306","elements_chain": "","event": "$pageleave","id": "01869def-3cbd-0001-5a5a-a92b6b31df78","is__identified": "false","person": "1863ac817a022f-03dc9795fbd578-16525635-1d73c0-1863ac817a12306","properties": "$active_feature_flags","timestamp": "2023-03-01T16:09:06.972000+00:00","type": "main-page"}')
const event = { 
    json: object
}
rows.push(event)

async function fillBQconfig() {
    try {
        bigqueryClient = new BigQuery({
            keyFilename: 'key.json',
            projectId: "posthog-374908"
        });
        var datasetCreated = false
        console.log("// Fetching BigQuery data")
        const [datasets] = await bigqueryClient.getDatasets();
        datasets.forEach(dataset => {
            if(dataset.id=="memphisLoadTest") {
                console.log(`- Dataset ${datasetId} found`)
                datasetCreated=true;
            }
        });
        // Create the dataset
        if(!datasetCreated){
            console.log("- Dataset not found. Creating")
            const [dataset] = await bigqueryClient.createDataset("memphisLoadTest");
            console.log(`- Dataset ${dataset.id} created.`);
        }
    } catch (ex) {
        console.log(ex)
    }  
    try {
        // Creates the table
        console.log("- Checking if table exists. If not, creating.")
        const [table] = await bigqueryClient
        .dataset(datasetId)
        .createTable(tableId,{ schema: BIG_QUERY_TABLE_FIELDS})
        console.log(`- Table ${table.id} created.`)

        console.log('- Insert new record')
        await bigqueryClient
        .dataset(datasetId)
        .table(tableId)
        .insert(rows,insertOptions)

        fillBQconfigRan = true
        return
    } catch (ex) {
        if(ex.code == 409) {
            console.log(`- Table ${tableId} already exist`)
            console.log('- Insert new record')
            await bigqueryClient
            .dataset(datasetId)
            .table(tableId)
            .insert(rows, insertOptions)
            fillBQconfigRan = true
            return
        }
        else console.log(ex)
    }  
}

// const firstPull = async () => {
//     // This function will only run once, and only during program initilization 
//     // to define the proper schema on the BigQuery side
//     try {
//         var sample_message;
//         memphisConnection = await memphis.connect({
//             host: 'broker.sandbox.memphis.dev',
//             username: 'bigquery',
//             connectionToken: 'XrHmszw6rgm8IyOPNNTy'
//         });
//         const msgs = await memphis.fetchMessages({
//             stationName: memphis_station,
//             consumerName: 'bigquery-tester',
//             consumerGroup: 'bigquery-tester4', // defaults to the consumer name.
//             batchSize: 1, // defaults to 10
//             batchMaxTimeToWaitMs: 5000, // defaults to 5000
//             maxAckTimeMs: 30000, // defaults to 30000
//             maxMsgDeliveries: 10, // defaults to 10
//             genUniqueSuffix: false, // defaults to false
//             startConsumeFromSequence: 1, // start consuming from a specific sequence. defaults to 1
//             lastMessages: -1 // consume the last N messages, defaults to -1 (all messages in the station)
//         });
//         console.log("- Pulling a small batch of messages to define BQ table schema")
//         // console.log(message)
//         if(msgs.length>0){
//             msgs[0].ack();
//             sample_message = JSON.parse(msgs[0].getData().toString());
//             if (memphisConnection) memphisConnection.close();
//             console.log("- Building the schema")
//             console.log(sample_message)
//             // BIG_QUERY_TABLE_FIELDS = await GenerateSchema.bigquery(sample_message)
//             await fillBQconfig(sample_message);
//         }
//     } catch (ex) {
//         console.log(ex);
//         if (memphisConnection) memphisConnection.close();
//     }
// }

// Run the server!
const start = async () => {
    try {
        memphisConnection = await memphis.connect({
            host: 'broker.sandbox.memphis.dev',
            username: 'bigquery',
            connectionToken: 'XrHmszw6rgm8IyOPNNTy'
        });

        const consumer = await memphisConnection.consumer({
            stationName: memphis_station,
            consumerName: 'bigquery-tester',
            consumerGroup: 'bigquery-tester4',
            batchSize: 10,
            genUniqueSuffix: false,
        });

        consumer.setContext({ key: "value" });
        while(!fillBQconfigRan){}
        consumer.on('message', (message, context) => {
            message.getData().toString();
            message.ack();
        });

        consumer.on('error', (error) => {});
    } catch (ex) {
        console.log(ex);
        if (memphisConnection) memphisConnection.close();
    }
}

if (clusterWorkerSize > 1) {
    if (cluster.isMaster) {
        // for (let i=0; i < clusterWorkerSize; i++) {
        for (let i=0; i < 1; i++) {
            cluster.fork();
        }

        cluster.on("exit", function(worker) {
            console.log("Worker", worker.id, " has exited.")
        })
    } else {
        if (!fillBQconfigRan) {
            fillBQconfig();
            // start();
        } else {
            // start();
        }
    }
} else {
    if (!fillBQconfigRan) {
        fillBQconfig();
        // start();
    } else {
        // start();
    }
}