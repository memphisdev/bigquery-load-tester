// Require the framework and instantiate it
const os = require("os");
const cluster = require("cluster");
const { memphis } = require('memphis-dev');
const {BigQuery} = require('@google-cloud/bigquery');
const process = require('process');
const fs = require('fs');
const key_file = 'key.json';
let memphis_hostname, memphis_username, memphis_connectionToken, memphis_station;
let bigQuery_projectId;

if(process.argv.length < 7){
    console.log("\nMissing arguments! Example:\n")
    console.log('node consumer.js --memphis_hostname="memphis.dev" --memphis_username="root" --memphis_connectionToken="memphis" --memphis_station="test_station" --bigQuery_projectId="loadtest12"')
    console.log('\nPlease make sure you preserve the arguments order of appearence')
    process.exit(3)
}

if (!fs.existsSync(key_file)) {
  console.log('"key.json" could not be found in the root directory');
  process.exit(3)
}

function identify_arguments() {
    //[2] memphis_hostname, [3] memphis_username, [4] memphis_connectionToken, [5] memphis_station, [6] bigQuery_projectId, 
    try{
        console.log("- Initilize app arguments")
        memphis_hostname = process.argv[2].split("=")[1]
        memphis_username = process.argv[3].split("=")[1]
        memphis_connectionToken = process.argv[4].split("=")[1]
        memphis_station = process.argv[5].split("=")[1]
        bigQuery_projectId = process.argv[6].split("=")[1]
    } catch (ex) {
        console.log("Something happen during arguments identification. Exit.")
        console.log(ex)
        process.exit(4)
    }  
}

const clusterWorkerSize = os.cpus().length;
let bigqueryClient;
let fillBQconfigRan = false;
let datasetId = "memphisLoadTest";
let tableId = "memphisLoadTest";
let memphisConnection;
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

async function fillBQconfig() {
    try {
        bigqueryClient = new BigQuery({
            keyFilename: key_file,
            projectId: bigQuery_projectId
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
        fillBQconfigRan = true
        return
    } catch (ex) {
        if(ex.code == 409) {
            console.log(`- Table ${tableId} already exist`)
            fillBQconfigRan = true
            return
        }
        else console.log(ex)
    }  
}

// Run the server!
async function start() {
    var rows;
    try {
        memphisConnection = await memphis.connect({
            host: memphis_hostname,
            username: memphis_username,
            connectionToken: memphis_connectionToken
        });

        const consumer = await memphisConnection.consumer({
            stationName: memphis_station,
            consumerName: 'bigquery-tester-new',
            consumerGroup: 'bigquery-tester-new',
            batchSize: 50,
            genUniqueSuffix: true,
        });
        console.log("- Start consuming messages and insert them into BigQuery")
        consumer.on('message', (message, context) => {
            rows = [{json: message.getDataAsJson()}]; // get data as json
            message.ack();
            bigqueryClient
            .dataset(datasetId)
            .table(tableId)
            .insert(rows, insertOptions)
        });

        consumer.on('error', (error) => {});
    } catch (ex) {
        console.log(ex);
        if (memphisConnection) memphisConnection.close();
    }
}

async function main(){
if (clusterWorkerSize > 1) {
    if (cluster.isMaster) {
        for (let i=0; i < clusterWorkerSize; i++) {
        // for (let i=0; i < 1; i++) {
            cluster.fork();
        }

        cluster.on("exit", function(worker) {
            console.log("Worker", worker.id, " has exited.")
        })
    } else {
        await identify_arguments();
        await fillBQconfig();
        start();
    }
} else {
    await identify_arguments();
    await fillBQconfig();
    start();
}
}
main()