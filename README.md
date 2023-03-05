<div align="center">
  
  ![Memphis light logo](https://github.com/memphisdev/memphis/blob/master/logo-white.png?raw=true#gh-dark-mode-only)
  
</div>

<div align="center">
  
  ![Memphis light logo](https://github.com/memphisdev/memphis/blob/master/logo-black.png?raw=true#gh-light-mode-only)
  
</div>

<div align="center">
<h4>Simple as RabbitMQ, robust as Apache Kafka, and perfect for busy developers.</h4>
<img width="750" alt="Memphis UI" src="https://user-images.githubusercontent.com/70286779/204081372-186aae7b-a387-4253-83d1-b07dff69b3d0.png"><br>

  
  <a href="https://landscape.cncf.io/?selected=memphis"><img width="200" alt="CNCF Silver Member" src="https://github.com/cncf/artwork/raw/master/other/cncf-member/silver/white/cncf-member-silver-white.svg#gh-dark-mode-only"></a>
  
</div>

<div align="center">
  
  <img width="200" alt="CNCF Silver Member" src="https://github.com/cncf/artwork/raw/master/other/cncf-member/silver/color/cncf-member-silver-color.svg#gh-light-mode-only">
  
</div>
 
 <p align="center">
  <a href="https://sandbox.memphis.dev/" target="_blank">Sandbox</a> - <a href="https://memphis.dev/docs/">Docs</a> - <a href="https://twitter.com/Memphis_Dev">Twitter</a> - <a href="https://www.youtube.com/channel/UCVdMDLCSxXOqtgrBaRUHKKg">YouTube</a>
</p>

<p align="center">
<a href="https://discord.gg/WZpysvAeTf"><img src="https://img.shields.io/discord/963333392844328961?color=6557ff&label=discord" alt="Discord"></a>
<a href="https://github.com/memphisdev/memphis/issues?q=is%3Aissue+is%3Aclosed"><img src="https://img.shields.io/github/issues-closed/memphisdev/memphis?color=6557ff"></a> 
  <img src="https://img.shields.io/npm/dw/memphis-dev?color=ffc633&label=installations">
<a href="https://github.com/memphisdev/memphis/blob/master/CODE_OF_CONDUCT.md"><img src="https://img.shields.io/badge/Code%20of%20Conduct-v1.0-ff69b4.svg?color=ffc633" alt="Code Of Conduct"></a> 
<a href="https://docs.memphis.dev/memphis/release-notes/releases/v0.4.2-beta"><img alt="GitHub release (latest by date)" src="https://img.shields.io/github/v/release/memphisdev/memphis?color=61dfc6"></a>
<img src="https://img.shields.io/github/last-commit/memphisdev/memphis?color=61dfc6&label=last%20commit">
</p>

**[Memphis](https://memphis.dev)** is a next-generation alternative to traditional message brokers.<br><br>
A simple, robust, and durable cloud-native message broker wrapped with<br>
an entire ecosystem that enables cost-effective, fast, and reliable development of modern queue-based use cases.<br><br>
Memphis enables the building of modern queue-based applications that require<br>
large volumes of streamed and enriched data, modern protocols, zero ops, rapid development,<br>
extreme cost reduction, and a significantly lower amount of dev time for data-oriented developers and data engineers.

## This repo
This repo is for load tests only, between Memphis station to BigQuery table.
The application consumes messages in a stream manner, inserting each message to a newly created BigQuery dataset and table named `memphisLoadTest`

## Important before getting started!
1. This application is for tests only!<br>
In order for it to work properly, the consumed events **must**<br>
be complient with the following schema -
```json
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
```
2. After cloning the repo, a GCP `key.json` must be provided and located within this app root directory

## Getting started
### Step 1: Install [node.js](https://nodejs.org/en/download/) with npm
### Step 2: Clone this repo
### Step 3: Install app dependencies using `npm install`
### Step 4: Run the app
Example:
```shell
node consumer.js --memphis_hostname="memphis.test.com" --memphis_username="bigquery" --memphis_connectionToken="XrAAAAw6rgm8888PNNTy" --memphis_station="bigquery-load-test" --bigQuery_projectId="project-1234"
```