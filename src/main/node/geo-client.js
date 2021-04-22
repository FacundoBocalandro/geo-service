const PROTO_PATH = __dirname + '/../protobuf/geoService.proto';
const {promisify} = require('util');
const { Etcd3 } = require('etcd3');

const client = new Etcd3();
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const packageDefinition = protoLoader.loadSync(
    PROTO_PATH,
    {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
    });

const geo_proto = grpc.loadPackageDefinition(packageDefinition).geoservice;

const getClients = async () => {
    const keys = await client.getAll().prefix('/service/geo')
    return Object.values(keys).map(value => ({key: value, service: promisifyAll(new geo_proto.GeoService(`localhost:${value}`, grpc.credentials.createInsecure()))}))
}

async function initClient(){
    let indexHealthyClient = 0
    let healthyClients = []

    // await client.watch()
    //     .prefix('/service/geo')
    //     .create()
    //     .then(watcher => {
    //         console.log("watcher")
    //         watcher
    //
    //             .on('put', kv => {
    //                 console.log("put")
    //                 healthyClients = [...healthyClients, {key: kv.value, service: promisifyAll(new geo_proto.GeoService(`localhost:${kv.value}`, grpc.credentials.createInsecure()))}]
    //             })
    //             .on('delete', kv => healthyClients = healthyClients.filter(client => client.key !== kv.value))
    //     })

    for await (const ip of ips) {
        healthyClients = await getClients()
        while (healthyClients.length === 0){
            await new Promise(resolve => setTimeout(resolve, 2000))
            healthyClients = await getClients();
        }
        let currentClient = healthyClients[indexHealthyClient].service
        console.log("client index: " + indexHealthyClient)
        currentClient.CountryAndProvinceByIP({ip: ip}).then(response => {
            console.log('IP: ', ip)
            console.log('Country: ', response.country.name)
            console.log('Province: ', response.province.name)
            console.log('Index: ', indexHealthyClient)
            console.log('\n')
        }).catch(() => {
            healthyClients.splice(indexHealthyClient)
        })
        indexHealthyClient = indexHealthyClient >= healthyClients.length - 1 ? 0 : indexHealthyClient + 1
    }
}

function promisifyAll(client) {
    const to = {};
    for (var k in client) {
        if (typeof client[k] != 'function') continue;
        to[k] = promisify(client[k].bind(client));
    }
    return to;
}

const ips = Array(15).fill().map(() => "8.8.8.8")


initClient();