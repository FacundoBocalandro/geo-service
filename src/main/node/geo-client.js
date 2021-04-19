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

async function initClient(){
    let indexHealthyClient = 0
    let healthyClients = []
    let interval = null;

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
            interval = setInterval(getClients, 2000)
        }
        interval = null;
        console.log("has client")
        let currentClient = healthyClients[indexHealthyClient].service
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
//     const client1 = new geo_proto.GeoService("127.0.1.1:50000", grpc.credentials.createInsecure())
//     const client2 = new geo_proto.GeoService("localhost:50001", grpc.credentials.createInsecure())
//     const client3 = new geo_proto.GeoService("localhost:50002", grpc.credentials.createInsecure())
//
//     let healthyClients = [client1, client2, client3].map(promisifyAll)
//     let indexHealthyClient = 0
//     let unhealthyClients = []
//
//     for await (const ip of ips) {
//              
//         awaitcurrentClient.CountryAndProvinceByIP({ip: ip}).then(response => {
//             console.log('IP: ', ip)
//             console.log('Country: ', response.country.name)
//             console.log('Province: ', response.province.name)
//             console.log('Index: ', indexHealthyClient)
//             console.log('\n')
//         }).catch(() => {
//             const unhealthyClient = healthyClients.splice(indexHealthyClient, 1)
//             unhealthyClients = [...unhealthyClients, ...unhealthyClient]
//         })
//
//
//         if (ips.indexOf(ip) % 10 === 0){
//             const{newHealthy, newUnhealthy} = await checkHealthy(healthyClients, unhealthyClients)
//             healthyClients = newHealthy
//             unhealthyClients = newUnhealthy
//         }
//     }
// }
//
// async function checkHealthy(healthyClients, unhealthyClients) {
//     let newHealthy = [...healthyClients]
//     let newUnhealthy = [...unhealthyClients]
//     for (const client of unhealthyClients) {
//         await client.Ping({}).then(() => {
//             newHealthy = [...newHealthy, client]
//             newUnhealthy.splice(newUnhealthy.indexOf(client), 1)
//         }).catch( () => {})
//     }
//     return {newHealthy, newUnhealthy}
// }
//
function promisifyAll(client) {
    const to = {};
    for (var k in client) {
        if (typeof client[k] != 'function') continue;
        to[k] = promisify(client[k].bind(client));
    }
    return to;
}


initClient();