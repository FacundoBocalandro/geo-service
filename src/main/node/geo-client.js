const PROTO_PATH = __dirname + '/../protobuf/geoService.proto';
const {promisify} = require('util');

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

async function initClient(ips) {

    const client1 = new geo_proto.GeoService("localhost:50000", grpc.credentials.createInsecure())
    const client2 = new geo_proto.GeoService("localhost:50001", grpc.credentials.createInsecure())
    const client3 = new geo_proto.GeoService("localhost:50002", grpc.credentials.createInsecure())

    let healthyClients = [client1, client2, client3].map(promisifyAll)
    let indexHealthyClient = 0
    let unhealthyClients = []

    for await (const ip of ips) {
        let currentClient = healthyClients[indexHealthyClient]
        await currentClient.CountryAndProvinceByIP({ip: ip}).then(response => {
            console.log('IP: ', ip)
            console.log('Country: ', response.country.name)
            console.log('Province: ', response.province.name)
            console.log('Index: ', indexHealthyClient)
            console.log('\n')
        }).catch(() => {
            const unhealthyClient = healthyClients.splice(indexHealthyClient, 1)
            unhealthyClients = [...unhealthyClients, ...unhealthyClient]
        })
        indexHealthyClient = indexHealthyClient >= healthyClients.length - 1 ? 0 : indexHealthyClient + 1

        if (ips.indexOf(ip) % 10 === 0){
            const{newHealthy, newUnhealthy} = await checkHealthy(healthyClients, unhealthyClients)
            healthyClients = newHealthy
            unhealthyClients = newUnhealthy
        }
    }
}

async function checkHealthy(healthyClients, unhealthyClients) {
    let newHealthy = [...healthyClients]
    let newUnhealthy = [...unhealthyClients]
    for (const client of unhealthyClients) {
        await client.Ping({}).then(() => {
            newHealthy = [...newHealthy, client]
            newUnhealthy.splice(newUnhealthy.indexOf(client), 1)
        }).catch( () => {})
    }
    return {newHealthy, newUnhealthy}
}

function promisifyAll(client) {
    const to = {};
    for (var k in client) {
        if (typeof client[k] != 'function') continue;
        to[k] = promisify(client[k].bind(client));
    }
    return to;
}


initClient(['181.46.160.42', '8.8.8.8', '181.169.9.160', '181.46.160.42', '8.8.8.8', '181.169.9.160', '181.46.160.42', '8.8.8.8', '181.169.9.160',
                '181.46.160.42', '8.8.8.8', '181.169.9.160', '181.46.160.42', '8.8.8.8', '181.169.9.160', '181.46.160.42', '8.8.8.8', '181.169.9.160']);