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

    // let promises = clients.map(async (c, index) => {
    //     try{
    //         await c.Ping({})
    //         return (index)
    //     } catch (e) {
    //         unhealthyClients
    //     }
    // })
    //
    // let responses = await Promise.all(promises)
    //
    // let aliveClients = responses.filter(r => r !== undefined);

    for await (const ip of ips) {
        let currentClient = healthyClients[indexHealthyClient]
        let valid = false
        debugger
        while (!valid){
            valid = await currentClient.CountryAndProvinceByIP({ip: ip}).then(response => {
                console.log('IP: ', ip)
                console.log('Country: ', response.country.name)
                console.log('Province: ', response.province.name)
                console.log('\n')
                return true
            }).catch(() => {
                const unhealthyClient = healthyClients.splice(indexHealthyClient, 1)
                unhealthyClients = [...unhealthyClients, ...unhealthyClient]
                return false
            })
            indexHealthyClient = indexHealthyClient >= healthyClients.length - 1 ? 0 : indexHealthyClient + 1
            debugger
        }
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


initClient(['181.46.160.42', '8.8.8.8', '181.169.9.160', '181.46.160.42', '8.8.8.8', '181.169.9.160', '181.46.160.42', '8.8.8.8', '181.169.9.160']);