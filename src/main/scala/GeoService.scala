import geoservice.geoService.GeoServiceGrpc.{GeoService, GeoServiceStub}
import geoservice.geoService.{City, Country, GeoServiceGrpc, GetCitiesByProvinceReply, GetCitiesByProvinceRequest, GetCountriesListReply, GetCountriesListRequest, GetCountryAndProvinceByIPReply, GetCountryAndProvinceByIPRequest, GetProvincesByCountryReply, GetProvincesByCountryRequest, PingReply, PingRequest, Province}
import io.grpc.{ManagedChannelBuilder, ServerBuilder}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.io._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

class MyService extends GeoService {
  val locationDatabase = new CSVReader()

  override def countriesList(request: GetCountriesListRequest): Future[GetCountriesListReply] = {
    val reply = GetCountriesListReply(countries = locationDatabase.getCountries)
    Future.successful(reply)
  }

  override def provincesByCountry(request: GetProvincesByCountryRequest): Future[GetProvincesByCountryReply] = {
    val reply = GetProvincesByCountryReply(provinces = locationDatabase.getProvincesByCountry(Country(name = request.countryName)))
    Future.successful(reply)
  }

  override def citiesByProvince(request: GetCitiesByProvinceRequest): Future[GetCitiesByProvinceReply] = {
    val reply = GetCitiesByProvinceReply(cities = locationDatabase.getCitiesByProvince(Province(name = request.provinceName, country = Option(Country(name = request.countryName)))))
    Future.successful(reply)
  }

  override def countryAndProvinceByIP(request: GetCountryAndProvinceByIPRequest): Future[GetCountryAndProvinceByIPReply] = {
    val replyTuple = locationDatabase.getCountryAndProvinceByIP(request.ip)
    val reply = GetCountryAndProvinceByIPReply(country = Option(replyTuple._1), province = Option(replyTuple._2))
    Future.successful(reply)
  }

  override def ping(request: PingRequest): Future[PingReply] = {
    Future.successful(PingReply())
  }
}

trait LocationDatabase {
  def getCountries: List[Country]

  def getProvincesByCountry(country: Country): List[Province]

  def getCitiesByProvince(province: Province): List[City]

  def getCountryAndProvinceByIP(ip: String): (Country, Province)
}

class CSVReader extends LocationDatabase {

  import CSVReader._
  import io.circe._
  import io.circe.parser._

  val data: List[WorldCity] = getWorldCities("src/main/world-cities.csv")

  def getCountries: List[Country] = {
    data.map(_.country).distinct
  }

  def getProvincesByCountry(country: Country): List[Province] = {
    data.filter(_.country == country).map(_.province).distinct
  }

  def getCitiesByProvince(province: Province): List[City] = {
    data.filter(_.province == province).map(_.city).distinct
  }

  def getCountryAndProvinceByIP(ip: String): (Country, Province) = {
    val source = Source.fromURL("https://ipwhois.app/json/" + ip )
    val content: Json = parse(source.mkString).getOrElse(null)
    source.close()
    val countryString: String = content.\\("country").head.asString.getOrElse("")
    val provinceString: String = content.\\("region").head.asString.getOrElse("")
    val country: Country = Country(name = countryString)
    (country, Province(name = provinceString, Option(country)))
  }
}

case class IPResponseObject(country: Country, province: Province)

case class WorldCity(city: City, country: Country, province: Province, geoNameId: Long)

object CSVReader {

  def getWorldCities(filePath: String): List[WorldCity] = {
    val fileSource = Source.fromFile(filePath)
    val data: List[List[String]] = fileSource.getLines().toList.map(_.split(',').toList)
    fileSource.close()
    data.map {
      case List(cityString, countryString, provinceString, geoNameId) =>
        val country: Country = Country(name = countryString)
        val province: Province = Province(name = provinceString, country = Option(country))
        val city: City = City(name = cityString, province = Option(province))
        WorldCity(city, country, province, geoNameId.toLong)
    }
  }

}

object GeoServiceServer extends App {

  import io.etcd.jetcd._

  // create client
  val client: Client = Client.builder().endpoints("http://127.0.0.1:2379").build()
  val kvClient: KV = client.getKVClient

  val key = ByteSequence.from("/service/geo".getBytes())
  val value = ByteSequence.from(args(0).getBytes())

  // put the key-value
  kvClient.put(key, value).get()

  System.in.read()

//
//  // get the CompletableFuture
//  val getFuture = kvClient.get(key);
//
//  // get the value from CompletableFuture
//  val response = getFuture.get();
//
//  // delete the key
//  kvClient.delete(key).get();








  //  import org.etcd4s.{Etcd4sClientConfig, Etcd4sClient}
  //  import org.etcd4s.implicits._
  //  import org.etcd4s.formats._
  //  import org.etcd4s.pb.etcdserverpb._
  //  import io.grpc.stub.CallStreamObserver
  //  import java.util.concurrent._
  //
  //  import scala.concurrent.Future
  //  import scala.concurrent.duration._
  //  import cats.effect.IO
  //  import java.util.concurrent._
  //
  //  implicit val ec = scala.concurrent.ExecutionContext.Implicits.global
  //  //  implicit val cs = IO.contextShift(ec)
  //
  //  //   create the client
  //  val config = Etcd4sClientConfig(
  //      address = "127.0.0.1",
  //      port = 2379
  //  )
  //
  //  val client = Etcd4sClient.newClient(config)
  //
  //  IO(client.leaseGrant(90))
  //    .attempt.map {
  //    case Left(x) => print(x)
  //    case Right(x) => print(x)
  //  }
  //

  //  print("Aca llego")
  //  client.leaseApi.leaseGrant(LeaseGrantRequest)
  //
  //  //  client.leaseGrant(90).onComplete {
  //  //    case Success(value) =>
  //  //      print("maxi re putito")
  //  client.kvApi.put(PutRequest(key = s"/service/geo/${args(0)}", value = args(0), lease = args(1)))
  //  //    case Failure(exception) => println("exception: " + exception)
  //  //  }
  //
  //  val builder = ServerBuilder.forPort(args(0).toInt)
  //
  //  builder.addService(
  //    GeoService.bindService(new MyService(), ExecutionContext.global)
  //  )
  //
  //  val server = builder.build()
  //  server.start()
  //
  //  println("Running....")
  //
  //  val ex = new ScheduledThreadPoolExecutor(1)
  //  val task = new Runnable {
  //    def run() = {
  //      client.leaseApi.leaseKeepAlive(LeaseKeepAliveRequest(iD = args(1).toLong))
  //      print("keep alive")
  //    }
  //  }
  //  val f = ex.scheduleAtFixedRate(task, 10, 10, TimeUnit.SECONDS)
  //  f.cancel(false)
  //
  //  server.awaitTermination()
}

object ClientDemo extends App {

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global


  def createStub(ip: String, port: Int = 50000): GeoServiceStub = {
    val builder = ManagedChannelBuilder.forAddress(ip, port)
    builder.usePlaintext()
    val channel = builder.build()

    GeoServiceGrpc.stub(channel)
  }

  val stub1 = createStub("127.0.0.1", 50000)
  val stub2 = createStub("127.0.0.1", 50001)

  val stubs = List(stub1, stub2)
  val healthyStubs = stubs

//    val response: Future[GetCountriesListReply] = stub1.countriesList(GetCountriesListRequest())
//  val response: Future[GetProvincesByCountryReply] = stub1.provincesByCountry(GetProvincesByCountryRequest(countryName = "Argentina"))
//  val response: Future[GetCitiesByProvinceReply] = stub1.citiesByProvince(GetCitiesByProvinceRequest(provinceName = "Buenos Aires", countryName = "Argentina"))
  val response: Future[GetCountryAndProvinceByIPReply] = stub1.countryAndProvinceByIP(GetCountryAndProvinceByIPRequest(ip = "8.8.8.8"))

  response.onComplete { r =>
    println("Response: " + r)
  }


  System.in.read()
}