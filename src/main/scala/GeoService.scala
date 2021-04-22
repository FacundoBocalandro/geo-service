import GeoServiceServer.kvClient
import geoservice.geoService.GeoServiceGrpc.{GeoService, GeoServiceStub}
import geoservice.geoService.{City, Country, GeoServiceGrpc, GetCitiesByProvinceReply, GetCitiesByProvinceRequest, GetCountriesListReply, GetCountriesListRequest, GetCountryAndProvinceByIPReply, GetCountryAndProvinceByIPRequest, GetProvincesByCountryReply, GetProvincesByCountryRequest, PingReply, PingRequest, Province}
import io.etcd.jetcd.{ByteSequence, Client}
import io.etcd.jetcd.options.PutOption
import io.grpc.{ManagedChannelBuilder, ServerBuilder}
import scalacache._
import scalacache.memcached._

import java.util.concurrent.{Executors, TimeUnit}
import scalacache.modes.try_._
import scalacache.serialization.binary._

import scala.io._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import com.google.common.base.Charsets.UTF_8

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

class MyService(port: String, leaseId: Long) extends GeoService {
  val client: Client = Client.builder().endpoints("http://127.0.0.1:2379").build()
  val electionClient = client.getElectionClient
  val locationDatabase = new CSVReader()
  var master = false
  var stub = None

  implicit val context: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  def createStub(ip: String, port: Int): GeoServiceStub = {
    val builder = ManagedChannelBuilder.forAddress(ip, port)
    builder.usePlaintext()
    val channel = builder.build()

    GeoServiceGrpc.stub(channel)
  }

  val f2 = Future {
    electionClient.campaign(ByteSequence.from("/service/geo/election".getBytes()), leaseId, ByteSequence.from(port.getBytes())).get()
  }

  f2 map { response => {
    master = true
  }}

  override def countriesList(request: GetCountriesListRequest): Future[GetCountriesListReply] = {
    if (master) {
      val reply = GetCountriesListReply(countries = locationDatabase.getCountries)
      Future.successful(reply)
    } else {
      val master = electionClient.leader(ByteSequence.from("/service/geo/election".getBytes())).get().getKv.getValue.toString(UTF_8).toInt
      createStub("127.0.0.1", master).countriesList(request)
    }
  }

  override def provincesByCountry(request: GetProvincesByCountryRequest): Future[GetProvincesByCountryReply] = {
    if (master) {
      val reply = GetProvincesByCountryReply(provinces = locationDatabase.getProvincesByCountry(Country(name = request.countryName)))
      Future.successful(reply)
    } else {
      val master = electionClient.leader(ByteSequence.from("/service/geo/election".getBytes())).get().getKv.getValue.toString(UTF_8).toInt
      createStub("127.0.0.1", master).provincesByCountry(request)
    }
  }

  override def citiesByProvince(request: GetCitiesByProvinceRequest): Future[GetCitiesByProvinceReply] = {
    if (master) {
      val reply = GetCitiesByProvinceReply(cities = locationDatabase.getCitiesByProvince(Province(name = request.provinceName, country = Option(Country(name = request.countryName)))))
      Future.successful(reply)
    } else {
      val master = electionClient.leader(ByteSequence.from("/service/geo/election".getBytes())).get().getKv.getValue.toString(UTF_8).toInt
      createStub("127.0.0.1", master).citiesByProvince(request)
    }
  }

  override def countryAndProvinceByIP(request: GetCountryAndProvinceByIPRequest): Future[GetCountryAndProvinceByIPReply] = {
    if (master) {
      println("master response")
      val replyTuple = locationDatabase.getCountryAndProvinceByIP(request.ip)
      val reply = GetCountryAndProvinceByIPReply(country = Option(replyTuple.country), province = Option(replyTuple.province))
      Future.successful(reply)
    } else {
      val master = electionClient.leader(ByteSequence.from("/service/geo/election".getBytes())).get().getKv.getValue.toString(UTF_8).toInt
      createStub("127.0.0.1", master).countryAndProvinceByIP(request)
    }
  }

  override def ping(request: PingRequest): Future[PingReply] = {
    Future.successful(PingReply())
  }
}

trait LocationDatabase {
  def getCountries: List[Country]

  def getProvincesByCountry(country: Country): List[Province]

  def getCitiesByProvince(province: Province): List[City]

  def getCountryAndProvinceByIP(ip: String): CountryProvince
}

final case class CountryProvince(country: Country, province: Province)

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

  def getCountryAndProvinceByIP(ip: String): CountryProvince = {
    val client: Client = Client.builder().endpoints("http://127.0.0.1:2379").build()
    val kvClient = client.getKVClient

    val cacheTtl: String = kvClient.get(ByteSequence.from("/config/services/geo/cache/ttl", UTF_8)).get().getKvs.get(0).getValue.toString(UTF_8)
    val cacheUrl: String = kvClient.get(ByteSequence.from("/config/services/geo/cache/url", UTF_8)).get().getKvs.get(0).getValue.toString(UTF_8)

    println(s"cache-ttl: ${cacheTtl}")
    println(s"cache-url: ${cacheUrl}")

    implicit val countryCache: Cache[CountryProvince] = MemcachedCache(cacheUrl)

    val result = caching(ip) (ttl = Option(Duration(cacheTtl.toLong, TimeUnit.SECONDS))) {
      val source = Source.fromURL("https://ipwhois.app/json/" + ip)
      val content: Json = parse(source.mkString).getOrElse(null)
      source.close()
      val countryString: String = content.\\("country").head.asString.getOrElse("")
      val provinceString: String = content.\\("region").head.asString.getOrElse("")
      val country: Country = Country(name = countryString)
      CountryProvince(country, Province(name = provinceString, Option(country)))
    }
    result.get
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
  import io.etcd.jetcd.options.PutOption
  import java.util.concurrent.TimeUnit
  import java.util.concurrent.Executors
  import concurrent.{ExecutionContext, Future}
  import concurrent.duration._
  import akka.actor._
  import com.google.common.base.Charsets.UTF_8


  // create client
  val client: Client = Client.builder().endpoints("http://127.0.0.1:2379").build()
  val kvClient = client.getKVClient
  val leaseClient = client.getLeaseClient

  val key = ByteSequence.from(s"/service/geo/${args(0)}".getBytes())
  val value = ByteSequence.from(args(0).getBytes())

  val ttl: String = kvClient.get(ByteSequence.from("/config/services/geo/lease/ttl", UTF_8)).get().getKvs.get(0).getValue.toString(UTF_8)
  val keepAliveTime: String = kvClient.get(ByteSequence.from("/config/services/geo/lease/keep-alive-time", UTF_8)).get().getKvs.get(0).getValue.toString(UTF_8)

  println(s"ttl: ${ttl}")
  println(s"keep-alive: ${keepAliveTime}")

  val leaseId = leaseClient.grant(ttl.toLong).get.getID
  println("Hex lease: " + leaseId.toHexString)

  // put the key-value
  kvClient.put(key, value, PutOption.newBuilder().withLeaseId(leaseId).build()).get()

  implicit val context: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  val f = Future {
    val actorSystem = ActorSystem()
    val scheduler = actorSystem.scheduler
    val task = new Runnable {
      def run(): Unit = {
        println("keep alive")
        leaseClient.keepAliveOnce(leaseId)
      }
    }

    scheduler.schedule(
      initialDelay = Duration(keepAliveTime.toLong, TimeUnit.SECONDS),
      interval = Duration(keepAliveTime.toLong, TimeUnit.SECONDS),
      runnable = task)(actorSystem.dispatcher)
  }

  val builder = ServerBuilder.forPort(args(0).toInt)
  builder.addService(
    GeoService.bindService(new MyService(args(0), leaseId), ExecutionContext.global)
  )
  val server = builder.build()
  server.start()

  println("Running....")

  server.awaitTermination()
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
  val response2: Future[GetCountryAndProvinceByIPReply] = stub1.countryAndProvinceByIP(GetCountryAndProvinceByIPRequest(ip = "8.8.8.9"))

  response.onComplete { r =>
    println("Response: " + r)
  }

  response2.onComplete { r =>
    println("Response: " + r)
  }

  System.in.read()
}