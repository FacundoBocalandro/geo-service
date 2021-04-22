import com.fasterxml.jackson.annotation.JsonCreator.Mode
import geoservice.geoService.GeoServiceGrpc.{GeoService, GeoServiceStub}
import geoservice.geoService.{City, Country, GeoServiceGrpc, GetCitiesByProvinceReply, GetCitiesByProvinceRequest, GetCountriesListReply, GetCountriesListRequest, GetCountryAndProvinceByIPReply, GetCountryAndProvinceByIPRequest, GetProvincesByCountryReply, GetProvincesByCountryRequest, PingReply, PingRequest, Province}
import io.etcd.jetcd.options.PutOption
import io.grpc.{ManagedChannelBuilder, ServerBuilder}
import scalacache.Cache
import scalacache.memcached.MemcachedCache
import scalacache._
import scalacache.memcached._
import scalacache.serialization.binary._
import scalacache.modes.sync.mode

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

    implicit val countryCache: Cache[(Country, Province)] = MemcachedCache("localhost:11211")
    caching(ip) (ttl = None) {
      println("Aca no cacheo")
      val source = Source.fromURL("https://ipwhois.app/json/" + ip)
      val content: Json = parse(source.mkString).getOrElse(null)
      source.close()
      val countryString: String = content.\\("country").head.asString.getOrElse("")
      val provinceString: String = content.\\("region").head.asString.getOrElse("")
      val country: Country = Country(name = countryString)
      (country, Province(name = provinceString, Option(country)))
    }
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
  import scalacache._
  import scalacache.memcached._
  import scalacache.serialization.binary._
  import scalacache.modes.sync.mode

  // create client
  val client: Client = Client.builder().endpoints("http://127.0.0.1:2379").build()
  val kvClient: KV = client.getKVClient
  val leaseClient: Lease = client.getLeaseClient

  val key = ByteSequence.from("/service/geo".getBytes())
  val value = ByteSequence.from(args(0).getBytes())

  val leaseId = leaseClient.grant(90).get.getID

  // put the key-value
  kvClient.put(key, value, PutOption.newBuilder().withLeaseId(leaseId).build()).get()

  implicit var countryCache: Cache[(Country, Province)] = MemcachedCache("localhost:11211")

  val country = Country("Argentina")
  val province = Province("Mendoza", Option(country))


  private val ip1 = "192.168.0.1"
  println(countryCache.get(ip1))


  System.in.read()

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