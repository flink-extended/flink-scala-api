//> using toolkit default
//> using dep "org.flinkextended::flink-scala-api:1.18.1_1.2.3"
//> using dep "org.apache.flink:flink-clients:1.18.1"
//> using dep org.apache.flink:flink-connector-gcp-pubsub:3.1.0-1.18

import org.apache.flink.api.common.serialization.{AbstractDeserializationSchema, SerializationSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.gcp.pubsub.{PubSubSink, PubSubSource}
import org.apache.flinkx.api.*
import org.apache.flinkx.api.serializers.*
import upickle.default.*

import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate}

/** This code is available in article version with more details of the implementation:
  * https://dev.to/geazi_anc/data-engineering-with-scala-mastering-real-time-data-processing-with-apache-flink-and-google-pubsub-3b39
  */

// model definitions
final case class CreatedCustomer(fullName: String, birthDate: String) derives ReadWriter:
  val firstName = fullName.split(" ").head
  val lastName  = fullName.split(" ").last
  val age       = ChronoUnit.YEARS.between(LocalDate.parse(birthDate), LocalDate.now()).toInt

final case class RegisteredCustomer(firstName: String, lastName: String, age: Int, isActive: Boolean, createdAt: String)
    derives ReadWriter

object RegisteredCustomer:
  def apply(firstName: String, lastName: String, age: Int): RegisteredCustomer =
    RegisteredCustomer(firstName, lastName, age, true, Instant.now().toString)

// JSON serializers and deserializers
class CreatedCustomerDeserializer extends AbstractDeserializationSchema[CreatedCustomer]:
  override def deserialize(message: Array[Byte]): CreatedCustomer = read[CreatedCustomer](new String(message, "UTF-8"))

class RegisteredCustomerSerializer extends SerializationSchema[RegisteredCustomer]:
  override def serialize(element: RegisteredCustomer): Array[Byte] =
    write[RegisteredCustomer](element).getBytes("UTF-8")

// main
val parameters       = ParameterTool.fromArgs(args)
val projectName      = parameters.get("project")
val subscriptionName = parameters.get("subscription-name")
val topicName        = parameters.get("topic-name")

val pubsubSource = PubSubSource
  .newBuilder()
  .withDeserializationSchema(new CreatedCustomerDeserializer())
  .withProjectName(projectName)
  .withSubscriptionName(subscriptionName)
  .build()
val pubsubSink = PubSubSink
  .newBuilder()
  .withSerializationSchema(new RegisteredCustomerSerializer())
  .withProjectName(projectName)
  .withTopicName(topicName)
  .build()

val env = StreamExecutionEnvironment.getExecutionEnvironment
env.enableCheckpointing(1000L)

env
  .addSource(pubsubSource)
  .map(cc => RegisteredCustomer(cc.firstName, cc.lastName, cc.age))
  .map(rc => if rc.age >= 30 then rc.copy(isActive = false) else rc)
  .addSink(pubsubSink)

env.execute("customerRegistering")
