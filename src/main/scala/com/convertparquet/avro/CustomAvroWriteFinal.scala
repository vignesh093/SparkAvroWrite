package com.convertparquet.avro

import java.nio.ByteBuffer
import java.sql.Date
import java.sql.Timestamp
import java.util.HashMap
import java.util.Properties
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroValue
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object CustomAvroBinaryWriteFinal {

  class CustomAvroBinaryWriteFinal(schema: StructType, recordName: String, recordNamespace: String) {
    private lazy val converter = createConverterToAvro(schema, recordName, recordNamespace)
    def createConverterToAvro(dataType: DataType, structName: String, recordNamespace: String): (Any) => Any = {
      dataType match {
        case BinaryType => (item: Any) => item match {
          case null => null
          case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
        }
        case ByteType | ShortType | IntegerType | LongType |
          FloatType | DoubleType | StringType | BooleanType => identity
        case _: DecimalType => (item: Any) => if (item == null) null else item.toString
        case TimestampType => (item: Any) =>
          if (item == null) null else item.asInstanceOf[Timestamp].getTime
          case DateType => (item: Any) =>
          if (item == null) null else item.asInstanceOf[Date].getTime
          case ArrayType(elementType, _) =>
          val elementConverter = createConverterToAvro(
            elementType,
            structName,
            SchemaConverters.getNewRecordNamespace(elementType, recordNamespace, structName))
          (item: Any) => {
            if (item == null) {
              null
            } else {
              val sourceArray = item.asInstanceOf[Seq[Any]]
              val sourceArraySize = sourceArray.size
              val targetArray = new Array[Any](sourceArraySize)
              var idx = 0
              while (idx < sourceArraySize) {
                targetArray(idx) = elementConverter(sourceArray(idx))
                idx += 1
              }
              targetArray
            }
          }
          case MapType(StringType, valueType, _) =>
          val valueConverter = createConverterToAvro(
            valueType,
            structName,
            SchemaConverters.getNewRecordNamespace(valueType, recordNamespace, structName))
          (item: Any) => {
            if (item == null) {
              null
            } else {
              val javaMap = new HashMap[String, Any]()
              item.asInstanceOf[Map[String, Any]].foreach {
                case (key, value) =>
                  javaMap.put(key, valueConverter(value))
              }
              javaMap
            }
          }
          case structType: StructType =>
          val builder = SchemaBuilder.record(structName).namespace(recordNamespace)
          val schema: Schema = SchemaConverters.convertStructToAvro(
            structType, builder, recordNamespace)
          val fieldConverters = structType.fields.map(field =>
            createConverterToAvro(
              field.dataType,
              field.name,
              SchemaConverters.getNewRecordNamespace(field.dataType, recordNamespace, field.name)))
          (item: Any) => {
            if (item == null) {
              null
            } else {
              val record = new Record(schema)
              val convertersIterator = fieldConverters.iterator
              val fieldNamesIterator = dataType.asInstanceOf[StructType].fieldNames.iterator
              val rowIterator = item.asInstanceOf[Row].toSeq.iterator

              while (convertersIterator.hasNext) {
                val converter = convertersIterator.next()
                record.put(fieldNamesIterator.next(), converter(rowIterator.next()))
              }
              record
            }
          }
      }
    }
    def getavrovalue(row: Row): GenericRecord =
      {
        return new AvroValue(converter(row).asInstanceOf[GenericRecord]).datum()
        
      }
  }

  def CustomConvertToAvro(parquetdf: DataFrame, dataSchema: StructType) {

    parquetdf.foreachPartition { partition =>
      {

        val props = new Properties()
        props.put("bootstrap.servers", "clouderavm01.centralindia.cloudapp.azure.com:9092")
        props.put("schema.registry.url", "http://clouderavm01.centralindia.cloudapp.azure.com:8081")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
        props.put("acks", "1")
        val producer = new KafkaProducer[String, GenericRecord](props)
        partition.foreach { row =>

          val customavrobinarywrite = new CustomAvroBinaryWriteFinal(dataSchema, "topLevelRecord", "")
          val avroValue = customavrobinarywrite.getavrovalue(row)

          val record = new ProducerRecord("avrotest5", "hello", avroValue)
          try {
            val ret = producer.send(record).get
            println("Message sent")
          } catch {

            case e: Exception => println(e.printStackTrace())
          }

        }
      }

    }

  }

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("ParquetToAvro").master("local[*]").getOrCreate()

    val parquetdf = spark.read.parquet("D:\\Users\\vignesh.i\\Documents\\climatedata_header_changed.parquet")
    val dataSchema = parquetdf.schema

    CustomConvertToAvro(parquetdf, dataSchema)

    spark.stop()

  }
}