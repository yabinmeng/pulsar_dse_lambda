package main

import (
   "context"
   "fmt"
   "log"
   "os"
   "time"
   "io/ioutil"
   "github.com/apache/pulsar-client-go/pulsar"
)

func main() {
   fmt.Println("Loading Avro Schema Definition  ...")
   warningSensorAvroSchema, err := ioutil.ReadFile("</path/to/warning_sensor_data.avsc>")
   if err != nil {
       log.Fatal(err)
       fmt.Println("  > Failed to initiate Pulsar cluster!")
       os.Exit(10)
   }

   fmt.Println("Initiating Pulsar client ...")
   pulsarSvcURI := "pulsar://<pulsar_server_ip>:6650"
   pulsarTopic := "persistent://public/default/warning_sensor_data"
   pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{
      URL:                     pulsarSvcURI})
   if err != nil {
      log.Fatal(err)
      fmt.Println("  > Failed to initiate Pulsar cluster!")
      os.Exit(20)
   }

   defer pulsarClient.Close()

   fmt.Println("Creating Pulsar producer ...")
   sensorDataSchema := pulsar.NewAvroSchema(string(warningSensorAvroSchema), nil)
   producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
      Topic:  pulsarTopic,
      Schema: sensorDataSchema,
   })
   if err != nil || producer == nil {
      if err != nil {
         log.Fatal(err)
      }
      fmt.Println("  > Failed to create a Pulsar producer")
      os.Exit(30)
   }

   defer producer.Close()

   ctx := context.Background()

   fmt.Println("Publishing one testing message ...")
   type WarningSensorData struct {
      DrillID       string
      SensorID      string
      ReadingDate   time.Time
   	  ReadingTime   time.Time
   	  SensorType    string
   	  ReadingValue  float32
   }

   mydate, err := time.Parse("2006-01-02", "2021-04-05")
   mytime, err := time.Parse("2006-01-02T15:04:05.000Z", "2021-04-05T17:10:22")

   sensorData := WarningSensorData{
      DrillID: "DRL-001",
      SensorID: "SNS-temp-99",
      ReadingDate: mydate,
      ReadingTime: mytime,
      SensorType: "temperature",
      ReadingValue: 333}

   msg := pulsar.ProducerMessage{
      Value: sensorData,
   }

   // Attempt to send the message
   if _, err := producer.Send(ctx, &msg); err != nil {
      log.Fatal(err)
   } else {
      fmt.Printf("   > msg %s successfully published\n", string(msg.Payload))
   }
}