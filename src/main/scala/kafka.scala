import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.values.KV
import org.joda.time.Duration
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

object KafkaBeamPipeline {
  def main(args: Array[String]): Unit = {
    val pipeline = Pipeline.create()

    val kafkaMessages = pipeline.apply(
      KafkaIO
        .read[String, String]()
        .withBootstrapServers("localhost:9092")
        .withTopic("my_topic")
        .withKeyDeserializer(classOf[StringDeserializer])
        .withValueDeserializer(classOf[StringDeserializer])
        .withoutMetadata()
    )

    // Extract values
    val extractedValues = kafkaMessages
      .apply("ExtractValues", ParDo.of(new ExtractValuesFn()))

    // Apply Fixed Windows with Triggers
    // val windowedValues = extractedValues
    //   .apply(
    //     "ApplyFixedWindowing",
    //     Window
    //       .into[String](FixedWindows.of(Duration.standardMinutes(2))) // Specify type explicitly
    //       .triggering(
    //         Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(10)))
    //       ) // Trigger every 10 seconds after processing starts
    //       .withAllowedLateness(Duration.ZERO) // No late data allowed
    //       .accumulatingFiredPanes() // Accumulate results in fired panes
    //   )
    val windowedValues = extractedValues
      .apply(
        "ApplyFixedWindowing",
        Window
          .into[String](
            FixedWindows.of(Duration.standardMinutes(2))
          ) // Specify type explicitly
          .triggering(
            Repeatedly.forever(
              AfterProcessingTime
                .pastFirstElementInPane()
                .plusDelayOf(Duration.standardSeconds(10))
            )
          ) // Trigger every 10 seconds after processing starts
          .withAllowedLateness(Duration.ZERO) // No late data allowed
          .discardingFiredPanes() // Discard panes after they are fired, to prevent duplication
      )

    // Write results to files
    windowedValues
      .apply(
        "WriteToFiles",
        TextIO
          .write()
          .to("output/windowed")
          .withWindowedWrites()
          .withSuffix(".txt")
      )

    pipeline.run().waitUntilFinish()
  }
}

class ExtractValuesFn extends DoFn[KV[String, String], String] {
  @ProcessElement
  def processElement(context: ProcessContext): Unit = {
    context.output(context.element().getValue)
  }
}
