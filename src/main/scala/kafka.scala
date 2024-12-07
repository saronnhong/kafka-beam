// import org.apache.beam.sdk.Pipeline
// import org.apache.beam.sdk.io.kafka.KafkaIO
// import org.apache.beam.sdk.io.TextIO
// import org.apache.beam.sdk.transforms.{DoFn, ParDo}
// import org.apache.beam.sdk.transforms.windowing._
// import org.apache.beam.sdk.values.KV
// import org.joda.time.Duration
// import org.apache.kafka.common.serialization.StringDeserializer
// import org.apache.beam.sdk.transforms.DoFn.ProcessElement

// object KafkaBeamPipeline {
//   def main(args: Array[String]): Unit = {
//     val pipeline = Pipeline.create()

//     val kafkaMessages = pipeline.apply(
//       KafkaIO
//         .read[String, String]()
//         .withBootstrapServers("localhost:9092")
//         .withTopic("my_topic")
//         .withKeyDeserializer(classOf[StringDeserializer])
//         .withValueDeserializer(classOf[StringDeserializer])
//         .withoutMetadata()
//     )

//     // Extract values
//     val extractedValues = kafkaMessages
//       .apply("ExtractValues", ParDo.of(new ExtractValuesFn()))

//     val windowedValues = extractedValues
//       .apply(
//         "ApplyFixedWindowing",
//         Window
//           .into[String](
//             FixedWindows.of(Duration.standardMinutes(2))
//           ) // Specify type explicitly
//           .triggering(
//             Repeatedly.forever(
//               AfterProcessingTime
//                 .pastFirstElementInPane()
//                 .plusDelayOf(Duration.standardSeconds(10))
//             )
//           ) // Trigger every 10 seconds after processing starts
//           .withAllowedLateness(Duration.ZERO) // No late data allowed
//           .discardingFiredPanes() // Discard panes after they are fired, to prevent duplication
//       )

//     // Write results to files
//     windowedValues
//       .apply(
//         "WriteToFiles",
//         TextIO
//           .write()
//           .to("output/windowed")
//           .withWindowedWrites()
//           .withSuffix(".txt")
//       )

//     pipeline.run().waitUntilFinish()
//   }
// }

// // class ExtractValuesFn extends DoFn[KV[String, String], String] {
  
// //   @ProcessElement
// //   def processElement(context: ProcessContext): Unit = {
// //     val value = context.element().getValue
// //     val columns = value.split(",") // Assuming CSV format with comma separation

// //     // Define the expected number of columns in the CSV
// //     val expectedColumnCount = 22  // Replace with your actual expected number of columns

// //     // Check if the record has the expected number of columns
// //     if (columns.length == expectedColumnCount) {
// //       // If valid, output the data to the next stage in the pipeline
// //       context.output(value)
// //     } else {
// //       // If invalid, print it to the console for verification
// //       println(s"Invalid data: $value")  // Print invalid data to the console
// //     }
// //   }
// // }

// class ExtractValuesFn extends DoFn[KV[String, String], String] {
  
//   // This will track if the first row (header) has been processed
//   private var isFirstRow = true

//   @ProcessElement
//   def processElement(context: ProcessContext): Unit = {
//     val value = context.element().getValue
//     val columns = value.split(",") // Assuming CSV format with comma separation

//     // Define the expected number of columns in the CSV
//     val expectedColumnCount = 22  // Replace with your actual expected number of columns

//     // Skip the first row (header)
//     if (isFirstRow) {
//       isFirstRow = false
//       return // Skip this row
//     }

//     // Check if the record has the expected number of columns
//     if (columns.length == expectedColumnCount) {
//       // If valid, output the data to the next stage in the pipeline
//       context.output(value)
//     } else {
//       // If invalid, print it to the console for verification
//       println(s"Invalid data: $value")  // Print invalid data to the console
//     }
//   }
// }

//2nd version of working code
// import org.apache.beam.sdk.Pipeline
// import org.apache.beam.sdk.io.kafka.KafkaIO
// import org.apache.beam.sdk.io.TextIO
// import org.apache.beam.sdk.transforms.{DoFn, ParDo, Combine}
// import org.apache.beam.sdk.transforms.windowing._
// import org.apache.beam.sdk.values.{KV, PCollection}
// import org.joda.time.Duration
// import org.apache.kafka.common.serialization.StringDeserializer

// // Add this import for ProcessElement annotation
// import org.apache.beam.sdk.transforms.DoFn.ProcessElement

// // A case class to hold the sums of each column
// case class ColumnSums(sums: Array[Double])

// object KafkaBeamPipeline {
//   def main(args: Array[String]): Unit = {
//     val pipeline = Pipeline.create()

//     val kafkaMessages = pipeline.apply(
//       KafkaIO
//         .read[String, String]()
//         .withBootstrapServers("localhost:9092")
//         .withTopic("my_topic")
//         .withKeyDeserializer(classOf[StringDeserializer])
//         .withValueDeserializer(classOf[StringDeserializer])
//         .withoutMetadata()
//     )

//     // Extract values from Kafka messages
//     val extractedValues = kafkaMessages
//       .apply("ExtractValues", ParDo.of(new ExtractValuesFn()))

//     val windowedValues = extractedValues
//       .apply(
//         "ApplyFixedWindowing",
//         Window
//           .into[ColumnSums](FixedWindows.of(Duration.standardMinutes(2)))
//           .triggering(
//             Repeatedly.forever(
//               AfterProcessingTime
//                 .pastFirstElementInPane()
//                 .plusDelayOf(Duration.standardSeconds(10))
//             )
//           )
//           .withAllowedLateness(Duration.ZERO)
//           .discardingFiredPanes()
//       )

//     // Combine the results into a single PCollection of summed values
//     val columnSums = windowedValues
//       .apply("SumColumns", Combine.globally(new SumColumnsFn()).withoutDefaults())

//     // Convert ColumnSums to String for output
//     val columnSumsAsString = columnSums.apply("FormatColumnSums", ParDo.of(new FormatColumnSumsFn()))

//     // Write the aggregated result to a file
//     columnSumsAsString
//       .apply(
//         "WriteToFiles",
//         TextIO.write()
//           .to("output/windowed")
//           .withWindowedWrites()
//           .withSuffix(".txt")
//       )

//     pipeline.run().waitUntilFinish()
//   }
// }

// // A DoFn to extract and process the columns
// class ExtractValuesFn extends DoFn[KV[String, String], ColumnSums] {

//   private var isFirstRow = true

//   @ProcessElement
//   def processElement(context: ProcessContext): Unit = {
//     val value = context.element().getValue
//     val columns = value.split(",") // Assuming CSV format with comma separation

//     // Define the expected number of columns in the CSV
//     val expectedColumnCount = 22

//     // Skip the first row (header)
//     if (isFirstRow) {
//       isFirstRow = false
//       return // Skip this row
//     }

//     // Check if the record has the expected number of columns
//     if (columns.length == expectedColumnCount) {
//       // Convert each column to a double and create a ColumnSums object
//       val numericValues = columns.map(c => try {
//         c.toDouble
//       } catch {
//         case _: NumberFormatException => 0.0 // Handle invalid numeric values
//       })
      
//       // Output the column sums
//       context.output(ColumnSums(numericValues))
//     } else {
//       // If invalid, print it to the console for verification
//       println(s"Invalid data: $value")  // Print invalid data to the console
//     }
//   }
// }

// // A CombineFn to sum the columns
// class SumColumnsFn extends Combine.CombineFn[ColumnSums, Array[Double], ColumnSums] {

//   // Initialize the accumulator (array of zeros)
//   override def createAccumulator(): Array[Double] = Array.fill(22)(0.0)

//   // Add elements to the accumulator
//   override def addInput(accumulator: Array[Double], input: ColumnSums): Array[Double] = {
//     accumulator.zip(input.sums).map { case (acc, sum) => acc + sum }
//   }

//   // Merge accumulators
//   override def mergeAccumulators(accumulators: java.lang.Iterable[Array[Double]]): Array[Double] = {
//     accumulators.iterator().next() // Assuming only one accumulator per window
//   }

//   // Extract the result from the accumulator
//   override def extractOutput(accumulator: Array[Double]): ColumnSums = {
//     ColumnSums(accumulator)
//   }
// }

// // A DoFn to format ColumnSums as a String for output
// class FormatColumnSumsFn extends DoFn[ColumnSums, String] {
//   @ProcessElement
//   def processElement(context: ProcessContext): Unit = {
//     val columnSums = context.element()
//     // Convert the sums to a comma-separated string
//     val result = columnSums.sums.mkString(",")
//     context.output(result)
//   }
// }




import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.transforms.{DoFn, ParDo, Combine}
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.values.{KV, PCollection}
import org.joda.time.Duration
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.beam.sdk.transforms.DoFn.ProcessElement

// A case class to hold the sums of each column for 0 and 1 categories, while keeping the first column value
case class ColumnSums(firstColumn: Double, sumsForZero: Array[Double], sumsForOne: Array[Double])

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

    // Extract values from Kafka messages
    val extractedValues = kafkaMessages
      .apply("ExtractValues", ParDo.of(new ExtractValuesFn()))

    val windowedValues = extractedValues
      .apply(
        "ApplyFixedWindowing",
        Window
          .into[ColumnSums](FixedWindows.of(Duration.standardMinutes(3)))
          .triggering(
            Repeatedly.forever(
              AfterProcessingTime
                .pastFirstElementInPane()
                .plusDelayOf(Duration.standardSeconds(10))
            )
          )
          .withAllowedLateness(Duration.ZERO)
          .discardingFiredPanes()
      )

    // Combine the results into a single PCollection of summed values
    val columnSums = windowedValues
      .apply("SumColumns", Combine.globally(new SumColumnsFn()).withoutDefaults())

    // Convert ColumnSums to String for output
    val columnSumsAsString = columnSums.apply("FormatColumnSums", ParDo.of(new FormatColumnSumsFn()))

    // Write the aggregated result to a file
    columnSumsAsString
      .apply(
        "WriteToFiles",
        TextIO.write()
          .to("output/windowed")
          .withWindowedWrites()
          .withSuffix(".txt")
      )

    pipeline.run().waitUntilFinish()
  }
}

// A DoFn to extract and process the columns, keeping track of 0 and 1 in the first column
class ExtractValuesFn extends DoFn[KV[String, String], ColumnSums] {

  private var isFirstRow = true

  @ProcessElement
  def processElement(context: ProcessContext): Unit = {
    val value = context.element().getValue
    val columns = value.split(",") // Assuming CSV format with comma separation

    // Define the expected number of columns in the CSV
    val expectedColumnCount = 22

    // Skip the first row (header)
    if (isFirstRow) {
      isFirstRow = false
      return // Skip this row
    }

    // Check if the record has the expected number of columns
    if (columns.length == expectedColumnCount) {
      // Convert each column to a double and create a ColumnSums object
      val numericValues = columns.map(c => try {
        c.toDouble
      } catch {
        case _: NumberFormatException => 0.0 // Handle invalid numeric values
      })

      // Preserve the first column's value (0 or 1) and sum the other columns
      val firstColumnValue = columns(0).toDouble

      // Check the first column (index 0) to determine if it's 0 or 1
      if (firstColumnValue == 0) {
        // Create ColumnSums for when first column is 0
        context.output(ColumnSums(firstColumnValue, numericValues.tail, Array.fill(numericValues.length - 1)(0.0)))
      } else if (firstColumnValue == 1) {
        // Create ColumnSums for when first column is 1
        context.output(ColumnSums(firstColumnValue, Array.fill(numericValues.length - 1)(0.0), numericValues.tail))
      } else {
        // If first column is neither 0 nor 1, do nothing or handle error
        println(s"Unexpected value in first column: ${columns(0)}")
      }
    } else {
      // If invalid, print it to the console for verification
      println(s"Invalid data: $value")  // Print invalid data to the console
    }
  }
}

// A CombineFn to sum the columns separately for 0 and 1 categories
class SumColumnsFn extends Combine.CombineFn[ColumnSums, (Array[Double], Array[Double]), ColumnSums] {

  // Initialize the accumulator (array of zeros for both sets)
  override def createAccumulator(): (Array[Double], Array[Double]) = (Array.fill(22)(0.0), Array.fill(22)(0.0))

  // Add elements to the accumulator
  override def addInput(accumulator: (Array[Double], Array[Double]), input: ColumnSums): (Array[Double], Array[Double]) = {
    val (sumsForZero, sumsForOne) = accumulator
    val newSumsForZero = sumsForZero.zip(input.sumsForZero).map { case (acc, sum) => acc + sum }
    val newSumsForOne = sumsForOne.zip(input.sumsForOne).map { case (acc, sum) => acc + sum }
    (newSumsForZero, newSumsForOne)
  }

  // Merge accumulators
  override def mergeAccumulators(accumulators: java.lang.Iterable[(Array[Double], Array[Double])]): (Array[Double], Array[Double]) = {
    val iter = accumulators.iterator()
    val (firstZeroSums, firstOneSums) = iter.next()
    val (secondZeroSums, secondOneSums) = iter.next()

    val mergedZeroSums = firstZeroSums.zip(secondZeroSums).map { case (sum1, sum2) => sum1 + sum2 }
    val mergedOneSums = firstOneSums.zip(secondOneSums).map { case (sum1, sum2) => sum1 + sum2 }

    (mergedZeroSums, mergedOneSums)
  }

  // Extract the result from the accumulator
  override def extractOutput(accumulator: (Array[Double], Array[Double])): ColumnSums = {
    ColumnSums(0, accumulator._1, accumulator._2)  // First column is not summed, so it remains 0
  }
}

// A DoFn to format ColumnSums as a String for output
class FormatColumnSumsFn extends DoFn[ColumnSums, String] {
  @ProcessElement
  def processElement(context: ProcessContext): Unit = {
    val columnSums = context.element()
    // Convert the sums to a comma-separated string (for both sets: 0 and 1)
    val result = s"First Column: ${columnSums.firstColumn} | Sum for 0: ${columnSums.sumsForZero.mkString(",")} | Sum for 1: ${columnSums.sumsForOne.mkString(",")}"
    context.output(result)
  }
}
