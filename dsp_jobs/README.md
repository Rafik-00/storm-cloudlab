
This project implements the following streaming queries in Apache Flink.

### [WordCount](https://github.com/pratyushagnihotri03/benchmarking_parallel_stream_processing/blob/old_git/flinkAdapter/src/main/java/WordCount/WordCount.java)

To execute this query you need the following parameters:
  - ```--parallelism```: The parallelism level
  - ```--mode```: Takes two values, either 'file' or 'kafka'. Depicts the source and input type. For 'file' the lines of the text file will be read, processed and the results will be written in a file. Otherwise, for 'kafka' the data arriving in a Kafka topic will be used as the input stream and after it has been processed, the results will be written in another Kafka topic.
  - ```--input```: Depicts the input file (in the case when 'mode'=='file') or the Kafka topic that will be used as input (in the case when 'mode'=='kafka').
  - ```--output```: Depicts the output folder (in the case when 'mode'=='file') or the Kafka topic that will be used as output (in the case when 'mode'=='kafka').
  - ```--kafka-server```: Depicts the address of the Kafka instance in the case when 'mode'=='kafka'. This parameter is not necessary if 'mode'=='file'.

  A sample command to start this query is the following:
  ```
  ./bin/flink run -c WordCount.WordCount <path to the compiled Jar file of this application> --parallelism 2 --mode kafka --input inputTopic --output outputTopic --kafka-server localhost:9092
  ```

### [SentimentAnalysis](https://github.com/pratyushagnihotri03/benchmarking_parallel_stream_processing/blob/old_git/flinkAdapter/src/main/java/SentimentAnalysis/SentimentAnalysis.java)

To execute this query you need the following parameters:
  - ```--parallelism```: The parallelism level
  - ```--mode```: Takes two values, either 'file' or 'kafka'. Depicts the source and input type. For 'file' the lines of the text file will be read, processed and the results will be written in a file. Otherwise, for 'kafka' the data arriving in a Kafka topic will be used as the input stream and after it has been processed, the results will be written in another Kafka topic.
  - ```--input```: Depicts the input file (in the case when 'mode'=='file') or the Kafka topic that will be used as input (in the case when 'mode'=='kafka').
  - ```--output```: Depicts the output folder (in the case when 'mode'=='file') or the Kafka topic that will be used as output (in the case when 'mode'=='kafka').
  - ```--kafka-server```: Depicts the address of the Kafka instance in the case when 'mode'=='kafka'. This parameter is not necessary if 'mode'=='file'.

  A sample command to start this query is the following:
  ```
  ./bin/flink run -c SentimentAnalysis.SentimentAnalysis <path to the compiled Jar file of this application> --parallelism 2 --mode kafka --input inputTopic --output outputTopic --kafka-server localhost:9092
```

### [AdsAnalytics](https://github.com/pratyushagnihotri03/benchmarking_parallel_stream_processing/blob/old_git/flinkAdapter/src/main/java/AdsAnalytics/AdsAnalytics.java):  

To execute this query you need the following parameters:
  - ```--parallelism```: The parallelism level
  - ```--mode```: Takes two values, either 'file' or 'kafka'. Depicts the source and input type. For 'file' the lines of the text file will be read, processed and the results will be written in a file. Otherwise, for 'kafka' the data arriving in a Kafka topic will be used as the input stream and after it has been processed, the results will be written in another Kafka topic.
  - ```--input```: Depicts the input file (in the case when 'mode'=='file') or the Kafka topic that will be used as input (in the case when 'mode'=='kafka').
  - ```--output```: Depicts the output folder (in the case when 'mode'=='file') or the Kafka topic that will be used as output (in the case when 'mode'=='kafka').
  - ```--kafka-server```: Depicts the address of the Kafka instance in the case when 'mode'=='kafka'. This parameter is not necessary if 'mode'=='file'.
  - ```--size```: This query uses processing time sliding window, the size of the sliding window in seconds is needed.
  - ```--slide```: This query uses processing time sliding window, the sliding size of the sliding window in seconds is needed.

  A sample command to start this query with a sliding window size of 5 seconds and window slide of 1 second is the following:
  ```
  ./bin/flink run -c AdsAnalytics.AdsAnalytics <path to the compiled Jar file of this application> --parallelism 2 --mode kafka --input inputTopic --output outputTopic --kafka-server localhost:9092 --size 5 --slide 1
```

### [SpikeDetection](https://github.com/pratyushagnihotri03/benchmarking_parallel_stream_processing/blob/old_git/flinkAdapter/src/main/java/SpikeDetection/SpikeDetection.java):  

To execute this query you need the following parameters:
  - ```--parallelism```: The parallelism level
  - ```--mode```: Takes two values, either 'file' or 'kafka'. Depicts the source and input type. For 'file' the lines of the text file will be read, processed and the results will be written in a file. Otherwise, for 'kafka' the data arriving in a Kafka topic will be used as the input stream and after it has been processed, the results will be written in another Kafka topic.
  - ```--input```: Depicts the input file (in the case when 'mode'=='file') or the Kafka topic that will be used as input (in the case when 'mode'=='kafka').
  - ```--output```: Depicts the output folder (in the case when 'mode'=='file') or the Kafka topic that will be used as output (in the case when 'mode'=='kafka').
  - ```--kafka-server```: Depicts the address of the Kafka instance in the case when 'mode'=='kafka'. This parameter is not necessary if 'mode'=='file'.
  - ```--size```: This query uses event time sliding window, the size of the sliding window in seconds is needed.
  - ```--slide```: This query uses event sliding window, the slding size of the sliding window in seconds is needed.
  - ```--lateness```: This query uses event sliding window, the time, in seconds, that the query will wait for results coming out of order is needed. This parameter is lateness.

  A sample command to start this query with a sliding window size of 5 seconds, window slide of 1 second and lateness of 10 seconds is the following:
  ```
  ./bin/flink run -c SpikeDetection.SpikeDetection <path to the compiled Jar file of this application> --parallelism 2 --mode kafka --input inputTopic --output outputTopic --kafka-server localhost:9092 --size 5 --slide 1 --lateness 10
```

### [SmartGrid](https://github.com/pratyushagnihotri03/benchmarking_parallel_stream_processing/blob/old_git/flinkAdapter/src/main/java/SmartGrid/SmartGrid.java)

  SmartGrid consists of three internal queries. To execute them the following parameters are needed:
  - ```--parallelism```: The parallelism level
  - ```--mode```: Takes two values, either 'file' or 'kafka'. Depicts the source and input type. For 'file' the lines of the text file will be read, processed and the results will be written in a file. Otherwise, for 'kafka' the data arriving in a Kafka topic will be used as the input stream and after it has been processed, the results will be written in another Kafka topic.
  - ```--input```: Depicts the input file (in the case when 'mode'=='file') or the Kafka topic that will be used as input (in the case when 'mode'=='kafka').
  - ```--output```: Depicts the output folder (in the case when 'mode'=='file') or the Kafka topic that will be used as output (in the case when 'mode'=='kafka').
  - ```--query```: Depicts the query which will be run. Takes one of the follwoing values '1', '2' or '3' for the first, second and third query respectively.
  - ```--kafka-server```: Depicts the address of the Kafka instance in the case when 'mode'=='kafka'. This parameter is not necessary if 'mode'=='file'.
  - ```--size```: This query uses event time sliding window, the size of the sliding window in seconds is needed.
  - ```--slide```: This query uses event sliding window, the slding size of the sliding window in seconds is needed.
  - ```--lateness```: This query uses event sliding window, the time, in seconds, that the query will wait for results coming out of order is needed. This parameter is lateness.

  A sample command to start the first query of SmartGrid with a sliding window size of 5 seconds, window slide of 1 second and lateness of 10 seconds is the following:
  ```
  ./bin/flink run -c SmartGrid.SmartGrid <path to the compiled Jar file of this application> --parallelism 2 --mode kafka --input inputTopic --output outputTopic --kafka-server localhost:9092 --query 1 --size 5 --slide 1 --lateness 10
```
  Another example Command
  ```
  ./bin/flink run -c com.kom.dsp.smartgrid.SmartGridJob /home/legion/Desktop/park/dsp_jobs/target/dsp_jobs-1.0-SNAPSHOT.jar -parallelism [1] -size 100 -slide 1 -mode kafka -input SmartGridIn -output SmartGridOut -query 1 -kafka-server localhost:9092

  ```
### [GoogleCloudMonitoring](https://github.com/pratyushagnihotri03/benchmarking_parallel_stream_processing/blob/old_git/flinkAdapter/src/main/java/GoogleCloudMonitoring/GoogleCloudMonitoring.java)

  To execute this query you need the following parameters:
  - ```--parallelism```: The parallelism level
  - ```--mode```: Takes two values, either 'file' or 'kafka'. Depicts the source and input type. For 'file' the lines of the text file will be read, processed and the results will be written in a file. Otherwise, for 'kafka' the data arriving in a Kafka topic will be used as the input stream and after it has been processed, the results will be written in another Kafka topic.
  - ```--input```: Depicts the input file (in the case when 'mode'=='file') or the Kafka topic that will be used as input (in the case when 'mode'=='kafka').
  - ```--output```: Depicts the output folder (in the case when 'mode'=='file') or the Kafka topic that will be used as output (in the case when 'mode'=='kafka').
  - ```--kafka-server```: Depicts the address of the Kafka instance in the case when 'mode'=='kafka'. This parameter is not necessary if 'mode'=='file'.
  - ```--size```: This query uses event time sliding window, the size of the sliding window in seconds is needed.
  - ```--slide```: This query uses event sliding window, the slding size of the sliding window in seconds is needed.
  - ```--lateness```: This query uses event sliding window, the time, in seconds, that the query will wait for results coming out of order is needed. This parameter is lateness.

  A sample command to start this query with a sliding window size of 5 seconds, window slide of 1 second and lateness of 10 seconds is the following:
  ```
  ./bin/flink run -c GoogleCloudMonitoring.GoogleCloudMonitoring <path to the compiled Jar file of this application> --parallelism 2 --mode kafka --input inputTopic --output outputTopic --kafka-server localhost:9092 --query 1 --size 5 --slide 1 --lateness 10
```

### [SyntheticQueryGenerator](https://github.com/pratyushagnihotri03/benchmarking_parallel_stream_processing/blob/old_git/flinkAdapter/src/main/java/SyntheticQueryGenerator/SyntheticDataGenerator.java)

  The SyntheticQueryGenerator consists of three templates.
  
  #### First template
  The following parameters are required to run a query of the first template:
  - ```--template```: Defines the template that will be used. Takes one of the following values 'template_1', 'template_2' or 'template_3'. For the first template it must be 'template_1'.
  - ```--firstFilter```: Is a boolean defining if the first filter should be applied or not.
  - ```--groupBy```: Is a boolean defining if a groupBy operator should be applied or not.
  - ```--secondFilter```: Is a boolean defining if the second filter should be applied or not.
  - ```--windowType```: Defines the window type to be used. Takes one of the following values 'duration' or 'count'.
  - ```--windowSize```: Defines the size of the window. If 'windowType'=='duration' then the window size is given in seconds.
  - ```--windowSlide```: Defines the slide of the window. If 'windowType'=='duration' then the window slide is given in seconds.
  - ```--integerRange```: Defines the maximal number of integers in the data tuple. It randomly chooses the number of integers between 1 and 'integerRange' (inclusive).
  - ```--doubleRange```: Defines the maximal number of doubles in the data tuple. It randomly chooses the number of doubles between 1 and 'doubleRange' (inclusive).
  - ```--stringRange```: Defines the maximal number of strings in the data tuple. It randomly chooses the number of strings between 1 and 'stringRange' (inclusive).
  - ```--eventRate```: The rate (events/s) at which the data tuples are produced.
  - ```--queryDuration```: Defines the run time of the query (in seconds). After that time has passed, the job is cancelled on Flink. Provide the value '-1' if you want the query to run indefinitely.
  - ```--operatorChaining```: Is a boolean enabling (true) or disabling (false) the operator chaining during the execution in Flink.
  - ```--parallelism```: The parallelism level

  A sample command to start a query of the first template is the following:
  ```
  ./bin/flink run -c SyntheticQueryGenerator.SyntheticDataGenerator <path to the compiled Jar file of this application> --template template_1 --firstFilter true --groupBy true --secondFilter true --windowType count --windowSize 7 --windowSlide 3 --integerRange 2 --doubleRange 5 --stringRange 7 --eventRate 20 --queryDuration -1 --operatorChaining true --parallelism 2
```

#### Second template
  The following parameters are required to run a query of the second template:
  - ```--template```: Defines the template that will be used. Takes one of the following values 'template_1', 'template_2' or 'template_3'. For the second template it must be 'template_2'.
  - ```--firstFilterFirstStream```: Is a boolean defining if the first filter should be applied in the first stream or not.
  - ```--firstFilterSecondStream```: Is a boolean defining if the first filter should be applied in the second stream or not.
  - ```--groupBy```: Is a boolean defining if a groupBy operator should be applied or not.
  - ```--joinWindowSize```: Defines the size of the joining window.
  - ```--joinWindowSlide```: Defines the slide of the joining window.
  - ```--secondFilter```: Is a boolean defining if the second filter should be applied or not.
  - ```--integerRangeFirstStream```: Defines the maximal number of integers in the data tuple of the first stream. It randomly chooses the number of integers between 1 and 'integerRangeFirstStream' (inclusive).
  - ```--doubleRangeFirstStream```: Defines the maximal number of doubles in the data tuple of the first stream. It randomly chooses the number of doubles between 1 and 'doubleRangeFirstStream' (inclusive).
  - ```--stringRangeFirstStream```: Defines the maximal number of strings in the data tuple of the first stream. It randomly chooses the number of strings between 1 and 'stringRangeFirstStream' (inclusive).
  - ```--integerRangeSecondStream```: Defines the maximal number of integers in the data tuple of the second stream. It randomly chooses the number of integers between 1 and 'integerRangeSecondStream' (inclusive).
  - ```--doubleRangeSecondStream```: Defines the maximal number of doubles in the data tuple of the second stream. It randomly chooses the number of doubles between 1 and 'doubleRangeSecondStream' (inclusive).
  - ```--stringRangeSecondStream```: Defines the maximal number of strings in the data tuple of the second stream. It randomly chooses the number of strings between 1 and 'stringRangeSecondStream' (inclusive).
  - ```--eventRateFirstStream```: The rate (events/s) at which the data tuples of the first stream are produced.
  - ```--eventRateSecondStream```: The rate (events/s) at which the data tuples of the second stream are produced.
  - ```--queryDuration```: Defines the run time of the query (in seconds). After that time has passed, the job is cancelled on Flink. Provide the value '-1' if you want the query to run indefinitely.
  - ```--operatorChaining```: Is a boolean enabling (true) or disabling (false) the operator chaining during the execution in Flink.
  - ```--parallelism```: The parallelism level

  A sample command to start a query of the second template is the following:
  ```
  ./bin/flink run -c SyntheticQueryGenerator.SyntheticDataGenerator <path to the compiled Jar file of this application> --template template_2 --firstFilterFirstStream true --firstFilterSecondStream true --groupBy true --joinWindowSize 7 --joinWindowSlide 3 --secondFilter true --integerRangeFirstStream 2 --doubleRangeFirstStream 5 --stringRangeFirstStream 7 --integerRangeSecondStream 5 --doubleRangeSecondStream 6 --stringRangeSecondStream 3 --eventRateFirstStream 30 --eventRateSecondStream 200 --queryDuration 120 --operatorChaining true --parallelism 2

```

#### Third template
  The following parameters are required to run a query of the third template:
  - ```--template```: Defines the template that will be used. Takes one of the following values 'template_1', 'template_2' or 'template_3'. For the third template it must be 'template_3'.
  - ```--firstFilterFirstStream```: Is a boolean defining if the first filter should be applied in the first stream or not.
  - ```--firstFilterSecondStream```: Is a boolean defining if the first filter should be applied in the second stream or not.
  - ```--firstFilterThirdStream```: Is a boolean defining if the first filter should be applied in the third stream or not.
  - ```--groupBy```: Is a boolean defining if a groupBy operator should be applied or not.
  - ```--joinWindowSize```: Defines the size of the joining window.
  - ```--joinWindowSlide```: Defines the slide of the joining window.
  - ```--secondFilter```: Is a boolean defining if the second filter should be applied or not.
  - ```--integerRangeFirstStream```: Defines the maximal number of integers in the data tuple of the first stream. It randomly chooses the number of integers between 1 and 'integerRangeFirstStream' (inclusive).
  - ```--doubleRangeFirstStream```: Defines the maximal number of doubles in the data tuple of the first stream. It randomly chooses the number of doubles between 1 and 'doubleRangeFirstStream' (inclusive).
  - ```--stringRangeFirstStream```: Defines the maximal number of strings in the data tuple of the first stream. It randomly chooses the number of strings between 1 and 'stringRangeFirstStream' (inclusive).
  - ```--integerRangeSecondStream```: Defines the maximal number of integers in the data tuple of the second stream. It randomly chooses the number of integers between 1 and 'integerRangeSecondStream' (inclusive).
  - ```--doubleRangeSecondStream```: Defines the maximal number of doubles in the data tuple of the second stream. It randomly chooses the number of doubles between 1 and 'doubleRangeSecondStream' (inclusive).
  - ```--stringRangeSecondStream```: Defines the maximal number of strings in the data tuple of the second stream. It randomly chooses the number of strings between 1 and 'stringRangeSecondStream' (inclusive).
  - ```--integerRangeThirdStream```: Defines the maximal number of integers in the data tuple of the third stream. It randomly chooses the number of integers between 1 and 'integerRangeThirdStream' (inclusive).
  - ```--doubleRangeThirdStream```: Defines the maximal number of doubles in the data tuple of the third stream. It randomly chooses the number of doubles between 1 and 'doubleRangeThirdStream' (inclusive).
  - ```--stringRangeThirdStream```: Defines the maximal number of strings in the data tuple of the third stream. It randomly chooses the number of strings between 1 and 'stringRangeThirdStream' (inclusive).
  - ```--eventRateFirstStream```: The rate (events/s) at which the data tuples of the first stream are produced.
  - ```--eventRateSecondStream```: The rate (events/s) at which the data tuples of the second stream are produced.
  - ```--eventRateThirdStream```: The rate (events/s) at which the data tuples of the third stream are produced.
  - ```--queryDuration```: Defines the run time of the query (in seconds). After that time has passed, the job is cancelled on Flink. Provide the value '-1' if you want the query to run indefinitely.
  - ```--operatorChaining```: Is a boolean enabling (true) or disabling (false) the operator chaining during the execution in Flink.
  - ```--parallelism```: The parallelism level

  A sample command to start a query of the third template is the following:
  ```
  ./bin/flink run -c SyntheticQueryGenerator.SyntheticDataGenerator <path to the compiled Jar file of this application> --template template_3 --firstFilterFirstStream true --firstFilterSecondStream true --firstFilterThirdStream true --groupBy true --joinWindowSize 7 --joinWindowSlide 3 --secondFilter true --integerRangeFirstStream 2 --doubleRangeFirstStream 5 --stringRangeFirstStream 7 --integerRangeSecondStream 5 --doubleRangeSecondStream 6 --stringRangeSecondStream 3 --integerRangeThirdStream 5 --doubleRangeThirdStream 6 --stringRangeThirdStream 3 --eventRateFirstStream 30 --eventRateSecondStream 200 --eventRateThirdStream 100 --queryDuration 120 --operatorChaining true --parallelism 2
```

Note: In the case when the the run time of the query is not indefinite (```--queryDuration != -1```), the metrics API of the Flink cluster will be crawled for each operator and sub-operator and the data will be saved in a log file when the execution of the job is terminated. For more details see [MetricsObserver.java](https://github.com/pratyushagnihotri03/benchmarking_parallel_stream_processing/blob/old_git/flinkAdapter/src/main/java/SyntheticQueryGenerator/MetricsObserver.java)
***
