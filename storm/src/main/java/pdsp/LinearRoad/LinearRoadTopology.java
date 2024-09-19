package pdsp.LinearRoad;

import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import pdsp.common.AbstractTopology;
import pdsp.common.LoggerBolt;


public class LinearRoadTopology extends AbstractTopology {

    int operationSpecification;

    public LinearRoadTopology(String topologyName, String mode, String filePath, String kafkaTopic, int operationSpecification, pdsp.config.Config config) {
        super(topologyName, mode, filePath, kafkaTopic, config);

        this.operationSpecification = operationSpecification;
    }

    @Override
    protected void buildTopology() {

        int vehicleEventParserParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int calculateAverageSpeedPerSegmentParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int accidentDetectionParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int calculateTollParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int createTollNotificationParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int dailyExpenditureCalculatorParallelism = this.parallelismEnumerator.getRandomParallelismHint();


        this.parallelism = (int) Math.round((vehicleEventParserParallelism + calculateAverageSpeedPerSegmentParallelism + accidentDetectionParallelism + calculateTollParallelism + createTollNotificationParallelism + dailyExpenditureCalculatorParallelism) / 6.0);
        System.out.println("Parallelism in TOPOLOGY: " + this.parallelism);

        BaseRichSpout spout = getSpout();

        // Define the topology
        builder.setSpout("source-spout", spout,parallelism);
        builder.setBolt("vehicleEvent-parser-bolt", new VehicleEventParserBolt(), vehicleEventParserParallelism )
                .shuffleGrouping("source-spout");
        builder.setBolt("calculate-AverageSpeed-PerSegment-bolt", new CalculateAverageSpeedPerSegmentBolt(), calculateAverageSpeedPerSegmentParallelism)
                .shuffleGrouping("vehicleEvent-parser-bolt");

        // 1 = Accidentdetection
        if (operationSpecification == 1) {
            //if more than one vehicle is at the same place at the same time there is an accident
            builder.setBolt("accident-detection-bolt", new AccidentDetectionBolt(), accidentDetectionParallelism)
                    .fieldsGrouping("vehicleEvent-parser-bolt", new Fields("xway", "segment", "lane", "position", "dow", "tod", "day", "time"));

            builder.setBolt("logger-bolt", new LoggerBolt(), 1)
                    .shuffleGrouping("accident-detection-bolt");


        }


        // 2 = calculate toll & emit toll notification
        if (operationSpecification == 2) {
            // Bolt which calculates tollPerSegment from  calculateAverageSpeedperSegment
            builder.setBolt("calculate-Toll-Bolt", new CalculateTollBolt(), calculateTollParallelism)
                    .fieldsGrouping("calculate-AverageSpeed-PerSegment-bolt", new Fields("segmentID"));

            builder.setBolt("create-toll-notification", new CreateTollNotificationBolt(), createTollNotificationParallelism)
                    .fieldsGrouping("calculate-Toll-Bolt", new Fields("segmentID"))
                    .fieldsGrouping("vehicleEvent-parser-bolt", new Fields("xway", "segment"));

            builder.setBolt("logger-bolt", new LoggerBolt(), 1)
                    .shuffleGrouping("create-toll-notification");


        }

        // calculate Daily Expenditure
        if (operationSpecification == 3) {

            // Bolt which calculates tollPerSegment from  calculateAverageSpeedperSegment
            builder.setBolt("calculate-Toll-Bolt", new CalculateTollBolt(), calculateTollParallelism)
                    .fieldsGrouping("calculate-AverageSpeed-PerSegment-bolt", new Fields("segmentID"));

            builder.setBolt("daily-expenditure-bolt", new DailyExpenditureCalculatorBolt(), dailyExpenditureCalculatorParallelism)
                    .fieldsGrouping("calculate-Toll-Bolt", new Fields("segmentID"))
                    .fieldsGrouping("vehicleEvent-parser-bolt", new Fields("xway", "segment"));

            builder.setBolt("logger-bolt", new LoggerBolt(), 1)
                    .shuffleGrouping("daily-expenditure-bolt");


        }


        // 4 = vehicleReport
        if (operationSpecification == 4) {


            builder.setBolt("calculate-time-travelled-bolt", new CalculateTimeTravelledBolt()).shuffleGrouping("vehicleEvent-parser-bolt");


            builder.setBolt("logger-bolt", new LoggerBolt(), 1)
                    .shuffleGrouping("calculate-time-travelled-bolt");


        }

    }
}
