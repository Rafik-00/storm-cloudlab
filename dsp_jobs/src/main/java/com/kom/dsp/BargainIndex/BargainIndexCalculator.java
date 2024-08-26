package com.kom.dsp.BargainIndex;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class BargainIndexCalculator implements FlatMapFunction<Tuple4<String, Double, Long, Double>, Tuple3<String, Double, Long>> {

    private double calculateBargainIndex(double quotePrice, double vwap) {
        if (quotePrice > vwap) {
            return quotePrice / vwap;
        } else {
            return 0;
        }
    }

    // input is (symbol, VWAP, totalVolume, quotePrice)
    @Override
    public void flatMap(Tuple4<String, Double, Long, Double> in, Collector<Tuple3<String, Double, Long>> collector) throws Exception {
        double bargainIndex = calculateBargainIndex(in.f3, in.f1); // Use in.f3 (quotePrice) and in.f1 (VWAP)
        collector.collect(new Tuple3<>(in.f0, bargainIndex, in.f2));
    }
}
