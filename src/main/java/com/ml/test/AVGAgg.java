package com.ml.test;

import org.apache.flink.api.common.functions.AggregateFunction;




public class AVGAgg implements AggregateFunction<TestDto,AverageAccumulator,Double> {
    @Override
    public AverageAccumulator createAccumulator() {
        return new AverageAccumulator();
    }

    @Override
    public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
        a.count += b.count;
        a.sum += b.sum;
        return a;
    }

    @Override
    public AverageAccumulator add(TestDto testDto, AverageAccumulator acc) {
        acc.sum += testDto.amount;
        acc.count++;
        return acc;
    }

    @Override
    public Double getResult(AverageAccumulator acc) {
        return acc.sum / (double) acc.count;
    }
}
