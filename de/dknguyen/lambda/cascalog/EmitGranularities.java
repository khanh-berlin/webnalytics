package de.dknguyen.lambda.cascalog;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

public class EmitGranularities extends CascalogFunction {
        public void operate(FlowProcess process, FunctionCall call) {
            int hourBucket = call.getArguments().getInteger(0);
            int dayBucket = hourBucket / 24;
            int weekBucket = dayBucket / 7;
            int monthBucket = dayBucket / 28;
            call.getOutputCollector().add(new Tuple("h", hourBucket));
            call.getOutputCollector().add(new Tuple("d", dayBucket));
            call.getOutputCollector().add(new Tuple("w", weekBucket));
            call.getOutputCollector().add(new Tuple("m", monthBucket));
        }
    }

