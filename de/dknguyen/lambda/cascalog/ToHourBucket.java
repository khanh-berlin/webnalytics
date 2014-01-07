package de.dknguyen.lambda.cascalog;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

public class ToHourBucket extends CascalogFunction {
        private static final int HOUR_SECS = 60 * 60;

        public void operate(FlowProcess process, FunctionCall call) {
            int timestamp = call.getArguments().getInteger(0);
            int hourBucket = timestamp / HOUR_SECS;
            call.getOutputCollector().add(new Tuple(hourBucket));
        }
}

