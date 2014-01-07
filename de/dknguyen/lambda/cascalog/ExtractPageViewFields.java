package de.dknguyen.lambda.cascalog;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import de.dknguyen.lambda.thrift.Data;
import de.dknguyen.lambda.thrift.PageID;
import de.dknguyen.lambda.thrift.PageViewEdge;

public class ExtractPageViewFields extends CascalogFunction {

        public void operate(FlowProcess process, FunctionCall call) {
            Data data = (Data) call.getArguments().getObject(0);
            PageViewEdge pageview = data.getDataunit()
                    .getPage_view();
            if(pageview.getPage().getSetField() ==
                    PageID._Fields.URL) {
                call.getOutputCollector().add(new Tuple(
                        pageview.getPage().getUrl(),
                        pageview.getPerson(),
                        data.getPedigree().getTrue_as_of_secs()
                ));
            }
        }
}
