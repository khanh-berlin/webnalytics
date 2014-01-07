package de.dknguyen.lambda.cascalog;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

import java.net.MalformedURLException;
import java.net.URL;

import de.dknguyen.lambda.thrift.Data;
import de.dknguyen.lambda.thrift.DataUnit;
import de.dknguyen.lambda.thrift.PageID;
import org.apache.thrift.TUnion;


public class NormalizeURL extends CascalogFunction {
    public void operate(FlowProcess process, FunctionCall call) {
        Data data = ((Data) call.getArguments()
                .getObject(0)).deepCopy();
        DataUnit du = data.getDataunit();
        if(du.getSetField() == DataUnit._Fields.PAGE_VIEW) {
            normalize(du.getPage_view().getPage());
        } else if(du.getSetField() ==
                DataUnit._Fields.PAGE_PROPERTY) {
            normalize(du.getPage_property().getId());
        }
        call.getOutputCollector().add(new Tuple(data));
    }

    private void normalize(PageID page) {
        if(page.getSetField() == PageID._Fields.URL) {
            String urlStr = page.getUrl();
            try {
                URL url = new URL(urlStr);
                page.setUrl(url.getProtocol() + "://" +
                        url.getHost() + url.getPath());
            } catch(MalformedURLException e) {
            }
        }
    }
}

