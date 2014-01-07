package de.dknguyen.lambda.pail;

import de.dknguyen.lambda.pail.ThriftPailStructure;
import de.dknguyen.lambda.thrift.Data;

public class DataPailStructure extends ThriftPailStructure<Data> {
    @Override
    protected Data createThriftObject() {
        return new Data();
    }

    @Override
    public Class getType() {
        return Data.class;
    }
}
