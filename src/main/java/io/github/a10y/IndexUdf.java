package io.github.a10y;

import org.apache.flink.table.functions.ScalarFunction;

public final class IndexUdf extends ScalarFunction {
    public String eval(String[] array, int index) {
        return array[index];
    }
}
