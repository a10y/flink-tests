package io.github.a10y;

import org.apache.flink.table.functions.ScalarFunction;

public final class SplitUdf extends ScalarFunction {
    public String[] eval(String line, String separator) {
        return line.split(",");
    }
}
