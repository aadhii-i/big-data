package com.college;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class IsTopper extends EvalFunc<Boolean> {

    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null)
            return false;

        int marks = (Integer) input.get(0);
        return marks > 90;
    }
}
