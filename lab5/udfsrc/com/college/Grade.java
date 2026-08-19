package com.college;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class Grade extends EvalFunc<String> {
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null)
            return null;

        int marks = (Integer) input.get(0);

        if (marks >= 90) return "A";
        else if (marks >= 75) return "B";
        else return "C";
    }
}
