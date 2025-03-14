package com.mahfooz.spark.sqlserver.parameter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class Parameter {
    private String key;
    private String value;
    private String format;

}
