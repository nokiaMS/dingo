/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.scalar.DecimalType;
import lombok.Getter;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

@Getter
@JsonTypeName("values")
@JsonPropertyOrder({"tuples"})
public class ValuesParam extends SourceParam {

    private final List<Object[]> tuples;
    @JsonProperty("schema")
    private final DingoType schema;

    public ValuesParam(List<Object[]> tuples, DingoType schema) {
        List<Integer> decimalFiledPos = new ArrayList<Integer>();
        for (int i = 0; i < schema.fieldCount(); i++) {
            if (schema.getChild(i) instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) schema.getChild(i);
                if (decimalType.getScale() == 0) {
                    decimalFiledPos.add(i);
                }
            }
        }

        if (!decimalFiledPos.isEmpty()) {
            for (int i = 0; i < tuples.size(); i++) {
                Object[] tuple = tuples.get(i);
                for(int j = 0; j < decimalFiledPos.size(); j++) {
                    int ind =  decimalFiledPos.get(j);
                    tuple[ind] = ((BigDecimal) tuple[ind]).setScale(0, RoundingMode.HALF_UP);
                }
            }
        }

        this.tuples = tuples;
        this.schema = schema;
    }

}
