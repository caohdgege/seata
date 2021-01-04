/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.sqlparser.druid.postgresql;

import java.util.List;
import java.util.Objects;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.dialect.postgresql.visitor.PGOutputVisitor;
import io.seata.common.util.StringUtils;
import io.seata.sqlparser.ParametersHolder;
import io.seata.sqlparser.druid.BaseRecognizer;
import io.seata.sqlparser.struct.Null;

/**
 * @author will
 */
public abstract class BasePostgresqlRecognizer extends BaseRecognizer {

    /**
     * Instantiates a new postgresql base recognizer
     *
     * @param originalSql the original sql
     */
    public BasePostgresqlRecognizer(String originalSql) {
        super(originalSql);
    }

    public PGOutputVisitor createOutputVisitor(final ParametersHolder parametersHolder,
        final List<Object> paramAppenderList, final StringBuilder sb) {
        PGOutputVisitor visitor = new PGOutputVisitor(sb) {

            @Override
            public boolean visit(SQLVariantRefExpr x) {
                if ("?".equals(x.getName())) {
                    Object param = parametersHolder.getParameters().get(x.getIndex() + 1);
                    paramAppenderList.add(param instanceof Null ? null : param);

                }
                return super.visit(x);
            }
        };
        return visitor;
    }

    public String getWhereCondition(SQLExpr where, final ParametersHolder parametersHolder,
        final List<Object> paramAppenderList) {
        if (Objects.isNull(where)) {
            return StringUtils.EMPTY;
        }
        StringBuilder sb = new StringBuilder();
        executeVisit(where, createOutputVisitor(parametersHolder, paramAppenderList, sb));
        return sb.toString();
    }

    public String getWhereCondition(SQLExpr where) {
        if (Objects.isNull(where)) {
            return StringUtils.EMPTY;
        }

        StringBuilder sb = new StringBuilder();
        executeVisit(where, new PGOutputVisitor(sb));
        return sb.toString();
    }
}
