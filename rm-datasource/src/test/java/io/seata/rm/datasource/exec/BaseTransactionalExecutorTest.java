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
package io.seata.rm.datasource.exec;

import io.seata.common.exception.NotSupportYetException;
import io.seata.rm.GlobalLockTemplate;
import io.seata.rm.datasource.ConnectionContext;
import io.seata.rm.datasource.ConnectionProxy;
import io.seata.rm.datasource.PreparedStatementProxy;
import io.seata.rm.datasource.StatementProxy;
import io.seata.rm.datasource.exec.mysql.MySQLInsertExecutor;
import io.seata.rm.datasource.exec.oracle.OracleInsertExecutor;
import io.seata.rm.datasource.sql.struct.Field;
import io.seata.rm.datasource.sql.struct.TableMeta;
import io.seata.rm.datasource.sql.struct.TableRecords;
import io.seata.sqlparser.SQLInsertRecognizer;
import io.seata.sqlparser.SQLRecognizer;
import io.seata.sqlparser.util.JdbcConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class BaseTransactionalExecutorTest {

    private ConnectionProxy connectionProxy;

    private AbstractDMLBaseExecutor executor;

    private java.lang.reflect.Field branchRollbackFlagField;

    @BeforeEach
    public void initBeforeEach() throws Exception {
        branchRollbackFlagField = ConnectionProxy.LockRetryPolicy.class.getDeclaredField("LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT");
        java.lang.reflect.Field modifiersField = java.lang.reflect.Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(branchRollbackFlagField, branchRollbackFlagField.getModifiers() & ~Modifier.FINAL);
        branchRollbackFlagField.setAccessible(true);
        boolean branchRollbackFlag = (boolean) branchRollbackFlagField.get(null);
        Assertions.assertTrue(branchRollbackFlag);

        Connection targetConnection = Mockito.mock(Connection.class);
        connectionProxy = Mockito.mock(ConnectionProxy.class);
        Mockito.doThrow(new LockConflictException())
                .when(connectionProxy).commit();
        Mockito.when(connectionProxy.getAutoCommit())
                .thenReturn(Boolean.TRUE);
        Mockito.when(connectionProxy.getTargetConnection())
                .thenReturn(targetConnection);
        Mockito.when(connectionProxy.getContext())
                .thenReturn(new ConnectionContext());
        PreparedStatementProxy statementProxy = Mockito.mock(PreparedStatementProxy.class);
        Mockito.when(statementProxy.getConnectionProxy())
                .thenReturn(connectionProxy);
        StatementCallback statementCallback = Mockito.mock(StatementCallback.class);
        SQLInsertRecognizer sqlInsertRecognizer = Mockito.mock(SQLInsertRecognizer.class);
        TableMeta tableMeta = Mockito.mock(TableMeta.class);
        executor = Mockito.spy(new MySQLInsertExecutor(statementProxy, statementCallback, sqlInsertRecognizer));
        Mockito.doReturn(tableMeta)
                .when(executor).getTableMeta();
        TableRecords tableRecords = new TableRecords();
        Mockito.doReturn(tableRecords)
                .when(executor).beforeImage();
        Mockito.doReturn(tableRecords)
                .when(executor).afterImage(tableRecords);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testExecuteWithGlobalLockSet() throws Exception {

        //initial objects
        ConnectionProxy connectionProxy = new ConnectionProxy(null, null);
        StatementProxy statementProxy = new StatementProxy<>(connectionProxy, null);

        BaseTransactionalExecutor<Object, Statement> baseTransactionalExecutor
                = new BaseTransactionalExecutor<Object, Statement>(statementProxy, null, (SQLRecognizer) null) {
            @Override
            protected Object doExecute(Object... args) {
                return null;
            }

            @Override
            protected Object executeAutoCommitFalse(Object[] args) throws Exception {
                return null;
            }

            @Override
            protected TableRecords afterImage(TableRecords beforeImage) throws SQLException {
                return null;
            }

            @Override
            protected TableRecords beforeImage() throws SQLException {
                return null;
            }
        };
        GlobalLockTemplate<Object> globalLockLocalTransactionalTemplate = new GlobalLockTemplate<>();

        // not in global lock context
        try {
            baseTransactionalExecutor.execute(new Object());
            Assertions.assertFalse(connectionProxy.isGlobalLockRequire(), "conectionContext set!");
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        //in global lock context
        globalLockLocalTransactionalTemplate.execute(() -> {
            try {
                baseTransactionalExecutor.execute(new Object());
                Assertions.assertTrue(connectionProxy.isGlobalLockRequire(), "conectionContext not set!");
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
            return null;
        });

    }

    @Test
    public void testBuildLockKey() {
        //build expect data
        String tableName = "test_name";
        String fieldOne = "1";
        String fieldTwo = "2";
        String split1 = ":";
        String split2 = ",";
        String pkColumnName="id";
        //test_name:1,2
        String buildLockKeyExpect = tableName + split1 + fieldOne + split2 + fieldTwo;
        // mock field
        Field field1 = mock(Field.class);
        when(field1.getValue()).thenReturn(fieldOne);
        Field field2 = mock(Field.class);
        when(field2.getValue()).thenReturn(fieldTwo);
        List<Map<String,Field>> pkRows =new ArrayList<>();
        pkRows.add(Collections.singletonMap(pkColumnName, field1));
        pkRows.add(Collections.singletonMap(pkColumnName, field2));

        // mock tableMeta
        TableMeta tableMeta = mock(TableMeta.class);
        when(tableMeta.getTableName()).thenReturn(tableName);
        when(tableMeta.getPrimaryKeyOnlyName()).thenReturn(Arrays.asList(new String[]{pkColumnName}));
        // mock tableRecords
        TableRecords tableRecords = mock(TableRecords.class);
        when(tableRecords.getTableMeta()).thenReturn(tableMeta);
        when(tableRecords.size()).thenReturn(pkRows.size());
        when(tableRecords.pkRows()).thenReturn(pkRows);
        // mock executor
        BaseTransactionalExecutor executor = mock(BaseTransactionalExecutor.class);
        when(executor.buildLockKey(tableRecords)).thenCallRealMethod();
        when(executor.getTableMeta()).thenReturn(tableMeta);
        assertThat(executor.buildLockKey(tableRecords)).isEqualTo(buildLockKeyExpect);
    }

    @Test
    public void testBuildLockKeyWithMultiPk() {
        //build expect data
        String tableName = "test_name";
        String pkOneValue1 = "1";
        String pkOneValue2 = "2";
        String pkTwoValue1 = "one";
        String pkTwoValue2 = "two";
        String split1 = ":";
        String split2 = ",";
        String split3 = "_";
        String pkOneColumnName="id";
        String pkTwoColumnName="userId";
        //test_name:1_one,2_two
        String buildLockKeyExpect = tableName + split1 + pkOneValue1+ split3 + pkTwoValue1  + split2 + pkOneValue2 + split3 + pkTwoValue2;
        // mock field
        Field pkOneField1 = mock(Field.class);
        when(pkOneField1.getValue()).thenReturn(pkOneValue1);
        Field pkOneField2 = mock(Field.class);
        when(pkOneField2.getValue()).thenReturn(pkOneValue2);
        Field pkTwoField1 = mock(Field.class);
        when(pkTwoField1.getValue()).thenReturn(pkTwoValue1);
        Field pkTwoField2 = mock(Field.class);
        when(pkTwoField2.getValue()).thenReturn(pkTwoValue2);
        List<Map<String,Field>> pkRows =new ArrayList<>();
        Map<String, Field> row1 = new HashMap<String, Field>() {{
            put(pkOneColumnName, pkOneField1);
            put(pkTwoColumnName, pkTwoField1);
        }};
        pkRows.add(row1);
        Map<String, Field> row2 = new HashMap<String, Field>() {{
            put(pkOneColumnName, pkOneField2);
            put(pkTwoColumnName, pkTwoField2);
        }};
        pkRows.add(row2);

        // mock tableMeta
        TableMeta tableMeta = mock(TableMeta.class);
        when(tableMeta.getTableName()).thenReturn(tableName);
        when(tableMeta.getPrimaryKeyOnlyName()).thenReturn(Arrays.asList(new String[]{pkOneColumnName,pkTwoColumnName}));
        // mock tableRecords
        TableRecords tableRecords = mock(TableRecords.class);
        when(tableRecords.getTableMeta()).thenReturn(tableMeta);
        when(tableRecords.size()).thenReturn(pkRows.size());
        when(tableRecords.pkRows()).thenReturn(pkRows);
        // mock executor
        BaseTransactionalExecutor executor = mock(BaseTransactionalExecutor.class);
        when(executor.buildLockKey(tableRecords)).thenCallRealMethod();
        when(executor.getTableMeta()).thenReturn(tableMeta);
        assertThat(executor.buildLockKey(tableRecords)).isEqualTo(buildLockKeyExpect);
    }

    @Test
    public void testLockRetryPolicyRollbackOnConflict() throws Exception {
        boolean oldBranchRollbackFlag = (boolean) branchRollbackFlagField.get(null);
        branchRollbackFlagField.set(null, true);
        Assertions.assertThrows(LockWaitTimeoutException.class, executor::execute);
        Mockito.verify(connectionProxy.getTargetConnection(), Mockito.atLeastOnce())
                .rollback();
        Mockito.verify(connectionProxy, Mockito.never()).rollback();
        branchRollbackFlagField.set(null, oldBranchRollbackFlag);
    }

    @Test
    public void testLockRetryPolicyNotRollbackOnConflict() throws Throwable {
        boolean oldBranchRollbackFlag = (boolean) branchRollbackFlagField.get(null);
        branchRollbackFlagField.set(null, false);
        Assertions.assertThrows(LockConflictException.class, executor::execute);
        Mockito.verify(connectionProxy.getTargetConnection(), Mockito.times(1)).rollback();
        Mockito.verify(connectionProxy, Mockito.never()).rollback();
        branchRollbackFlagField.set(null, oldBranchRollbackFlag);
    }

    @Test
    public void testOnlySupportMysqlWhenUseMultiPk(){
        Mockito.when(connectionProxy.getContext())
                .thenReturn(new ConnectionContext());
        PreparedStatementProxy statementProxy = Mockito.mock(PreparedStatementProxy.class);
        Mockito.when(statementProxy.getConnectionProxy())
                .thenReturn(connectionProxy);
        StatementCallback statementCallback = Mockito.mock(StatementCallback.class);
        SQLInsertRecognizer sqlInsertRecognizer = Mockito.mock(SQLInsertRecognizer.class);
        TableMeta tableMeta = Mockito.mock(TableMeta.class);
        executor = Mockito.spy(new OracleInsertExecutor(statementProxy, statementCallback, sqlInsertRecognizer));
        Mockito.when(executor.getDbType()).thenReturn(JdbcConstants.ORACLE);
        Mockito.doReturn(tableMeta).when(executor).getTableMeta();
        Mockito.when(tableMeta.getPrimaryKeyOnlyName()).thenReturn(Arrays.asList("id","userCode"));
        Assertions.assertThrows(NotSupportYetException.class,()-> executor.executeAutoCommitFalse(null));
    }
}
