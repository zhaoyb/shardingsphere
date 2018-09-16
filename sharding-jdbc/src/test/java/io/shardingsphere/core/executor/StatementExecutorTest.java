/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
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
 * </p>
 */

package io.shardingsphere.core.executor;

import io.shardingsphere.core.constant.ConnectionMode;
import io.shardingsphere.core.constant.SQLType;
import io.shardingsphere.core.event.ShardingEventType;
import io.shardingsphere.core.executor.sql.execute.threadlocal.ExecutorExceptionHandler;
import io.shardingsphere.core.merger.QueryResult;
import io.shardingsphere.core.routing.RouteUnit;
import io.shardingsphere.core.routing.SQLUnit;
import org.junit.Test;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public final class StatementExecutorTest extends AbstractBaseExecutorTest {

    private static final String DQL_SQL = "SELECT * FROM table_x";
    
    private static final String DML_SQL = "DELETE FROM table_x";
    
    private StatementExecutor actual;
    
    @Override
    public void setUp() throws SQLException, ReflectiveOperationException {
        super.setUp();
        actual = new StatementExecutor(1, 1, 1, getConnection());
    }
    
    @Test
    public void assertNoStatement() throws SQLException, ReflectiveOperationException {
        assertFalse(actual.execute());
        assertThat(actual.executeUpdate(), is(0));
        assertThat(actual.executeQuery().size(), is(0));
    }
    
    @Test
    public void assertExecuteQueryForSingleStatementSuccess() throws SQLException, ReflectiveOperationException {
        Statement statement = mock(Statement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getInt(1)).thenReturn(1);
        when(statement.executeQuery(DQL_SQL)).thenReturn(resultSet);
        when(statement.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DQL);
        setExecuteGroups(Collections.singletonList(statement), SQLType.DQL);
        assertThat((int) actual.executeQuery().iterator().next().getValue(1, int.class), is(resultSet.getInt(1)));
        verify(statement).executeQuery(DQL_SQL);
        verify(getEventCaller(), times(2)).verifyIsParallelExecute(false);
        verify(getEventCaller(), times(0)).verifyException(null);
    }
    
    private void setSQLType(final SQLType sqlType) throws ReflectiveOperationException {
        Field field2 = StatementExecutor.class.getDeclaredField("sqlType");
        field2.setAccessible(true);
        field2.set(actual, sqlType);
    }
    
    private void setExecuteGroups(final List<Statement> statements, final SQLType sqlType) throws ReflectiveOperationException {
        Collection<ShardingExecuteGroup<StatementExecuteUnit>> executeGroups = new LinkedList<>();
        List<StatementExecuteUnit> statementExecuteUnits = new LinkedList<>();
        executeGroups.add(new ShardingExecuteGroup<>(statementExecuteUnits));
        for (Statement each : statements) {
            List<List<Object>> parameterSets = new LinkedList<>();
            String sql = SQLType.DQL.equals(sqlType) ? DQL_SQL : DML_SQL;
            parameterSets.add(Collections.singletonList((Object) 1));
            statementExecuteUnits.add(new StatementExecuteUnit(new RouteUnit("ds_0", new SQLUnit(sql, parameterSets)), each, ConnectionMode.MEMORY_STRICTLY));
        }
        Field field = StatementExecutor.class.getDeclaredField("executeGroups");
        field.setAccessible(true);
        field.set(actual, executeGroups);
    }

    @Test
    public void assertExecuteQueryForMultipleStatementsSuccess() throws SQLException, ReflectiveOperationException {
        Statement statement1 = mock(Statement.class);
        Statement statement2 = mock(Statement.class);
        ResultSet resultSet1 = mock(ResultSet.class);
        ResultSet resultSet2 = mock(ResultSet.class);
        when(resultSet1.getInt(1)).thenReturn(1);
        when(resultSet2.getInt(1)).thenReturn(2);
        when(statement1.executeQuery(DQL_SQL)).thenReturn(resultSet1);
        when(statement1.getConnection()).thenReturn(mock(Connection.class));
        when(statement2.executeQuery(DQL_SQL)).thenReturn(resultSet2);
        when(statement2.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DQL);
        setExecuteGroups(Arrays.asList(statement1, statement2), SQLType.DQL);
        List<QueryResult> result = actual.executeQuery();
        List<ResultSet> resultSets = Arrays.asList(resultSet1, resultSet2);
        for (int i = 0; i < result.size(); i++) {
            assertThat((int) result.get(i).getValue(1, int.class), is(resultSets.get(i).getInt(1)));
        }
        verify(statement1).executeQuery(DQL_SQL);
        verify(statement2).executeQuery(DQL_SQL);
        verify(getEventCaller(), times(2)).verifyIsParallelExecute(false);
        verify(getEventCaller(), times(0)).verifyException(null);
    }

    @Test
    public void assertExecuteQueryForSingleStatementFailure() throws SQLException, ReflectiveOperationException {
        Statement statement = mock(Statement.class);
        SQLException exp = new SQLException();
        when(statement.executeQuery(DQL_SQL)).thenThrow(exp);
        when(statement.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DQL);
        setExecuteGroups(Collections.singletonList(statement), SQLType.DQL);
        assertThat(actual.executeQuery(), is(Collections.singletonList((QueryResult) null)));
        verify(statement).executeQuery(DQL_SQL);
        verify(getEventCaller(), times(2)).verifyIsParallelExecute(false);
        verify(getEventCaller()).verifyException(exp);
    }

    @Test
    public void assertExecuteQueryForMultipleStatementsFailure() throws SQLException, ReflectiveOperationException {
        Statement statement1 = mock(Statement.class);
        Statement statement2 = mock(Statement.class);
        SQLException exp = new SQLException();
        when(statement1.executeQuery(DQL_SQL)).thenThrow(exp);
        when(statement2.executeQuery(DQL_SQL)).thenThrow(exp);
        when(statement1.getConnection()).thenReturn(mock(Connection.class));
        when(statement2.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DQL);
        setExecuteGroups(Arrays.asList(statement1, statement2), SQLType.DQL);
        List<QueryResult> actualResultSets = actual.executeQuery();
        assertThat(actualResultSets, is(Arrays.asList((QueryResult) null, null)));
        verify(statement1).executeQuery(DQL_SQL);
        verify(statement2).executeQuery(DQL_SQL);
        verify(getEventCaller(), times(4)).verifyDataSource("ds_0");
        verify(getEventCaller(), times(4)).verifySQL(DQL_SQL);
        verify(getEventCaller(), times(4)).verifyParameters(Collections.singletonList((Object) 1));
        verify(getEventCaller(), times(2)).verifyEventExecutionType(ShardingEventType.BEFORE_EXECUTE);
        verify(getEventCaller(), times(2)).verifyEventExecutionType(ShardingEventType.EXECUTE_FAILURE);
        verify(getEventCaller(), times(2)).verifyException(exp);
    }

    @Test
    public void assertExecuteUpdateForSingleStatementSuccess() throws SQLException, ReflectiveOperationException {
        Statement statement = mock(Statement.class);
        when(statement.executeUpdate(DML_SQL)).thenReturn(10);
        when(statement.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DML);
        setExecuteGroups(Collections.singletonList(statement), SQLType.DML);
        assertThat(actual.executeUpdate(), is(10));
        verify(statement).executeUpdate(DML_SQL);
        verify(getEventCaller(), times(2)).verifyDataSource("ds_0");
        verify(getEventCaller(), times(2)).verifySQL(DML_SQL);
        verify(getEventCaller(), times(2)).verifyParameters(Collections.singletonList((Object) 1));
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.BEFORE_EXECUTE);
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.EXECUTE_SUCCESS);
        verify(getEventCaller(), times(0)).verifyException(null);
    }

    @Test
    public void assertExecuteUpdateForMultipleStatementsSuccess() throws SQLException, ReflectiveOperationException {
        Statement statement1 = mock(Statement.class);
        Statement statement2 = mock(Statement.class);
        when(statement1.executeUpdate(DML_SQL)).thenReturn(10);
        when(statement2.executeUpdate(DML_SQL)).thenReturn(20);
        when(statement1.getConnection()).thenReturn(mock(Connection.class));
        when(statement2.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DML);
        setExecuteGroups(Arrays.asList(statement1, statement2), SQLType.DML);
        assertThat(actual.executeUpdate(), is(30));
        verify(statement1).executeUpdate(DML_SQL);
        verify(statement2).executeUpdate(DML_SQL);
        verify(getEventCaller(), times(4)).verifyDataSource("ds_0");
        verify(getEventCaller(), times(4)).verifySQL(DML_SQL);
        verify(getEventCaller(), times(4)).verifyParameters(Collections.singletonList((Object) 1));
        verify(getEventCaller(), times(2)).verifyEventExecutionType(ShardingEventType.BEFORE_EXECUTE);
        verify(getEventCaller(), times(2)).verifyEventExecutionType(ShardingEventType.EXECUTE_SUCCESS);
        verify(getEventCaller(), times(0)).verifyException(null);
    }

    @Test
    public void assertExecuteUpdateForSingleStatementFailure() throws SQLException, ReflectiveOperationException {
        Statement statement = mock(Statement.class);
        SQLException exp = new SQLException();
        when(statement.executeUpdate(DML_SQL)).thenThrow(exp);
        when(statement.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DML);
        setExecuteGroups(Collections.singletonList(statement), SQLType.DML);
        assertThat(actual.executeUpdate(), is(0));
        verify(statement).executeUpdate(DML_SQL);
        verify(getEventCaller(), times(2)).verifyDataSource("ds_0");
        verify(getEventCaller(), times(2)).verifySQL(DML_SQL);
        verify(getEventCaller(), times(2)).verifyParameters(Collections.singletonList((Object) 1));
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.BEFORE_EXECUTE);
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.EXECUTE_FAILURE);
        verify(getEventCaller()).verifyException(exp);
    }

    @Test
    public void assertExecuteUpdateForMultipleStatementsFailure() throws SQLException, ReflectiveOperationException {
        Statement statement1 = mock(Statement.class);
        Statement statement2 = mock(Statement.class);
        SQLException exp = new SQLException();
        when(statement1.executeUpdate(DML_SQL)).thenThrow(exp);
        when(statement2.executeUpdate(DML_SQL)).thenThrow(exp);
        when(statement1.getConnection()).thenReturn(mock(Connection.class));
        when(statement2.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DML);
        setExecuteGroups(Arrays.asList(statement1, statement2), SQLType.DML);
        assertThat(actual.executeUpdate(), is(0));
        verify(statement1).executeUpdate(DML_SQL);
        verify(statement2).executeUpdate(DML_SQL);
        verify(getEventCaller(), times(4)).verifyDataSource("ds_0");
        verify(getEventCaller(), times(4)).verifySQL(DML_SQL);
        verify(getEventCaller(), times(4)).verifyParameters(Collections.singletonList((Object) 1));
        verify(getEventCaller(), times(2)).verifyEventExecutionType(ShardingEventType.BEFORE_EXECUTE);
        verify(getEventCaller(), times(2)).verifyEventExecutionType(ShardingEventType.EXECUTE_FAILURE);
        verify(getEventCaller(), times(2)).verifyException(exp);
    }

    @Test
    public void assertExecuteUpdateWithAutoGeneratedKeys() throws SQLException, ReflectiveOperationException {
        Statement statement = mock(Statement.class);
        when(statement.executeUpdate(DML_SQL, Statement.NO_GENERATED_KEYS)).thenReturn(10);
        when(statement.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DML);
        setExecuteGroups(Collections.singletonList(statement), SQLType.DML);
        assertThat(actual.executeUpdate(Statement.NO_GENERATED_KEYS), is(10));
        verify(statement).executeUpdate(DML_SQL, Statement.NO_GENERATED_KEYS);
        verify(getEventCaller(), times(2)).verifyDataSource("ds_0");
        verify(getEventCaller(), times(2)).verifySQL(DML_SQL);
        verify(getEventCaller(), times(2)).verifyParameters(Collections.singletonList((Object) 1));
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.BEFORE_EXECUTE);
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.EXECUTE_SUCCESS);
        verify(getEventCaller(), times(0)).verifyException(null);
    }

    @Test
    public void assertExecuteUpdateWithColumnIndexes() throws SQLException, ReflectiveOperationException {
        Statement statement = mock(Statement.class);
        when(statement.executeUpdate(DML_SQL, new int[] {1})).thenReturn(10);
        when(statement.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DML);
        setExecuteGroups(Collections.singletonList(statement), SQLType.DML);
        assertThat(actual.executeUpdate(new int[] {1}), is(10));
        verify(statement).executeUpdate(DML_SQL, new int[] {1});
        verify(getEventCaller(), times(2)).verifyDataSource("ds_0");
        verify(getEventCaller(), times(2)).verifySQL(DML_SQL);
        verify(getEventCaller(), times(2)).verifyParameters(Collections.singletonList((Object) 1));
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.BEFORE_EXECUTE);
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.EXECUTE_SUCCESS);
        verify(getEventCaller(), times(0)).verifyException(null);
    }

    @Test
    public void assertExecuteUpdateWithColumnNames() throws SQLException, ReflectiveOperationException {
        Statement statement = mock(Statement.class);
        when(statement.executeUpdate(DML_SQL, new String[] {"col"})).thenReturn(10);
        when(statement.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DML);
        setExecuteGroups(Collections.singletonList(statement), SQLType.DML);
        assertThat(actual.executeUpdate(new String[] {"col"}), is(10));
        verify(statement).executeUpdate(DML_SQL, new String[] {"col"});
        verify(getEventCaller(), times(2)).verifyDataSource("ds_0");
        verify(getEventCaller(), times(2)).verifySQL(DML_SQL);
        verify(getEventCaller(), times(2)).verifyParameters(Collections.singletonList((Object) 1));
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.BEFORE_EXECUTE);
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.EXECUTE_SUCCESS);
        verify(getEventCaller(), times(0)).verifyException(null);
    }

    @Test
    public void assertExecuteForSingleStatementSuccessWithDML() throws SQLException, ReflectiveOperationException {
        Statement statement = mock(Statement.class);
        when(statement.execute(DML_SQL)).thenReturn(false);
        when(statement.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DML);
        setExecuteGroups(Collections.singletonList(statement), SQLType.DML);
        assertFalse(actual.execute());
        verify(statement).execute(DML_SQL);
        verify(getEventCaller(), times(2)).verifyDataSource("ds_0");
        verify(getEventCaller(), times(2)).verifySQL(DML_SQL);
        verify(getEventCaller(), times(2)).verifyParameters(Collections.singletonList((Object) 1));
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.BEFORE_EXECUTE);
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.EXECUTE_SUCCESS);
        verify(getEventCaller(), times(0)).verifyException(null);
    }

    @Test
    public void assertExecuteForMultipleStatementsSuccessWithDML() throws SQLException, ReflectiveOperationException {
        Statement statement1 = mock(Statement.class);
        Statement statement2 = mock(Statement.class);
        when(statement1.execute(DML_SQL)).thenReturn(false);
        when(statement2.execute(DML_SQL)).thenReturn(false);
        when(statement1.getConnection()).thenReturn(mock(Connection.class));
        when(statement2.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DML);
        setExecuteGroups(Arrays.asList(statement1, statement2), SQLType.DML);
        assertFalse(actual.execute());
        verify(statement1).execute(DML_SQL);
        verify(statement2).execute(DML_SQL);
        verify(getEventCaller(), times(4)).verifyDataSource("ds_0");
        verify(getEventCaller(), times(4)).verifySQL(DML_SQL);
        verify(getEventCaller(), times(4)).verifyParameters(Collections.singletonList((Object) 1));
        verify(getEventCaller(), times(2)).verifyEventExecutionType(ShardingEventType.BEFORE_EXECUTE);
        verify(getEventCaller(), times(2)).verifyEventExecutionType(ShardingEventType.EXECUTE_SUCCESS);
        verify(getEventCaller(), times(0)).verifyException(null);
    }

    @Test
    public void assertExecuteForSingleStatementFailureWithDML() throws SQLException, ReflectiveOperationException {
        Statement statement = mock(Statement.class);
        SQLException exp = new SQLException();
        when(statement.execute(DML_SQL)).thenThrow(exp);
        when(statement.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DML);
        setExecuteGroups(Collections.singletonList(statement), SQLType.DML);
        assertFalse(actual.execute());
        verify(statement).execute(DML_SQL);
        verify(getEventCaller(), times(2)).verifyDataSource("ds_0");
        verify(getEventCaller(), times(2)).verifySQL(DML_SQL);
        verify(getEventCaller(), times(2)).verifyParameters(Collections.singletonList((Object) 1));
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.BEFORE_EXECUTE);
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.EXECUTE_FAILURE);
        verify(getEventCaller()).verifyException(exp);
    }

    @Test
    public void assertExecuteForMultipleStatementsFailureWithDML() throws SQLException, ReflectiveOperationException {
        Statement statement1 = mock(Statement.class);
        Statement statement2 = mock(Statement.class);
        SQLException exp = new SQLException();
        when(statement1.execute(DML_SQL)).thenThrow(exp);
        when(statement2.execute(DML_SQL)).thenThrow(exp);
        when(statement1.getConnection()).thenReturn(mock(Connection.class));
        when(statement2.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DML);
        setExecuteGroups(Arrays.asList(statement1, statement2), SQLType.DML);
        assertFalse(actual.execute());
        verify(statement1).execute(DML_SQL);
        verify(statement2).execute(DML_SQL);
        verify(getEventCaller(), times(4)).verifyDataSource("ds_0");
        verify(getEventCaller(), times(4)).verifySQL(DML_SQL);
        verify(getEventCaller(), times(4)).verifyParameters(Collections.singletonList((Object) 1));
        verify(getEventCaller(), times(2)).verifyEventExecutionType(ShardingEventType.BEFORE_EXECUTE);
        verify(getEventCaller(), times(2)).verifyEventExecutionType(ShardingEventType.EXECUTE_FAILURE);
        verify(getEventCaller(), times(2)).verifyException(exp);
    }

    @Test
    public void assertExecuteForSingleStatementWithDQL() throws SQLException, ReflectiveOperationException {
        Statement statement = mock(Statement.class);
        when(statement.execute(DQL_SQL)).thenReturn(true);
        when(statement.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DQL);
        setExecuteGroups(Collections.singletonList(statement), SQLType.DQL);
        assertTrue(actual.execute());
        verify(statement).execute(DQL_SQL);
        verify(getEventCaller(), times(2)).verifyDataSource("ds_0");
        verify(getEventCaller(), times(2)).verifySQL(DQL_SQL);
        verify(getEventCaller(), times(2)).verifyParameters(Collections.singletonList((Object) 1));
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.BEFORE_EXECUTE);
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.EXECUTE_SUCCESS);
        verify(getEventCaller(), times(0)).verifyException(null);
    }

    @Test
    public void assertExecuteForMultipleStatements() throws SQLException, ReflectiveOperationException {
        Statement statement1 = mock(Statement.class);
        Statement statement2 = mock(Statement.class);
        when(statement1.execute(DQL_SQL)).thenReturn(true);
        when(statement2.execute(DQL_SQL)).thenReturn(true);
        when(statement1.getConnection()).thenReturn(mock(Connection.class));
        when(statement2.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DQL);
        setExecuteGroups(Arrays.asList(statement1, statement2), SQLType.DQL);
        assertTrue(actual.execute());
        verify(statement1).execute(DQL_SQL);
        verify(statement2).execute(DQL_SQL);
        verify(getEventCaller(), times(4)).verifyDataSource("ds_0");
        verify(getEventCaller(), times(4)).verifySQL(DQL_SQL);
        verify(getEventCaller(), times(4)).verifyParameters(Collections.singletonList((Object) 1));
        verify(getEventCaller(), times(2)).verifyEventExecutionType(ShardingEventType.BEFORE_EXECUTE);
        verify(getEventCaller(), times(2)).verifyEventExecutionType(ShardingEventType.EXECUTE_SUCCESS);
        verify(getEventCaller(), times(0)).verifyException(null);
    }

    @Test
    public void assertExecuteWithAutoGeneratedKeys() throws SQLException, ReflectiveOperationException {
        Statement statement = mock(Statement.class);
        when(statement.execute(DML_SQL, Statement.NO_GENERATED_KEYS)).thenReturn(false);
        when(statement.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DML);
        setExecuteGroups(Collections.singletonList(statement), SQLType.DML);
        setExecuteGroups(Collections.singletonList(statement), SQLType.DML);
        assertFalse(actual.execute(Statement.NO_GENERATED_KEYS));
        verify(statement).execute(DML_SQL, Statement.NO_GENERATED_KEYS);
        verify(getEventCaller(), times(2)).verifyDataSource("ds_0");
        verify(getEventCaller(), times(2)).verifySQL(DML_SQL);
        verify(getEventCaller(), times(2)).verifyParameters(Collections.singletonList((Object) 1));
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.BEFORE_EXECUTE);
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.EXECUTE_SUCCESS);
        verify(getEventCaller(), times(0)).verifyException(null);
    }

    @Test
    public void assertExecuteWithColumnIndexes() throws SQLException, ReflectiveOperationException {
        Statement statement = mock(Statement.class);
        when(statement.execute(DML_SQL, new int[] {1})).thenReturn(false);
        when(statement.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DML);
        setExecuteGroups(Collections.singletonList(statement), SQLType.DML);
        assertFalse(actual.execute(new int[] {1}));
        verify(statement).execute(DML_SQL, new int[] {1});
        verify(getEventCaller(), times(2)).verifyDataSource("ds_0");
        verify(getEventCaller(), times(2)).verifySQL(DML_SQL);
        verify(getEventCaller(), times(2)).verifyParameters(Collections.singletonList((Object) 1));
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.BEFORE_EXECUTE);
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.EXECUTE_SUCCESS);
        verify(getEventCaller(), times(0)).verifyException(null);
    }

    @Test
    public void assertExecuteWithColumnNames() throws SQLException, ReflectiveOperationException {
        Statement statement = mock(Statement.class);
        when(statement.execute(DML_SQL, new String[] {"col"})).thenReturn(false);
        when(statement.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DML);
        setExecuteGroups(Collections.singletonList(statement), SQLType.DML);
        assertFalse(actual.execute(new String[] {"col"}));
        verify(statement).execute(DML_SQL, new String[] {"col"});
        verify(getEventCaller(), times(2)).verifyDataSource("ds_0");
        verify(getEventCaller(), times(2)).verifySQL(DML_SQL);
        verify(getEventCaller(), times(2)).verifyParameters(Collections.singletonList((Object) 1));
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.BEFORE_EXECUTE);
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.EXECUTE_SUCCESS);
        verify(getEventCaller(), times(0)).verifyException(null);
    }

    @Test
    public void assertOverallExceptionFailure() throws SQLException, ReflectiveOperationException {
        ExecutorExceptionHandler.setExceptionThrown(true);
        Statement statement = mock(Statement.class);
        SQLException exp = new SQLException();
        when(statement.execute(DML_SQL)).thenThrow(exp);
        when(statement.getConnection()).thenReturn(mock(Connection.class));
        setSQLType(SQLType.DML);
        setExecuteGroups(Collections.singletonList(statement), SQLType.DML);
        try {
            assertFalse(actual.execute());
        } catch (final SQLException ignore) {
        }
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.BEFORE_EXECUTE);
        verify(getEventCaller()).verifyEventExecutionType(ShardingEventType.EXECUTE_FAILURE);
    }

}
