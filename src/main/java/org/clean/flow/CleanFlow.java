package org.clean.flow;

import org.clean.flow.core.CatalogPool;
import org.clean.flow.core.Consumer;
import org.clean.flow.core.Producer;
import org.clean.flow.entity.*;
import org.clean.flow.util.CleanFlowUtils;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

public class CleanFlow {

    private Config config;
    private CleanFlowUtils utils;
    private Vector<String> isDateTime;
    private ExecutorService executorService;
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String RESULT_NUM = "RESULT_NUM";
    private static final String RESULT_DATE = "RESULT_DATE";
    private static final String RESULT_STR = "RESULT_STR";
    private static final String RES_ID = "RES_ID";
    private static final String SITE_NO = "SITE_NO";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public CleanFlow() {
        config = Config.getInstance().installConfig();
        utils = CleanFlowUtils.getInstance();
        utils.setConfig(config);
        isDateTime = utils.getVector(config.getIsDateTime(), String.class);
    }

    public void setUp() {
        boolean isCredit = true;
        submitHandle(isCredit);
        isCredit = false;
        submitHandle(isCredit);
    }

    public void submitHandle(boolean isCredit) {
        try {
            Vector<Catalog> catalogVector = getCatalogVector(isCredit);
            Vector<Vector<Catalog>> doubleCatalogVector = collectingAndDistributing(catalogVector);
            int taskNum = doubleCatalogVector.size();
            int nThreads = taskNum << 1;
            executorService = Executors.newFixedThreadPool(nThreads);
            Vector<FutureTask<Vector<Catalog>>> futureTaskVector = new Vector<FutureTask<Vector<Catalog>>>(taskNum);
            CleanFlow cleanFlow = this;
            for (int i = taskNum - 1; i >= 0; i--) {
                CatalogPool catalogPool = new CatalogPool(ExecutableOp.QUERY_PRIMARY_KEY_RECORD, cleanFlow, utils.getPostgresConnection(), utils.getPostgresConnection(), doubleCatalogVector.get(i));
                executorService.execute(new Producer(catalogPool));
                FutureTask<Vector<Catalog>> futureTask = new FutureTask<Vector<Catalog>>(new Consumer(catalogPool));
                executorService.submit(futureTask);
                futureTaskVector.add(futureTask);
            }
            for (int i = taskNum - 1; i >= 0; i--) {
                try {
                    catalogVector = futureTaskVector.remove(i).get();
                    CatalogPool catalogPool = new CatalogPool(ExecutableOp.QUERY_REMAIN_STRUCT, cleanFlow, utils.getPostgresConnection(), isCredit ? utils.getXyOracleConnection() : utils.getZhOracleConnection(), catalogVector);
                    executorService.execute(new Producer(catalogPool));
                    FutureTask<Vector<Catalog>> futureTask = new FutureTask<Vector<Catalog>>(new Consumer(catalogPool));
                    executorService.submit(futureTask);
                    futureTaskVector.add(futureTask);
                } catch (InterruptedException e) {
                } catch (ExecutionException e) {
                }
            }
            for (int i = taskNum - 1; i >= 0; i--) {
                try {
                    catalogVector = futureTaskVector.remove(i).get();
                    CatalogPool catalogPool = new CatalogPool(ExecutableOp.SAVE_PROBLEM_DATA, cleanFlow, isCredit ? utils.getXyOracleConnection() : utils.getZhOracleConnection(), utils.getPostgresConnection(), catalogVector);
                    executorService.execute(new Producer(catalogPool));
                    FutureTask<Vector<Catalog>> futureTask = new FutureTask<Vector<Catalog>>(new Consumer(catalogPool));
                    executorService.submit(futureTask);
                    futureTaskVector.add(futureTask);
                } catch (InterruptedException e) {
                } catch (ExecutionException e) {
                }
            }
            for (int i = taskNum - 1; i >= 0; i--) {
                try {
                    catalogVector = futureTaskVector.remove(i).get();
                    CatalogPool catalogPool = new CatalogPool(ExecutableOp.EXPLORATION_PROBLEM, cleanFlow, utils.getPostgresConnection(), null, catalogVector);
                    executorService.execute(new Producer(catalogPool));
                } catch (InterruptedException e) {
                } catch (ExecutionException e) {
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (executorService != null && !executorService.isShutdown()) {
                executorService.shutdown();
            }
        }
    }

    public Vector<Catalog> getCatalogVector(boolean isCredit) throws SQLException {
        Connection connection = utils.getPostgresConnection();
        Vector<Catalog> catalogVector = null;
        if (connection != null) {
            Statement statement = null;
            ResultSet resultSet = null;
            try {
                statement = connection.createStatement();
                if (isCredit) {
                    resultSet = statement.executeQuery(String.format(config.getQueryCatalogInfo(), config.getXyDataStore(), config.getXyLogicTopic()));
                } else {
                    resultSet = statement.executeQuery(String.format(config.getQueryCatalogInfo(), config.getZhDataStore(), config.getZhLogicTopic()));
                }
                catalogVector = new Vector<Catalog>();
                while (resultSet.next()) {
                    Catalog catalog = new Catalog();
                    catalog.setResId(resultSet.getString(RES_ID));
                    catalog.setTableName(resultSet.getString(TABLE_NAME));
                    catalog.setSiteNo(resultSet.getString(SITE_NO));
                    catalog.setSiteName(resultSet.getString(RESULT_STR));
                    catalog.setCatalogName(resultSet.getString(COLUMN_NAME));
                    catalog.setPrimaryKey(config.getCommonlyPrimaryKey());
                    catalogVector.add(catalog);
                }
            } catch (SQLException e) {
                throw e;
            } finally {
                utils.release(resultSet);
                utils.release(statement);
                utils.release(connection);
            }
        }
        return catalogVector;
    }

    public void getPrepareToCleanCatalog(Connection connection, Catalog catalog) {
        if (catalog != null && connection != null) {
            Statement statement = null;
            ResultSet resultSet = null;
            try {
                statement = connection.createStatement();
                resultSet = statement.executeQuery(String.format(config.getQueryPrepareToCleanCatalog(), catalog.getTableName(), config.getNeedHandler()));
                while (resultSet.next()) {
                    catalog.setRecorderNum(resultSet.getLong(RESULT_NUM));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                utils.release(resultSet);
                utils.release(statement);
            }
        }
    }

    public void getBuildNonInverseSqlByRuleValue(Connection connection, Catalog catalog) {
        if (catalog != null && connection != null) {
            Statement statement = null;
            ResultSet resultSet = null;
            try {
                statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                resultSet = statement.executeQuery(String.format(config.getQueryCleanRuleInfo(), catalog.getResId(), catalog.getTableName()));
                int row, counter = 0;
                resultSet.last();
                row = resultSet.getRow();
                if (row > 0) {
                    StringBuilder queryPrimaryKeyValueSqlBuilder = new StringBuilder();
                    queryPrimaryKeyValueSqlBuilder.append("SELECT ").append(catalog.getPrimaryKey())
                            .append(" AS RESULT_STR FROM ").append(catalog.getTableName())
                            .append(" WHERE status = '%s' ORDER BY ").append(catalog.getPrimaryKey()).append(" DESC LIMIT %d OFFSET %d");
                    catalog.setQueryPrimaryKeyValueSql(queryPrimaryKeyValueSqlBuilder.toString());

                    resultSet.beforeFirst();
                    List<String> columnNames = new ArrayList<>();

                    String fields = "SELECT ".concat(catalog.getPrimaryKey());
                    while (resultSet.next()) {
                        String columnName = resultSet.getString(COLUMN_NAME);
                        if (columnName != null && !columnName.equals("")) {
                            if (columnName.contains(",")) {
                                String[] columnNameTemps = columnName.split(",");
                                for (int j = 0; j < columnNameTemps.length; j++) {
                                    if (!columnNames.contains(columnNameTemps[j])) {
                                        fields = fields.concat(", ").concat(columnNameTemps[j]);
                                        columnNames.add(columnNameTemps[j]);
                                    }
                                }
                            } else {
                                if (!columnNames.contains(columnName)) {
                                    fields = fields.concat(", ").concat(columnName);
                                    columnNames.add(columnName);
                                }
                            }
                        }
                    }

                    StringBuilder nonInverseSqlBuilder = new StringBuilder();
                    nonInverseSqlBuilder.append("SELECT ").append(catalog.getPrimaryKey()).append(" AS RESULT_STR FROM (");
                    nonInverseSqlBuilder.append(fields).append(" FROM ").append(catalog.getTableName()).append(" WHERE status = '%s'");
                    nonInverseSqlBuilder.append(" ORDER BY ").append(catalog.getPrimaryKey()).append(" DESC LIMIT %d OFFSET %d) T WHERE ");
                    String tryExplorationSql = nonInverseSqlBuilder.toString();
                    List<String> tryExplorationSqlList = new ArrayList<String>();
                    List<String> tryExplorationDescList = new ArrayList<String>();
                    resultSet.beforeFirst();
                    while (resultSet.next()) {
                        counter++;
                        String desc = resultSet.getString(SITE_NO);
                        tryExplorationDescList.add(desc == null ? "" : desc);
                        String operator = resultSet.getString(RESULT_NUM);
                        String ruleValue = resultSet.getString(RESULT_STR).replaceAll("%", "%%");
                        if (operator.equals(OperatorType.SPECIFIC_VALUE.getType())) {
                            String columnName = resultSet.getString(COLUMN_NAME);
                            tryExplorationSqlList.add(tryExplorationSql.concat(columnName).concat(" ").concat(resultSet.getString(RESULT_DATE)).concat(" ").concat(ruleValue));
                            nonInverseSqlBuilder.append(columnName).append(" ").append(resultSet.getString(RESULT_DATE)).append(" ").append(ruleValue);
                        } else if (operator.equals(OperatorType.SQL.getType())) {
                            tryExplorationSqlList.add(tryExplorationSql.concat(ruleValue));
                            nonInverseSqlBuilder.append("(").append(ruleValue).append(")");
                        } else if (operator.equals(OperatorType.PATTERN.getType())) {
                            String columnName = resultSet.getString(COLUMN_NAME);
                            tryExplorationSqlList.add(tryExplorationSql.concat(columnName).concat("::CHAR ~* '").concat(ruleValue).concat("'"));
                            nonInverseSqlBuilder.append(columnName).append("::CHAR ~* '").append(ruleValue).append("'");
                        }
                        if (counter < row) {
                            nonInverseSqlBuilder.append(" AND ");
                        }
                    }
                    catalog.setNonInverseSql(nonInverseSqlBuilder.toString());
                    catalog.setTryExplorationSqlList(tryExplorationSqlList);
                    catalog.setTryExplorationDescList(tryExplorationDescList);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                utils.release(resultSet);
                utils.release(statement);
            }
        }
    }

    public void getPrimaryKeyValue(Connection connection, Catalog catalog) {
        if (catalog != null && connection != null) {
            Statement statement = null;
            try {
                int resultSetType = ResultSet.TYPE_FORWARD_ONLY | ResultSet.TYPE_SCROLL_INSENSITIVE;
                int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY | ResultSet.CONCUR_UPDATABLE;
                statement = connection.createStatement(resultSetType, resultSetConcurrency);
                statement.setFetchSize(10000);
                statement.setFetchDirection(ResultSet.FETCH_REVERSE);
                int numOfPer = (int) Math.min(catalog.getRecorderNum(), 100000);
                long fullSize = catalog.getRecorderNum() / numOfPer;
                long remainSize = catalog.getRecorderNum() % numOfPer;
                int taskNum = (int) (remainSize == 0 ? fullSize : fullSize + 1);
                catalog.setPrimaryKeyValues(new ArrayList<String>());
                catalog.setRightPrimaryKeyValues(new ArrayList<String>());
                for (int i = 0; i < taskNum; i++) {
                    ResultSet resultSet = null;
                    try {
                        int offset = i * numOfPer;
                        String strSQL = String.format(catalog.getQueryPrimaryKeyValueSql(), config.getNeedHandler(), i < taskNum - 1 ? numOfPer : catalog.getRecorderNum() - offset, offset);
                        resultSet = statement.executeQuery(strSQL);
                        while (resultSet.next()) {
                            catalog.getPrimaryKeyValues().add(resultSet.getString(RESULT_STR));
                        }
                        utils.release(resultSet);
                        strSQL = String.format(catalog.getNonInverseSql(), config.getNeedHandler(), i < taskNum - 1 ? numOfPer : catalog.getRecorderNum() - offset, offset);
                        resultSet = statement.executeQuery(strSQL);
                        while (resultSet.next()) {
                            catalog.getRightPrimaryKeyValues().add(resultSet.getString(RESULT_STR));
                        }
                        utils.release(resultSet);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } finally {
                        utils.release(resultSet);
                    }
                }
                try {
                    connection.setAutoCommit(false);
                    for (String primaryKeyValue : catalog.getRightPrimaryKeyValues()) {
                        statement.addBatch("UPDATE ".concat(catalog.getTableName()).concat(" SET status = '").concat(config.getRightRecorder()).concat("' WHERE ").concat(catalog.getPrimaryKey()).concat(" = ").concat(primaryKeyValue));
                    }
                    catalog.getPrimaryKeyValues().removeAll(catalog.getRightPrimaryKeyValues());
                    for (String primaryKeyValue : catalog.getPrimaryKeyValues()) {
                        statement.addBatch("UPDATE ".concat(catalog.getTableName()).concat(" SET status = '").concat(config.getErrorRecorder()).concat("' WHERE ").concat(catalog.getPrimaryKey()).concat(" = ").concat(primaryKeyValue));
                    }
                    statement.executeBatch();
                } catch (SQLException e) {
                    connection.rollback();
                } finally {
                    try {
                        statement.clearBatch();
                        connection.setAutoCommit(true);
                    } catch (SQLException e) {
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                utils.release(statement);
            }
        }
    }

    public void getRemainProblemCatalogTableStruct(Connection connection, Catalog catalog) {
        if (catalog != null && connection != null) {
            Statement statement = null;
            ResultSet resultSet = null;
            try {
                statement = connection.createStatement();
                StringBuilder saveProblemRecordSql = new StringBuilder();
                StringBuilder queryProblemRecordSql = new StringBuilder();
                StringBuilder paramsBuild = new StringBuilder();
                resultSet = statement.executeQuery(String.format(config.getQueryTableStruct(), catalog.getTableName().concat("_ERR")));
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                int j = resultSetMetaData.getColumnCount();
                saveProblemRecordSql.append("INSERT INTO ").append(catalog.getTableName()).append("_ERR(");
                queryProblemRecordSql.append("SELECT ");
                paramsBuild.delete(0, paramsBuild.length());
                for (int i = 1; i <= j; i++) {
                    saveProblemRecordSql.append(resultSetMetaData.getColumnLabel(i));
                    queryProblemRecordSql.append(resultSetMetaData.getColumnLabel(i));
                    paramsBuild.append("?");
                    if (i < j) {
                        saveProblemRecordSql.append(", ");
                        queryProblemRecordSql.append(", ");
                        paramsBuild.append(", ");
                    }
                }
                saveProblemRecordSql.append(") VALUES(").append(paramsBuild).append(")");
                queryProblemRecordSql.append(" FROM ").append(catalog.getTableName()).append(" WHERE ").append(catalog.getPrimaryKey()).append(" = ?");
                catalog.setSaveProblemRecordSql(saveProblemRecordSql.toString());
                catalog.setQueryProblemRecordSql(queryProblemRecordSql.toString());
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                utils.release(resultSet);
                utils.release(statement);
            }
        }
    }

    public void getProblemRecordByPrimaryKeyValueAndSave(Connection problemConnection, Connection connection, Catalog catalog) {
        if (catalog != null && connection != null && problemConnection != null) {
            PreparedStatement preparedStatement = null;
            ResultSet resultSet = null;
            PreparedStatement problemPreparedStatement = null;
            try {
                StringBuilder builder = new StringBuilder();
                List<String> primaryKeyValues = catalog.getPrimaryKeyValues();
                if (primaryKeyValues != null && !primaryKeyValues.isEmpty()) {
                    builder.delete(0, builder.length());
                    for (int i = 1, j = primaryKeyValues.size(); i <= j; i++) {
                        builder.append("(").append(catalog.getQueryProblemRecordSql()).append(")");
                        if (i < j) {
                            builder.append(" UNION ALL ");
                        }
                    }
                    int resultSetType = ResultSet.TYPE_FORWARD_ONLY | ResultSet.TYPE_SCROLL_INSENSITIVE;
                    int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY | ResultSet.CONCUR_UPDATABLE;
                    preparedStatement = connection.prepareStatement(builder.toString(), resultSetType, resultSetConcurrency);
                    preparedStatement.setFetchSize(10000);
                    preparedStatement.setFetchDirection(ResultSet.FETCH_REVERSE);
                    for (int i = 1, j = primaryKeyValues.size(); i <= j; i++) {
                        preparedStatement.setLong(i, Long.parseLong(primaryKeyValues.get(i - 1)));
                    }
                    resultSet = preparedStatement.executeQuery();
                    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                    try {
                        problemConnection.setAutoCommit(false);
                        problemPreparedStatement = problemConnection.prepareStatement(catalog.getSaveProblemRecordSql());
                        while (resultSet.next()) {
                            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                                if (isDateTime.contains(resultSetMetaData.getColumnTypeName(i).toLowerCase())) {
                                    problemPreparedStatement.setTimestamp(i, resultSet.getTimestamp(i));
                                } else {
                                    problemPreparedStatement.setObject(i, resultSet.getObject(i));
                                }
                            }
                            problemPreparedStatement.addBatch();
                        }
                        problemPreparedStatement.executeBatch();
                    } catch (SQLException e) {
                        problemConnection.rollback();
                    } finally {
                        preparedStatement.clearParameters();
                        problemPreparedStatement.clearBatch();
                        problemConnection.setAutoCommit(true);
                        utils.release(resultSet);
                        utils.release(preparedStatement);
                        utils.release(problemPreparedStatement);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                utils.release(resultSet);
                utils.release(preparedStatement);
                utils.release(problemPreparedStatement);
            }
        }
    }

    public void getExplorationProblem(Connection connection, Catalog catalog) {
        if (catalog != null && connection != null) {
            Statement statement = null;
            ResultSet resultSet = null;
            try {
                statement = connection.createStatement();
                List<String> primaryKeyValues = catalog.getPrimaryKeyValues();
                List<String> tryExplorationSqlList = catalog.getTryExplorationSqlList();
                if (primaryKeyValues != null && !primaryKeyValues.isEmpty() &&
                        tryExplorationSqlList != null && !tryExplorationSqlList.isEmpty()) {
                    int remainProblemSize = catalog.getPrimaryKeyValues().size();
                    int numOfPer = Math.min(remainProblemSize, 100000);
                    long fullSize = remainProblemSize / numOfPer;
                    long remainSize = remainProblemSize % numOfPer;
                    int taskNum = (int) (remainSize == 0 ? fullSize : fullSize + 1);
                    for (int i = tryExplorationSqlList.size() - 1; i >= 0; i--) {
                        String tryExplorationSql = tryExplorationSqlList.get(i);
                        if (tryExplorationSql != null && !tryExplorationSql.equals("")) {
                            List<String> hasNotCurrentProblemList = null;
                            for (int j = 0; j < taskNum; j++) {
                                int offset = j * numOfPer;
                                resultSet = statement.executeQuery(String.format(tryExplorationSql, config.getErrorRecorder(), j < taskNum - 1 ? numOfPer : remainProblemSize - offset, offset));
                                while (resultSet.next()) {
                                    if (hasNotCurrentProblemList == null) {
                                        hasNotCurrentProblemList = new ArrayList<String>();
                                    }
                                    hasNotCurrentProblemList.add(resultSet.getString(RESULT_STR));
                                }
                                utils.release(resultSet);
                            }
                            if (hasNotCurrentProblemList != null) {
                                if (primaryKeyValues.size() == hasNotCurrentProblemList.size()) {
                                    catalog.getTryExplorationDescList().remove(i);
                                }
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                utils.release(resultSet);
                utils.release(statement);
            }
        }
    }

    public void saveWorkSheetByExplorationProblem(Connection connection, Catalog catalog) {
        if (catalog != null && connection != null) {
            Statement statement = null;
            try {
                statement = connection.createStatement();
                if (statement != null) {
                    connection.setAutoCommit(false);
                    List<String> primaryKeyValues = catalog.getPrimaryKeyValues();
                    if (primaryKeyValues != null && !primaryKeyValues.isEmpty()) {
                        String date = DATE_FORMAT.format(new Date());
                        String qualityId = UUID.randomUUID().toString();
                        List<String> tryExplorationDescList = catalog.getTryExplorationDescList();
                        String desc = "";
                        if (tryExplorationDescList != null && !tryExplorationDescList.isEmpty()) {
                            for (int i = 0, j = tryExplorationDescList.size() - 1; i <= j; i++) {
                                desc = desc.concat(tryExplorationDescList.get(i));
                                if (i < j) {
                                    desc = desc.concat("。");
                                }
                            }
                        }
                        statement.addBatch(String.format(config.getSaveQuality(), qualityId, catalog.getSiteNo(), catalog.getSiteName(), date, date, desc, catalog.getResId(), catalog.getCatalogName(), primaryKeyValues.size()));
                        statement.addBatch(String.format(config.getSaveQualityDispose(), UUID.randomUUID().toString(), qualityId, date));
                        for (String primaryKeyValue : primaryKeyValues) {
                            statement.addBatch(String.format(config.getSaveQualityResource(), UUID.randomUUID().toString(), qualityId, catalog.getTableName(), primaryKeyValue));
                        }
                        statement.executeBatch();
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                }
            } finally {
                try {
                    statement.clearBatch();
                    connection.setAutoCommit(true);
                }catch (SQLException e) {
                }
                utils.release(statement);
            }
        }
    }

    private Vector<Vector<Catalog>> collectingAndDistributing(Vector<Catalog> catalogVector) throws SQLException {
        if (catalogVector != null && !catalogVector.isEmpty()) {
            Vector<Vector<Catalog>> doubleCatalogVector = new Vector<Vector<Catalog>>();
            String cacheCatalogVector = utils.toJson(catalogVector);
            try {
                Vector<Cost> costVector = utils.loadCache(config.getCostCacheFile(), Cost.class);
                Collections.sort(costVector);
                int first = 0;
                long maxCost = costVector.get(first).getCost(), totalCost = 0;
                boolean enter = false;
                Vector<Catalog> subCatalogVector = null;
                while (costVector.size() > 0) {
                    if (totalCost == 0) {
                        subCatalogVector = new Vector<Catalog>();
                        doubleCatalogVector.add(subCatalogVector);
                    }
                    Cost firstCost = costVector.get(first);
                    Cost lastCost = costVector.get(costVector.size() - 1);
                    totalCost += firstCost.getCost() + lastCost.getCost();
                    Catalog catalog;
                    if (totalCost < maxCost) {
                        catalog = findCatalog(firstCost, catalogVector);
                        if (catalog != null) {
                            subCatalogVector.add(catalog);
                        }
                        catalog = findCatalog(lastCost, catalogVector);
                        if (catalog != null) {
                            subCatalogVector.add(catalog);
                        }
                        costVector.remove(firstCost);
                        costVector.remove(lastCost);
                        enter = true;
                    } else {
                        if (!enter) {
                            catalog = findCatalog(firstCost, catalogVector);
                            if (catalog != null) {
                                subCatalogVector.add(catalog);
                            }
                            costVector.remove(firstCost);
                            totalCost = 0;
                        } else {
                            totalCost -= firstCost.getCost();
                            if (totalCost < maxCost) {
                                catalog = findCatalog(lastCost, catalogVector);
                                if (catalog != null) {
                                    subCatalogVector.add(catalog);
                                }
                                costVector.remove(lastCost);
                            } else {
                                totalCost = 0;
                                enter = false;
                            }
                        }
                    }
                }
                if (!catalogVector.isEmpty()) {
                    subCatalogVector.addAll(catalogVector);
                }
            } catch (SQLException e) {
                doubleCatalogVector.clear();
                Vector<Map> mapVector = utils.getMapVector(cacheCatalogVector);
                catalogVector = utils.toEntityVector(mapVector, Catalog.class);
                doubleCatalogVector.add(catalogVector);
            }
            return doubleCatalogVector;
        } else {
            throw new SQLException();
        }
    }

    private Catalog findCatalog(Cost cost, Vector<Catalog> catalogVector) throws SQLException {
        if (catalogVector != null && !catalogVector.isEmpty()) {
            Iterator<Catalog> iterator = catalogVector.iterator();
            while (iterator.hasNext()) {
                Catalog catalog = iterator.next();
                if (catalog.getTableName().equals(cost.getTableName())) {
                    catalogVector.remove(catalog);
                    return catalog;
                }
            }
        }
        throw new SQLException();
    }

}
