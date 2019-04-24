package org.clean.flow.core;

import org.clean.flow.CleanFlow;
import org.clean.flow.entity.Catalog;
import org.clean.flow.entity.ExecutableOp;
import org.clean.flow.util.CleanFlowUtils;

import java.sql.Connection;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CatalogPool {

    private Lock lock;
    private boolean closetProducer;
    private boolean closetConsumer;
    private Vector<Catalog> producerCatalogVector;
    private Vector<Catalog> consumerCatalogVector;
    private Vector<Catalog> catalogVector;
    private Condition producerCondition;
    private Condition consumerCondition;
    private CleanFlow cleanFlow;
    private Connection producerConnection;
    private Connection consumerConnection;
    private ExecutableOp executableOp;
    private AtomicInteger currentPosition;

    public CatalogPool(ExecutableOp executableOp, CleanFlow cleanFlow, Connection producerConnection, Connection consumerConnection, Vector<Catalog> catalogVector) {
        this.lock = new ReentrantLock();
        this.producerCondition = this.lock.newCondition();
        this.consumerCondition = this.lock.newCondition();
        this.executableOp = executableOp;
        this.cleanFlow = cleanFlow;
        this.producerConnection = producerConnection;
        this.consumerConnection = consumerConnection;
        this.producerCatalogVector = new Vector<>();
        this.consumerCatalogVector = new Vector<>();
        this.closetProducer = false;
        this.closetConsumer = false;
        this.catalogVector = catalogVector;
        this.currentPosition = new AtomicInteger(0);
    }

    public void put() {
        try {
            lock.lock();
            if (!catalogVector.isEmpty()) {
                Catalog catalog = catalogVector.firstElement();
                catalogVector.remove(catalog);
                switch (executableOp) {
                    case QUERY_PRIMARY_KEY_RECORD:
                        {
                            cleanFlow.getPrepareToCleanCatalog(producerConnection, catalog);
                            if (catalog != null && catalog.getRecorderNum() > 0) {
                                producerCatalogVector.add(catalog);
                                consumerCondition.signalAll();
                            }
                        }
                        break;
                    case QUERY_REMAIN_STRUCT:
                        {
                            cleanFlow.getPrimaryKeyValue(producerConnection, catalog);
                            if (catalog != null) {
                                List<String> primaryKeyValues = catalog.getPrimaryKeyValues();
                                if (primaryKeyValues != null && !primaryKeyValues.isEmpty()) {
                                    producerCatalogVector.add(catalog);
                                    consumerCondition.signalAll();
                                }
                            }
                        }
                        break;
                    case SAVE_PROBLEM_DATA:
                        {
                            cleanFlow.getProblemRecordByPrimaryKeyValueAndSave(producerConnection, consumerConnection, catalog);
                            if (catalog != null) {
                                producerCatalogVector.add(catalog);
                                consumerCondition.signalAll();
                            }
                        }
                        break;
                    case EXPLORATION_PROBLEM:
                        cleanFlow.saveWorkSheetByExplorationProblem(producerConnection, catalog);
                        break;
                }
            } else {
                closetProducer = true;
                consumerCondition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    public void take() throws Exception {
        try {
            lock.lock();
            if (producerCatalogVector.isEmpty() && closetProducer) {
                closetConsumer = true;
            } else {
                if (!producerCatalogVector.isEmpty()) {
                    if (!catalogVector.isEmpty()) {
                        producerCondition.signalAll();
                    } else {
                        closetProducer = true;
                    }
                    if (consumerCatalogVector.size() == producerCatalogVector.size() && closetProducer) {
                        System.err.println("consumer:" + CleanFlowUtils.getInstance().toJson(consumerCatalogVector));
                        closetConsumer = true;
                    } else {
                        int index = currentPosition.getAndIncrement();
                        if (producerCatalogVector.size() <= index) {
                            currentPosition.decrementAndGet();
                        } else {
                            Catalog catalog = producerCatalogVector.get(index);
                            switch (executableOp) {
                                case QUERY_PRIMARY_KEY_RECORD:
                                    cleanFlow.getBuildNonInverseSqlByRuleValue(consumerConnection, catalog);
                                    break;
                                case QUERY_REMAIN_STRUCT:
                                    cleanFlow.getRemainProblemCatalogTableStruct(consumerConnection, catalog);
                                    break;
                                case SAVE_PROBLEM_DATA:
                                    cleanFlow.getExplorationProblem(consumerConnection, catalog);
                                    break;
                            }
                            if (catalog != null) {
                                consumerCatalogVector.add(catalog);
                            }
                        }
                    }
                } else {
                    try {
                        consumerCondition.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean isClosetProducer() {
        return closetProducer;
    }

    public boolean isClosetConsumer() {
        return closetConsumer;
    }

    public void releaseProducer() {
        CleanFlowUtils.getInstance().release(producerConnection);
    }

    public void releaseConsumer() {
        CleanFlowUtils.getInstance().release(consumerConnection);
    }

    public Vector<Catalog> getCatalogVector() {
        return consumerCatalogVector;
    }

}
