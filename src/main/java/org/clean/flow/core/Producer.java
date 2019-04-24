package org.clean.flow.core;

public class Producer implements Runnable {

    private CatalogPool catalogPool;

    public Producer(CatalogPool catalogPool) {
        this.catalogPool = catalogPool;
    }

    public void run() {
        while (true) {
            if (catalogPool.isClosetProducer()) {
                catalogPool.releaseProducer();
                break;
            } else {
                catalogPool.put();
            }
        }
    }

}
