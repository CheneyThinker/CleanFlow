package org.clean.flow.core;

import org.clean.flow.entity.Catalog;

import java.util.Vector;
import java.util.concurrent.Callable;

public class Consumer implements Callable<Vector<Catalog>> {

    private CatalogPool catalogPool;

    public Consumer(CatalogPool catalogPool) {
        this.catalogPool = catalogPool;
    }

    public Vector<Catalog> call() throws Exception {
        while (true) {
            if (catalogPool.isClosetConsumer()) {
                catalogPool.releaseConsumer();
                return catalogPool.getCatalogVector();
            } else {
                catalogPool.take();
            }
        }
    }

}
