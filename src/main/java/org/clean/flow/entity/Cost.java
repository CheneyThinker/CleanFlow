package org.clean.flow.entity;

import lombok.Data;

@Data
public class Cost implements Comparable<Cost> {

    private long cost;
    private String tableName;

    public int compareTo(Cost cost) {
        if (cost.getCost() < this.getCost()) {
            return -1;
        } else if (cost.getCost() == this.getCost()) {
            return 0;
        } else {
            return 1;
        }
    }

}
