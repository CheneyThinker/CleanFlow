package org.clean.flow.entity;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class Catalog implements Comparable<Catalog>, Serializable {

    private String resId;
    private String catalogName;
    private String siteNo;
    private String siteName;
    private String tableName;
    private String primaryKey;
    private long recorderNum;
    private String queryPrimaryKeyValueSql;
    private String nonInverseSql;
    private List<String> primaryKeyValues;
    private List<String> rightPrimaryKeyValues;
    private String saveProblemRecordSql;
    private String queryProblemRecordSql;
    private List<String> tryExplorationSqlList;
    private List<String> tryExplorationDescList;

    public int compareTo(Catalog catalog) {
        if (catalog.getRecorderNum() < this.getRecorderNum()) {
            return -1;
        } else if (catalog.getRecorderNum() == this.getRecorderNum()) {
            return 0;
        } else {
            return 1;
        }
    }

}
