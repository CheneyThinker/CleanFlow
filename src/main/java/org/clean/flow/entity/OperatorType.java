package org.clean.flow.entity;

public enum OperatorType {

    SPECIFIC_VALUE("0"),
    SQL("1"),
    PATTERN("2");

    final String type;

    OperatorType(final String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

}
