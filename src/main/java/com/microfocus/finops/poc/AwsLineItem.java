package com.microfocus.finops.poc;

public class AwsLineItem {

    String usageAccountId;
    String lineItemType;

    public String getUsageAccountId() {
        return usageAccountId;
    }

    public void setUsageAccountId(String usageAccountId) {
        this.usageAccountId = usageAccountId;
    }

    public String getLineItemType() {
        return lineItemType;
    }

    public void setLineItemType(String lineItemType) {
        this.lineItemType = lineItemType;
    }
}
