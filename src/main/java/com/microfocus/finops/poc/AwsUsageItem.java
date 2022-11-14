package com.microfocus.finops.poc;

public class AwsUsageItem {

    private AwsLineItem lineItem;
    private AwsBill bill;

    public AwsLineItem getLineItem() {
        return lineItem;
    }

    public void setLineItem(AwsLineItem lineItem) {
        this.lineItem = lineItem;
    }

    public AwsBill getBill() {
        return bill;
    }

    public void setBill(AwsBill bill) {
        this.bill = bill;
    }
}
