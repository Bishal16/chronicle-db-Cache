package com.telcobright.oltp.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PackageAccountCdc {
    @JsonProperty("id_packageaccount")
    public Long id;

    @JsonProperty("id_PackagePurchase")
    public Long packagePurchaseId;

    public String name;
    public String lastAmount;     // Base64 string from Debezium
    public String balanceBefore;  // Base64 string
    public String balanceAfter;   // Base64 string
    public String uom;
    public Integer isSelected;    // tinyint(1)
}
