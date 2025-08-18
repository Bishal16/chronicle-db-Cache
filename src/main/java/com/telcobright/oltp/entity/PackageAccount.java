package com.telcobright.oltp.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Optional;
import com.telcobright.core.cache.annotations.Column;
import com.telcobright.core.cache.annotations.Table;

@Table(name = "packageaccount")
@Data
public class PackageAccount {

    @Column(name = "id_packageaccount", primaryKey = true)
    @JsonProperty("id_packageaccount")
    private Long id;

    @Column(name = "id_PackagePurchase")
    @JsonProperty("id_PackagePurchase")
    private Long packagePurchaseId;

    @Column(name = "name")
    @JsonProperty("name")
    private String name;

    @Column(name = "lastAmount")
    @JsonProperty("lastAmount")
    private BigDecimal lastAmount;

    @Column(name = "balanceBefore")
    @JsonProperty("balanceBefore")
    private BigDecimal balanceBefore;

    @Column(name = "balanceAfter")
    @JsonProperty("balanceAfter")
    private BigDecimal balanceAfter;

    @Column(name = "uom")
    @JsonProperty("uom")
    private String uom;

    @Column(name = "isSelected")
    @JsonProperty("isSelected")
    private Boolean isSelected = false;

    public PackageAccount() {
    }

    public PackageAccount(Long id, Long packagePurchaseId, String name, BigDecimal lastAmount,
                          BigDecimal balanceBefore, BigDecimal balanceAfter, String uom, Boolean isSelected) {
        this.id = id;
        this.packagePurchaseId = packagePurchaseId;
        this.name = name;
        this.lastAmount = lastAmount;
        this.balanceBefore = balanceBefore;
        this.balanceAfter = balanceAfter;
        this.uom = uom;
        this.isSelected = Optional.ofNullable(isSelected).orElse(false);
    }

    public synchronized void applyDelta(BigDecimal deltaAmount) {
        if (deltaAmount == null || deltaAmount.signum() <= 0) {
            throw new IllegalArgumentException("Delta amount must be a positive value for subtract operation");
        }
        this.lastAmount = deltaAmount;
        this.balanceBefore = this.balanceAfter;
        this.balanceAfter = this.balanceAfter.subtract(deltaAmount);
    }
}