package com.telcobright.oltp.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.*;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Optional;

@Entity
@Table(name = "packageaccount")
@Data
public class PackageAccount implements CacheableEntity {

    @Id
    @Column(name = "id_packageaccount")
    @JsonProperty("id_packageaccount")
    private Long id;

    @Column(name = "id_PackagePurchase", nullable = false)
    @JsonProperty("id_PackagePurchase")
    private Long packagePurchaseId;

    @Column(nullable = false)
    @JsonProperty("name")
    private String name;

    @Column(nullable = false, precision = 20, scale = 6)
    @JsonProperty("lastAmount")
    private BigDecimal lastAmount;

    @Column(nullable = false, precision = 20, scale = 6)
    @JsonProperty("balanceBefore")
    private BigDecimal balanceBefore;

    @Column(nullable = false, precision = 20, scale = 6)
    @JsonProperty("balanceAfter")
    private BigDecimal balanceAfter;

    @Column(nullable = false)
    @JsonProperty("uom")
    private String uom;

    @Column(nullable = false)
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

    public void insert(EntityManager entityManager) {
        entityManager.persist(this);
    }

    public PackageAccount update(EntityManager entityManager) {
        return entityManager.merge(this);
    }

    @Override
    public synchronized void applyDelta(BigDecimal deltaAmount) {
        if (deltaAmount == null || deltaAmount.signum() <= 0) {
            throw new IllegalArgumentException("Delta amount must be a positive value for subtract operation");
        }
        this.lastAmount = deltaAmount;
        this.balanceBefore = this.balanceAfter;
        this.balanceAfter = this.balanceAfter.subtract(deltaAmount);
    }

    @Override
    public String getTableName() {
        return "packageaccount";
    }
}