package com.telcobright.oltp.entity;

import jakarta.persistence.*;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Optional;

@Entity
@Table(name = "packageaccount")
@Data
public class PackageAccount {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id_packageaccount")
    private Long id;

    @Column(name = "id_PackagePurchase", nullable = false)
    private Long packagePurchaseId;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false, precision = 20, scale = 6)
    private BigDecimal lastAmount;

    @Column(nullable = false, precision = 20, scale = 6)
    private BigDecimal balanceBefore;

    @Column(nullable = false, precision = 20, scale = 6)
    private BigDecimal balanceAfter;

    @Column(nullable = false)
    private String uom;

    @Column(nullable = false)
    private Boolean isSelected = false;

    public PackageAccount() {
    }

    // Example constructor for quick object creation
    public PackageAccount(Long packagePurchaseId, String name, BigDecimal lastAmount,
                          BigDecimal balanceBefore, BigDecimal balanceAfter, String uom, Boolean isSelected) {
        this.packagePurchaseId = packagePurchaseId;
        this.name = name;
        this.lastAmount = lastAmount;
        this.balanceBefore = balanceBefore;
        this.balanceAfter = balanceAfter;
        this.uom = uom;
        this.isSelected = Optional.ofNullable(isSelected).orElse(false);
    }

    // === ACTIVE RECORD STYLE METHODS ===

    public void insert(EntityManager entityManager) {
        entityManager.persist(this);
    }

    public PackageAccount update(EntityManager entityManager) {
        return entityManager.merge(this);
    }

    public synchronized void applyDelta(BigDecimal deltaAmount) {
        this.lastAmount = deltaAmount;
        this.balanceBefore = this.balanceAfter;
        this.balanceAfter = this.balanceAfter.subtract(deltaAmount);
    }


    // === Getters and Setters ===

//    public Long getId() {
//        return id;
//    }
//
//    public Long getPackagePurchaseId() {
//        return packagePurchaseId;
//    }
//
//    public void setPackagePurchaseId(Long packagePurchaseId) {
//        this.packagePurchaseId = packagePurchaseId;
//    }
//
//    public String getName() {
//        return name;
//    }
//
//    public void setName(String name) {
//        this.name = name;
//    }
//
//    public BigDecimal getLastAmount() {
//        return lastAmount;
//    }
//
//    public void setLastAmount(BigDecimal lastAmount) {
//        this.lastAmount = lastAmount;
//    }
//
//    public BigDecimal getBalanceBefore() {
//        return balanceBefore;
//    }
//
//    public void setBalanceBefore(BigDecimal balanceBefore) {
//        this.balanceBefore = balanceBefore;
//    }
//
//    public BigDecimal getBalanceAfter() {
//        return balanceAfter;
//    }
//
//    public void setBalanceAfter(BigDecimal balanceAfter) {
//        this.balanceAfter = balanceAfter;
//    }
//
//    public String getUom() {
//        return uom;
//    }
//
//    public void setUom(String uom) {
//        this.uom = uom;
//    }
//
//    public Boolean getIsSelected() {
//        return isSelected;
//    }
//
//    public void setIsSelected(Boolean selected) {
//        isSelected = selected;
//    }
}
