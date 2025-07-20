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
}