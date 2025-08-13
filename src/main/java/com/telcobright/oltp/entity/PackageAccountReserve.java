package com.telcobright.oltp.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.*;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;

@Entity
@Table(name = "packageaccountreserve")
@Data
public class PackageAccountReserve implements CacheableEntity {

    @Id
    @Column(name = "id_packageaccountreserve")
    @JsonProperty("id_packageaccountreserve")
    private Long id;

    @Column(name = "id_packageaccount", nullable = false)
    @JsonProperty("id_packageaccount")
    private Long packageAccountId;

    @Column(name = "sessionId", nullable = false)
    @JsonProperty("sessionId")
    private String sessionId;

    @Column(nullable = false, precision = 20, scale = 6)
    @JsonProperty("reservedAmount")
    private BigDecimal reservedAmount;

    @Column(nullable = false)
    @JsonProperty("reservedAt")
    private LocalDateTime reservedAt;

    @Column
    @JsonProperty("releasedAt")
    private LocalDateTime releasedAt;

    @Column(nullable = false)
    @JsonProperty("status")
    private String status; // RESERVED, RELEASED, EXPIRED

    @Column(nullable = false, precision = 20, scale = 6)
    @JsonProperty("currentReserve")
    private BigDecimal currentReserve;

    public PackageAccountReserve() {
    }

    public PackageAccountReserve(Long id, Long packageAccountId, String sessionId, 
                                BigDecimal reservedAmount, LocalDateTime reservedAt,
                                String status, BigDecimal currentReserve) {
        this.id = id;
        this.packageAccountId = packageAccountId;
        this.sessionId = sessionId;
        this.reservedAmount = reservedAmount;
        this.reservedAt = reservedAt;
        this.status = Optional.ofNullable(status).orElse("RESERVED");
        this.currentReserve = currentReserve;
    }

    @Override
    public synchronized void applyDelta(BigDecimal deltaAmount) {
        if (deltaAmount == null) {
            throw new IllegalArgumentException("Delta amount must not be null");
        }
        
        if (deltaAmount.signum() > 0) {
            // Positive delta means adding reservation
            this.currentReserve = this.currentReserve.add(deltaAmount);
            this.reservedAmount = deltaAmount;
            this.status = "RESERVED";
        } else if (deltaAmount.signum() < 0) {
            // Negative delta means releasing reservation
            BigDecimal releaseAmount = deltaAmount.abs();
            if (this.currentReserve.compareTo(releaseAmount) < 0) {
                throw new IllegalStateException("Cannot release more than currently reserved. Current: " + 
                    this.currentReserve + ", Attempting to release: " + releaseAmount);
            }
            this.currentReserve = this.currentReserve.subtract(releaseAmount);
            if (this.currentReserve.compareTo(BigDecimal.ZERO) == 0) {
                this.status = "RELEASED";
                this.releasedAt = LocalDateTime.now();
            }
        }
    }

    @Override
    public String getTableName() {
        return "packageaccountreserve";
    }

    public void insert(EntityManager entityManager) {
        entityManager.persist(this);
    }

    public PackageAccountReserve update(EntityManager entityManager) {
        return entityManager.merge(this);
    }
}