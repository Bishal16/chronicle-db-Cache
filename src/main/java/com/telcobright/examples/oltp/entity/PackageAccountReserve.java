package com.telcobright.examples.oltp.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;
import com.telcobright.core.cache.annotations.Column;
import com.telcobright.core.cache.annotations.Table;

@Table(name = "packageaccountreserve")
@Data
public class PackageAccountReserve {

    @Column(name = "id", primaryKey = true)
    @JsonProperty("id")
    private Long id;

    @Column(name = "packageAccountId")
    @JsonProperty("packageAccountId")
    private Long packageAccountId;

    @Column(name = "sessionId")
    @JsonProperty("sessionId")
    private String sessionId;

    @Column(name = "reservedAmount")
    @JsonProperty("reservedAmount")
    private BigDecimal reservedAmount;

    @Column(name = "reservedAt")
    @JsonProperty("reservedAt")
    private LocalDateTime reservedAt;

    @Column(name = "releasedAt")
    @JsonProperty("releasedAt")
    private LocalDateTime releasedAt;

    @Column(name = "status")
    @JsonProperty("status")
    private String status; // RESERVED, RELEASED, EXPIRED

    @Column(name = "currentReserve")
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

}