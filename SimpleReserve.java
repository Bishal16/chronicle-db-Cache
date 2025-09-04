public class SimpleReserve {
    private Long id;
    private Long accountId;
    private Double amount;
    
    public SimpleReserve() {}
    
    public SimpleReserve(Long id, Long accountId, Double amount) {
        this.id = id;
        this.accountId = accountId;
        this.amount = amount;
    }
    
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public Long getAccountId() { return accountId; }
    public void setAccountId(Long accountId) { this.accountId = accountId; }
    
    public Double getAmount() { return amount; }
    public void setAmount(Double amount) { this.amount = amount; }
}