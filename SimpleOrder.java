public class SimpleOrder {
    private Long id;
    private Long customerId;
    private Double total;
    private String status;
    
    public SimpleOrder() {}
    
    public SimpleOrder(Long id, Long customerId, Double total) {
        this.id = id;
        this.customerId = customerId;
        this.total = total;
    }
    
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public Long getCustomerId() { return customerId; }
    public void setCustomerId(Long customerId) { this.customerId = customerId; }
    
    public Double getTotal() { return total; }
    public void setTotal(Double total) { this.total = total; }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
}