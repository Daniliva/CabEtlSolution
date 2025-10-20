namespace CabEtlSolution.Models;

public class Trip
{
    public DateTime PickupDatetime { get; set; }
    public DateTime DropoffDatetime { get; set; }
    public int PassengerCount { get; set; }
    public decimal TripDistance { get; set; }
    public string StoreAndFwdFlag { get; set; }
    public int PULocationID { get; set; }
    public int DOLocationID { get; set; }
    public decimal FareAmount { get; set; }
    public decimal TipAmount { get; set; }
}