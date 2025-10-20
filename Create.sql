USE CabData;
CREATE TABLE Trips (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    PickupDatetime DATETIME NOT NULL,
    DropoffDatetime DATETIME NOT NULL,
    PassengerCount INT NOT NULL,
    TripDistance DECIMAL(10,2) NOT NULL,
    StoreAndFwdFlag VARCHAR(3) NOT NULL,
    PULocationID INT NOT NULL,
    DOLocationID INT NOT NULL,
    FareAmount DECIMAL(10,2) NOT NULL,
    TipAmount DECIMAL(10,2) NOT NULL,
    TripDuration AS DATEDIFF(SECOND, PickupDatetime, DropoffDatetime) PERSISTED,
    CONSTRAINT UK_Trips UNIQUE (PickupDatetime, DropoffDatetime, PassengerCount)
);
CREATE INDEX IX_PULocationID ON Trips (PULocationID);
CREATE INDEX IX_TripDistance ON Trips (TripDistance DESC);
CREATE INDEX IX_TripDuration ON Trips (TripDuration DESC);
CREATE INDEX IX_PULocationID_TipAmount ON Trips (PULocationID, TipAmount);