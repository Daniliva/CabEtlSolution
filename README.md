 CabEtlSolution

A C# ETL application for processing NYC taxi trip data from CSV files, transforming it, and loading it into a SQL Server database. Optimized for large datasets (e.g., 10GB), it handles data cleaning, duplicate removal, and timezone conversion, with SQL authentication and automatic database setup.

## Features
- **Efficient ETL**: Reads CSV in 10,000-row chunks, uses `SqlBulkCopy` with batch size of 10,000 for fast inserts.
- **Duplicate Handling**: Removes duplicates based on `PickupDatetime`, `DropoffDatetime`, and `PassengerCount`, logs to `duplicates.csv`.
- **Data Transformation**: Converts `store_and_fwd_flag` ('N' to 'No', 'Y' to 'Yes'), trims whitespace, converts EST to UTC.
- **Error Handling**: Logs invalid data to `error_log.txt`, skips bad records.
- **Database Setup**: Automatically creates `CabData` database and `Trips` table if they don't exist, using SQL authentication (`sa` user).
- **Scalability**: Optimized for 10GB files with chunked processing and indexing.
- **Dockerized SQL Server**: Runs SQL Server 2025 in a container with persistent storage.

## Prerequisites
- .NET 8 SDK
- Docker Desktop (or Docker CLI on Linux)
- Input CSV file (e.g., `sample-cab-data.csv`)
- `config.json` and `CabData.sql` in the project root

## Project Structure
```
CabEtlSolution/
├── DataAccess/
│   ├── ICsvDataReader.cs
│   ├── CsvDataReader.cs
│   ├── ISqlDataWriter.cs
│   ├── SqlDataWriter.cs
├── Models/
│   ├── Trip.cs
├── Services/
│   ├── IDataProcessor.cs
│   ├── CabDataProcessor.cs
├── CabData.sql
├── config.json
├── docker-compose.yml
├── Program.cs
├── CabEtlSolution.csproj
├── README.md
```

## Setup Instructions
1. **Clone Repository**:
   ```bash
   git clone <repository-url>
   cd CabEtlSolution
   ```

2. **Configure Files**:
   - Ensure `config.json` exists:
     ```json
     {
       "CsvFilePath": "sample-cab-data.csv",
       "DuplicatesFilePath": "duplicates.csv",
       "ConnectionString": "Server=localhost;Database=CabData;User Id=sa;Password=P@ssw0rd!;TrustServerCertificate=True;"
     }
     ```
   - Ensure `CabData.sql` exists in the project root (see provided code).

3. **Start SQL Server**:
   ```bash
   docker-compose up -d
   ```
   - Starts SQL Server 2025 container (`sqlpreview`) on port 1433 with persistent storage in `./mssql-data`.

4. **Build and Run**:
   ```bash
   dotnet restore
   dotnet build
   dotnet run
   ```
   - Creates `CabData` database and `Trips` table if missing, processes CSV, and outputs: `Rows inserted: 307` (or fewer if duplicates/invalid data).

5. **Verify Database**:
   ```bash
   sqlcmd -S localhost -U sa -P P@ssw0rd! -d CabData -Q "SELECT COUNT(*) FROM Trips;"
   ```

6. **Stop Container**:
   ```bash
   docker-compose down
   ```

## 10GB File Optimizations
- **Chunked Reading**: Processes CSV in 10,000-row chunks to minimize memory usage.
- **Batch Inserts**: Uses `SqlBulkCopy` with `BatchSize=10000` for efficient writes.
- **Duplicate Prevention**: SQL `MERGE` and unique constraint (`UK_Trips`) ensure no duplicates.
- **Error Logging**: Invalid data logged to `error_log.txt`, duplicates to `duplicates.csv`.
- **Indexing**: Indexes on `PULocationID`, `TripDistance`, `TripDuration`, and `TipAmount` for query performance.
- **Retry Logic**: Handles transient SQL connection issues with 3 retries.

## Notes
- **Rows**: 307 (or fewer, depending on duplicates/invalid data).
- **Assumptions**: CSV in EST timezone (no DST), valid `config.json` and `CabData.sql`.
- **Troubleshooting**:
  - Check `docker logs sqlpreview` for SQL Server errors.
  - For Windows hosts, run `sfc /scannow` if `0xC0000001` errors occur.
  - Ensure `MSSQL_MEMORY_LIMIT_MB=4096` is sufficient; increase to 8192 if needed.

## Dependencies
- .NET 8
- CsvHelper (33.0.1)
- System.Data.SqlClient (4.8.6)
- Microsoft.Extensions.Configuration (8.0.0)

- Row - 29889