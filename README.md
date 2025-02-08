# Project Overview
This project was developed as part of an assignment for the University of Piraeus, Department of Digital Systems, M.Sc. in **Information Systems and Services**, and is **co-authored by Panis Konstantinos**.

This project focuses on designing and implementing a NoSQL database using MongoDB to manage a large dataset related to vessel movements and maritime data. It includes ship positions, trajectories, vessel types, geographic locations, and meteorological conditions. The goal is to create an optimized data model that ensures efficient storage and indexing to support spatial and spatio-temporal queries, facilitating advanced maritime analysis while maintaining high performance.

## Prerequisites
### MongoDB Installation (Standalone)
1. Download MongoDB Community Server from [MongoDB Official Website](https://www.mongodb.com/try/download/community) using the following settings:
   - **Version**: 8.0.4
   - **Platform**: Windows x64
   - **Package**: MSI
2. Install the MongoDB server and ensure that the **MongoDB Compass** checkbox is checked during installation.
3. After installation, open **MongoDB Compass** and connect to the default URI:
   ```
   mongodb://localhost:27017/
   ```
4. Ensure that **MongoSH** (MongoDB Shell) is accessible for running commands.

### Required Dependencies
Before proceeding, install the necessary Python dependencies by running:
```bash
pip install -r requirements.txt
```

## Running MongoDB using Docker (Alternative Setup)
### Prerequisites
- Ensure that **Docker** is installed on your machine. If not, download and install it from [Docker Official Website](https://www.docker.com/get-started).

### Setting Up MongoDB in Docker
1. Open a terminal and run the following command to create and start a MongoDB container:
   ```bash
   docker run --name mongodb -p 27017:27017 -d mongo:8.0.4
   ```
2. Once the container is running, connect to MongoDB using:
   ```bash
   docker exec -it mongodb mongosh
   ```

## Database Setup
After setting up MongoDB, follow the commands in the README files inside the following directories to load the necessary data:
- `load_database/noaa_weather`
- `load_database/dynamic`

These steps ensure that the required datasets are properly loaded into MongoDB before performing maritime data analysis.
