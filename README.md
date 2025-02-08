## Project Overview
This project was developed as part of an assignment for the University of Piraeus, Department of Digital Systems, M.Sc. in “Information Systems and Services,” and is **co-authored by Panis Konstantinos**.

This project focuses on designing and implementing a NoSQL database using MongoDB to manage a large dataset related to vessel movements and maritime data, including ship positions, trajectories, vessel types, geographic locations, and meteorological conditions. The goal is to create an optimized data model that ensures efficient storage and indexing to support spatial and spatio-temporal queries, facilitating advanced maritime analysis while maintaining high performance.

## Prerequisites / Steps
1) Download MongoDB Community Server from https://www.mongodb.com/try/download/community (version: 8.0.4, platform: Windows x64, package: msi). Install server and ensure that MongoDB Compass checkbox is ckecked too.

2) After MongoDB installation, open MongoDB Compass and connect to URI mongodb://localhost:27017/ (default URI). Make sure you can use MongoSH.
   
3) Before proceeding, ensure that all necessary dependencies are installed. Run the following command to install the required modules:

```bash
pip install -r requirements.txt
```

**If you prefer working with docker :**
## Prerequisites

- Docker must be installed on your machine.  
  If you don't have Docker, [download and install it here](https://www.docker.com/get-started).

## To Set Up MongoDB

### 1. **Run MongoDB Container with Docker**

Open a terminal and run the following command to create and start a MongoDB container. This will expose MongoDB's port 27017 on your localhost.

```bash
$ docker run --name mongodb -p 27017:27017 -d mongo:8.0.4
```

Once the container is running, connect to MongoDB by entering the following command:

```bash
$ docker exec -it mongodb mongosh
```

4) Additionally, follow the commands in the README files inside the load_database/noaa_weather and load_database/dynamic folders to set up the necessary data. 
