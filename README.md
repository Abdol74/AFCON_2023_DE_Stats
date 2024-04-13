# Africa cup of nations dashboard (2023)

# What's case about? (Problem describtion)

Talented players catch the eyes of many European, Latin, and African managers    during the Africa Cup of Nations (AFCON) tournaments. Occurring every two years, these tournaments consistently produce previously unheard-of talent, giving managers an advantage in scouting for players to join one of the top five leagues, such as the Premier League. We provide analytics and statistics on all players during the tournaments, along with comprehensive statistics total matches played throughout the tournament.


# Technology Stack

## Google Cloud Platform (GCP)
- **Compute Engine**: Hosting VM instance.
- **Google Cloud Storage (Datalake)**: Where data lands.
- **BigQuery (Datawarehouse)**: Where data is stored in dimensional modeling.
- **Spark (Data Processing Layer)**: Local cluster on VM instance.

## Mage
- **Orchestration Tool**: Used for our data pipeline flow.

## DBT (Data Build Tool)
- **Reporting Layer**: Built in models.

## Docker
- **Containerization**: Wrapping Mage and Spark in containers.

## Terraform (Infrastructure as Code - Iaas)
- **Deployment**: Used to deploy all the needed resources on GCP.