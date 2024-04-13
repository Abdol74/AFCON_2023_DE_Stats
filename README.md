# Africa cup of nations dashboard (2023)

# What's case about? (Problem describtion)

Talented players catch the eyes of many European, Latin, and African managers    during the Africa Cup of Nations (AFCON) tournaments. Occurring every two years, these tournaments consistently produce previously unheard-of talent, giving managers an advantage in scouting for players to join one of the top five leagues, such as the Premier League. We provide analytics and statistics on all players during the tournaments, along with comprehensive statistics total matches played throughout the tournament.


-	Technology Stack 

o	Google Cloud Platform (GCP):
	Compute Engine: Hosting VM instance.
	Google Cloud Storage (Datalake): Where data land.
	BigQuery (Datawarehouse): Where data land but in dimensional modeling.
	Spark (Data Processing Layer): Local cluster on VM instance.

o	Mage:  Orchestration tool for our data pipeline flow.
o	DBT (Data Build Tool): build our reporting layer in models.
o	Docker: Wrapping Mage and spark in container.
o	Terraform  (Iaac): Deploy all the needed resources on GCP.
