from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

@transform(
    insurance_data=Output("/path/to/your/insurance_claims_dataset"),
    policyholders=Output("/path/to/your/policyholders_dataset"),
    agents=Output("/path/to/your/agents_dataset")
)
def generate_insurance_demo_data(insurance_data, policyholders, agents):
    """
    Generate comprehensive insurance demo data for fraud network widget.
    Creates interconnected claims, policyholders, and agents with realistic
    fraud patterns for extensive demo scenarios.
    """
    
    spark = insurance_data.spark
    
    # 1. Generate AGENTS (50 unique)
    agents_df = spark.range(50).select(
        F.lit(f"AGENT_{F.col('id')}").alias("agentId"),
        F.expr("concat('Agent ', id + 1)").alias("name"),
        F.choice([F.lit("Senior"), F.lit("Junior"), F.lit("Manager")]).alias("level"),
        F.round(F.rand() * 1000000, 2).alias("commissionYTD"),
        F.lit("Active").alias("status")
    )
    agents_df.write.mode("overwrite").saveAsTable("agents")
    
    # 2. Generate POLICYHOLDERS (200 unique with agent assignments)
    policyholders_df = spark.range(200).select(
        F.lit(f"PH_{F.col('id')}").alias("policyholderId"),
        F.expr("concat(first_name(), ' ', last_name())").alias("name"),
        F.lit("Individual").alias("type"),
        F.round(F.rand() * 50000 + 25000, 2).alias("annualIncome"),
        F.date_add(F.lit(datetime(1980,1,1)), (F.col("id") * 100).cast("int")).alias("birthDate"),
        F.when(F.rand() < 0.1, "High Risk").otherwise("Standard").alias("riskProfile"),
        # Assign agents - create fraud clusters
        F.when(F.col("id") < 40, F.lit("AGENT_0"))
         .when((F.col("id") >= 40) & (F.col("id") < 80), F.lit("AGENT_1"))
         .when((F.col("id") >= 80) & (F.col("id") < 120), F.lit("AGENT_2"))
         .otherwise(F.expr("concat('AGENT_', floor(rand()*47)+3)")).alias("primaryAgent")
    )
    policyholders_df.write.mode("overwrite").saveAsTable("policyholders")
    
    # 3. Generate INSURANCE CLAIMS (1500 records with fraud patterns)
    claims_df = spark.range(1500).select(
        F.lit(f"CLAIM_{F.col('id')}").alias("claimId"),
        # Fraud cluster 1: High-value claims via AGENT_0 (claims 0-299)
        F.when(F.col("id") < 300, F.round(F.rand() * 50000 + 25000, 2))  # $25k-$75k
         .when((F.col("id") >= 300) & (F.col("id") < 600), F.round(F.rand() * 15000 + 5000, 2))  # $5k-$20k
         .otherwise(F.round(F.rand() * 5000 + 500, 2)).alias("claimAmount"),  # <$5k
        
        # Claim status with fraud progression
        F.when(F.col("id") < 100, "Under Investigation")  # Fraud cluster
         .when((F.col("id") >= 100) & (F.col("id") < 250), "Pending Review")
         .when(F.rand() < 0.05, "Denied")  # 5% denial rate
         .otherwise("Approved").alias("status"),
        
        # Anomaly score (0-100) - higher = more suspicious
        F.when(F.col("id") < 300, F.round(F.rand() * 40 + 60, 1))  # Fraud cluster: 60-100
         .when((F.col("id") >= 300) & (F.col("id") < 600), F.round(F.rand() * 30 + 20, 1))  # 20-50
         .otherwise(F.round(F.rand() * 20, 1)).alias("anomalyScore"),  # 0-20
        
        # Link to policyholder (create dense connections for demo)
        F.when(F.col("id") < 40, F.lit("PH_0"))
         .when((F.col("id") >= 40) & (F.col("id") < 80), F.lit("PH_1"))
         .when((F.col("id") >= 80) & (F.col("id") < 120), F.lit("PH_2"))
         .otherwise(F.expr("concat('PH_', floor(rand()*197)+3)")).alias("policyholderId"),
        
        # Claim date (recent 6 months)
        F.date_sub(F.current_date(), (F.col("id") % 180).cast("int")).alias("claimDate"),
        
        # Fraud indicators
        F.choice([
            F.lit("Auto"), F.lit("Home"), F.lit("Health"), F.lit("Liability")
        ]).alias("claimType"),
        
        F.when(F.rand() < 0.15, True).otherwise(False).alias("multipleClaimsSameDay"),
        F.when(F.rand() < 0.08, True).otherwise(False).alias("sameAddressMultiplePolicies"),
        F.round(F.rand() * 30, 1).alias("processingDays")
    )
    
    # Add realistic names/descriptions for demo polish
    claims_final = claims_df.withColumn("description", 
        F.concat(F.lit("Damage to "), 
                 F.when(F.col("claimType") == "Auto", F.lit("vehicle VIN ending in XXX"))
                  .when(F.col("claimType") == "Home", F.lit("roof due to storm"))
                  .when(F.col("claimType") == "Health", F.lit("emergency room visit"))
                  .otherwise(F.lit("property damage")),
                 F.lit(" - Reported "),
                 F.to_date(F.col("claimDate"), "yyyy-MM-dd")))
    
    # Write outputs
    claims_final.write.mode("overwrite").saveAsTable("insurance_claims")
    policyholders_df.write.mode("overwrite").saveAsTable("policyholders")
    agents_df.write.mode("overwrite").saveAsTable("agents")
    
    # Demo-ready summary stats
    print("🚀 Demo Data Generated Successfully!")
    print(f"📊 {claims_final.count()} claims created")
    print(f"👥 {policyholders_df.count()} policyholders")
    print(f"👨‍💼 {agents_df.count()} agents")
    print("\n🔍 Key Demo Features:")
    claims_final.groupBy("status").count().orderBy(F.desc("count")).show()
    claims_final.select(F.round(F.avg("claimAmount"), 2).alias("avg"), 
                       F.round(F.max("claimAmount"), 2).alias("max"),
                       F.round(F.avg("anomalyScore"), 2).alias("avg_anomaly")).show()

# BONUS: Quick Ontology write function (run in Code Workbook)
def create_object_sets():
    """Create pre-filtered Object Sets for instant demo load"""
    from ontology_sdk import FoundryClient
    
    client = FoundryClient()
    ontology = client.ontology
    
    # High-risk claims Object Set (for widget parameter)
    high_risk_claims = ontology.object_sets["InsuranceClaim"].filter(
        ontology["InsuranceClaim"]["anomalyScore"] > 70
    )
    high_risk_claims.writeback()
    
    print("✅ Object Sets created: 'High Risk Claims'")
