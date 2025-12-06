"""
Palantir Foundry Insurance Demo Data Generator
Run locally → Generate CSV files → Upload to Foundry datasets
Perfect for quick demo setup without Foundry transforms.
"""

import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os

fake = Faker()
np.random.seed(42)
random.seed(42)

# Configuration for extensive demo
NUM_AGENTS = 50
NUM_POLICYHOLDERS = 200
NUM_CLAIMS = 1500

def generate_agents():
    """Generate 50 insurance agents with realistic data"""
    agents = []
    for i in range(NUM_AGENTS):
        agent = {
            'agentId': f'AGENT_{i}',
            'name': f'Agent {i+1} {"Smith"| "Johnson"| "Brown"| "Davis"| "Miller"}',
            'level': np.random.choice(['Senior', 'Junior', 'Manager'], p=[0.3, 0.5, 0.2]),
            'commissionYTD': round(np.random.uniform(50000, 250000), 2),
            'status': 'Active',
            'region': np.random.choice(['North', 'South', 'East', 'West'])
        }
        agents.append(agent)
    return pd.DataFrame(agents)

def generate_policyholders():
    """Generate 200 policyholders with fraud clustering"""
    policyholders = []
    for i in range(NUM_POLICYHOLDERS):
        # Fraud clusters: first 120 tied to suspicious agents
        primary_agent = ('AGENT_0' if i < 40 else 
                        'AGENT_1' if i < 80 else 
                        'AGENT_2' if i < 120 else 
                        f'AGENT_{random.randint(3, NUM_AGENTS-1)}')
        
        ph = {
            'policyholderId': f'PH_{i}',
            'name': fake.name(),
            'type': np.random.choice(['Individual', 'Family', 'Business'], p=[0.7, 0.25, 0.05]),
            'annualIncome': round(np.random.normal(75000, 25000), 2),
            'birthDate': fake.date_of_birth(minimum_age=25, maximum_age=75).strftime('%Y-%m-%d'),
            'riskProfile': 'High Risk' if np.random.random() < 0.12 else 'Standard',
            'primaryAgent': primary_agent,
            'address': fake.address().replace('\n', ', '),
            'phone': fake.phone_number()
        }
        policyholders.append(ph)
    return pd.DataFrame(policyholders)

def generate_claims(policyholders_df):
    """Generate 1500 claims with sophisticated fraud patterns"""
    claims = []
    
    for i in range(NUM_CLAIMS):
        # Fraud Cluster 1: High-value claims (0-299)
        if i < 300:
            claim_amount = round(np.random.uniform(25000, 75000), 2)
            anomaly_score = round(np.random.uniform(60, 100), 1)
            status = np.random.choice(['Under Investigation', 'Pending Review', 'Approved'], p=[0.4, 0.4, 0.2])
        # Fraud Cluster 2: Medium suspicious (300-599)
        elif i < 600:
            claim_amount = round(np.random.uniform(5000, 20000), 2)
            anomaly_score = round(np.random.uniform(20, 50), 1)
            status = np.random.choice(['Pending Review', 'Approved', 'Denied'], p=[0.5, 0.4, 0.1])
        # Normal claims
        else:
            claim_amount = round(np.random.uniform(500, 5000), 2)
            anomaly_score = round(np.random.uniform(0, 20), 1)
            status = np.random.choice(['Approved', 'Denied'], p=[0.9, 0.1])
        
        # Link to policyholder (dense connections for network demo)
        ph_idx = min(i % len(policyholders_df), len(policyholders_df)-1)
        policyholder_id = policyholders_df.iloc[ph_idx]['policyholderId']
        
        claim_date = (datetime.now() - timedelta(days=random.randint(0, 180))).strftime('%Y-%m-%d')
        
        claim = {
            'claimId': f'CLAIM_{i}',
            'policyholderId': policyholder_id,
            'claimAmount': claim_amount,
            'status': status,
            'anomalyScore': anomaly_score,
            'claimDate': claim_date,
            'claimType': np.random.choice(['Auto', 'Home', 'Health', 'Liability']),
            'multipleClaimsSameDay': np.random.random() < 0.15,
            'sameAddressMultiplePolicies': np.random.random() < 0.08,
            'processingDays': round(np.random.uniform(1, 45), 1),
            'description': f"{'Vehicle collision' if claim['claimType']=='Auto' else 'Home damage' if claim['claimType']=='Home' else 'Medical emergency' if claim['claimType']=='Health' else 'Property liability'} - Filed {claim_date}",
            'agentId': policyholders_df.iloc[ph_idx]['primaryAgent']
        }
        claims.append(claim)
    
    return pd.DataFrame(claims)

def create_demo_files():
    """Generate all CSV files in ./foundry_demo_data/ folder"""
    os.makedirs('foundry_demo_data', exist_ok=True)
    
    print("🚀 Generating Insurance Demo Data...")
    
    # Generate dataframes
    agents_df = generate_agents()
    policyholders_df = generate_policyholders()
    claims_df = generate_claims(policyholders_df)
    
    # Save CSVs
    agents_df.to_csv('foundry_demo_data/agents.csv', index=False)
    policyholders_df.to_csv('foundry_demo_data/policyholders.csv', index=False)
    claims_df.to_csv('foundry_demo_data/insurance_claims.csv', index=False)
    
    # Create relationships CSV for easy Ontology linking
    relationships = []
    for _, claim in claims_df.iterrows():
        relationships.append({
            'claimId': claim['claimId'],
            'policyholderId': claim['policyholderId'],
            'agentId': claim['agentId']
        })
    pd.DataFrame(relationships).to_csv('foundry_demo_data/claim_relationships.csv', index=False)
    
    print("✅ Files created in 'foundry_demo_data/' folder:")
    print("- agents.csv (50 records)")
    print("- policyholders.csv (200 records)") 
    print("- insurance_claims.csv (1500 records)")
    print("- claim_relationships.csv (links for Ontology)")
    
    # Demo statistics
    print("\n📊 Demo Highlights:")
    print(f"   💰 Total claim value: ${claims_df['claimAmount'].sum():,.0f}")
    print(f"   🚨 High anomaly claims (>70): {len(claims_df[claims_df['anomalyScore'] > 70])}")
    print(f"   🔍 Fraud clusters ready for widget demo")
    
    print("\n🎯 Upload Instructions:")
    print("1. Foundry → Data → Datasets → Upload Files")
    print("2. Drag all 4 CSVs from 'foundry_demo_data/'")
    print("3. Object Explorer → Create Objects:")
    print("   - InsuranceClaim (primaryKey=claimId)")
    print("   - Policyholder (primaryKey=policyholderId)")
    print("   - Agent (primaryKey=agentId)")
    print("4. Create linksets using claim_relationships.csv")

if __name__ == "__main__":
    create_demo_files()
