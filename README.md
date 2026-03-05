Modern Data Engineering with Databricks
Hosted by: Gambill Data
Instructor: Chris Gambill, Principal Architect
Welcome to the companion repository for the Modern Data Engineering Workshop. This repository contains the code, configuration, and architectural patterns required to build a production-grade Medallion architecture on Databricks.
🏛️ The Gambill Data Pillars
Everything in this repository is built on three core philosophies:
Code to Cash: If code doesn’t solve a business problem or save money, it doesn’t have a reason for existence. We deploy value, not just scripts.
Anti-Fragility: We don't try to prevent errors; we build systems that survive them. We handle bad data gracefully through quarantines and automated retries.
Strategy > Syntax: Anyone can ask an LLM for syntax. Few can architect for production. We prioritize robust frameworks over clever hacking.
📋 Prerequisites
Before starting the labs, ensure you have the following:
Databricks Free Edition: A personal workspace with Unity Catalog enabled. (Sign Up Here)
Salesforce Developer Account: A sandbox environment with generated security tokens. (Sign Up Here)
GitHub / GitLab Account: A personal access token (PAT) to sync your workspace to version control.
🗺️ Workshop Roadmap & Lab Guide
Module 0: The Governance Foundation
Rule #1: We don't write code until we have a place to put it.
In this module, we build our "Mini-Enterprise" in Unity Catalog to enforce absolute isolation between Development and Production.
Dev Catalogs: bronze_dev, silver_dev, gold_dev
Prod Catalogs: bronze_prod, silver_prod, gold_prod (Left strictly empty for CI/CD)
Module 1: Strategic Ingestion (Lakeflow Connect)
We ingest data dynamically from a live SaaS application without writing custom API pollers.
Action: Configure a Lakeflow Connect pipeline to pull Account, Contact, and Opportunity objects from Salesforce.
Target: bronze_dev.salesforce schema.
Module 2: The Core Logic (Declarative Pipelines)
We transition from Imperative micromanagement to Declarative leadership using Delta Live Tables (DLT).
Bronze/Silver: Utilize Streaming Tables (@dlt.table) for low-latency, stateful processing. Apply Data Quality Expectations (@dlt.expect_or_drop) to drop negative revenue amounts.
Gold: Utilize Materialized Views for complex aggregations (joining Account and Opportunity into a Revenue Summary).
Action: Deploy the pipeline manually via the DLT UI ("ClickOps").
Modules 3 & 4: From ClickOps to CI/CD (DABs)
We execute the "Senior Transition." We convert our UI-built pipeline into Infrastructure as Code (IaC).
Action: Initialize a Databricks Asset Bundle (databricks.yml).
FinOps: Destroy the manual Dev pipeline to free up Free-Tier compute.
Deploy to Dev: databricks bundle deploy -t dev
Promote to Prod: databricks bundle deploy -t prod (Watch the engine dynamically populate the empty _prod catalogs).
Module 5: Anti-Fragility & Observability
We refactor our code to implement the Quarantine Pattern.
Instead of silently dropping bad data, we route it to a Dead Letter Queue (quarantine_opportunities) for RevOps triage.
We prove the pipeline's ROI by querying the DLT Event Log to build a DQX (Data Quality) reporting dashboard.
💻 Databricks Asset Bundles (DABs) Cheat Sheet
If you are using the Databricks Workspace Terminal, here are the core commands you will use during Modules 3, 4, and 5:
# Initialize a new bundle project
databricks bundle init

# Validate your YAML configuration syntax
databricks bundle validate

# Deploy your infrastructure to the Dev target
databricks bundle deploy -t dev

# Run your deployed pipeline
databricks bundle run -t dev

# Tear down infrastructure (FinOps/Cost Savings)
databricks bundle destroy -t dev


🔗 Resources
Databricks Free Edition Documentation
Delta Live Tables (DLT) Guide
Databricks Asset Bundles (DABs) Reference
© 2026 Gambill Data. Engineered for Production.
