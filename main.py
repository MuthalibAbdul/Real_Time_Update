from fastapi import FastAPI, Request
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
 
# Load the .env file
load_dotenv()
 
# Access variables
server = os.getenv("server")
database = os.getenv("database")
username = os.getenv("username")
password = os.getenv("password")
app = FastAPI()

def preprocess(data):
    df = pd.DataFrame([data])

    df = df.rename(columns={
        "Intervention Reason": "Intervention_Reason",
        "Billing Date": "Billing_Date",
        "Billed Yes/No": "Billed",
        "SNF/Telehealth": "Mode",
        "Date of Service": "Date_of_Service",
        "Note Posted": "Note_Posted",
        "Patient Name (Last, First)": "Patient_Name",
        "CPT Code": "CPT_Code"
    })

    # Convert date-like columns
    for col in ["DOB", "Date_of_Service", "Billing_Date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        df[col] = pd.to_datetime(df[col]).dt.strftime("%m/%d/%Y")
    df = df[df['Location'].notnull() & (df['Location'] != "")]
    df = df[df['Date_of_Service'].notnull() & (df['Date_of_Service'] != "")]
    df = df[df['Patient_Name'].notnull() & (df['Patient_Name'] != "")]
    df = df[df['Id'].notnull() & (df['Id'] != "")]

    df["inserted_at"] = datetime.utcnow()
    return df

def insert_data(df):
# Replace with your details

    # Create SQLAlchemy engine (uses pyodbc under the hood)
    engine = create_engine(
        f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server"
    )
    # Replace table each time
    print("inserting")
    df.to_sql(
        name="StagingIDS_table_v2",  # target table
        con=engine,
        if_exists="append",       # append new rows
        index=False              
    )

def upsert_ids():

    # Create SQLAlchemy engine (uses pyodbc under the hood)
    engine = create_engine(
        f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server"
    )
 
    # -------------------------------
    # T-SQL MERGE statement
    # -------------------------------
    merge_sql = f"""
    WITH LatestStaging AS (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY Id
                       ORDER BY inserted_at DESC
                   ) AS rn
            FROM db_owner.stagingIDS_table_v2
        ) t
        WHERE rn = 1
    )
    MERGE db_owner.IDS_table_v2 AS target
    USING LatestStaging AS source
        ON target.Id = source.Id
    WHEN MATCHED THEN
        UPDATE SET
            Intervention_Reason = source.Intervention_Reason,
            Billing_Date        = source.Billing_Date,
            Billed              = source.Billed,
            Mode                = source.Mode,
            Date_of_Service     = source.Date_of_Service,
            Note_Posted         = source.Note_Posted,
            Diagnosis1          = source.Diagnosis1,
            Diagnosis2          = source.Diagnosis2,
            Diagnosis3          = source.Diagnosis3,
            Comments            = source.Comments,
            Practitioner_Name   = source.Practitioner_Name
            updated_at          = {datetime.utcnow()}
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            Id, Intervention_Reason, Billing_Date, Billed, Location, Mode,
            Date_of_Service, Note_Posted, Patient_Name, DOB, CPT_Code,
            Diagnosis1, Diagnosis2, Diagnosis3, Comments, Practitioner_Name, inserted_at
        )
        VALUES (
            source.Id, source.Intervention_Reason, source.Billing_Date, source.Billed, source.Location, source.Mode,
            source.Date_of_Service, source.Note_Posted, source.Patient_Name, source.DOB, source.CPT_Code,
            source.Diagnosis1, source.Diagnosis2, source.Diagnosis3, source.Comments, source.Practitioner_Name, {datetime.utcnow()}
        );
    """
 
    # -------------------------------
    # Execute the MERGE
    # -------------------------------
    with engine.begin() as conn:  # ensures transaction
        conn.execute(text(merge_sql))
        print("✅ Merge/upsert completed successfully!")


@app.post("/sheet-webhook")
async def sheet_webhook(request: Request):
    data = await request.json()
    print("✅ Received update:", data)
    df = preprocess(data)
    insert_data(df)
    upsert_ids()
    return {"status": "ok"}
    