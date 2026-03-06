
import boto3
from botocore.exceptions import ClientError
from elasticsearch import Elasticsearch
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from typing import List, Optional
from pydantic import BaseModel
import logging
from dotenv import load_dotenv
import os
import uvicorn
import ssl
import certifi

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Print default cert paths
print("Default SSL cert file:", ssl.get_default_verify_paths().cafile)
print("Default SSL cert path:", ssl.get_default_verify_paths().capath)
print("Certifi cert path:", certifi.where())

# Initialize FastAPI
app = FastAPI()

from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load environment variables from .env file
load_dotenv()

# Access the environment variables
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = "ap-south-1"
s3_bucket_name = "icc-courtcases"

# Initialize the S3 client with credentials
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    if isinstance(exc, HTTPException):
        status_code = exc.status_code
        detail = exc.detail
    else:
        status_code = 500
        detail = "Internal server error"
    
    return JSONResponse(
        status_code=status_code,
        content={"detail": detail},
    )

class ElasticsearchClient_SSLConnection:
    def __init__(self):
        url = "elasticsearch-190712-0.cloudclusters.net"
        port = 10043
        self.conn = Elasticsearch(
            hosts=[{"host": url, "port": port, "scheme": "https"}],
            http_auth=("elastic", "HmtoTvKY"),
            verify_certs=True,
            ca_certs="/Users/billionaire/Downloads/Fiverr/certs/ca_certificate.pem",
            ssl_certfile="/Users/billionaire/Downloads/Fiverr/certs/client_elasticsearch-190712-0.cloudclusters.net_certificate.pem",
            ssl_keyfile="/Users/billionaire/Downloads/Fiverr/certs/client_elasticsearch-190712-0.cloudclusters.net_key.pem",
            compatibility_mode=False,
            headers={"Content-Type": "application/json"}# Update path if needed
        )
        logger.info(f"Elasticsearch connected: {self.conn.ping()}")

try:
    es_client = ElasticsearchClient_SSLConnection()
except Exception as e:
    es_client = None  

pdf_mappings = {}
filename_to_key = {}

class SearchResponse(BaseModel):
    total: int
    results: List[dict]
    facets: dict

@app.exception_handler(HTTPException)
async def unified_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )

@app.get("/api/search", response_model=SearchResponse)
async def search(
    q: str = Query(None),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
    yearFrom: Optional[str] = None,
    yearTo: Optional[str] = None,
    court: Optional[str] = None,
    sortOrder: Optional[str] = Query(None, regex="^(asc|desc)$") 
):
    if not es_client:
        raise HTTPException(status_code=500, detail="Elasticsearch connection failed")

    try:
        from_value = (page - 1) * size
        filter_conditions = []

        


        if yearFrom or yearTo:
            year_range = {}
            if yearFrom:
                year_range["gte"] = str(yearFrom).zfill(4)
            if yearTo:
                year_range["lte"] = str(yearTo).zfill(4)
            filter_conditions.append({
                "range": {
                    "JudgmentMetadata.CaseDetails.JudgmentYear.keyword": year_range
                }
            })

        # Add court filter based on _id prefix
        if court in ["SC", "HC"]:
            filter_conditions.append({
                "prefix": {"JudgmentMetadata.DocumentID.keyword": court}
            })


        query = {  
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": q,
                            "fields": ["*"],
                            "type": "best_fields"
                        }
                    }
                ],
                "filter": filter_conditions 
            }
        }
        sort_clause = []
        if sortOrder:
            # Sort by year (primary) and relevance (secondary)
            sort_clause.append(
                {"JudgmentMetadata.CaseDetails.JudgmentYear.keyword": {"order": sortOrder}}
            )
            sort_clause.append({"_score": {"order": "desc"}})  # Secondary sort for relevance
        else:
            # Default to relevance only (no year sorting)
            sort_clause.append({"_score": {"order": "desc"}})

        
        aggs = {
            "years": {
                "terms": {
                    "field": "JudgmentMetadata.CaseDetails.JudgmentYear.keyword",
                    "size": 50,
                    **({"order": {"_key": sortOrder}} if sortOrder else {})
                }
            },
            "courts": {
                "terms": {
                    "script": {
                        "source": "doc['_id'].value.substring(0,2)",
                        "lang": "painless"
                    },
                    "size": 50
                }
            }
        }

        response = es_client.conn.search(
            index="judgements-index",
            body={
                "query": query,  # Now correctly placed at top level
                "aggs": aggs,
                "from": from_value,
                "size": size,
                "track_total_hits": True,
                "sort": sort_clause
            }
        )

        results = []
        for hit in response["hits"]["hits"]:
            metadata = hit["_source"]["JudgmentMetadata"]
            doc_id = metadata["DocumentID"]
            mapped_filename = pdf_mappings.get(doc_id)
            result_dict = {"id": doc_id, "_score": hit["_score"], **hit["_source"]}

            if mapped_filename:
                # Normalize filename to citation format (remove .pdf, adjust casing)
                corrected_citation = mapped_filename.replace('.pdf', '').replace('SUPREME COURT', 'SC')
                # Override the citation in the result
                if "Citations" in metadata and metadata["Citations"]:
                    result_dict["JudgmentMetadata"]["Citations"][0]["Citation"] = corrected_citation
                else:
                    # Handle cases where Citations might be missing or empty
                    result_dict["JudgmentMetadata"]["Citations"] = [{"Citation": corrected_citation}]
                logger.debug(f"Overrode citation for doc_id={doc_id} to {corrected_citation}")

            results.append(result_dict)
        
        court_buckets = [
            {"key": "SC", "doc_count": 0},
            {"key": "HC", "doc_count": 0}
        ]
        for bucket in response["aggregations"]["courts"]["buckets"]:
            if bucket["key"] in ["SC", "HC"]:
                for cb in court_buckets:
                    if cb["key"] == bucket["key"]:
                        cb["doc_count"] = bucket["doc_count"]

        return {
            "total": response["hits"]["total"]["value"],
            "results": results,
            "facets": {
                "years": response["aggregations"]["years"],
                "courts": {"buckets": court_buckets}
            }
        }
        
    except Exception as e:
        logger.error(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
@app.on_event("startup")
async def load_pdf_mappings():
    global filename_to_key
    try:
        response = s3_client.get_object(
            Bucket=s3_bucket_name,
            Key="pdf-cleaned/unique_id.txt"
        )
        content = response['Body'].read().decode('utf-8').splitlines()
        
        for line in content:
            line = line.strip()  # Remove leading/trailing whitespace
            if '-' in line:
                # Split only on the first hyphen to handle filenames with hyphens
                parts = line.split('-', 1)
                doc_id = parts[0].strip()
                filename = parts[1].strip()
                pdf_mappings[doc_id] = filename
                logger.debug(f"Mapped doc_id={doc_id} to filename={filename}")
        
        paginator = s3_client.get_paginator('list_objects_v2')
        operation_parameters = {
            'Bucket': s3_bucket_name,
            'Prefix': 'pdf-cleaned/'
        }
        
        for page in paginator.paginate(**operation_parameters):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('.pdf'):
                        filename = key.split('/')[-1]
                        filename_to_key[filename] = key
        
        logger.info(f"Loaded {len(pdf_mappings)} PDF mappings and {len(filename_to_key)} S3 keys")
        
    except Exception as e:
        logger.error(f"Startup error: {e}")
        raise

@app.get("/api/get-pdf-url")
async def get_pdf_url(doc_id: str):
    try:
        # Step 1: Retrieve the filename from unique_id.txt mappings
        pdf_filename = pdf_mappings.get(doc_id)
        if not pdf_filename:
            logger.error(f"No mapping found for doc_id={doc_id}")
            raise HTTPException(status_code=404, detail="PDF mapping not found")
        logger.debug(f"Found filename={pdf_filename} for doc_id={doc_id}")
        # Step 2: Get the S3 key from filename_to_key
        pdf_key = filename_to_key.get(pdf_filename)
        if not pdf_key:
            logger.error(f"No S3 key found for filename={pdf_filename}")
            raise HTTPException(status_code=404, detail="PDF not found in S3")
        logger.debug(f"Found key={pdf_key} for filename={pdf_filename}")
        # Step 3: Validate the S3 object exists
        s3_client.head_object(Bucket=s3_bucket_name, Key=pdf_key)

        # Step 4: Generate the presigned URL
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': s3_bucket_name,
                'Key': pdf_key,
                'ResponseContentDisposition': 'inline',
                'ResponseContentType': 'application/pdf'
            },
            ExpiresIn=3600
        )
        return {"url": url}

    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            raise HTTPException(status_code=404, detail="PDF not found")
        logger.error(f"S3 error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    except Exception as e:
        logger.error(f"PDF fetch error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/", tags=["Health Check"])
async def root():
    return {
        "message": "Judgment Search API is running",
        "status": "healthy",
        "es_connected": bool(es_client and es_client.conn.ping()),
        "api_version": "1.0",
        "endpoints": {
            "/api/search": "Search endpoint",
            "/api/get-pdf-url": "PDF retrieval",
            "/api/autocomplete": "Autocomplete suggestions"
        }
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
