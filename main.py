import boto3
from botocore.exceptions import ClientError
from elasticsearch import Elasticsearch
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from pydantic import BaseModel
import logging
from dotenv import load_dotenv
import os
import uvicorn

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://elastic-search-react-u30628.vm.elestio.app",
        "https://elastic-search-python-u30628.vm.elestio.app"
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=3600,
)

# Load environment variables from .env file
load_dotenv()

# Access the environment variables
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = "ap-south-1"
s3_bucket_name = "xxxxxxxx"

# Initialize the S3 client with credentials
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

class ElasticsearchClient_SSLConnection:
    def __init__(self):
        url = "elasticsearch-xxxxxxx-0.cloudclusters.net"
        port = ""
        try:
            self.conn = Elasticsearch(
                hosts=[{"host": url, "port": port, "scheme": "https"}],
                http_auth=("xxxxxxx", "xxxxxxx"),
                verify_certs=True,
                ca_certs="path_to/ca_certificate.pem",
            )
            if not self.conn.ping():
                logger.error("Ping failed - using connection anyway")
        except Exception as e:
            logger.error(f"Elasticsearch init error: {e}")
            raise

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

@app.middleware("http")
async def add_cors_headers(request, call_next):
    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "https://elastic-search-react-u30628.vm.elestio.app"
    response.headers["Access-Control-Allow-Credentials"] = "true"
    return response

@app.middleware("http")
async def log_requests(request, call_next):
    logger.debug(f"Incoming request: {request.method} {request.url}")
    logger.debug(f"Headers: {request.headers}")
    response = await call_next(request)
    logger.debug(f"Response status: {response.status_code}")
    logger.debug(f"Response headers: {response.headers}")
    return response

@app.get("/api/search", response_model=SearchResponse)
async def search(
    q: str = Query(None),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
    yearFrom: Optional[str] = None,
    yearTo: Optional[str] = None,
    court: Optional[str] = None,
    sortOrder: str = Query("desc", regex="^(asc|desc)$") 
):
    if not es_client:
        raise HTTPException(status_code=500, detail="Elasticsearch connection failed")

    try:
        from_value = (page - 1) * size
        must_conditions = []

        # Search query
        if q:
            must_conditions.append({
                "multi_match": {
                    "query": q,
                    "fields": ["*"]
                }
            })

        # Year range filter
        if yearFrom or yearTo:
            year_range = {}
            if yearFrom:
                year_range["gte"] = str(yearFrom).zfill(4)  # Ensure 4-digit format (e.g., "1950")
            if yearTo:
                year_range["lte"] = str(yearTo).zfill(4)
            must_conditions.append({
                "range": {
                    "JudgmentMetadata.CaseDetails.JudgmentYear.keyword": year_range  # Correct nested path
                }
            })
        # Court filter using script
        if court in ["SC", "HC"]:
            must_conditions.append({
                "script": {
                    "script": {
                        "source": "doc['_id'].value.startsWith(params.prefix)",
                        "params": {"prefix": court}
                    }
                }
            })

        query = {"bool": {"must": must_conditions or [{"match_all": {}}]}}

        # Aggregations
        aggs = {
            "years": {
                "terms": {
                    "field": "JudgmentMetadata.CaseDetails.JudgmentYear.keyword",
                    "size": 50,
                    "order": {"_key": "desc"}
                }
            },
            "courts": {
                "terms": {
                    "script": {
                        "source": "doc['_id'].value.substring(0,2)",
                        "lang": "painless"
                    },
                    "size": 50  # Increased from 2 to capture all court types
                }
            }
        }

        response = es_client.conn.search(
            index="judgements-index",
            body={"query": query, "aggs": aggs, "from": from_value, "size": size, "track_total_hits": True, "sort": [
                {"JudgmentMetadata.CaseDetails.JudgmentYear.keyword": {"order": sortOrder}},
                {"_id": "asc"}  # Secondary sort for stability
            ]}
        )

        # Process results
        results = [{"id": hit["_id"], **hit["_source"]} for hit in response["hits"]["hits"]]
        
        # Process court facets
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
    global filename_to_key  # Add this
    try:
        # Load unique_id mappings
        response = s3_client.get_object(
            Bucket=s3_bucket_name,
            Key="pdf-cleaned/unique_id.txt"
        )
        content = response['Body'].read().decode('utf-8').splitlines()
        
        for line in content:
            try:
                if '-' in line:
                    doc_id, filename = line.split('-', 1)
                    pdf_mappings[doc_id.strip()] = filename.strip()
            except Exception as e:
                logger.error(f"Skipping invalid line: {line} | Error: {e}")
        
        # Build filename-to-S3-key mapping
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
        pdf_filename = pdf_mappings.get(doc_id)
        if not pdf_filename:
            raise HTTPException(status_code=404, detail="PDF mapping not found")
        
        # Get full S3 key from filename mapping
        pdf_key = filename_to_key.get(pdf_filename)
        if not pdf_key:
            raise HTTPException(status_code=404, detail="PDF not found in S3")
        
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': s3_bucket_name,
                'Key': pdf_key,
                'ResponseContentDisposition': 'inline',  # Force browser to display
                'ResponseContentType': 'application/pdf'  # Explicit MIME type
            },
            ExpiresIn=3600
        )
        return {"url": url}
        
    except ClientError as e:
        logger.error(f"S3 error: {e}")
        raise HTTPException(status_code=404, detail="PDF not found")
    except Exception as e:
        logger.error(f"PDF fetch error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/api/autocomplete")
async def autocomplete(q: str = Query(...)):
    try:
        response = es_client.conn.search(
            index="judgements-index",
            body={
                "suggest": {
                    "judgement-suggest": {
                        "prefix": q,
                        "completion": {
                            "field": "suggest",
                            "skip_duplicates": True,
                            "size": 5
                        }
                    }
                }  
            },
            request_timeout=30 
        )
        return [opt["text"] for opt in response["suggest"]["judgement-suggest"][0]["options"]]
    except Exception as e:
        return []

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
