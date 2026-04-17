from fastapi import FastAPI
from pymongo import MongoClient
import os

app = FastAPI(title="Kafka Mongo Dashboard API")

mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
mongo_db = os.getenv("MONGO_DB", "streaming_demo")
mongo_collection = os.getenv("MONGO_COLLECTION", "alertas_temperatura")
mongo_raw_collection = os.getenv("MONGO_RAW_COLLECTION", "eventos_raw")

client = MongoClient(mongo_uri)
collection = client[mongo_db][mongo_collection]
raw_collection = client[mongo_db][mongo_raw_collection]


@app.get("/health")
def health():
    try:
        client.admin.command("ping")
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.get("/alerts_total")
def alerts_total():
    total = collection.count_documents({})
    return [{"value": total}]


@app.get("/alerts_summary")
def alerts_summary():
    total = collection.count_documents({})
    latest = collection.find_one(sort=[("TIMESTAMP", -1)], projection={"_id": 0})
    return {
        "total_alerts": total,
        "latest_alert": latest
    }


@app.get("/alerts_latest")
def alerts_latest(limit: int = 20):
    docs = list(
        collection.find({}, {"_id": 0})
        .sort("TIMESTAMP", -1)
        .limit(limit)
    )
    return docs


@app.get("/alerts_by_type")
def alerts_by_type():
    pipeline = [
        {"$group": {"_id": "$TIPO", "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "label": "$_id", "count": 1}},
        {"$sort": {"count": -1}}
    ]
    return list(collection.aggregate(pipeline))


@app.get("/alerts_by_alert")
def alerts_by_alert():
    pipeline = [
        {"$group": {"_id": "$ALERTA", "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "label": "$_id", "count": 1}},
        {"$sort": {"count": -1}}
    ]
    return list(collection.aggregate(pipeline))


@app.get("/alerts_timeseries")
def alerts_timeseries():
    pipeline = [
        {
            "$addFields": {
                "ts": {
                    "$dateFromString": {
                        "dateString": "$TIMESTAMP",
                        "onError": None,
                        "onNull": None
                    }
                }
            }
        },
        {"$match": {"ts": {"$ne": None}}},
        {
            "$group": {
                "_id": {
                    "$dateTrunc": {
                        "date": "$ts",
                        "unit": "minute"
                    }
                },
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"_id": 1}},
        {
            "$project": {
                "_id": 0,
                "time": "$_id",
                "count": 1
            }
        }
    ]
    return list(collection.aggregate(pipeline))

@app.get("/raw_total")
def raw_total():
    total = raw_collection.count_documents({})
    return [{"value": total}]


@app.get("/raw_latest")
def raw_latest(limit: int = 20):
    docs = list(
        raw_collection.find({}, {"_id": 0})
        .sort("timestamp", -1)
        .limit(limit)
    )
    return docs


@app.get("/raw_by_sensor")
def raw_by_sensor():
    pipeline = [
        {"$group": {"_id": "$sensor_id", "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "label": "$_id", "count": 1}},
        {"$sort": {"count": -1}}
    ]
    return list(raw_collection.aggregate(pipeline))


@app.get("/raw_by_type")
def raw_by_type():
    pipeline = [
        {"$group": {"_id": "$tipo", "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "label": "$_id", "count": 1}},
        {"$sort": {"count": -1}}
    ]
    return list(raw_collection.aggregate(pipeline))


@app.get("/raw_avg_by_sensor")
def raw_avg_by_sensor():
    pipeline = [
        {
            "$group": {
                "_id": "$sensor_id",
                "avg_valor": {"$avg": "$valor"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "label": "$_id",
                "avg_valor": {"$round": ["$avg_valor", 2]}
            }
        },
        {"$sort": {"avg_valor": -1}}
    ]
    return list(raw_collection.aggregate(pipeline))


@app.get("/raw_timeseries")
def raw_timeseries():
    pipeline = [
        {
            "$addFields": {
                "ts": {
                    "$dateFromString": {
                        "dateString": "$timestamp",
                        "onError": None,
                        "onNull": None
                    }
                }
            }
        },
        {"$match": {"ts": {"$ne": None}}},
        {
            "$group": {
                "_id": {
                    "$dateTrunc": {
                        "date": "$ts",
                        "unit": "minute"
                    }
                },
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"_id": 1}},
        {
            "$project": {
                "_id": 0,
                "time": "$_id",
                "count": 1
            }
        }
    ]
    return list(raw_collection.aggregate(pipeline))


@app.get("/raw_value_timeseries")
def raw_value_timeseries():
    pipeline = [
        {
            "$addFields": {
                "ts": {
                    "$dateFromString": {
                        "dateString": "$timestamp",
                        "onError": None,
                        "onNull": None
                    }
                }
            }
        },
        {"$match": {"ts": {"$ne": None}}},
        {
            "$group": {
                "_id": {
                    "time": {
                        "$dateTrunc": {
                            "date": "$ts",
                            "unit": "minute"
                        }
                    },
                    "sensor_id": "$sensor_id"
                },
                "avg_valor": {"$avg": "$valor"}
            }
        },
        {"$sort": {"_id.time": 1}},
        {
            "$project": {
                "_id": 0,
                "time": "$_id.time",
                "sensor_id": "$_id.sensor_id",
                "avg_valor": {"$round": ["$avg_valor", 2]}
            }
        }
    ]
    return list(raw_collection.aggregate(pipeline))

@app.get("/raw_avg_temperature")
def raw_avg_temperature():
    pipeline = [
        {"$match": {"tipo": "temperatura"}},
        {
            "$group": {
                "_id": None,
                "avg_valor": {"$avg": "$valor"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "value": {"$round": ["$avg_valor", 2]}
            }
        }
    ]
    result = list(raw_collection.aggregate(pipeline))
    return result if result else [{"value": 0}]

@app.get("/pipeline_summary")
def pipeline_summary():
    raw_total = raw_collection.count_documents({})
    alerts_total = alerts_collection.count_documents({})
    ratio = round((alerts_total / raw_total) * 100, 2) if raw_total > 0 else 0
    return [{
        "raw_total": raw_total,
        "alerts_total": alerts_total,
        "alert_ratio_pct": ratio
    }]

