from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
from pymongo import MongoClient, ReplaceOne
from pymongo.collection import Collection

load_dotenv()

_client: MongoClient | None = None


def _get_client() -> MongoClient:
    global _client
    if _client is None:
        uri = os.environ["MONGODB_URI"]
        _client = MongoClient(uri)
    return _client


def get_collection() -> Collection:
    db_name = os.environ.get("MONGODB_DB", "nucleus")
    return _get_client()[db_name]["user_wise_metrics"]


def fetch_user_doc(user_id: str) -> dict[str, Any] | None:
    return get_collection().find_one({"user_id": user_id}, {"_id": 0})


def replace_user_doc(doc: dict[str, Any]) -> None:
    get_collection().replace_one({"user_id": doc["user_id"]}, doc, upsert=True)


def fetch_all_user_docs() -> dict[str, dict[str, Any]]:
    return {doc["user_id"]: doc for doc in get_collection().find({}, {"_id": 0})}


def bulk_replace_user_docs(docs: list[dict[str, Any]]) -> None:
    if not docs:
        return
    ops = [ReplaceOne({"user_id": doc["user_id"]}, doc, upsert=True) for doc in docs]
    get_collection().bulk_write(ops, ordered=False)


def ensure_index() -> None:
    get_collection().create_index("user_id", unique=True)
