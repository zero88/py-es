#!/usr/bin/python

import uuid
from datetime import datetime


def __to_bool__(s: str):
    if not s:
        return s
    if s.lower() in ['true', '1']:
        return True
    if s.lower() in ['false', '0']:
        return False
    return s


def transform(datum: dict) -> dict:
    datum = {k.lower(): v for k, v in datum.items()}
    key_id = datum.get('_id', None)
    datum['_id'] = (key_id.get('$oid', None) if isinstance(key_id, dict) else (key_id or datum.get('id', None))) or uuid.uuid4()
    datum['_indexed_date'] = datetime.utcnow()
    return datum
