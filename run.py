#!/usr/bin/python

import argparse
import json
import logging
import os
import sys
import uuid

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from requests_aws4auth import AWS4Auth

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config", action="store", default="config.json", help="Config file in json")
parser.add_argument("-e", "--env", action="store", default="local", choices=['local', 'prod', 'aws'], help="Environment name")
parser.add_argument("-d", "--data", action="store", required=True, help="Data json file")
parser.add_argument("-i", "--index", action="store", default="test", help="Elastic search index")
parser.add_argument("-t", "--type", action="store", default="doc", help="Elastic search type")
parser.add_argument("-s", "--script", action="store", help="Data Conversion python script file")
parser.add_argument("-v", "--verbose", action='store_true', help="Verbose log")
args = parser.parse_args()

logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, handlers=[logging.StreamHandler()],
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger('es_tool')

logger.info('=' * 20)
for k, v in vars(args).items():
    logger.info('%s:%s', k, v)
logger.info('*' * 20)


def load_json(file):
    with open(file, 'r', encoding='utf8') as file_json:
        try:
            return json.load(file_json)
        except Exception:
            raise RuntimeError('File is not JSON format')


def load_script(script):
    if not script:
        return None
    abs_path = os.path.abspath(script)
    exists = os.path.isfile(abs_path)
    if not exists:
        logger.warning('File not found or not is file')
        return None
    base = os.path.splitext(os.path.basename(abs_path))
    if len(base) != 2 or base[1] != '.py':
        logger.warning('Not python file')
        return None
    logger.info('Script: %s', abs_path)
    sys.path.append(abs_path)
    return __import__(base[0])


def validate_input(env_cfg) -> dict:
    if not env_cfg:
        raise RuntimeError('Missing environment configuration')
    if not env_cfg.get('urls', None):
        raise RuntimeError('Missing urls')
    return env_cfg


def init_aws_auth(auth):
    if not auth:
        raise AttributeError('Missing AWS authentication')
    return AWS4Auth(auth.get('access_key', None), auth.get('secret_key', None),
                    auth.get('region', None), auth.get('service', None))


def create_es_client(env) -> Elasticsearch:
    if args.env == 'aws':
        auth = init_aws_auth(env.get('auth', None))
    else:
        auth = env.get('http_auth', None)
    client = Elasticsearch(env.get('urls'), http_auth=auth, **env.get('extras', {}))
    if not client.ping():
        raise RuntimeError('Cannot connect to Elastic Search server')
    return client


def transform(script, data):
    converter = load_script(script)
    for datum in data:
        yield converter.transform(datum) if converter else def_transform(datum)


def def_transform(datum):
    key_id = datum.get('_id', None)
    datum['_id'] = (key_id.get('$oid', None) if isinstance(key_id, dict) else (key_id or datum.get('id', None))) or uuid.uuid4()
    return datum


def run():
    config = load_json(args.config)
    env_cfg = validate_input(config.get(args.env, None))
    client = create_es_client(env_cfg)
    for ok, result in streaming_bulk(client, transform(args.script, load_json(args.data)),
                                     index=args.index, doc_type=args.type, chunk_size=50):
        action, result = result.popitem()
        doc_id = '/%s/doc/%s' % (args.index, result['_id'])
        if not ok:
            logger.error('Failed to %s document %s: %r', action, doc_id, result)
        else:
            logger.info(doc_id)


run()
