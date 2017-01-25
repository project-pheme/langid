#!/usr/bin/env python2.7
# invocation: python langid_kafka.py <in topic> <out topic prefix>

import dbm
import json
import langid
from kafka import KafkaConsumer, SimpleProducer, KafkaClient
import random

import logging
import sys
import time

in_topic = sys.argv[1]
out_prefix = sys.argv[2]

print time.strftime('%c'), 'started'


root = logging.getLogger()
root.setLevel(logging.DEBUG)

#ch = logging.StreamHandler(sys.stderr)
#ch.setLevel(logging.DEBUG)
#formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#ch.setFormatter(formatter)
#root.addHandler(ch)

db = dbm.open('offset.langid', 'c')
if 'offset' not in db:
	db['offset'] = str(0)
prev_offset = int(db['offset'])

server = 'gatezkt1.storm:9092'
kafka = KafkaClient(server)
out 		= SimpleProducer(kafka)

raw_in	 	= KafkaConsumer(in_topic, group_id='langid', bootstrap_servers=[server], auto_offset_reset='smallest', auto_commit_interval_ms=10000)

print time.strftime('%c'), 'connected; waiting for content'

for message in raw_in:
	if message:

#		if message.offset < prev_offset - 1000: # give a jitter window
#			continue

#		if prev_offset < message.offset:
#			prev_offset = message.offset

#		print(message)
		try:
			capture_src = json.loads(message.value.decode("utf-8"))
		except:
			print('bad raw content, offset', message.offset)
			print(message.value)
			continue

		# unpack from atos scrape format
		try:
			doc = dict(capture_src['raw_json'])
		except:
			print('failed to unpack, offset', message.offset)
			print(doc)
			continue

		# propagate selected fields
		for field_to_propagate in ('dc_id', 'raw_json', 'user_screen_name', 'source_type', 'text', 'id_str', 'created_at', 'source_type'):
			doc[field_to_propagate] = capture_src[field_to_propagate]

		# do lang id
		try:
			result = langid.classify(doc['text'])
		except:
			print('content not in spec, offset', message.offset, message.value[:450])
			continue

		# do we discard doc?
		if result[0] not in ('en','de','bg'):
			continue

		# add field
		doc['langid'] = result


		# post to kakfa
		if result[0] in ('bg', 'en', 'de'):
			out_json = json.dumps(doc).encode("utf-8")
			try:
				out.send_messages(out_prefix + result[0], out_json)
				print time.strftime('%c'), message.offset, doc['id_str'], result[0], json.dumps(doc)[:130]
			except:
				print time.strftime('%c'), message.offset, doc['id_str'], 'FAIL', result[0], json.dumps(doc)[:130]

		if random.random() < 0.01:
			db['offset'] = str(prev_offset)


