#-*- coding:utf-8 -*- 
from apscheduler.schedulers.blocking import BlockingScheduler
from elasticsearch import Elasticsearch
from datetime import datetime
import json
import  functools

user = 'admin'
password = 'robinwu2017'
host = '127.0.0.1:9200'
# 传入daemonic参数，表示执行线程是非守护的
sched = BlockingScheduler(daemonic=False)
# print u'----------------- decorator ---------------'
def esjson(func):
	@functools.wraps(func)
	def wrapper(*args,**kw):
		res = func(*args,**kw)
		data = json.dumps(res,sort_keys=True,indent=4,separators=(',',':'),encoding='gbk',ensure_ascii=True)
		return res,data 
	return wrapper 

@esjson 
def printjson(func):
	return func 

# print u'----------------- es client --------------' 	
es = Elasticsearch(
		['http://%s:%s@%s' % (user,password,host)],
		verify_certs=False 
		)	

# 查询.kibana,获取所有索引模板
def get_aliasindex():
	aliasindex = []
	data = printjson(es.search(index='.kibana',doc_type='index-pattern',q='*',_source=False,from_=0,size=100))[0]
	for n in range(0,len(data['hits']['hits'])):
		if data['hits']['hits'][n].get('_id') not in aliasindex:
			aliasindex.append(data['hits']['hits'][n].get('_id'))
	return aliasindex 

# 将耗时操作放于后台执行	  
# 查询一个索引模板，统计该索引模板下所有的类型以及对应类型下的主机来源	  
def aggs_aliasindex(index_id):
	body = {
		"size": 0,
		"query": {
			"match_all": {}
		},
		"aggs": {
			"group_by_type": {
				"terms": {
					"field": "_type",
					"size": 100
				},
				"aggs": {
					"group_by_host": {
						"terms": {
							"field": "host",
							"size": 1000
						}
					}
				}
			}
		}
	}
	data = printjson(es.search(index=index_id,body=body))[0]
	buckets = []
	for m in range(0,len(data['aggregations']['group_by_type']['buckets'])):
		indextype = data['aggregations']['group_by_type']['buckets'][m]['key']
		hostgroups = []
		for n in range(0,len(data['aggregations']['group_by_type']['buckets'][m]['group_by_host']['buckets'])):
			host = data['aggregations']['group_by_type']['buckets'][m]['group_by_host']['buckets'][n]['key']
			if host not in hostgroups:
				hostgroups.append(host)	
		if {'type': indextype,"hosts": hostgroups} not in buckets:
			buckets.append({'type': indextype,"hosts": hostgroups})
	return buckets	
		
def insert_kibana(buffer_index):
	for alias in get_aliasindex():
		# 如果索引存在
		if printjson(es.indices.exists(index=alias))[0]:
			body = {
				"buckets": aggs_aliasindex(alias),
				"timestamp": datetime.now()
			}
			es.index(index=buffer_index,doc_type='select-buffter',id=alias,op_type='index',body=body)
		
	
def delete_kibana():
	actions = [
		{"delete":{"_id":"softload-log"}},
		{"delete":{"_id":"ads-log"}},
		{"delete":{"_id":"waf-log"}},
		{"delete":{"_id":"system-log"}},
		{"delete":{"_id":"SSFirewall-log"}},
		{"delete":{"_id":"All-log"}}
	]
	print printjson(es.bulk(index=".kibana",doc_type="select-buffter",body=actions))[1]

@sched.scheduled_job('cron',day="*",hour=2,minute=0,second=0)
def main():
	insert_kibana('.kibana')


if __name__ == '__main__':
	#print printjson(es.info())[1]
	#print es.ping()
	#insert_kibana('.kibana')
	#main() 
	#delete_kibana() 
	print sched.get_jobs()
	sched.start()


