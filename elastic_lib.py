import json
import os
from time import sleep
from typing import Any, Callable
from elasticsearch import Elasticsearch, helpers
from returns.result import safe
import tqdm

from file_lib import cleanup_escape_characters, generate_action_json
from model import InjestTask


@safe
def create_elastic_client(injest_task:InjestTask, logg: Callable[[str],Any])->Elasticsearch:
    logg(f'Create a new instance of the Elasticsearch client')
    client = Elasticsearch(f'{injest_task.host}:{injest_task.port}/', basic_auth=(injest_task.user,injest_task.password))
    if not client.info():
        raise Exception('Cannot connect to Elasticsearch')
    return client

        
               
#@safe
#def doc_stream():                                                          
   #''' generator function for stream of actions '''                      
   #for i in range(1,100):                                                  
       #yield {'_index': 'my_index',                                   
              #'_source': {'my_field': getrandbits(32)} } 
@safe
def generate_actions(data_folder:str, index_name:str, pipeline:str=None):
  for file in os.listdir(data_folder):
    if file.endswith(".json"):
      with open(os.path.join(data_folder, file)) as json_file:
        f = cleanup_escape_characters(json_file.read())
        source = json.loads(f)
        yield generate_action_json(index_name, pipeline, source)

@safe
def injest_data(es_client:Elasticsearch, task:InjestTask, data_iterator, logg: Callable[[str],Any])->str:
    logg(f'Injest data to Elasticsearch')
    progress = tqdm.tqdm(unit="docs", total=task.total_count)
    for status_ok, response in helpers.streaming_bulk(es_client,
                                          actions=data_iterator, 
                                          chunk_size=task.chunk_size):
      progress.update(task.chunk_size)
      sleep(task.sleep_between_chunks)
      if not status_ok:                                                           
          logg(response)
          raise Exception(f'Data injest failed {response}')