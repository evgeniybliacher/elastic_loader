import os
from typing import Any, Callable, Dict
from returns.result import safe

@safe
def validate_full_file_path(full_path:str)->str:
    if os.path.exists(full_path):
        return full_path
    raise Exception(f'File {full_path} does not exist')

@safe       
def read_file_content(file_path:str, logg: Callable[[str],Any])->str:
    logg(f'Read content from file: {file_path}')
    with open(file_path) as f:
        return f.read()
        

def generate_action_json(index_name:str, pipeline:str, source: Dict[str,str]):
  match pipeline:
    case None:
        return {
                "_index": index_name,
                "_source": source
            }
    case _:
        return {
                "_index": index_name,
                "_source": source,
                "pipeline": pipeline
            }

def cleanup_escape_characters(content:str)->str:
  escapes = ''.join([chr(char) for char in range(1, 32)])
  translator = str.maketrans('', '', escapes)
  return content.translate(translator)