import argparse
from returns.result import  Success, Failure
from returns.pipeline import flow 
from returns.pointfree import bind
import sys
from loguru import logger

from elastic_lib import create_elastic_client, generate_actions, injest_data
from file_lib import read_file_content, validate_full_file_path
from model import InjestTask
from task_lib import define_injest_task
  

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Injest data to Elasticsearch')
  # Required positional argument
  parser.add_argument('--task_path', type=str,
                    help='Full path to the task file. The file is required.')
  args = parser.parse_args()
  logger.remove(0) # remove the default handler configuration
  logger.add(sys.stdout, level="DEBUG", serialize=False)

  working_dir:str = './test_data'
  injest_task_file_name:str = 'injest.task'

  pipeline_result = flow(
    validate_full_file_path(args.task_path),
    bind(lambda x: read_file_content(x, logger.debug)),
    bind(lambda x: define_injest_task(x, logger.debug)),
  )
  match pipeline_result:
      case Success(data):
          logger.info(f'Elasticsearch task data: {data}')
      case Failure(e):
          logger.critical(e)
          sys.exit(1)
  injest_task:InjestTask = pipeline_result.unwrap()
    
  pipeline_result = flow(
    create_elastic_client(injest_task, logger.debug),
    bind(lambda x: injest_data(x, injest_task, generate_actions(injest_task.data_folder, injest_task.index).unwrap(), logger.debug))
  )
  match pipeline_result:
      case Success(data):
          logger.info(f'Data injest was successful')
      case Failure(e):
          logger.critical(e)
          sys.exit(1)
