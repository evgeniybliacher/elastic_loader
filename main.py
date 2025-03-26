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
  # Optional positional argument
  parser.add_argument('--username', type=str, nargs='?',
                    help='An optional string positional argument for basic authentication. Default is None.')
   
  parser.add_argument('--password', type=str, nargs='?',
                    help='An optional string positional argument for basic authentication. Default is None.') 

  args = parser.parse_args()
  logger.remove(0) # remove the default handler configuration
  logger.add(sys.stdout, level="DEBUG", serialize=False)


  pipeline_result = flow(
    validate_full_file_path(args.task_path),
    bind(lambda x: read_file_content(x, logger.debug)),
    bind(lambda x: define_injest_task(x, logger.debug)),
  )
  match pipeline_result:
      case Success(data):
          injest_task:InjestTask = pipeline_result.unwrap()
          if args.username is not None:
            injest_task.user = args.username
          if args.password is not None:
            injest_task.password = args.password
          logger.info(f'Injest task: {injest_task}')
      case Failure(e):
          logger.critical(e)
          sys.exit(1)
  if (injest_task.user is None or injest_task.password is None):
      logger.critical('Usernam e and password are required. You can define them as optional positional arguments or in the task file')
      sys.exit(1)
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
