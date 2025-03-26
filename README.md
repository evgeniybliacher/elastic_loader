# Elastic loader


## Usage
The command tool to load data to elasticsearch instance. The tool managed by task json file.

Run command: ***main.py --task_path [full path to task file]*** --username [user name for session] --password [password for session]

***Required***
--task_path

***Optional***
--username
--password

## Task file
The example of task file:

```json
{
  "index":[index name],
  "host": [elastic host include http/https],
  "port": [elastic port],
  "user": [elastic username],
  "password": [elastic password],
  "chunk_size": [cunk size of bulk operation],
  "data_folder": [folder to json files]
}
```

User and password are optional fields
