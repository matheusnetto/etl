import json
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pandas as pd
import pandas_gbq as gbq

credentials = service_account.Credentials.from_service_account_file(
    'key.json') # Json service account
client = bigquery.Client(project = 'project-123', credentials=credentials)


def upd_backup_views():
    projects = ["project-123","project-456","project-789"] # Project List
    dataset_list = []
    project_list = []
    table_list = []
    table = []
    query_list = []
    modified_list = []
    created_list = []

    for project in projects:
        client = bigquery.Client(project = project, credentials=credentials)
        datasets = list(client.list_datasets())
        for dataset in datasets:
            tables = list(client.list_tables(dataset = dataset.dataset_id))
            for table in tables:
                if table.table_type == 'VIEW':
                    print(project, dataset.dataset_id, table.table_id)
                    project_list.append(project)
                    dataset_list.append(dataset.dataset_id)
                    table_list.append(table.table_id)
                    table_id = project +'.'+ dataset.dataset_id +'.'+ table.table_id
                    query_list.append(client.get_table(table_id).view_query)
                    created_list.append(client.get_table(table_id).created)
                    modified_list.append(client.get_table(table_id).modified)
                
    df = pd.DataFrame()
    df['project'] = project_list
    df['dataset'] = dataset_list
    df['table'] = table_list
    df['query'] = query_list
    df['db_updated'] = datetime.now()

    df.to_gbq('dataset.views_backup', 'project-123', credentials=credentials, if_exists='append')
    print("Append realizado com sucesso.")


def remove_duplicates():
    query = """
    delete from `project-123.dataset.views_backup`
    where concat(db_updated,project,dataset,table,query) in

    (with 
    base as 
        (SELECT 
            concat(db_updated,project,dataset,table,query) as fields_concat,
            row_number() over (partition by project,dataset,table,query order by db_updated desc) as rn
        FROM `project-123.dataset.views_backup`)
    select 
        fields_concat
    from base
    where rn > 1)
    """
    client.query(query).result()
    print("Dados duplicados removidos")
