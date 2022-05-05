#!/usr/bin/python

import psycopg2
from dblib import create_tables, insert_project
import json

if __name__ == '__main__':
    create_tables()


    with open('./projects.json', 'r') as file:
        project_master = json.load(file)

    project = project_master['results'][0]
    project_dict = {}

    project_dict['name'] = project['name']
    project_dict['country'] = project['settings']['country']['label']
    project_dict['sector'] = project['settings']['sector']['label']
    project_dict['description'] = project['settings']['description']
    project_dict['url'] = project['url']
    project_dict['owner'] = project['owner']
    project_dict['uid'] = project['uid']
    project_dict['kind'] = project['kind']
    project_dict['asset_type'] = project['asset_type']
    project_dict['version_id'] = project['version_id']
    project_dict['date_created'] = project['date_created']

    table = 'projects'
    for k in project_dict.keys():
        columns = list(project_dict.keys())
    # query = f"INSERT INTO {table}"
    # query += "(" + ", ".join(columns) + ")\nVALUES "

    values = []
    for value in project_dict.values():
        if type(value) ==str:
            value = value.replace("'", "''")
            value = "'" +value + "'"
        values += [str(value)]


    project_id = insert_project(table, columns, values)