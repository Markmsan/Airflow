dags_config = [
    {
        "name": "DAG_5",
        "enabled": True,
        "priority": 1
    },
    {
        "name": "DAG_6",
        "enabled": True,
        "priority": 2
    },
    {
        "name": "DAG_7",
        "enabled": False,
        "priority": 2
    }
]

def schedule_dags():
    enabled_dags = [dag for dag in dags_config if dag['enabled']]
    disabled_dag = [dag for dag in dags_config if not dag['enabled']]

    sorted_dag = sorted(enabled_dags, key=lambda d: d['priority'])
    disabled_sorted = sorted(disabled_dag, key=lambda d: d['priority'])
    
    dags_to_trigger = 3
    
    dags_ids_to_trigger = [
        {'name': dag['name'], 'enabled': dag['enabled']}
        for dag in sorted_dag[:dags_to_trigger]
    ]
    dags_ids_not_to_trigger = [
        {'name': dag['name'], 'enabled': dag['enabled']}
        for dag in disabled_sorted
    ]
    
    return dags_ids_to_trigger,dags_ids_not_to_trigger


trigger_dag_id, no_trigger = schedule_dags()

for dag_id in trigger_dag_id:
    c =0
    condition = dag_id['enabled']
    print(condition)
    if c == 1:
        print( [f'already_triggered_{dag_id['name']}'])
    else:
        if condition == False:
            print ([f'skip_{dag_id['name']}'])
        else:
            print ([f'trigger_{dag_id['name']}'])

for dag_id in no_trigger:
    c =0
    condition = dag_id['enabled']
    print(condition)
    if c == 1:
        print( [f'already_triggered_{dag_id['name']}'])
    else:
        if condition == False:
            print ([f'skip_{dag_id['name']}'])
        else:
            print ([f'trigger_{dag_id['name']}'])