dags_config = [
    {
        "name": "DAG_1",
        "enabled": True,
        "priority": 1
        , "triggered" : False
    },
    {
        "name": "DAG_2",
        "enabled": True,
        "priority": 2
        , "triggered" : False
    },
    {
        "name": "DAG_3",
        "enabled": True,
        "priority": 3
        , "triggered" : False
    },
    {
        "name": "DAG_4",
        "enabled": True,
        "priority": 4
        , "triggered" : False
    },
    {
        "name": "DAG_5",
        "enabled": True,
        "priority": 5
        , "triggered" : False
    },
    {
        "name": "DAG_6",
        "enabled": True,
        "priority": 6
        , "triggered" : False
    },
    {
        "name": "DAG_7",
        "enabled": True,
        "priority": 7
        ,"triggered" : False
    },
     {
        "name": "DAG_8",
        "enabled": True,
        "priority": 8
        ,"triggered" : False
    }
]
def get_task_trigger_count(dag):
    for dags in dags_config:
        if dags['name'] == dag:
            return dags['triggered']

def schedule_dags():

    to_trigger = []
    to_skip = []
    
    max_dag_run = 1
    to_skip += [dag['name'] for dag in dags_config if not dag['enabled']]

    enabled_dags = [dag for dag in dags_config if dag['enabled']]
    sorted_dag = sorted(enabled_dags, key=lambda d: d['priority'],reverse=True)

    for dag in sorted_dag:
        if len(to_trigger) == max_dag_run or get_task_trigger_count(dag["name"]):
            to_skip.append(dag['name'])
        else:
            to_trigger.append(dag["name"])

    return to_trigger, to_skip


    
def trigger(dags):
    for dag in dags_config:
        if dag['name'] in dags:
            dag['triggered'] = True
i = True
while i:
    a,b = schedule_dags()
    print("To Trigger",a)
    trigger(a)

    a,b = schedule_dags()
    if len(a) == 0:
        print("Cannot Trigger more")
    i = len(a) > 0
    


