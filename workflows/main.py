'''
Created on Oct 7, 2016

@author: dearj019
'''

import json
import sys
import generator
import networkx as nx
import os
import matplotlib.pyplot as plt

def create_json_entry(workflow, actions_metadata, 
                      start_action_id, end_action_id, conf):
    
    to_return_dict = {}
    to_return_dict['name'] = conf['workflow']['name']
    to_return_dict['version'] = conf['workflow']['version']
    to_return_dict['startActionId'] = start_action_id
    to_return_dict['endActionId'] = end_action_id
    to_return_dict['actions'] = []
    
    for action_id in workflow.nodes():
        action = {}
        action['name'] = 'action' + str(action_id)
        action['actionId'] = action_id
        action['type'] = 'COMMAND_LINE'
        action['mainClassName'] = conf['workflow']['main_class_name']
        action['actionFolder'] = conf['workflow']['action_folder']
        action['forceComputation'] = False
        action['parentActions'] = workflow.predecessors(action_id)
        action['additionalInput'] = [
            { "key": "sizeInMB", "value": str(actions_metadata[action_id][1])}, 
            { "key": "timeInSeconds", "value": str(actions_metadata[action_id][2]) },
            { "key": "nameNode", "value": conf['workflow']['nameNode']},
            { "key": "uniqueRandomInput", "value": str(actions_metadata[action_id][3])}
        ]
        to_return_dict['actions'].append(action)
        
    return json.dumps(to_return_dict, indent = 4, sort_keys = True)

def write_json_entries_to_folder(json_entries, folder_path):
    for i in range(len(json_entries)):
        json_entry = json_entries[i]
        
        with open(os.path.join(folder_path, "generated_workflow_" + str(i)), "w+") as f:
            f.write(json_entry)

def main():
    with open(sys.argv[1]) as f:
        conf = json.load(f)
    
    output_folder_path = conf['output_folder_path']
    actions_metadata = generator.actions(conf)
    workflows = generator.history(actions_metadata, conf)
    nx.draw(workflows[0])
    plt.show()
    workflows_json = []
    for workflow in workflows:
        t_sort = nx.algorithms.dag.topological_sort(workflow)
        if len(t_sort) == 0:
            continue
        start_action_id = t_sort[0] 
        end_action_id = t_sort[len(t_sort) - 1]
        json_workflow = create_json_entry(workflow, actions_metadata, 
                          start_action_id, end_action_id, conf)
        workflows_json.append(json_workflow)
    
    write_json_entries_to_folder(workflows_json, output_folder_path)
    
    
    
if __name__ == "__main__":
    main()