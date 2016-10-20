'''
Created on Oct 19, 2016

@author: dearj019
'''
import sys
import json
import os
from collections import deque

def itemSize(item): return item[0]
def itemValue(item): return item[1]
def itemName(item): return item[2]

def knapsack(items, sizeLimit):
    '''
    Each item is represented as triple (size, value, name)
    
    Example of parameters:
    items = [(3, 3, 'A'), (4, 5, 'B'), (10, 3, 'C')]
    sizeLimit = 4
    '''
    P = {}
    for nItems in range(len(items) + 1):
        for lim in range(sizeLimit + 1):
            if nItems == 0:
                P[nItems, lim] = 0
            elif itemSize(items[nItems-1]) > lim:
                P[nItems, lim] = P[nItems - 1, lim]
            else:
                P[nItems, lim] = max(P[nItems - 1, lim],
                                     P[nItems - 1, lim - itemSize(items[nItems-1])]
                                     + itemValue(items[nItems - 1]))
        
    L = []
    nItems = len(items)
    lim = sizeLimit
    while nItems > 0:
        if P[nItems, lim] == P[nItems - 1, lim]:
            nItems -= 1
        else:
            nItems -= 1
            L.append(items[nItems])
            lim -= itemSize(items[nItems])
    
    L.reverse()
    return L

def workflow_computation_time(workflow, A):
    '''
    Computes the time it takes to workflow to compute, given that A is the set
    of storaged datasets.
    '''
    computation_time = 0
    queue = deque()
    seen = set()
    
    for node in workflow.keys():
        if len(workflow[node]['children']) == 0:
            queue.append(node)
    
    while len(queue) > 0:
        item = queue.popleft()
        seen.add(next)
        if item in A:
            continue
        else:
            computation_time += itemValue(item)
            parents = workflow[item]['parents']
            for parent in parents:
                if not parent in seen:
                    queue.append(parent)
    
    return computation_time
    
def computation_time_left(d, P_H, m):
    time_left = 0
    for t in range(m, len(P_H)):
        if d in P_H[t]:
            time_left = time_left + itemValue(d)
    return time_left
    
def ideal_computation_time(size_limit, workflow_graphs_l, workflow_items_l):
    '''
    See Algorithm 5 Ideal Computation TIme of chapter 4 in thesis
    '''
    #1. Compute Mt, Nt and Pt and c
    #1.1 Compute e
    total_time = 0
    e = {}
    for i in range(1, len(workflow_items_l) + 1):
        workflow_items = workflow_items_l[-i]
        for item in workflow_items:       
            if item not in e:
                e[item] = i - 1
    
    M_H = []
    N_H = []
    P_H = []
    already_seen = set()
    for i in range(len(workflow_items_l)):
        workflow_items = workflow_items_l[i]
        P_i = set(workflow_items)
        M_i = set()
        N_i = set()
        
        for item in already_seen:
            if e[item] >= i:
                N_i.add(item)
                M_i.add(item)
        
        for item in P_i:
            if e[item] >= i:
                M_i.add(item)
            
        M_H.append(list(M_i))
        N_H.append(list(N_i))
        P_H.append(P_i)
        already_seen.update(P_i)
    
    for t in range(len(N_H)):
        N_t = N_H[t]
        N_t_new = []
        for item in N_t:
            value = computation_time_left(item, P_H, t)
            size = itemSize(item)
            name = itemName(item)
            N_t_new.append((size, value, name))
            
        A = knapsack(N_t_new, size_limit)
        print(A)
        
        et = workflow_computation_time(workflow_graphs_l[t], A)
        total_time = total_time + et
    
    return total_time
        

def parse_workflow(workflow_file_path):
    with open(workflow_file_path) as f:
        workflow = json.load(f)
    return workflow

def main():
    with open(sys.argv[1]) as f:
        conf = json.load(f)
    
    history_folder = conf['history_folder']
    size_limit = conf['size_limit']
    
    workflow_files = os.listdir(history_folder)
    workflows = [parse_workflow(os.path.join(history_folder, a)) for a in workflow_files]
    
    #0. Convert workflow actions to items
    workflow_items_l = []
    workflow_graphs_l = []
    
    for workflow in workflows:
        workflow_graph = {}
        items = []
        actions = workflow['actions']
        id_item_m = {}
        id_parents = {}
        for action in actions:
            class_name = action['mainClassName']
            action_id = action['actionId']
            additional_input = action['additionalInput']
            size_in_mb = int(float(additional_input[0]['value']))
            time_in_seconds = int(float(additional_input[1]['value']))
            random_string = additional_input[3]['value']
            
            item = (size_in_mb, time_in_seconds, (class_name, random_string))
            id_item_m[action_id] = item
            id_parents[action_id] = action['parentActions']
            items.append(item)
        
        for id in id_parents.keys():
            parents = id_parents[id]
            if not id_item_m[id] in workflow_graph:
                workflow_graph[id_item_m[id]] = {
                    'parents': [],
                    'children': []
                }
            
            workflow_graph[id_item_m[id]]['parents'] = [id_item_m[a] for a in parents]
            for parent_id in parents:
                if not id_item_m[parent_id] in workflow_graph:
                    workflow_graph[id_item_m[parent_id]] = {
                        'parents': [],
                        'children': []
                    }
                workflow_graph[id_item_m[parent_id]]['children'].append(id_item_m[id])
        
        workflow_items_l.append(items)
        workflow_graphs_l.append(workflow_graph)
                
    ict = ideal_computation_time(size_limit, workflow_graphs_l, workflow_items_l)
    print(ict)
    
if __name__ == "__main__":
    main()