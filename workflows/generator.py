'''
Created on Oct 6, 2016

@author: dearj019
'''

import numpy as np
import networkx as nx
import random
import string

def workflow(previous_workflows_union, workflow_size,
             nb_previous_actions, total_nb_actions, conf):
    '''
    This is the main function of the simulation.  It creates a workflow
    that uses actions from previous workflows and new actions, and merges
    all that data together in a new workflow.
    
    Parameters
    ----------
    1. previous_workflows_union: Union of previous workflows 
    3. workflow_size: number of actions of this workflow
    4. nb_previous_actions: The number of previous actions to be used from 
    previous_workflows_union
    4. total_nb_actions: the total number of actions of the history
    5. conf: a map with the parameters needed to created the probability
    distributions that create the workflow. 
    
    Computations
    ------------
    1. Do a topological sort list T of previous_workflows_union
    2. Randomly select a list B of nb_previous_actions indexes from the topological sort list.
    3. Make a copy C of list B of indexes of actions to include in new workflow.
    3. previous_nodes_to_include = []
    4. While C's length greater than 1:
    4.0 new_C = []
    4.1 top_node_index = C[0]
    4.2 for i in range(1, len(C)):
           find shortest path S between T[top_node_index] and T[C[i]]
           if S is not empty: add all nodes in S to previous_nodes_to_include
           Otherwise add C[i] to new_C
    4.3 C now equals new_C
    4.4 if len(C) == 1, add C[0] to previous_nodes_to_include
    
    For each node on list_of_nodes_to_include, generate the number of children it can take using
    the parameters from parameters_map
    -create list that is range(len(nodes_previous_workflows_union.nodes), 
                                min(len(nodes_previous_workflows_union + workflow_size - nb_previous_actions)), total_nb_actions))
    -For each node on the new list, generate its number of parents and its number of children
    probabilistically. 
    
    -Do the following algorithm: For each action, for each children the action is allowed to
    pick up, children to that action from the list of actions that is allowed to pick up
    parents, if that action is not an ancestor of you.
    
    - Update the union of previous workflows with the newly generated workflow.
    - return the newly generated workflow.
    '''       
    #1. Algorithm to determine which are the previous actions to include.
    top_sort = nx.algorithms.dag.topological_sort(previous_workflows_union)
    previous_actions_indexes = random.sample(range(len(top_sort)), min(nb_previous_actions, len(top_sort)))
    previous_actions_to_include = []
    C = list(previous_actions_indexes)
    while len(C) > 1:
        new_C = []
        
        for i in range(1, len(C)):
            source = top_sort[C[0]]
            target = top_sort[C[i]]
            try:
                shortest_path = nx.algorithms.shortest_paths.shortest_path(previous_workflows_union, source, target)
                if len(shortest_path) > 1:
                    previous_actions_to_include.extend(shortest_path)
                else:
                    new_C.append(i)
            except:
                new_C.append(i)
        C = new_C
    
    if len(C) == 1:
        previous_actions_to_include.append(top_sort[C[0]])
    
    #2. Algorithm to determine the parameters of both the previous actions
    #and the new actions to include.
    actions_params = {}
    nb_children_mean, nb_children_std = conf['nb_children']['mean'], conf['nb_children']['std']
    nb_parent_mean, nb_parent_std = conf['nb_parent']['mean'], conf['nb_parent']['std']
    
    for action_id in previous_actions_to_include:
        nb_children = int(abs(np.random.normal(nb_children_mean, nb_children_std)))
        actions_params[action_id] = { 'nb_children': nb_children }
    
    new_actions_lower_bound = len(previous_workflows_union.node)
    new_actions_upper_bound = min(new_actions_lower_bound + workflow_size - nb_previous_actions, total_nb_actions)
    new_actions = range(new_actions_lower_bound, new_actions_upper_bound)
    for action_id in new_actions:
        nb_children = int(abs(np.random.normal(nb_children_mean, nb_children_std)))
        nb_parents = int(abs(np.random.normal(nb_parent_mean, nb_parent_std)))
        actions_params[action_id] = { 'nb_children': nb_children, 'nb_parents': nb_parents}
        
    #3. Create workflow graph
    #3.1 Add subgraph from previous workflows
    workflow = nx.DiGraph(previous_workflows_union.subgraph(previous_actions_to_include))
    
    #3.2 Add new items
    for action_id in previous_actions_to_include:
        nb_children = actions_params[action_id]['nb_children']    
        i = 0
        j = 0
        index_permutations = np.random.permutation(range(len(new_actions)))
        while i < nb_children and j < len(new_actions):
            tentative_id = new_actions[index_permutations[j]]
            nb_parents = actions_params[tentative_id]['nb_parents']
            if nb_parents > 0:
                actions_params[tentative_id]['nb_parents'] = nb_parents - 1
                workflow.add_edge(action_id, tentative_id)
                i = i + 1
                j = j + 1
                continue
            else:
                j = j + 1
                continue
                
    for action_id in new_actions:
        nb_children = actions_params[action_id]['nb_children']
        i = 0
        j = 0
        index_permutations = np.random.permutation(range(len(new_actions)))
        workflow.add_node(action_id)
        while i < nb_children and j < len(new_actions):
            tentative_id = new_actions[index_permutations[j]]
            nb_parents = actions_params[tentative_id]['nb_parents']
            if nb_parents > 0 and tentative_id != action_id and tentative_id not in nx.algorithms.dag.ancestors(workflow, action_id):
                actions_params[tentative_id]['nb_parents'] = nb_parents - 1
                workflow.add_edge(action_id, tentative_id)
                i = i + 1
                j = j + 1
                continue
            else:
                j = j + 1
                continue
    
    #5. Return workflow
    return workflow
    
def history(actions_metadata, conf):
    '''
    Creates a sequence of workflows constrained by the three parameters passed.
    
    Parameters
    ----------
    1. actions_metadata: contains the list of actions to be created.
       The action_id is also the index in the list.  The element at the index
       is a dictionary that contains the following attributes about the action:
       'output_size_mb', 'computation_time'.
    2. conf: Which will include, among others, the following relevant
       parameters for the history generator:
    2.1 nb_workflows: The number of workflows to create in the sequence
    2.2 previous_usage_param: Is a map that contains the mean and the std
       that will determine proportion of previous datasets that a current 
       workflow will use.
       
    Computations:
    -------------
    1. The function will first determine the mean and std of the distribution 
    that will determine the size of the workflows.  
    2. For i from 1 to nb_workflows:
    2.1 It will generate workflow i.  It will pass to the workflow generation
        function the following parameters: 
        - a map from action_id to workflow_id list, which keep tracks on what 
        workflow an action has been used previously
        - the list of the previous workflows already generated.
        - the size of the workflow to generate, which will be determined from the
        distribution in 1.
        - the conf, which it will use to determine, for each new action
        to use, the number of children and number of parents that that action 
        will have.  For actions from previous workflows, the number of parents is
        already determined.
    3. It will return the list of workflows generated.
    
    
    Returns:
    -------
    Returns a list of workflows, where each workflow is a graph with nodes being
    actions identified by action ids.
    '''
    nb_total_actions = len(actions_metadata)
    workflow_size_mean = conf['workflow_size']['mean']
    workflow_size_std = conf['workflow_size']['std']
    previous_actions_mean = conf['previous_actions']['mean']
    previous_actions_std = conf['previous_actions']['std']
    used_actions = 0
    
    workflows = []
    previous_workflows_union = nx.DiGraph()
    
    while used_actions < nb_total_actions:
        
        workflow_size = int(np.random.normal(workflow_size_mean, workflow_size_std))
        previous_actions = min(1, abs(np.random.normal(previous_actions_mean, 
                                                       previous_actions_std)))
        nb_previous_actions = int(previous_actions * workflow_size)
        workflow_size = min(workflow_size, 
                            nb_total_actions - used_actions + nb_previous_actions)
        
        new_workflow = workflow(previous_workflows_union,  
                                workflow_size, nb_previous_actions, 
                                nb_total_actions, conf)
        
        workflows.append(new_workflow)
        previous_workflows_union = nx.algorithms.operators.binary.compose(previous_workflows_union, new_workflow)
        used_actions = used_actions + workflow_size - nb_previous_actions
    
    return workflows

def id_generator(size = 10, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def actions(conf):
    '''
    Generates a list tuples that contain the action id and the metadata.
    The metadata is just the size of the action in MB and the computation time in seconds
    '''
    nb_actions = conf['nb_actions']
    action_size_mean = conf['action_size']['mean']
    action_size_std = conf['action_size']['std']
    action_time_mean = conf['action_time']['mean']
    action_time_std = conf['action_time']['std']
    
    actions = []
    for i in range(nb_actions):
        action_size = abs(np.random.normal(action_size_mean, action_size_std))
        action_time = abs(np.random.normal(action_time_mean, action_time_std))
        action_unique = id_generator()
        actions.append((i, action_size, action_time, action_unique))
        
    return actions