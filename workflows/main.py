'''
Created on Oct 7, 2016

@author: dearj019
'''

import json
import sys
import generator
import networkx as nx
import matplotlib.pyplot as plt

def main():
    with open(sys.argv[1]) as f:
        conf = json.load(f)
    
    actions_metadata = generator.actions(conf)
    workflows = generator.history(actions_metadata, conf)
    workflow1 = workflows[0]
    nx.draw(workflow1)
    plt.show()
    
if __name__ == "__main__":
    main()