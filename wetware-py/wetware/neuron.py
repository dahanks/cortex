#!/usr/bin/env python

import logging

def add_vertex_statement(*names):
    output_data = {'statements': []}
    statement = {'fxns': [], 'api': 'neuron'}
    for name in names:
        fxn = {'fxn': 'addVertex', 'name': name}
        statement['fxns'].append(fxn)
    output_data['statements'].append(statement)
    return output_data

def add_edge_statement(from_vertex, to_vertex, label):
    output_data = {'statements': []}
    statement = {'fxns': [], 'api': 'neuron'}
    fxn = {'fxn': 'addEdge',
           'fromVertex': from_vertex,
           'toVertex': to_vertex,
           'label': label }
    statement['fxns'].append(fxn)
    output_data['statements'].append(statement)
    return output_data

def add_vertex_property_statement(name, prop_name, prop_value):
    output_data = {'statements': []}
    statement = {'fxns': [], 'api': 'neuron'}
    fxn = {'fxn': 'addVertexProperty',
           'name': name,
           'property': prop_name,
           'value': prop_value }
    statement['fxns'].append(fxn)
    output_data['statements'].append(statement)
    return output_data

def get_vertex_property_statement(name, prop_name):
    output_data = {'statements': []}
    statement = {'fxns': [], 'api': 'neuron'}
    fxn = {'fxn': 'getVertexProperty',
           'name': name,
           'property': prop_name }
    statement['fxns'].append(fxn)
    output_data['statements'].append(statement)
    return output_data

#SHOULD BE PRIVATE
#CANNOT TAKE MULTIPLE STATEMENTS
def compose_raw_statement(statement):
    output_statement = {'fxns': []}
    for input_function in statement.split('.'):
        #these are the Tinker/TitanGraphs or traversal()
        # their usage is hardcoded at neuron
        if input_function == 'graph' or input_function == 'g':
            continue
        else:
            function_name = input_function.split('(')[0]
            arguments = []
            for argument in input_function.split('(')[1].split(')')[0].split(','):
                #don't put in empty strings
                if argument:
                    #strip whitespace, and strip quotes (on each side)
                    # there would be a problem here is quotes weren't there
                    arguments.append(argument.strip()[1:-1])
            output_function = {
                'fxn': function_name,
                'args': arguments
            }
            output_statement['fxns'].append(output_function)
    return output_statement

#SHOULD BE PRIVATE
#CANNOT TAKE MULTIPLE STATEMENTS
def compose_addedge_statement(raw_statement):
    #when neuron sees it's blueprints, it will check if fxn is addEdge
    # and handle this different api accordingly
    output_statement = {'fxns': []}
    addedge_statement = {}
    #format: Vertex.addEdge(label, Vertex, key, value, ... )
    # so, everything before .addEdge is the fromVertex Gremlin
    fromGremlin = raw_statement.split('.addEdge(')[0]
    # the label is the first comma-separated argument inside .addEdge()
    label = raw_statement.split('.addEdge(')[1].split(',')[0].strip()[1:-1]
    # properties are all strings from the end of the function ')'
    #  to the end of the toGremlin function, which also ends in ')'
    properties = []
    for prop in raw_statement.split(')')[-2].split(','):
        if prop:
            properties.append(prop.strip()[1:-1])
    # grab everything after 'label'
    #  start at g.V
    #  end at second instance (in reverse) of ')'
    toGremlinMess = raw_statement.split(label)[1]
    beginToGremlin = toGremlinMess.find('g.V')
    endToGremlin = toGremlinMess.rfind(')', 0, len(toGremlinMess) - 1) + 1
    toGremlin = toGremlinMess[beginToGremlin:endToGremlin]
    # wrap them with a bow
    addedge_statement['fxn'] = 'addEdge'
    addedge_statement['fromVertex'] = compose_raw_statement(fromGremlin)
    addedge_statement['toVertex'] = compose_raw_statement(toGremlin)
    addedge_statement['label'] = label
    addedge_statement['properties'] = properties
    output_statement['fxns'].append(addedge_statement)
    return output_statement

#can take multiple statements
#HIGHLY DISCOURAGED because it won't check for redundancy
def compose_blueprints_statement(*raw_statements):
    output_data = {'statements': []}
    for raw_statement in raw_statements:
        #addEdge is a special case, addVertex is not (and don't expect others to be)
        if 'addEdge' in raw_statement:
            statement = compose_addedge_statement(raw_statement)
        else:
            statement = compose_raw_statement(raw_statement)
        statement['api'] = 'blueprints'
        output_data['statements'].append(statement)
    return output_data

#can take multiple statements
#use this as much as you want (probably)
def compose_gremlin_statement(*raw_statements):
    output_data = {'statements': []}
    for raw_statement in raw_statements:
        #no special cases for Gremlin
        statement = compose_raw_statement(raw_statement)
        statement['api'] = 'gremlin'
        output_data['statements'].append(statement)
    return output_data
