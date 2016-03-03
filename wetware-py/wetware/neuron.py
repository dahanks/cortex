#!/usr/bin/env python

import logging

#This is just a dictionary that automatically has an array
# under the key 'statements', since that's what the Neuron
# API dictates.  All member functions add statements to
# 'statements', then you publish the Statement object.
class Statement(dict):
    def __init__(self, *arg, **kwargs):
        super(Statement, self).__init__(self, *arg, **kwargs)
        self['statements'] = []

    #Add a vertex and guarantee that it is unique by referring
    # to the "name" property (which will be our vertex index)"
    #Can make multiple vertices in the same call.
    def add_vertex(self, *names):
        statement = {'fxns': [], 'api': 'neuron'}
        for name in names:
            fxn = {'fxn': 'addVertex', 'name': name}
            statement['fxns'].append(fxn)
        self['statements'].append(statement)

    #Add an edge between two vertices and guarantee that it is unique
    # by making sure that only one edge with the "label" exists between
    # two vertices with the same "name" property.
    #Creating an Edge will also create the Vertices, if they don't exist.
    #This function takes either:
    #  3 arguments: from_vertex, label, to_vertex
    # OR
    #  a list of 3-tuples, each as (from_vertex, label, to_vertex)
    def add_edge(self, *tuples):
        statement = {'fxns': [], 'api': 'neuron'}
        #If you only want one edge, give the 3 args (not in a tuple), and
        # we'll handle the rest by making it one proper tuple in a list.
        if not isinstance(tuples[0], tuple):
            #This will throw an exception if you don't put it at least 3 args
            # ...and that's good!
            tuples = [(tuples[0], tuples[1], tuples[2])]
        for edge_tuple in tuples:
            fxn = {'fxn': 'addEdge',
                   'fromVertex': edge_tuple[0],
                   'label': edge_tuple[1],
                   'toVertex': edge_tuple[2] }
            statement['fxns'].append(fxn)
        self['statements'].append(statement)

    #Add a property of the specified key and value to the Vertex with the
    # "name" provided.  If the property already exists, its value will be
    # overridden.  If the Vertex with "name" does not exist, it will be
    # created.
    def add_vertex_property(self, name, prop_name, prop_value):
        statement = {'fxns': [], 'api': 'neuron'}
        fxn = {'fxn': 'addVertexProperty',
               'name': name,
               'property': prop_name,
               'value': prop_value }
        statement['fxns'].append(fxn)
        self['statements'].append(statement)

    #Return the value of the property specified on the Vertex that has the
    # "name" provided.
    def get_vertex_property(self, name, prop_name):
        statement = {'fxns': [], 'api': 'neuron'}
        fxn = {'fxn': 'getVertexProperty',
               'name': name,
               'property': prop_name }
        statement['fxns'].append(fxn)
        self['statements'].append(statement)

    #Composes a list Gremlin statements to run, and each will be returned as a
    # reply.  Multiple statements can be passed as arguments.
    def gremlin(self, *raw_statements):
        for raw_statement in raw_statements:
            #no special cases for Gremlin
            statement = self.__compose_raw_statement(raw_statement)
            statement['api'] = 'gremlin'
            self['statements'].append(statement)

    #DISCOURAGED USE
    #Composes a list of Blueprints statements.  Use of this function is discouraged
    # because it circumvents the true Neuron API by running the statements "raw."
    # Things like preventing duplication are not enforced.
    def blueprints(self, *raw_statements):
        for raw_statement in raw_statements:
            #addEdge is a special case, addVertex is not (and don't expect others to be)
            if 'addEdge' in raw_statement:
                statement = self.__compose_addedge_statement(raw_statement)
            else:
                statement = self.__compose_raw_statement(raw_statement)
            statement['api'] = 'blueprints'
            self['statements'].append(statement)

    #This private function is used to formulate a Gremlin or Blueprints statement
    # (with exceptions).  It is not meant to be called from the outside.
    def __compose_raw_statement(self, statement):
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

    #DEPRECATED
    #This private function is used to handle the exception case when a raw
    # Blueprints string is calling "addEdge()". It is not meant to be called from
    # the outside.  It is also inferior to the add_edge support above.
    def __compose_addedge_statement(self, raw_statement):
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
        addedge_statement['fromVertex'] = self.__compose_raw_statement(fromGremlin)
        addedge_statement['toVertex'] = self.__compose_raw_statement(toGremlin)
        addedge_statement['label'] = label
        addedge_statement['properties'] = properties
        output_statement['fxns'].append(addedge_statement)
        return output_statement
