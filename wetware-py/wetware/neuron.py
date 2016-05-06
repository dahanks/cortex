#!/usr/bin/env python

#TODO: create a static string representing the unique vertex indexer ('name')
#TODO: create a get_vertex query that takes in a filter (wrapper on gremlin)
#TODO: tenancy!

import logging
import json

NEURON_DESTINATION = '/queue/neuron.operation'

"""This is just a dictionary that automatically has arrays under the keys
   'statements' and 'responses', since that's what the Neuron API dictates.
   For publishing queries, member functions add statements to 'statements', then
   you publish the Statement object.

   You can pass a string into the constructor to immediately pass in the first
   statement value.
"""
class Statements(dict):
    def __init__(self, msg_string=None):
        self['statements'] = []
        if msg_string:
            self['statements'].append(msg_string)

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

"""This may be a little anti-Pythonic, but here it is:
   This is just a Python list that can be created by interpreting the
   response from Neuron.  It does very little, but it feels a bit nicer
   than having every Worker callback parse the same element from a json
   object.

   Feel free not to use this.
"""
class Responses(list):

    def __init__(self, frame):
        if 'statements' in json.loads(frame.body):
            list.__init__(self, json.loads(frame.body)['statements'])
        else:
            list.__init__(self)

    def get_vertex_objects(self, index=0):
        """Parses the responses and returns dicts for any well-formed vertices.

        This method will only look into one response at a time (defaulting to
        the first index) because we assume each response could include
        results for completely unrelated queries.

        You should only use this if the responses came from a publish using
        neuron.get_vertex_object() or a Gremlin query that ends with valueMap().
        There's no guarantee what will happen with random response text or
        otherwise formatted gremlin query.
        """
        vertices = []
        #this vertex list is one Titan/Gremlin encoded string;
        # definitely not natively interpretable by Python because it has
        # more colons and brackets. So...

        #Split by the end of a vertex by looking for ]],
        vertex_list = self[index][1:-1].split(']],')
        for vertex_str in vertex_list:
            #take out whitespace
            vertex_str = vertex_str.lstrip()
            if vertex_str:
                #add those closing brackets ']]' back in for uniformity
                if not vertex_str.endswith(']]'):
                    vertex_str += ']]'
                # get rid of the list brackets in the string
                vertex = vertex_str[1:-1]
                vertex_obj = {}
                for prop in vertex.split(','):
                    key = prop.split(':')[0].lstrip()
                    value = prop.split('[')[1].split(']')[0]
                    vertex_obj[key] = value
                vertices.append(vertex_obj)
        return vertices

def add_vertex_object(vertex_obj):
    """Take a Python dict and make a vertex in Neuron from it.

    Will generate properties based on key-values in object.  Will not
    attempt to create any kind of edges.
    """
    if 'name' not in vertex_obj or 'type' not in vertex_obj:
        raise NeuronException("Tried to create vertex with no 'name' or 'type' field!")
    elif not isinstance(vertex_obj, dict):
        raise NeuronException("Tried to create a vertex from a non-dict!")
    statements = Statements()
    for key in vertex_obj:
        if key != 'name':
            statements.add_vertex_property(vertex_obj['name'],
                                           key,
                                           vertex_obj[key])
    return statements

def get_vertex_object(*vertex_names):
    """Produce a Statements query for the vertices and all of their  properties

    The ultimate response from neuron will be a string, but you can use
    Response.get_vertex_object(response) to return the dict object.
    """
    statements = Statements()
    for name in vertex_names:
        statements.gremlin("g.V().has('name','" + str(name) + "').valueMap()")
    return statements

def gremlin(*gremlins):
    """Use wrapper around using Gremlin without having to manipulate Statements

    This way you can pass this function right into a publish() call.
    """
    statements = Statements()
    for gremlin in gremlins:
        statements.gremlin(gremlin)
    return statements

class NeuronException(Exception):
    pass
