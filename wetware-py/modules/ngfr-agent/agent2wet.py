#!/usr/bin/env python
import heapq
from collections import deque
import logging
import json

# these workers are designed to run with the module installed
# don't use relative paths
#from wetware.worker import Worker
#from wetware.worker import FrameException

from testScaffold import Worker
from testScaffold import FrameException
from testScaffold import getRules

class Memory():
    ''' This class implements a NARS memory structure, possibly with a graph-DB back end.  A NARS memory consists of (S,Cupola,P,F,C,Time) tuples denoting that S ('->' | '==' | '->c') P <F,C> was posted at Time.  S and P are specific NARS concepts. Cupola is '->', '==', or '->c' denoting that whether S is a subset of P, S equivalent to P, or there is another tuple denoting that P->S which was converted to facilitate abduction and induction.'''
    
    def __init__(self,k):
        self.fromTo = {}
        self.k = k
    
    def dump(self):
        ''' Print out the contents of memory. '''
        for n in self.fromTo:
            print "%s:%s"%(n,map(lambda a:"%s%s<%s,%s>"%(a[1],a[2],a[3],a[4]),self.fromTo[n]))
    
    def post(self,s,p,f,c,time):
        ''' Post a S->P <f,c> entry to memory with freshness 'time' '''
        assertion = (s,'->',p,f,c,time)
        neighborhood = self.fromTo.get(s,[])
        if assertion in neighborhood: return

        self.fromTo[s]= neighborhood + [assertion]
        reverse = (p,'<-',s,f,c,time)
        neighborhood = self.fromTo.get(p,[])
        self.fromTo[p]= neighborhood + [reverse]
    
    def retract(self,src,dst):
        ''' Remove all arcs between src and dst, if any. '''
        #print "retract(%s,%s)"%(src,dst)
        if src in self.fromTo:
            self.fromTo[src] = filter(lambda x: x[2]!=dst,self.fromTo[src])
        if dst in self.fromTo:
            self.fromTo[dst] = filter(lambda x: x[2]!=src,self.fromTo[dst])
        #print self.fromTo[src]
    
    def forget(self,concept):
        ''' Delete a concept from memory and all of its relevant arcs.  concept must be a value in a list returned by query(). '''
        #print "forget %s"%concept
        if concept in self.fromTo:
            neighborhood = list(set([p for (s,cupola,p,f,c,time) in self.fromTo[concept]]))
            for p in neighborhood:
                self.fromTo[p] = filter(lambda x: x[2]!=concept,self.fromTo[p])
                if self.fromTo[p] == []: del self.fromTo[p]
            del self.fromTo[concept]

    def query1(self,concept):
        ''' Get all of the arcs to/from a concept '''
        arcs = self.fromTo[concept]
        for node in list(set([p for (s,cupola,p,f,c,time) in arcs])):
            arcs += filter(lambda a: a[2]==concept,self.fromTo[node])
        return [a for a in arcs if a[1]!='<-']

    def query(self,s,cMax,cMin):
        ''' S is a concept relevant to NARS memory.  cMax & cMin denote bounds on what is returned.  In this case, only return concepts with maxConfidence paths within confidence between cMax and cMin. '''
        agenda = []
        visited = []
        result = []
        if s not in self.fromTo: return []
        heapq.heappush(agenda,(1,s))
        while agenda:
            (conf,node) =  heapq.heappop(agenda)
            if node not in visited:
                visited += [node]
                if cMin < conf and conf <= cMax: result += [node]
                for (s,cupola,p,f,c,time) in self.fromTo[node]:
                    if cupola == '<-': c = f*c/(f*c+self.k) # conversion
                    heapq.heappush(agenda,(conf*c,p))
        return result
    
    def queryPaths(self,source,destination,cMin):
        ''' Source and destination are concepts relevant to NARS memory.  cMin is minimum confidence bound on which paths to return. This returns a list of lists of arcs, where each list of arcs denotes a path from source to destination with collective confidence greater than cMin. '''
        if destination not in self.fromTo or source not in self.fromTo: return []
        agenda = []
        result = []
        heapq.heappush(agenda,(1,[source],[]))
        while agenda:
            (conf,path,arcs) = heapq.heappop(agenda)
            for x in self.fromTo.get(path[-1],[]):
                (s,cupola,p,f,c,time) = x
                if cupola == '<-': c = f*c/(f*c+self.k) # conversion
                if conf*c >= cMin:
                    if p == destination:
                        result += [arcs+[x]]
                    elif p not in path:
                        heapq.heappush(agenda,(conf*c,path+[p],arcs+[x]))
        return result

############################

#(s,cupola,p,f,c,time)
def combine(a,b):
    (aS,aCupola,aP,aF,aC,aTime) = a
    (bS,bCupola,bP,bF,bC,bTime) = b
    if aP != bS: print "path error"
    if aCupola == '->' and bCupola == '->': # deduction
        return (aS,'->',bP,aF*bF,aF*aC*bC,max(aTime,bTime))
    if aCupola == '->' and bCupola == '<-': # abduction
        return (aS,'->',bP,aF,bF*aC*bC/(bF*aC*bC+1) ,max(aTime,bTime))
    if aCupola == '<-' and bCupola == '->': # induction
        return (aS,'->',bP,bF,aF*aC*bC/(aF*aC*bC+1) ,max(aTime,bTime))
    if aCupola == '<-' and bCupola == '<-': # reverse deduction
        return (aS,'<-',bP,aF*bF,bF*aC*bC,max(aTime,bTime))

def revision(a,b):
    (aS,aCupola,aP,aF,aC,aTime) = a
    (bS,bCupola,bP,bF,bC,bTime) = b
    if aCupola=='<-':
       aCupola, aF, aC = '->',
    c = 1.0 if aC==1.0 or bC==1.0 else (aC+bC-2*aC*bC)/(1.0-aC*bC)
    if aC==bC:  return (aS,aCupola,aP,(aF+bF)/2.0,c,max(aTime,bTime))
    if aC==1.0: return (aS,aCupola,aP,aF,c,max(aTime,bTime))
    if bC==1.0: return (aS,aCupola,aP,bF,c,max(aTime,bTime))
    return (aS,aCupola,aP,(aF*aC+bF*bC-(aF+bF)*aC*bC)/(aC+bC-2.0*aC*bC),c,max(aTime,bTime))

def intersects(a,b):
    for x in a:
        if x in b: return True
    return False

def queryNAL1(memory,src,dst):
    paths = memory.queryPaths(src,dst,0.3)
    arcPaths = filter(lambda a: a[0][4]>0.0, [(reduce(combine,p),p) for p in paths])
    if arcPaths == []: return False
    result = arcPaths[0][0]
    agenda = deque(arcPaths)
    while agenda:
        (V,P) = agenda.popleft()
        if V[4]>result[4]: result = V # find the computation with the highest confidence.
        for (A,PP) in arcPaths:
            if not intersects(P,PP):
                agenda.append((revision(A,V), P+PP))
    return result[3]>0.5 and result[4]>0.1 # F,C exceeds this arbitrary threshold

##################################

hbaseline,gbaseline = 0.0, 0.0

def consume(assume,mem):
    global hbaseline
    global gbaseline
    objects = []
    if 'Feature_Value' in assume['Features']:
        if isinstance(assume['Features']['Feature_Value'], dict):
            attributes = [assume['Features']['Feature_Value']['_attributes']]
        else:
            attributes = [x['_attributes'] for x in assume['Features']['Feature_Value']]
        for a in attributes:
            if a['Name'].upper() == 'HEARTBEAT_BASELINE':
                hbaseline = float(a['Value'])
            if a['Name'].upper() == 'ALCOHOL_BASELINE':
                gbaseline = float(a['Value'])
        for a in attributes:
            if a['Name'].upper()== 'TEMP':
                a['Value'] = 'OK' if float(a['Value'])<900 else 'HIGH'
            if a['Name'].upper()== 'HEARTBEAT':
                a['Value'] = 'OK' if float(a['Value'])<hbaseline else 'HIGH'
            if a['Name'].upper()=='GAS_ALCOHOL':
                a['Value'] = 'OK' if float(a['Value'])<gbaseline else 'HIGH'

            obj ="obj%s"%a['Object_ID']
            if obj not in objects: objects += [obj]
            if a['Value'].upper() == 'TRUE':
               mem.post(obj,a['Name'].upper(),float(a['Conf']),0.99,float(a['Time']))
            elif a['Name'].upper() == 'IS_A':
               mem.post(obj,a['Value'].upper(),float(a['Conf']),0.99,float(a['Time']))
            else:
               mem.post(obj,"%s:%s"%(a['Name'].upper(),a['Value'].upper()),float(a['Conf']),0.99,float(a['Time']))
    for o in objects:
        mem.post(o,'new',1.0,0.99,0)

############################

def match (actions,terms,e,queue,mem):
    for t in terms:
        if t[0][0]=='!':
            if queryNAL1(mem,t[1],t[2]): return
        elif not queryNAL1(mem,t[1],t[2]): return
    for a in actions:
        queue.append(a)

############################

class MySpecialWorker(Worker):
    
    """ If you need to create a constructor, just make sure you call the super first """
    def __init__(self, subclass_section):
        super(MySpecialWorker, self).__init__(subclass_section)
        self.lm = Memory(1)
        #(s,p,f,c,time) for "s-->p <f,c> @ time"
        self.rules = getRules('init')

    def on_message(self, frame):
        ### This header should not be modified ###
        transaction = super(MySpecialWorker, self).on_message(frame)
        message = json.loads(frame.body)
        ############### End header ###############

        consume(message,self.lm)
        queue = []
        for r in self.rules:
            match(r[1],r[0],[],queue,self.lm)
        for action in queue:
            if action[0]=='post':
                self.lm.post(action[1],action[2],float(action[3]),float(action[4]),0)
            if action[0]=='forget':
                self.lm.forget(action[1])
            if action[0]=='sendMsg':
                self.publish(action[1].replace('_',' '))
            if action[0]=='context':
                self.lm = memory(1)
                rules = getRules(action[1])

def main():
    # Add this line if you want to see logging output
    logging.basicConfig(level=logging.DEBUG)
    
    # Instantiate your worker subclass.  The string you pass in the
    # constructor is the section name in the config file under which
    # you can add parameters specific to your worker.
    my_worker = MySpecialWorker("myspecialworker")
    
    # And run.  Your listener will wait for messages and do what you
    # want with them as they arrive.
    my_worker.run()

############################

if __name__ == '__main__':
    main()

