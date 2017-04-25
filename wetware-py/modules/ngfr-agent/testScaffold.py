initialMsg = '''
{
    "Features": {
        "Feature_Value": [{
                "_attributes": {
                    "Name": "911_LOCATION",
                    "ID": "3",
                    "Object_ID": "3",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "4800 Oak Grove Dr. Pasadena, CA 91109"
                }
            }, {
                "_attributes": {
                    "Name": "911_SUSPECT",
                    "ID": "2",
                    "Object_ID": "2",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "TRUE"
                }
            }, {
                "_attributes": {
                    "Name": "911_CALL_EVENT",
                    "ID": "1",
                    "Object_ID": "1",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "12/1/17 3:16:45 PM"
                }
            }, {
                "_attributes": {
                    "Name": "ASSUME_IMF",
                    "ID": "1",
                    "Object_ID": "1",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "True"
                }
            }
        ]
    }
}
'''

buildingMsg = '''
{
    "Features": {
        "Feature_Value": [{
                "_attributes": {
                    "Name": "IS_A",
                    "ID": "300",
                    "Object_ID": "66",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "BUILDING"
                }
            }, {
                "_attributes": {
                    "Name": "BUILT",
                    "ID": "301",
                    "Object_ID": "66",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "1923"
                }
            }, {
                "_attributes": {
                    "Name": "COMPOSITION",
                    "ID": "302",
                    "Object_ID": "66",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "WOOD"
                }
            }, {
                "_attributes": {
                    "Name": "FIRE_SUPRESSION",
                    "ID": "303",
                    "Object_ID": "66",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "FALSE"
                }
            }, {
                "_attributes": {
                    "Name": "DOORS",
                    "ID": "304",
                    "Object_ID": "66",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "2"
                }
            }, {
                "_attributes": {
                    "Name": "FUNCTION",
                    "ID": "305",
                    "Object_ID": "66",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "SCHOOL"
                }
            }, {
                "_attributes": {
                    "Name": "FUNCTION",
                    "ID": "306",
                    "Object_ID": "66",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "CHEMISTRY_LAB"
                }
            }, {
                "_attributes": {
                    "Name": "HAZARDS",
                    "ID": "307",
                    "Object_ID": "66",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "CHEMICALS"
                }
            }, {
                "_attributes": {
                    "Name": "HAZARDS",
                    "ID": "308",
                    "Object_ID": "66",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "GAS"
                }
            }, {
                "_attributes": {
                    "Name": "CONNECTIONS",
                    "ID": "309",
                    "Object_ID": "66",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "WOOD"
                }
            }, {
                "_attributes": {
                    "Name": "CONNECTIONS",
                    "ID": "310",
                    "Object_ID": "66",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "BEAMS"
                }
            }, {
                "_attributes": {
                    "Name": "LAST_INSPECTION",
                    "ID": "311",
                    "Object_ID": "66",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "8729817289"
                }
            }, {
                "_attributes": {
                    "Name": "INSPECTION_STATUS",
                    "ID": "312",
                    "Object_ID": "66",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "PASSED"
                }
            }, {
                "_attributes": {
                    "Name": "INSPECTION_STATUS",
                    "ID": "313",
                    "Object_ID": "66",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "CHANGES"
                }
            }
        ]
    }
}
'''

smoke1 = '''
{
    "Features": {
        "Feature_Value": {
            "_attributes": {
                "Name": "SMOKE",
                "ID": "600",
                "Object_ID": "600",
                "Time": "0",
                "Conf": "1.0",
                "Value": "FALSE"
            }
        }
    }
}
'''

smoke2 = '''
{
    "Features": {
        "Feature_Value": {
            "_attributes": {
                "Name": "SMOKE",
                "ID": "600",
                "Object_ID": "600",
                "Time": "0",
                "Conf": "1.0",
                "Value": "TRUE"
            }
        }
    }
}
'''

smoke3 = '''
{
    "Features": {
        "Feature_Value": [{
                "_attributes": {
                    "Name": "SMOKE",
                    "ID": "601",
                    "Object_ID": "601",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "TRUE"
                }
            }, {
                "_attributes": {
                    "Name": "SMOKE",
                    "ID": "602",
                    "Object_ID": "602",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "TRUE"
                }
            }
        ]
    }
}
'''

temp1 = '''
{
    "Features": {
        "Feature_Value": {
            "_attributes": {
                "Name": "TEMP",
                "ID": "500",
                "Object_ID": "500",
                "Time": "0",
                "Conf": "1.0",
                "Value": "560"
            }
        }
    }
}
'''

temp2 = '''
{
    "Features": {
        "Feature_Value": {
            "_attributes": {
                "Name": "TEMP",
                "ID": "500",
                "Object_ID": "500",
                "Time": "0",
                "Conf": "1.0",
                "Value": "825"
            }
        }
    }
}
'''

temp3 = '''
{
    "Features": {
        "Feature_Value": {
            "_attributes": {
                "Name": "TEMP",
                "ID": "500",
                "Object_ID": "500",
                "Time": "0",
                "Conf": "1.0",
                "Value": "1200"
            }
        }
    }
}
'''

tweat = '''
{
    "Features": {
        "Feature_Value": [{
                "_attributes": {
                    "Name": "Target Color",
                    "ID": "100013",
                    "Object_ID": "200001",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "Yellow"
                }
            }, {
                "_attributes": {
                    "Name": "Suspect",
                    "ID": "100008",
                    "Object_ID": "200001",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "True"
                }
            }, {
                "_attributes": {
                    "Name": "Shooter",
                    "ID": "100007",
                    "Object_ID": "200001",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "True"
                }
            }, {
                "_attributes": {
                    "Name": "Height Value",
                    "ID": "100005",
                    "Object_ID": "200001",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "72"
                }
            }, {
                "_attributes": {
                    "Name": "Gender",
                    "ID": "100009",
                    "Object_ID": "200001",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "Male"
                }
            }, {
                "_attributes": {
                    "Name": "Danger",
                    "ID": "100004",
                    "Object_ID": "200001",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "Shooter"
                }
            }, {
                "_attributes": {
                    "Name": "Color",
                    "ID": "100010",
                    "Object_ID": "200001",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "Yellow"
                }
            }, {
                "_attributes": {
                    "Name": "Clothing Type",
                    "ID": "100012",
                    "Object_ID": "200001",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "Shirt"
                }
            }, {
                "_attributes": {
                    "Name": "Clothing Attribute",
                    "ID": "100011",
                    "Object_ID": "200001",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "Stripped"
                }
            }, {
                "_attributes": {
                    "Name": "Build Category",
                    "ID": "100006",
                    "Object_ID": "200001",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "Medium"
                }
            }, {
                "_attributes": {
                    "Name": "Alert",
                    "ID": "100001",
                    "Object_ID": "200001",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "Initiate Lockdown Procedures"
                }
            }, {
                "_attributes": {
                    "Name": "Alert",
                    "ID": "100002",
                    "Object_ID": "200001",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "Seek Cover"
                }
            }, {
                "_attributes": {
                    "Name": "Alert",
                    "ID": "100003",
                    "Object_ID": "200001",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "Do Not Engage"
                }
            }
        ]
    }
}
'''

vision = '''
{
    "Features": {
        "Feature_Value": [{
                "_attributes": {
                    "Name": "HUMAN_LEG",
                    "ID": "7",
                    "Object_ID": "5",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "IMAGES/X4.JPL"
                }
            }, {
                "_attributes": {
                    "Name": "HUMAN_TORSO",
                    "ID": "6",
                    "Object_ID": "5",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "IMAGES/X3.JPL"
                }
            }, {
                "_attributes": {
                    "Name": "HUMAN_HEAD",
                    "ID": "5",
                    "Object_ID": "5",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "IMAGES/X2.JPL"
                }
            }, {
                "_attributes": {
                    "Name": "GUN",
                    "ID": "4",
                    "Object_ID": "4",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "IMAGES/X1.JPL"
                }
            }, {
                "_attributes": {
                    "Name": "HUMAN_THREAT",
                    "ID": "3",
                    "Object_ID": "3",
                    "Time": "0",
                    "Conf": "0.9264706",
                    "Value": "True"
                }
            }, {
                "_attributes": {
                    "Name": "HUMAN+WEAPON",
                    "ID": "4",
                    "Object_ID": "4",
                    "Time": "0",
                    "Conf": "0.9264706",
                    "Value": "True"
                }
            }, {
                "_attributes": {
                    "Name": "WEAPON",
                    "ID": "6",
                    "Object_ID": "6",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "True"
                }
            }, {
                "_attributes": {
                    "Name": "HUMAN",
                    "ID": "7",
                    "Object_ID": "7",
                    "Time": "0",
                    "Conf": "0.85294116",
                    "Value": "True"
                }
            }
        ]
    }
}
'''

fire = '''
   {"Features": {
        "Feature_Value": [{
                "_attributes": {
                    "Name": "FIRE",
                    "ID": "400",
                    "Object_ID": "400",
                    "Time": "0",
                    "Conf": "1.0",
                    "Value": "TRUE"
                }
            }, {
                "_attributes": {
                    "Name": "HUMAN+FIRE",
                    "ID": "14",
                    "Object_ID": "14",
                    "Time": "0",
                    "Conf": "0.9264706",
                    "Value": "True"
                }
            }]}} '''


heart1 = '''
{
    "Features": {
        "Feature_Value": [{
                "_attributes": {
                    "Name": "SENSOR_NODE",
                    "ID": "10000",
                    "Object_ID": "10000",
                    "Time": "12.759053",
                    "Conf": "1.0",
                    "Value": "SENSORNODE-FF1"
                }
            }, {
                "_attributes": {
                    "Name": "HEARTBEAT_BASELINE",
                    "ID": "10003",
                    "Object_ID": "10000",
                    "Time": "12.759053",
                    "Conf": "1.0",
                    "Value": "120"
                }
            }, {
                "_attributes": {
                    "Name": "ALCOHOL_BASELINE",
                    "ID": "10004",
                    "Object_ID": "10000",
                    "Time": "12.759053",
                    "Conf": "1.0",
                    "Value": "300"
                }
            }, {
                "_attributes": {
                    "Name": "GAS_ALCOHOL",
                    "ID": "10001",
                    "Object_ID": "10000",
                    "Time": "12.759053",
                    "Conf": "1.0",
                    "Value": "49"
                }
            }, {
                "_attributes": {
                    "Name": "HEARTBEAT",
                    "ID": "10002",
                    "Object_ID": "10000",
                    "Time": "12.759053",
                    "Conf": "1.0",
                    "Value": "95"
                }
            }, {
                "_attributes": {
                    "Name": "GPS_LATITUDE",
                    "ID": "10005",
                    "Object_ID": "10000",
                    "Time": "12.759053",
                    "Conf": "1.0",
                    "Value": "34.202164"
                }
            }, {
                "_attributes": {
                    "Name": "GPS_LONGITUDE",
                    "ID": "10006",
                    "Object_ID": "10000",
                    "Time": "12.759053",
                    "Conf": "1.0",
                    "Value": "-118.17423"
                }
            }
        ]
    }
}
'''

heart2 = '''
{
    "Features": {
        "Feature_Value": [{
                "_attributes": {
                    "Name": "SENSOR_NODE",
                    "ID": "20000",
                    "Object_ID": "20000",
                    "Time": "1.4713127",
                    "Conf": "1.0",
                    "Value": "SENSORNODE-FF2.FF.LOCAL"
                }
            }, {
                "_attributes": {
                    "Name": "HEARTBEAT",
                    "ID": "20001",
                    "Object_ID": "20000",
                    "Time": "1.4713127",
                    "Conf": "1.0",
                    "Value": "88"
                }
            }, {
                "_attributes": {
                    "Name": "GAS_ALCOHOL",
                    "ID": "20002",
                    "Object_ID": "20000",
                    "Time": "1.4713127",
                    "Conf": "1.0",
                    "Value": "30"
                }
            }
        ]
    }
}
'''

import re
def transformRules(chars):
    chars = re.sub("%.*\n", '', chars)                             # get rid of comments
    statements = [s.split('~>') for s in chars.replace('\n','').replace(' ','').split(';')]
    return [([pre.replace('(',' ').replace(')','').replace(',',' ').split() for pre in s[0].split('&')],
             [act.replace('(',' ').replace(')','').replace(',',' ').split() for act in s[1].split('||')]) for s in statements[:-1]]
    
rules = transformRules(
    ''' % arrival response
        queryNAL1(HEARTBEAT:HIGH,new)  ~>
          sendMsg(Notifying_fireman_of_high_heart_rate) ||
          forget(HEARTBEAT:HIGH);
       
        queryNAL1(GAS_ALCOHOL:HIGH,new)  ~>
          sendMsg(Notifying_fireman_of_high_gas_concentration) ||
          forget(GAS_ALCOHOL:HIGH);
       
        % fire responses
        queryNAL1(FIRE,new) & queryNAL1(HAZARDS:CHEMICALS,old) ~>
          sendMsg(Suggesting_fireman_requires_hazmat_team);
       
        queryNAL1(FIRE,new) & queryNAL1(SHOOTER,old) ~>
          sendMsg(Notifying_fireman_of_shooter_alert);
       
        queryNAL1(FIRE,new) ~>
          sendMsg(Suggesting_fireman_deploys) ||
          forget(new) || post(FIRE,->,old,1,0.99,0);
       
        % picture analytic response
        queryNAL1(HUMAN+WEAPON,new) ~>
          sendMsg(Suggesting_police_deploys) ||
          forget(new) || post(HUMAN+WEAPON,->,old,1,0.99,0);
       
        % tweat response
        queryNAL1(SHOOTER,new) ~>
          sendMsg(Audrey_accessing_camera) ||
          forget(new) || post(SHOOTER,->,old,1,0.99,0);
       
        % 911 response
        queryNAL1(911_SUSPECT,new) ~>
          sendMsg(Audrey_accessing_address_information) ||
          sendMsg(Audrey_accessing_geospatial_information) ||
          sendMsg(Audrey_accessing_to_social_media) ||
          forget(new) || post(911_SUSPECT,->,old,1,0.99,0);
       
        % building message
        queryNAL1(BUILDING,new) ~>
          forget(new) || post(BUILDING,->,old,1,0.99,0);
       
        % smoke message
        queryNAL1(SMOKE,new) ~>
          forget(new) || post(SMOKE,->,old,1,0.99,0);
       
        % high temperature message
        queryNAL1(TEMP:HIGH,new) & queryNAL1(SMOKE,old) ~>
          sendMsg(Firemen_Alert:_Flashover_Imminent) ||
          forget(new);
          ''')

def getRules(context):
    return rules

class Worker(object):
    def __init__(self,name):
        print "Created object [%s]\n"%name

    def on_message(self,frame):
        print "input: [%s]"%frame.name
        return 1

    def publish(self,msg):
        print "  output: [%s]"%msg

    def run(self):
        self.on_message(FrameException('init', initialMsg))
        self.on_message(FrameException('building', buildingMsg))
        self.on_message(FrameException('tweat',tweat))
        self.on_message(FrameException('vision',vision))
        self.on_message(FrameException('fire',fire))
        self.on_message(FrameException('smoke1',smoke1))
        self.on_message(FrameException('temp1',temp1))
        self.on_message(FrameException('smoke2',smoke2))
        self.on_message(FrameException('temp2',temp2))
        self.on_message(FrameException('smoke3',smoke3))
        self.on_message(FrameException('temp3',temp3))
        self.on_message(FrameException('heart1',heart1))
        self.on_message(FrameException('heart2',heart2))

class FrameException(object):
    def __init__(self,label,msg):
        self.name,self.body = label,msg

