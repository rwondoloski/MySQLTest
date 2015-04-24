'''
Created on Apr 23, 2015

@author: robert
'''
#!/usr/bin/python

''' Copyright (c) 2010-2013 Roger Light <roger@atchoo.org>
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Distribution License v1.0
# which accompanies this distribution. 
#
# The Eclipse Distribution License is available at 
#   http://www.eclipse.org/org/documents/edl-v10.php.
#
# Contributors:
#    Roger Light - initial implementation
# Copyright (c) 2010,2011 Roger Light <roger@atchoo.org>
# All rights reserved.

Robert Wondoloski - edit to connect, subscribe,  and write data to MySQL server
'''
import sys
import MySQLdb

try:
    import paho.mqtt.client as mqtt
except ImportError:

    # This part is only required to run the example from within the examples
    # directory when the module itself is not installed.
    #
    # If you have the module installed, just use "import paho.mqtt.client"
    import os
    import inspect
    cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"../src")))
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)
    import paho.mqtt.client as mqtt

'''
Define variables

'''
SQLServer = "localhost"
SQLUser = "root"
SQLPwd = "UQ732!"
SQLDatabase = "ThingWorx"



def on_connect(mqttc, obj, flags, rc):
    print("rc: "+str(rc))

def on_message(mqttc, obj, msg):
    topics=msg.topic.split('/')    
    if topics[0] == "ThingWorx" and topics[3] ==  "Temperature":
        print(msg.topic+" "+str(msg.qos)+" "+str(msg.payload))
        print
	print " 1. UID = ", topics[1]
	print " 2. DateTime ", topics[2]

        try:
	    WriteTemp(msg.payload,topics[2],topics[1])
        except:
	    print "message writeTemp error: "
 
def WriteTemp(temperature,msgtime,uid):
    global db,cursor 
    # Prepare SQL query to INSERT a record into the database.
    sql = "INSERT INTO Temperature(Temperature,TimeStamp,clientID) \
       VALUES ('%f','%s','%s')" % (float(temperature),msgtime,uid)
    ##  insert into `ThingWorx`.`Temperature` ( `Temperature`) values ( '32.0')       
    print float(temperature)
    try:
    # Execute the SQL command
        cursor.execute(sql)
    # Commit your changes in the database
        db.commit()
        return
    
    except Exception , error:
    # Rollback in case there is any error
        db.rollback()
        print "Failed to Write: ", error.args
        return


# Open database connection
db = MySQLdb.connect(SQLServer,SQLUser,SQLPwd,SQLDatabase)
# prepare a cursor object using cursor() method
cursor = db.cursor()

mqttc = mqtt.Client()
mqttc.on_message = on_message
mqttc.on_connect = on_connect

mqttc.username_pw_set("rwondoloski","UbiQuai7")
mqttc.connect("54.149.235.26", 1883, 60)
mqttc.subscribe("ThingWorx/#", 0)

mqttc.loop_forever()

