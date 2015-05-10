import socket,select,time,sys
import MySQLdb
	
'''
Define variables
'''
SQLServer = "localhost"
SQLUser = "IFS"
SQLPwd = "UbiQuai7"
SQLDatabase = "IFS"
SQLTableName = "CardMaster_CurrentData"

#host = '66.76.142.175'
host = 'localhost'
port = 4660
delimiter0 = '\x0d\x0a\x3a'   

class SQLclass:

	def __init__(self,SQLServer,SQLUser,SQLPwd,SQLDatabase,datab=None):
		# Open database connection
		if datab is None:
			try:
				self.datab = MySQLdb.connect(SQLServer,SQLUser,SQLPwd,SQLDatabase)
			except:
				print "Not able to connect to DataBase"
		else:
			self.datab=datab
			
	def cursor(self):
		self.cur = self.datab.cursor()
		return self.cur

	def ParseData(self,SQLTable,TableData,CurrentData):
		ConnectionNumber = host + ":" + str(port)
		ConnectionTime = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
		lines = CurrentData.split('\r\n')
		if lines[0] != 'B-':
			print "Incorrect Header - NOT equal to B-"
			sys.exit()
		else:
			for n in range(1,len(lines)):
				data = lines[n].split(',')
				if len(data) > 2:   # verify that there is data in the line...
                			data.append(ConnectionNumber)
                			data.append(ConnectionTime)
                			sql=self.WriteSQL(SQLTable,TableData,data)

	def WriteSQL(self,SQLTable,TableData,data):
		''' Prepare SQL query to INSERT a record into the database.'''
		count = 0
		FieldList = "" # List of the fields in the table
		DataList = ""  # Add Data[count-1] in a list for each data element
		Length = len(TableData)
		MaxIndex=self.GetIndexNumber(SQLTable)       # Get the current index Number
		print "Current Index = ", MaxIndex[0]
		NewIndex = MaxIndex[0] + 1
		data.append(NewIndex)           # need to make an incrimetal value for each data option
		sql = "INSERT INTO "+SQLTable+"(IndexNumber) VALUES ('%i')" % NewIndex # Create new database entry
		self.ExecuteSQL(sql)
		
		for FieldData in TableData:
			FieldList = FieldData[0]        # FieldData[0] is the field Name
			TLtemp = FieldData[1]           # FieldData[1] is teh field type
			
               		if TLtemp[0:2] == 'in':                 # integer
				try:
					data[count]=int(data[count])    #convert data to integer
				except:
					data[count]=0
				sql = "UPDATE "+SQLTable+" SET "+FieldList+" = %i WHERE IndexNumber =%i" % (data[count],NewIndex)
			elif TLtemp[0:2] == 'de':               # decimal
				try:
					data[count]=float(data[count])  #convert to float
				except:
					data[count]=0.0
				sql = "UPDATE "+SQLTable+" SET "+FieldList+" = '%d' WHERE IndexNumber ='%i'" % (data[count],NewIndex)
			else:                                   # String for all others
				sql = "UPDATE "+SQLTable+" SET "+FieldList+" = '%s' WHERE IndexNumber ='%i'" % (data[count],NewIndex)
			self.ExecuteSQL(sql)
			count += 1
 
	def ExecuteSQL(self,sql):

	    try:
	    	cursor.execute(sql)   # Execute the SQL command
	    	self.datab.commit()           # Commit your changes in the database
	    	return
	    except Exception , error:  # Rollback in case there is any error
        	self.datab.rollback()
        	print "Failed to Execute: ", error.args
        	return

	def GetTableFields(self,SQLTable,SQLDatabase):
		sql = "SHOW COLUMNS FROM "+ SQLTable + " FROM "+SQLDatabase
		print sql
		self.ExecuteSQL(sql)
		TableData = cursor.fetchall()
		print TableData
		return TableData
 
	def GetIndexNumber(self,SQLTable):
	
	    sql = "SELECT MAX(" + SQLTable+ ".IndexNumber) FROM "+SQLTable
	    print sql
	    self.ExecuteSQL(sql)
	    MaxIndex = cursor.fetchone()
	    return MaxIndex

class mysocket:

    def __init__(self, sock=None):
        if sock is None:
            self.sock = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            
        else:
            self.sock = sock
            
    def setblock(self):
    	self.sock.setblocking(0)
    	
    def connect(self, host, port):
    	try:
    		self.sock.connect((host, port))
    	except:
    		print "Not able to connect"
    		so.close()
    		sys.exit()
   			
    def sendmsg(self, msg):
        totalsent = 0
        while totalsent < len(msg):
        	sel = select.select([],[self.sock],[],40) # sel[0] = write, 1= read
        	if sel[1] != []:
        		sent = self.sock.send(msg[totalsent:])
        		if sent == 0:
        			raise RuntimeError("socket connection broken")
        		totalsent = totalsent + sent
        	else:
        		print sel
        		so.close()
        		print" No Data Sending....Closing connection"
        		sys.exit()
            
    def close(self):
    	self.sock.close()

    def receivemsg(self,so,delimiter):
        chunks = []
        chunk=''
        bytes_recd = 0
          
        while chunk.find(delimiter) == -1:  # Not contained in chunk
        	sel = select.select([self.sock],[],[],40) # sel[0] = write, 1= read
        	''' timer on select.select waits for at least 1 input, so putting in
        	2 inputs is a bad idea!'''
        	if sel[0] != []:
        		chunk = self.sock.recv(1024)
        		if chunk == '':
    				raise RuntimeError("socket connection broken")
    			chunks.append(chunk)        			
    			bytes_recd = bytes_recd + len(chunk)
    		else:
    			print sel
    			so.close()
    			print" No Data Recevied....Closing connection"
    			sys.exit()
        			
        return ''.join(chunks)
        
        
        
def loginCM():
	
	print "Sending P"
	so.sendmsg('P')
	response = so.receivemsg(so,': ')
	print response, "\x0d\x0aSending MASTER"
	so.sendmsg('MASTER\x0d')
	response = so.receivemsg(so,delimiter0)
	print response
	
def WriteFile(data):
	try:
		f=open('CurrentData.txt','w')
		f.write(data)
		f.close()
	except:
		print "File Failed to Write correctly! "
		f.close()
		sys.exit()
		
		
	
	
''' CONNECT Socket ******************'''
so=mysocket()
print "Sending connnection Request to : ", host,",",port
so.connect(host,port) # Need to connect before setblocking...
so.setblock()
loginCM()
'''GET DATA ******************'''
print "Requesting Data: \n\r"
so.sendmsg('B')
CurrentData= so.receivemsg(so,delimiter0)
print CurrentData

'''DataBase Setup *************'''
print dir(SQLclass)
db = SQLclass(SQLServer,SQLUser,SQLPwd,SQLDatabase)
print "Connecting to Database : ", SQLDatabase, " on Server : ", SQLServer
print "User: ",SQLUser
cursor = db.cursor()	# prepare a cursor object using cursor() method

TableData = db.GetTableFields(SQLTableName,SQLDatabase)

db.ParseData(SQLTableName,TableData,CurrentData)

