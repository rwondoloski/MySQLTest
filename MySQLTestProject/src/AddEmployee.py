'''
Created on Apr 15, 2015

@author: robert wondoloski
'''
#!/usr/bin/python

import MySQLdb

def AddTable(tablename,field1,fieldtype):
    sql= "CREATE TABLE `TestDB`.`%s` \
    ( `%s` %s) COMMENT='';" % (tablename,field1,fieldtype)
    try:
        cursor.execute(sql)
        db.commit()
        return
    except Exception , error:
        db.rollback()
	print "Failed to Execute: ", error.args
	return

def AddEmployee(first,last,age,sex,income):
# Prepare SQL query to INSERT a record into the database.
    sql = "INSERT INTO EMPLOYEE(FIRST_NAME, \
       LAST_NAME, AGE, SEX, INCOME) \
       VALUES ('%s', '%s', '%d', '%s', '%d' )" % \
       (first,last,age,sex,income)
    try:
    # Execute the SQL command
        cursor.execute(sql)
    # Commit your changes in the database
        db.commit()
        return
    
    except Exception , error:
    # Rollback in case there is any error
        db.rollback()
	print "Failed to Execute: ", error.args
        return

def FetchEmployee():
    
# Prepare SQL query to INSERT a record into the database.
    sql = "SELECT * FROM EMPLOYEE \
       WHERE INCOME > '%d'" % (1000)
    try:
    # Execute the SQL command
        cursor.execute(sql)
    # Fetch all the rows in a list of lists.
        results = cursor.fetchall()
        for row in results:
            fname = row[0]
            lname = row[1]
            age = row[2]
            sex = row[3]
            income = row[4]
        # Now print fetched result
            print "fname=%s,lname=%s,age=%d,sex=%s,income=%d" % \
             (fname, lname, age, sex, income )
        return
         
    except:
        print "Error: unable to fecth data"
        return
    
    
    
#############MAIN Function############################################

# Open database connection
db = MySQLdb.connect("localhost","testuser","test123","TestDB" )
# prepare a cursor object using cursor() method
cursor = db.cursor()

task1 = raw_input("(F)etch, (A)dd User (N)ew Table:  ")
if task1 == "F":
    FetchEmployee()
elif task1 == "A":
    first = raw_input("First Name")
    last = raw_input("Last Name")
    age = raw_input("Age")
    age = float(age)
    sex = raw_input("Sex")
    income = raw_input("Income")
    income = float(income)

    AddEmployee(first, last, age, sex, income)
elif task1 == "N":
    tablename = raw_input("TableName: ")
    field1 = raw_input("new Field Name: ")
    fieldtype = raw_input("varchar(255),int,float,datetime, or other valid type: ")
    
    AddTable(tablename,field1,fieldtype)
else:
    print("No Valid Command - Exiting")
    db.close() # disconnect from server

