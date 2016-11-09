#Mongo Driver Test - Mongo Shell - Javascript
# ID: python-pymongo
# ThisVersion: 1.0
# SpecLevel: 1.0
# Author(s): John Page
# LastUpdate: 16th-August-2016

from pymongo import *
import time
import datetime
import math
from bson.int64 import Int64
from multiprocessing import Process

#Change as required for testing - server should be fast as should network
#connectionString = "mongodb:#localhost:27017"

with open('./connection_string','r') as f:
    connection_string = f.readline().rstrip()
print "Connecting to " + connection_string

#connectionString = "mongodb:#localhost:27017,localhost:27018,localhost:27019/?replicaSet=replset"
numRecords = 1000000
language = "python-pymongo" # Format Language-variation/driver i.e. js-shell, js-node, python-pure

# Append batch size, 1000 is a good performance balance.
batchSize = 1000

# Definition of a 'Document'
topFields = 20 # 20 top level fields
arrObjSize = 10 # 10 fields in each array object
arrSize = 20 # Array of 20 elements
verbose = True

    # This is intended to be *Correct* code to do a bulkops write and handle things like replica
    # failover part way through a batch.

def process_batch(bulkops,collection):
    fatal_error = False #Cannot try again
    success = False #Suceeded
    retries = 0 
    max_retries = 10 #Retries on failover

    if len(bulkops) == 0:
        return True

    while fatal_error == False and retries < max_retries and success == False:
        retries=retries+1
        try:
            collection.bulk_write(bulkops, ordered=False)
            success = True
        except Exception as err:
            numErrs = 0
            try:
                numErrs = err.getWriteErrorCount()
            except Exception as inner_error:
                # This is some other type of error not a bulkopsWriteError object
                # Just retry - this covers network outages
                # In the Shell - autoreconnect is handled internally
                print("Error: " + err.message)
                continue
            

            for e in range(0,numErrs):
                #A duplicate in our first attempt is a hard fail
                #A duplicate later may be a partially applied batch and a failover
                if err.getWriteErrorAt(e).code != 11000 or retries == 1:
                    print(err.getWriteErrorAt(e).errmsg)
                    fatal_error == True
    return success


def generateRecord(thread, recnum):
    fldpfx = "val"
        #This is a shard friendly _id, a low cardinality, variable prefix then an incrementing value as string
    id = str((recnum % 256)) + "-" + str(recnum)
    rec = {
        '_id': id,
        'arr': []
    }
    for tf in range(0,topFields): 
        tp = tf % 4
        if tp == 0:
            fieldval = "Lorem ipsum dolor sit amet, consectetur adipiscing elit." #Text
        elif tp == 1:
            fieldval = datetime.datetime.utcfromtimestamp(int((tf * recnum)/1000)) #A date
        elif tp == 2:
            fieldval = math.pi * tf # A float    
        else:
            fieldval = Int64(recnum + tf) # A 64 bit integer
                
        rec[fldpfx + str(tf)] = fieldval
    
    # populate array of subdocuments
    for el in range(0,arrSize):
        subrec = {}
        for subRecField in range(0,arrObjSize):
            tp = subRecField % 4
            if tp == 0:
                fieldval = "Nunc finibus pretium dignissim. Aenean ut nisi finibus"
            elif tp == 1:
                fieldval = datetime.datetime.utcfromtimestamp(int((subRecField * recnum * el)/1000)) #A date
            elif tp == 2:
                fieldval = math.pi * subRecField * el
            else:
                fieldval = Int64(recnum + subRecField * el)

            subrec['subval' + str(subRecField)] = fieldval

        rec['arr'].append(subrec)
    return rec


def single_thread_test(test_data):
    #test_data is a global used for parameter passing with Mongo Parallel Shells

    threadnum = test_data['threadnum'] #Read the thread id from the fork()
    connectionString = test_data['connectionString']
    numRecords = test_data['numRecords']
    language = test_data['language']
    testMode = test_data['mode']
    verbose = test_data['verbose']
    section = test_data['section']

    bulkops = [] # bulkops operations

    # Total Records per thread in test

    
    connection = MongoClient(connection_string)
    db = connection.get_database("drvmark-" + language)
    logdb = connection.get_database("results")
    # Each thread has own collection , this is not testing
    # server consurrenct, although with WiredTiger it's not
    # An issue

    collection = db.get_collection("records_" + str(threadnum)) 

    if section == "create" or section == "all":
        #Drop the test collection
        collection.drop()

        # Test 1 - Insert a number of records 
        #Test linear insert speed
        start = int(time.time())

        for r in range(1,numRecords+1): 
            rec = generateRecord(threadnum, r)

            #Add to the array of operations as an InsertOne
            bulkops.append(InsertOne(rec))
            # Flush every N records
            if r % batchSize == 0:
                if process_batch(bulkops,collection) == True:
                    bulkops = []
                    if verbose == True:
                       print str(r) + " inserted"
                else:
                    print "Error in bulkops load"
                    exit(1)

        #Any remaining in the bulkops arrays
        if  process_batch(bulkops,collection) == True:
            bulkops = []
            if verbose == True:
                print str(r) + " inserted" 
        else:
            print "Error in bulkops load"
            exit(1)
        

        end = int(time.time())
        timeTaken = end - start;


        #Using $max here because of parallel threads

        updateobj = {}
        updateobj[testMode+".create"]=timeTaken # Otherwise key is taken as litteral
        logdb.get_collection("driver").update_one({ '_id' : language },
            { '$max' : updateobj},upsert=True) 
    

    # Fetch Records and get a single field from them 
    # Whilst a server side projection is optimal here we are not testing
    # that. We are testing the drivers ability to fetch a document and get a field from it.
    if section == "read" or section == "all":
        start = int(time.time())

        c = collection.find({})
        sum = 0
        for record in c:
            sum = sum + record['arr'][10]['subval2']
        
        if verbose == True:
            print "Sum is " + str(sum) 
        
        end =  int(time.time())
        timeTaken = end - start;
        updateobj = {}
        updateobj[testMode+".read"]=timeTaken # Otherwise key is taken as litteral
        logdb.get_collection("driver").update_one({ '_id' : language },
            { '$max' : updateobj},
            upsert=True) 
    

    #Update each record, again an update with $set is better however we are testing
    #Thre drivers ability to get a record, get data from it and either modify it or 
    #construct a new one (in some drivers, retrieved records are immutable)
    if section == "update" or section == "all":
        start = int(time.time())

        c = collection.find()
        bulkops = []
        r = 0
        
        for record in c:
            for field in record:
                # Add to each top level string
                if field != "_id" and type(record[field]) is unicode :
                    record[field] = record[field] + " (modified) "
                
            
            # Add to the array of operations as a replaceOne
            bulkops.append(ReplaceOne( {'_id': record['_id']},record))
            if r % batchSize == 0:
                if process_batch(bulkops,collection) == True:
                    bulkops = []
                    if verbose == True:
                        print str(r) + " modified"
                else:
                    print "Error in bulkops load"
                    exit(1)            
            r=r+1
        
        #Any remaining
        if  process_batch(bulkops,collection) == True:
            bulkops = []
            if verbose == True:
                print str(r) + " modified"
        else:
            print "Error in bulkops load"
            exit(1)
        
        end = int(time.time())
        timeTaken = end - start;
        updateobj = {}
        updateobj[testMode+".update"]=timeTaken # Otherwise key is taken as litteral
        logdb.get_collection("driver").update_one({ '_id' : language },{ '$max' : updateobj},upsert=True) 




# This code only exists in the Parent Process

# Place to write results
logconnection = MongoClient(connection_string)
logdb = logconnection.get_database("results")



#Data for child processes
test_data = {}
test_data['threadnum'] = 0 # Thread number for the standalone
test_data['connectionString'] = connection_string
test_data['numRecords'] = numRecords
test_data['language'] = language
test_data['mode'] = "linear"
test_data['verbose'] = verbose
test_data['section']  = "all"

#Drop old test results
logdb.get_collection("driver").delete_one({'_id':language})

#Test linear insert speed
start = int(time.time())

single_thread_test(test_data)

end = int(time.time())
timeTaken = end - start;
logdb.get_collection("driver").update_one({'_id':language},{ '$set' : { 'linear.total' : timeTaken}},upsert=True) 


#Test parallel speed
numthreads = 4 # This is up to the code designer - but we will run on 4 core clients

for section in ['create','read','update']:
    threadnum=0
    threads=[]

    start = int(time.time())

    for u in range(numthreads):
        test_data['threadnum'] = threadnum # Thread number for the standalone
        test_data['connectionString'] = connection_string
        test_data['numRecords'] = numRecords / numthreads # Split the load up
        test_data['language'] = language
        test_data['mode'] = "parallel"
        test_data['section'] = section
        p = Process(target=single_thread_test,args=(test_data,))
        p.start()
        threads.append(p)
        threadnum=threadnum+1

    for thread in threads:
        thread.join()

end = int(time.time())
timeTaken = end - start;
logdb.get_collection("driver").update_one({'_id':language},{ '$set' : { 'parallel.total' : timeTaken}},upsert=True) 
