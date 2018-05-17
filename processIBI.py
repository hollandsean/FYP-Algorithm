import schedule
import time
import pyrebase

start_time = time.time()

#========================================================================
# method to reads in the user database data
# this data contains the users MRN number
#========================================================================
def readMRN(db):
    
    MRNlist = []
    
    AllData = db.child("PATIENT_MRN_RAW_DATA").get()
    mrn = AllData.val()
    
    if mrn:
        for i in mrn:
            MRNlist.append(i)
            
    else:
        MRNlist.append("ERROR")
        
    return MRNlist

#========================================================================
# method to read in the user database data
# this data contains the Inter Beat Interval
#========================================================================
def readIBI(db, i):
    
    IBIlist = []
    error = ""
    
    IBIdata = db.child("PATIENT_MRN_RAW_DATA").child(i).child("IBI").get()
    ibi = IBIdata.val()
    
    if ibi != 0:
        for k, v in ibi.items():
            IBIlist.append(v)
    else:
        error = "NO IBI DATA AVAILABLE"
        
    return IBIlist, error

#========================================================================
# method to read in the user database data
# this data contains the Inter Beat Interval
#========================================================================
def readHR(db, i):
    
    HRlist = []
    error = ""
    
    HRdata = db.child("PATIENT_MRN_RAW_DATA").child(i).child("HR").get()
    hr = HRdata.val()
    
    if hr != 0:
        for k, v in hr.items():
            HRlist.append(v)
    else:
        error = "NO HR DATA AVAILABLE"
    
    return HRlist, error

#========================================================================
# method to detect irregularities in the IBI
# This is done by comparing the current value to the next value in a loop
# A binary list is returned showing occurences of irregular heart beats
#========================================================================
def irregularityRecorder(data):
    
    occurence = []
    length = len(data)
    
    if not data:
        
        occurence.append(0)
        
    else:
        
        for value in data:
           
            if value < length - 1:
                temp = data[value]
                current = data[value] + 1
                
                if abs(temp-current) >= 0.2:
                    occurence.append(1)
                    
                else:
                    occurence.append(0)
    
    return occurence

   
#========================================================================
# method to diagnose the presence of Atrial Fibrillation
# This is done by examining the binary list of irregular heart beats 
# This binary list is obtained from the method "irregularityTracker()"
# If a sequence of one's is detected, a running total of the IBI 
# duration is maintained, if this total is greater than 30 seconds,
# an episode of Atrial Fibrillation is diagnosed.
#========================================================================
def diagnosis(data, occurence):
    
    diagnosis = ""
    time = 0
    temp = 0
    episodes = 0
    
    for value in occurence:
        
        if value == 1:
            temp = data[value]
            time += temp
            
            if time >= 30:
                 diagnosis = "ATRIAL FIBRILLATION DETECTED"
                 episodes += 1
                 
        if value == 0:
            time = 0
            temp = 0
            
    if episodes == 0:
        diagnosis = "ATRIAL FIBRILLATION EPISODES WERE NOT DETECTED"
            
    return diagnosis, episodes

#========================================================================
# method to calculate the averageHR, maxHR and minHR from the dataframe.
#========================================================================
def beatsPerMinute(data):
    
    if data:
    
        maxHR = max(data)
        minHR = min(data)
        averageHR = sum(data)/int(len(data))
        
    else:
        maxHR = 0
        minHR = 0
        averageHR = 0
        
    
    return averageHR, maxHR, minHR

#========================================================================
# method to run the program funtions at a set time every day.
#========================================================================
def scheduledRun():
    
    abort = "ERROR"
    
    config = {
    #config removed as it contains api key
    }
     
    firebase = pyrebase.initialize_app(config)
    
    db = firebase.database()
    
    mrnList = readMRN(db)
    
    if mrnList[0] == abort:

        ref = db.child("FATAL_ERROR_LOG")
            
        data = {"ERROR": "NO PATIENT MRN DATA AVAILABLE", "Date andTime": time.ctime(int(time.time()))}

        ref.push(data)
    
    else:
    
        for i in mrnList:
        
            IBIdata,error1 = readIBI(db, i)
            HRdata,error2 = readHR(db, i)
            occurence = irregularityRecorder(IBIdata)
            result, number = diagnosis(IBIdata, occurence)
                
            averageHR, maxHR, minHR = beatsPerMinute(HRdata)
            
            error = error1 + "," + error2
            
            strippedError = error.replace(","," ")
            
            if not strippedError:
                error = "NONE"
           
            ref = db.child("PROCESSED_PATIENT_DATA")
            
            data = {"result": result, "numberOfEpisodes": number, "averageHR": averageHR, "maxHR": maxHR, "minHR": minHR, "Time": time.ctime(int(time.time())), "ERROR":error}
    
            patient_ref = ref.child(i)
            patient_ref.push(data)
        
            print("Result: {0} Number of episodes: {1} ".format(result, number))
            
            print("Average per minute: {0}, MAX: {1}, MIN: {2}".format(averageHR, maxHR, minHR))
            
            print("Execution time: {0} seconds".format(time.time() - start_time))
            
            #reset the raw MRN IBI data in the database
            db.child("PATIENT_MRN_RAW_DATA").child(i).child("HR").set(0)
            #reset the raw MRN HR data in the database
            db.child("PATIENT_MRN_RAW_DATA").child(i).child("IBI").set(0)


#========================================================================
# "Main" method of the program, calls the other methods.
#========================================================================
def main():
    
    print("waiting to run.....")
    
    schedule.every().day.at("11:58").do(scheduledRun)

    while True:
        schedule.run_pending()
        
#========================================================================
# Entry point of the program.
#========================================================================
if __name__ == "__main__":
    
    main()




