# This is the script to get the list of applications which are running for more than N hours in Tez Hive Yarn as Application
#Python 3.6 
#Author : Shahed Munir and Krishna Udathu
#The DeliverBI Way
#Version : 1.0
#Output Yarn Jobs that have been RUNNING for over 4 hours.
import json, urllib.request, time
# Passing the rest api url of the resource manager and filtering the applications to fetch the running ones
#You can have FINISHED or RUNNING , i would go with RUNNING
rm="http://127.0.0.1:8088/ws/v1/cluster/apps?states=RUNNING"
# Setting the threshold. In RM, time duration is measured in milliseconds
threshold=14400000
time_hours="0.0"
# Given 1 hour as threshold. You can change it as per requirements.
# Calling the RM api and storing the data in json. Added the decode('utf8') as python requires it for versions below 3.6
# You can check the results by entering the variable data in the python idle.
with urllib.request.urlopen(rm) as response:
    data=json.loads(response.read().decode('utf8'))
#print ("Please find the list of long running jobs.")
# The json has a dictionary key 'apps' which has applications as values (which are again nested key value pairs).
# Now we're iterating through the each app and check whether app's elapsed time is more than our threshold (1 hour).
# If it's running for more than an hour, the app details will be printed.
open('check_long_yarn_jobs.txt', "w").close()

if data['apps']==None:
        print ("0Jobs")
else:
 for running_apps in data['apps']['app']:
    if running_apps['elapsedTime']>threshold:
     with open('check_long_yarn_jobs.txt', 'a') as f:
        f.write('\n')
        f.write("******LCG Long Yarn Jobs Over 4 Hours Please Investigate****** \n")
        print ("\nstartedTime: {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(running_apps['startedTime']/1000))),file=f)
        print ("finishedTime: {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(running_apps['finishedTime']/1000))),file=f)
        print ("App Name: {}".format(running_apps['name']),file=f)
        print ("Application id: {}".format(running_apps['id']),file=f)
        print ('Running Containers: {}'.format(running_apps['runningContainers']),file=f)
        print ("CPU Vcores: {}".format(running_apps['allocatedVCores']),file=f)
        print ("Allocated MB: {}".format(running_apps['allocatedMB']),file=f)
# Elapsed time is given in milliseconds. So I divided by 1000, then 60, again 60 to convert it to hours.
        print ("Total elapsed time: {} hours".format(round(float(running_apps['elapsedTime']/1000/60/60),2)),file=f)
        time_hours=float(round(float(running_apps['elapsedTime']/1000/60/60),2))
        print ('Hive TezTracking Url Bitvise: '+'http://127.0.0.1:8188/tez-ui/#/tez-app/'+str(running_apps['id']),file=f)
        #print ("Tracking Url: ",running_apps['trackingUrl'])
        #The URL Above is the seeded one , we have the tez ui too.
        if  float(time_hours)>4:
            print('Alert WIll be Sent Email Query Has been running for Over 4 hours: '+ str(time_hours)+' -ApplicationId Please see output file check_long_yarn_jobs.txt')


