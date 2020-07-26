import pandas as pd

#read in initial large csv
#set the dtypes so the reader knows that they are strings
fire_df = pd.read_csv('../../../data/fire-department-calls-for-service.csv',
dtype={"Batallion": "object", "Station Area": "object", "Box": "object", 
"Call Type Group": "object", "Supervisor District": "object"})

#if the header split() contains a string that doesn't have "of"
#capitalize the first letter and append the rest of the word
#else just drop the word in. After the capitalization happens
#add concatenate the word and add them to the array of new headers
joined_headers = ["".join([j[0].capitalize() + j[1:] if j not in ["of"]  else j for j in i.split(" ")]) for i in list(fire_df) ]

#replace the current name of the header with the joined_headers procedurally
for name in range(len(joined_headers)):
    fire_df.rename(columns={ fire_df.columns[name]: joined_headers[name]},
    inplace=True)


#drop all the unncessary columns by column name
updated_fire_df = fire_df.drop(['ReceivedDtTm', 'EntryDtTm', 'OnSceneDtTm', 'TransportDtTm',
'HospitalDtTm', 'SupervisorDistricts', 'FirePreventionDistricts',
'CurrentPoliceDistricts', 'Neighborhoods-AnalysisBoundaries', 'ZipCodes',
'Neighborhoods(old)', 'PoliceDistricts',
'CivicCenterHarmReductionProjectBoundary', 'HSOCZones',
'CentralMarket/TenderloinBoundaryPolygon-Updated'], axis=1)

#add the Delay column which is the Response time - Dispatch time in seconds
updated_fire_df['Delay'] = (pd.to_datetime(updated_fire_df['ResponseDtTm']) -
pd.to_datetime(updated_fire_df['DispatchDtTm'])).dt.total_seconds()

#make our final data fram dropping the columns we no longer need
#after transforming them to make the Delay Column
final_df = updated_fire_df.drop(['ResponseDtTm', 'DispatchDtTm'], axis=1)

#rename the last 3 columns that don't match the schema for spark
final_df.rename(columns={'ZipcodeofIncident':'Zipcode'}, inplace=True)
final_df.rename(columns={ 'NumberofAlarms':'NumAlarms'}, inplace=True)
final_df.rename(columns={ 'Neighborhooods-AnalysisBoundaries':'Neighborhood'}, inplace=True)

#make a list to compare if our new header match our desired header
compare = ["CallNumber", "UnitID", "IncidentNumber", "CallType", "CallDate",
"WatchDate", "CallFinalDisposition", "AvailableDtTm", "Address", "City", "Zipcode",
"Battalion", "StationArea", "Box", "OriginalPriority", "Priority",
"FinalPriority", "ALSUnit", "CallTypeGroup", "NumAlarms", "UnitType",
"UnitSequenceInCallDispatch", "FirePreventionDistrict", "SupervisorDistrict",
"Neighborhood", "Location", "RowID", "Delay"]

#check the compare against the final_df header to make sure the names match
#exactly
for i in range(len(list(final_df))):
    if list(final_df)[i] != list(compare)[i]:
        print(list(final_df)[i], list(compare)[i])

#write the new dataframe as a csv
final_df.to_csv(r'../../../data/fire-department_updated.csv', index = False, header=True)

