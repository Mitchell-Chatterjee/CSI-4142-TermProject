# CSI4142 Project

## Data Staging scripts 

### Instructions to run the script
- The main script file is stage.py
- The script expects your Username and Password for the database to be stored in environment variables DBUSERNAME and DBPASSWORD respectively 

#### Unix instructions
Run the following commands to set your environment variables
```
export DBUSERNAME=yourUsernamehere
export DBPASSWORD=yourPasswordhere
```

#### Windows instructions
Run the one of following sections to set your environment variables

##### Powershell Instructions
```
$env:DBUSERNAME = "yourUsernamehere"
$env:DBPASSWORD = "yourPasswordhere"
```

##### CMD Instructions
```
set DBUSERNAME=yourUsernamehere
set DBPASSWORD=yourPasswordhere
```

#### Running the script
- Your current working directory must be: dataStaging
- Change to the directory dataStaging with:
```
cd dataStaging
```
- An example is shown below of what the contents of your current working directory should look like
```
PS C:\Users\rchan086\Desktop\CSI-4142-Project\dataStaging> ls


    Directory: C:\Users\rchan086\Desktop\CSI-4142-Project\dataStaging


Mode                LastWriteTime         Length Name
----                -------------         ------ ----
-a----       2020-02-21  11:36 AM           1982 connect.py
-a----       2020-02-21  11:36 AM           2207 filter_data_by_range.py
-a----       2020-02-21  11:36 AM           9047 location.py
-a----       2020-02-21  11:36 AM           7423 pop_crime.py
-a----       2020-02-21  11:36 AM           6370 stage.py
-a----       2020-02-21  11:36 AM           6304 stageDate.py
-a----       2020-02-21  11:36 AM           2015 stageEvent.py
-a----       2020-02-21  11:36 AM           4802 stageFact.py
```
- Run the Script with:
```
py -3 stage.py
```

