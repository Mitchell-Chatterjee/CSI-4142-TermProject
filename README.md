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
[I] /home/rchang/dev/CSI-4142-Project/dataStaging | readme
> ls -l
total 56
-rw-r--r-- 1 rchang rchang 1926 Feb 20 23:32 connect.py
-rw-r--r-- 1 rchang rchang 2139 Feb 20 23:32 filter_data_by_range.py
-rw-r--r-- 1 rchang rchang 8802 Feb 20 23:32 location.py
-rw-r--r-- 1 rchang rchang 7172 Feb 20 23:32 pop_crime.py
-rw-r--r-- 1 rchang rchang 6181 Feb 20 23:32 stageDate.py
-rw-r--r-- 1 rchang rchang 1977 Feb 20 23:32 stageEvent.py
-rw-r--r-- 1 rchang rchang 4568 Feb 20 23:32 stage.py
```
- Run the Script with:
```
py -3 stage.py
```

