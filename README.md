# search-engine

Assumptions prior to install
- Git is installed and accessible from the path or Git Bash (Windows)
- Python 3 is installed and accessible from the path

## Installation 

### Setting up a venv and activating it
- Reference: https://realpython.com/python-virtual-environments-a-primer/
- You can see your installed requirements from pip using the command
```bash
pip freeze
```

#### Linux instructions
Run the following commands
- Note that if you are using the fish shell to use "activate.fish" instead of "activate"
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

#### Windows instructions
Run the following commands
```
py -3 -m venv venv
venv\Scripts\activate
pip3 install -r requirements.txt
```

If you encounter the following error or something similar:
```
venv\Scripts\activate : File C:\Users\Raymo\Desktop\blah\search-engine\venv\Scripts\Activate.ps1 cannot be
loaded because running scripts is disabled on this system. For more information, see about_Execution_Policies
at https:/go.microsoft.com/fwlink/?LinkID=135170.
At line:1 char:1
+ venv\Scripts\activate
+ ~~~~~~~~~~~~~~~~~~~~~
    + CategoryInfo          : SecurityError: (:) [], PSSecurityException
    + FullyQualifiedErrorId : UnauthorizedAccess
```
1. Open PowerShell in Administrator mode
2. Input this in the prompt
```
set-executionpolicy remotesigned
```
3. Say yes

Reference: https://superuser.com/questions/106360/how-to-enable-execution-of-powershell-scripts

### Deactivating a venv
Run the following command
```
deactivate
```

