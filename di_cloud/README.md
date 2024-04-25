# DI CLOUD E2E Test

## Installation

Before using the framework you need to decide how to install it. Just as for the core framework there are also three options for the validation framework:

1. Install as Python package via pip (This requires to have Python 3.10 or greater installed. Most importantly it also requires the installation of the [Netweaver RFC SDK](https://sap.github.io/PyRFC/install.html) and the SAP Crypto Lib if you want to access ABAP systems via RFC or Websocket RFC.)

2. Clone the git repository 
   Make sure that you have Python 3.10 or greater installed. Also requires the installation of the dependencies via poetry and most importantly it also requires the installation of the Netweaver RFC SDK and the SAP Crypto Lib if you want to access ABAP systems via RFC or Websocket RFC.

- Please make sure that you have Poetry installed. If that is not yet the case, install it via pip.
  ```
    python -m pip install poetry
  ```

- In the root of folder di_cloud, there is a pyproject.toml file and this where you need to configure poetry.
  >[Using Poetry for managing dependencies](https://github.wdf.sap.corp/pages/bdh/di-qa-automated-e2e-testing/core/latest/using_poetry.html)

- Configure VS Code
  If you are using Visual Studio Code as development environment please also read this information on [how to choose the correct virtual environment](https://code.visualstudio.com/docs/python/environments#_select-and-activate-an-environment). 

- setup debugger environment 
  Add these configures into .vscode/launch.json
  ```
    "request": "test",
    "justMyCode": false,
  ```
  Here is a sample of launch.json as reference.
   ```
   {
    // Use IntelliSense to learn about possible attributes.
    // Hover to view descriptions of existing attributes.
    // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
    "version": "0.2.0",
    "configurations": [
            {
                "name": "Python: Current File",
                "type": "python",
                "request": "test",
                "program": "${file}",
                "console": "integratedTerminal",
                "env": {"PYTHONPATH": "${workspaceFolder}"},
                "justMyCode": false,
                "stopOnEntry": true
            }
        ]
    }
   ```