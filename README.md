# etl_template
Template for Data Team ETL repos.

# OVERVIEW PLEASE READ:
1. Create repo from template
2. Checkout a DEV branch
3. Rename files and placeholder { } variables
4. Start developing and installing the packages you will require, and then add them in the requirements.in
5. Once everything is done, test locally and in a local docker image
6. install invoke, pip-tools, lint
7. Invoke req-compile, invoke pylint
8. Raise PR

## Getting Started

* clone repo by clicking on "use this template" from github.
* clone new repo locally.
* checkout develop branch.
* rename all `__name__` placeholder files and folders to something of your choosing.
* Make sure you create a new virtual environment whenever you start a new project
* Make sure you set the src as a sources root directory by:
    a. If you are using Pycharm then right-click on the src file and then click on Mark Directory as. From there choose 
       Sources Root. 
    b. If you are using Spyder then change your PYTHONPATH to be the src absolute path.

## Helper Functions Setup

* To use the Helper functions make sure that you have set up a Github ssh key on your local machine or that you have a
  github token.
* If you have a github token then you need to run this command:
     ```shell script
    pip install git+https://{your_token_here}@github.com/energyaspects/helper_functions.git@latest
     ```
    on your terminal in pycharm or in an anaconda prompt while
    your environment for this project is active
* If you have an ssh key set up with your Github account then you need to run this command:
     ```shell
     pip install git+ssh://git@github.com/energyaspects/helper_functions.git@latest
     ```
     on your terminal in pycharm or in an anaconda prompt while
     your environment for this project is active
Useful links:
* Creating your personal github token: [link](https://help.github.com/en/github/authenticating-to-github/creating-a-personal-access-token-for-the-command-line)
* Creating your own ssh key and linking to github account: [link](https://help.github.com/en/enterprise/2.17/user/github/authenticating-to-github/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent)

## Correctly generating your requirements.txt

In order to correctly setup your requirements.txt file, you should input all your core packages in the folder 
`requirements/requirements.in`. The `requirements.in` file should have all the packages listed with no dependencies/versions,
unless you want to persist a certain package version which will persist that version in your `requirements.txt`.
Remember, you should **NOT** include the helper_functions_ea in your `requirements.in` as it is not PyPI installable, and it is 
separately installed either in your docker image or the ea-data image. Once you have generated the `requirements.in` file
you then need to run the following commands on your console:

```shell
pip install invoke pip-tools
inv req-compile
```
The second command uses the invoke task req_compile, located in the `tasks.py`, which constructs the requirements.txt 
file. 
> **NOTE**:
> 
> Remember, that if you have updated a package, and you want to reflect that in the `requirements.txt` file 
> you should run `req-upgrade` instead of `req-compile`. `req-compile` will upgrade all packages that can be upgraded 
> from the `requirements.in` file. If you only want to change specific packages you should persist the version of the 
> other packages in your `requirements.in` and then run req-upgrade. If you want to force specific versions for all 
> packages, just set the versions on the `requirements.txt` and run `inv req-compile` normally.


## Checks to perform before you transfer to airflow
* Run the linter to check that you have structured your code following PEP standards. 

* Try and running the setup.py file within your terminal and see if it installs the package correctly. You can do this by initiating
an anaconda prompt and creating a new environment and then navigating to the location of
your setup.py file and running this command:
    ```shell script
    python setup.py install
     ```
  
* Once you installed your package you need to check if your package runs properly by running the name of the console script 
you created in the setup.py within your prompt. (i.e for this example where in setup.py we create the console script with the name that we give 
to the variable command_name_. Thus in our prompt we will just input that name and run it. This should execute the function we automated.)

* Configure a local docker image and run the ETL console script there, and see if everything runs successfully. You can
follow the instructions [here](https://energyaspects.atlassian.net/l/cp/z20bo61A)

* Create a PR and only merge when you have 2 approvals.

* If you managed to perform all the above with no issues then you can create a DAG in a branch in ea-data project and 
automate your code using Airflow. Please make sure that you also ask someone to review your DAG addition.

> **NOTE**: If you are planning to create your own Docker Image, which will be used in your Airflow DAG, then please
> remember to configure your Cloud Trigger [here](https://console.cloud.google.com/cloud-build/triggers?project=ea-data-prod)
> and also ensure that your trigger successfully executes before you proceed to adding the Airflow DAG


## Related Links

[EA Data Repo](https://github.com/energyaspects/ea-data)

[Airflow Environment](https://s1f65d80672570f57p-tp.appspot.com/admin/)

[GCP Build Dashboard](https://console.cloud.google.com/cloud-build/builds?project=ea-data-prod)
