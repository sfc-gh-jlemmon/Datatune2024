
<h1> INTRODUCTION - Snowpark TechUp Snowday 2024 HOL </h1>

**HANDS-ON LAB GUIDE** 

**Works for Snowflake Enterprise and higher edition**

**Cloud Provider: Any**

**Approximate duration: 60-90  minutes**

****

****

# LAB OVERVIEW

This lab will introduce and explain the most common elements in core Snowpark data use and transformation. The focus will be the Dataframe API but will also contain an introduction to simple Stored Procedures and UDFs. The aim is to build your understanding and confidence with Snowpark for Python, so that you can engage with demos and quickstarts involving Snowpark. It is aimed at those with limited previous Python and Snowpark experience.

The lab uses standard data from the Tasty Bytes setup, but is not designed as an end-to-end scenario. Instead the data, primarily from the RAW\_POS schema, is used in simple examples and exercises.

### What you'll learn

The exercises in this lab will walk you through the steps to: 

- Read data into dataframes and perform basic manipulations such as filtering, aggregation, and joins. 
- Load data using Snowpark and save data at new tables or updating existing tables.
- Create and use UDFs and system functions and stored procedures.

### Prerequisites

You will get more out of this lab if you have completed the DataCamp courses Introduction to Python and Intermediate Python.  They will introduce you to the basics of Python syntax and the concept of a dataframe, including Pandas, which are not covered in this lab.  

If you have not taken them, this lab is still a useful example of using Snowpark Python for accessing/manipulating data.

> **Module 0: Setup**. This should be done prior to the HOL session, to reduce the loss of time on the day due to basic installation or installation issues.

# MODULE 0: SETUP

## 0.1 Snowflake Setup

You are likely to have an existing Tasty Bytes setup in a Snowflake account.

> **Note**: This HoL leaves a lot of additional tables in the RAW_POS schema. It is probably best to rerun the Tasty Bytes setup again afterwards to get a clean Tasty Bytes.

Ensure you have a userid and password with a grant to role `TASTY_DATA_ENGINEER`. You will need these credentials below. During the lab you may find it helpful to be logged into Snowsight with this userid, particularly for access to your Query History.

Since best practices are required to have MFA enabled for accounts with the `ACCOUNTADMIN` role, which can cause a lot of authentication interruptions, it is recommended you use another userid with `TASTY_DATA_ENGINEER` but not `ACCOUNTADMIN`, or create one e.g.

```sql
CREATE USER SNOWPARK_USER
COMMENT = 'Snowpark User'
PASSWORD = 'yoursecretpassword'
MUST_CHANGE_PASSWORD = True
LOGIN_NAME = 'SNOWPARK_USER'
FIRST_NAME = 'Snowpark'
LAST_NAME = 'User'
DISPLAY_NAME = 'Snowpark User'
EMAIL = '<your email>@domain.com'
DEFAULT_WAREHOUSE = 'TASTY_DE_WH'
DEFAULT_NAMESPACE = ''
DEFAULT_ROLE = 'TASTY_DATA_ENGINEER';

GRANT ROLE TASTY_DATA_ENGINEER TO USER SNOWPARK_USER;
```


## 0.2 Conda and Jupyter Setup

This document provides a suggested approach to setting up a Jupyter environment for use with this lab, particularly for those less familiar with Python. If you are more familiar with Python or have your own preferred IDE and experience of using it, feel free to do so.

Anaconda is the preferred distribution channel for Snowpark Python, with a Snowflake-specific channel designed to ensure that libraries at client and server ends can match. It also provides relatively simple to use capabilities for Python environment creation. Many Snowpark examples and Hands on Labs are shipped with a configuration file which can be used by conda to create a complete Python environment with dependencies.

You can download Anaconda from <https://www.anaconda.com/download>  - note that there are separate downloads for Mac Intel and Mac M1/M2.Click download and install anaconda, taking the defaults. This should install a base environment.  

Once installed you can check the conda environments that will be configured.   From the default installation you should have a single environment called base.  This can be verified as follows: 

Open up the Terminal application.

At the prompt type:
```
conda info --envs**
```
You should see something like this:
```
(base) yourname \~ % conda info --envs
# conda environments:
#
base                  *  /Users/yourname/opt/anaconda3
```
Or as you create more environments:
```
(base) yourname \~ % conda info --envs
# conda environments:
#
base                  *  /Users/yourname/opt/anaconda3
py38\_env\_tb1             /Users/yourname/opt/anaconda3/envs/py38\_env\_tb1
snowparkbasics             /Users/yourname/opt/anaconda3/envs/snowparkbasics
```

To return to the base environment, the recommendation is to use **conda activate** with no name rather than using the deactivate command.

To remove (for example if you want to recreate an environment without override):
```
conda remove --name snowparkbasics --all
```
Or
```
conda remove -n snowparkbasics --all
```
Creating a generic Snowpark environment may be useful, but it is likely that for specific demos, quickstarts or labs you will be offered a config file including all the required libraries for that project, together with a set of Python notebooks, likely held in github.  That is the case for this lab.

So the recommended approach is to do something like create a \~/snowpark directory, a project-specific directory for each project underneath, and a separate conda-created Python environment for each - this may require editing the config file on offer to ensure the name is unique, as pysnowpark seems a popular default for labs!

## 0.3 Snowpark Basics Setup

Navigate to the snowparkbasics folder

You will find a config file `snowparkbasics_env.yml` to create a Python environment for this lab.

Change the **name:** if required

Create a conda env for this quickstart, and then activate it

```sh
conda env create -f snowparkbasics_env.yml
```

> **Note:**  If you receive an error message similar to this, then you need to update your Xcode before proceeding. The error occurs because you have not accepted the Xcode license agreement.

```sh
clang -Wno-unused-result -Wsign-compare -Wunreachable-code -DNDEBUG -fwrapv -O2 -Wall -fPIC -O2 -isystem /Users/jfrink/anaconda3/envs/snowparkbasics/include -fPIC -O2 -isystem /Users/jfrink/anaconda3/envs/snowparkbasics/include -Isrc/snowflake/connector -Isrc/snowflake/connector/cpp/ArrowIterator -Isrc/snowflake/connector/cpp/Logging -I/Users/jfrink/anaconda3/envs/snowparkbasics/include/python3.10 -c src/snowflake/connector/arrow\_iterator.cpp -o build/temp.macosx-10.9-x86\_64-cpython-310/src/snowflake/connector/arrow\_iterator.o -isystem/private/var/folders/73/d9tlwnpd65ld8jmyh04rnnf80000gn/T/pip-build-env-\_k4k1cnf/overlay/lib/python3.10/site-packages/pyarrow/include -isystem/private/var/folders/73/d9tlwnpd65ld8jmyh04rnnf80000gn/T/pip-build-env-\_k4k1cnf/overlay/lib/python3.10/site-packages/numpy/core/include -std=c++17 -D\_GLIBCXX\_USE\_CXX11\_ABI=0 -mmacosx-version-min=10.13
      You have not agreed to the Xcode license agreements. Please run 'sudo xcodebuild -license' from within a Terminal window to review and agree to the Xcode and Apple SDKs license.
      error: command '/usr/bin/clang' failed with exit code 69
      [end of output]
note: This error originates from a subprocess, and is likely not a problem with pip.
  ERROR: Failed building wheel for snowflake-connector-python
ERROR: Could not build wheels for snowflake-connector-python, which is required to install pyproject.toml-based projects
failed
CondaEnvException: Pip failed
```

**If you received an error like the one above, on your Mac press Command key + space bar, type Xcode (or go to applications and open up Xcode). Follow the instructions, Accept the license agreement.  Once that is complete, rerun the create command.**

Activate the environment (in terminal):

```sh
conda activate snowparkbasics
```

Conda will automatically install snowflake-snowpark-python and all other dependencies for you. (Note this command will fail if an environment with this name already exists - you can override but it’s a useful check.)

Navigate to the lab folder and edit the creds.json file as follows. Note that holding credentials in a file like this is not great practice, but the initial setup of other authentication approaches can be a little complex.

File:   `creds.json`

```json
account: either orgname-accountname or accountlocator.region
user: as above
password: as above
```

Now, launch Jupyter Notebook from this directory (from within the activated environment)

```bash
jupyter notebook
```

You should see a screen similar to this:

![](https://github.com/snowflakecorp/techup-fy24snowday-snowpark/blob/main/images/jupyter_image1.png)

Double click the lab folder if the image looks like the one above.

Double click the first lab, to open and run  the start of the notebook - this is usually a good way to determine whether your environment is working OK. 

![](https://github.com/snowflakecorp/techup-fy24snowday-snowpark/blob/main/images/jupyter_image2.png)

Each notebook starts with cells to:

- Run imports for the Python libraries required.
- Read the connection parameters from the creds.json file.
- Create a session using the connection parameters loaded from the  creds.json file.

If all goes well the results from the last cell should be:

```sh
    Current Database and schema: "FROSTBYTE_TASTY_BYTES"."RAW_POS"
    Current Warehouse: "TASTY_DE_WH"
```

# PART 1: DATAFRAME BASICS

## 1.1 Setup

There is a Setup Section in each notebook. This imports a set of libraries and sets and tests the connection to your Snowflake account.  In Part 1, it also includes simple examples of how to modify your session from Snowpark.

## 1.2 Loading a Dataframe

This section compares loading CSV files  directly into Pandas with loading table data into Snowpark dataframes.  We then look at how to display the contents of a Snowpark dataframe, and obtain useful dataframe data.


## 1.3  Managing Columns

This section looks at adding, aliasing, and removing columns, and casting data types.

## 1.4  Simple Data Manipulation

This section looks at filtering, sorting and aggregating dataframes.  We also compare the generated SQL with how we might code the SQL directly.

## 1.5 Persist Transformations

Here we show how our transformed dataset can easily be saved as a new table.

## 1.X Your Turn

Generate a list of months for which we have data, the total order amount for each month (assume amounts are all held in the same currency), and the number of distinct locations visted in each month.

Hints: Functions you may find useful include `count_distinct` (aka `countDistinct`), `date_part`, `to_char` with numeric formatting '09' or 'FM09' and concat.

[count\_distinct](https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/api/snowflake.snowpark.functions.count_distinct) 

[date\_part](https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/api/snowflake.snowpark.functions.date_part)

[to\_char](https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/api/snowflake.snowpark.functions.to_char)

[concat](https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/api/snowflake.snowpark.functions.concat)  

# PART 2: JOINS AND VIEWS

## 2.1 Setup

Standard Setup section

## 2.2 Joins

This section looks at the Snowpark join syntax, including simple joins, multi-table joins, options for handling repeated column names arising from joins, and left joins.

## 2.3  Tables and Views

Our dataframes can be saved as actual tables, but also as view definitions.

## 2.X Your Turn

Building on what has been learnt in Parts 1 and 2, we have a new challenge:

You have been asked to analyze the numbers of different 'Beverage' items sold by location country for February 2022.  You are to present the answers in two ways:

by country, listing the most to least popular beverages

by beverage, listing the top to bottom countries

Then, save the data as a new table BEVERAGE202202

# PART 3: WRITING TO TABLES

## 3.1 Setup

Standard Setup section, plus creation of a TRUCK\_STAGE.

## 3.2 Loading Tables

This section looks at loading data from files into tables, using Snowpark syntax. We see how CSV loads have typically required detailed structure definitions, and how the new infer capabilities can help.

## 3.3 Writing To Tables

In this section we see how we can create a copy of a table, and insert data into an exciting table.

## 3.4 Updating, Deleting and Merging

Here we look at updating and deleting tables, directly or vis a join, and a simple merge example.

## 3.X Your Turn

Using the infer capability, create a new ONETRUCKHEADER table by loading from the header.csv file.  Then update the new table to set SHIFT\_ID to 99.

Create another table `TWOTRUCKHEADER` by copying data from `ORDER_HEADER` with `TRUCK_ID` `122` or `123`.  Update `TWOTRUCKHEADER` setting the `SHIFT_ID` to the one from `ONETRUCKHEADER` for the same `ORDER_ID`.

Finally check the update worked e.g. count `TWOTRUCKHEADER` rows grouped by `TRUCK_ID` and `SHIFT_ID`.

# PART 4: STORED PROCEDURES AND FUNCTIONS

## 4.1 Setup

Standard Setup section plus creation of stages `PROCSTAGE` and `UDFSTAGE`.

## 4.2 Using Stored Procedures

This section describes the various reasons for using stored procedures in Snowpark, and then demonstrates a practical example of turning a solution to Part 2 into a parameterised procedure.

## 4.3 Using Functions

This section explores using built-in functions or existing SQL UDFs from Snowpark.

## 4.4 Creating Functions

In this section we build a Python UDF from a SQL environment, then from Snowpark, then using the `@udf` secorator.  Finally we make our UDF vectorized.

## 4.X Your Turn

You realize that the line in the solution to part 1

```python
F.concat(
    F.to_char(F.date_part("year",'ORDER_TS')),
    F.to_char(F.date_part("month",'ORDER_TS'), 'FM09')
)
```

could also be written generically in Python as:

```python
return str(ts.year) + str(ts.month) where ts is the datetime type.
```

Create and register a Python UDF `char_month` to implement this and reproduce the answer to Part 1 using this. Start by separately defining a function and registering it. Then move on to decorators and vectorized UDFs if you wish...

> **Hint**: you will need to `import datetime from datetime`


