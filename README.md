
## nrtsompy

The `{nrtsompy}` (near real time storm overflow monitoring python) package is designed to 
help interact with the data provided via the  [National Storm Overflow Hub](https://www.streamwaterdata.co.uk/pages/storm-overflows-data)
(NSOH herein).  This package is pretty much a copy of the R version which you can find [here](https://github.com/a-jone5/nRtsom)

## Installation

To install the latest release of `{nrtsompy}` use the following code:

```
pip install git+https://github.com/a-jone5/nrtsompy.git
```

## Functions

There are currently three functions in the `{nrtsompy}` package:

- the `urls` function that simply makes urls available

- the `single_company` function will let you access data from one company
    - Provides access to and reformats:
      - all time stamps from unix to ISO8601
      - status from numeric (-1,0,1) to character (offline,end,start)

- the `all_company` function will let you access data from every company,
and uses the function above to format. Aligns all columns for consistency.


## Workflows
```
import nrtsompy as nrt
```

Find the url you are interested in, and use it with the `single_company`

``` 
## show all
print(nrt.urls())

## assign the data for the url you are interested in 
df = nrt.single_company(nrt.urls()["thames"])
```
alternatively call all the data in one go

``` 
df = nrt.all_company()
```

## Things on the horizon

- Integration with other data sets - although examples of this may sit better in another repo
- Some summary functions

## Data Sources

The data is made available through the [NSOH](https://www.streamwaterdata.co.uk/pages/storm-overflows-data) via [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
Companies that have published or made a data set available on the NSOH are each responsible for that data set. See more [here](https://www.streamwaterdata.co.uk/pages/the-national-storm-overflow-hub#:~:text=Parties%20that%20have,by%20another%20party)
