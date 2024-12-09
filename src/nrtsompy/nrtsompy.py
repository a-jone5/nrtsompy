import requests
import polars as pl

def single_company(url):

    ## from here 

    url = url

    ## see here https://developers.arcgis.com/rest/services-reference/enterprise/query-feature-service-layer/#features-added-throughout-releases
    ## search offset
    ## this took a bit of working out - ang worked on 1000 record count (everyone else 2000)
    ## so gather the data in the responses for each page, join them togther, then apply the 
    ## logic to get everything into a df.

    params = {
        'where': '1=1',
        'outFields': '*',
        'f': 'geojson',
        'resultOffset': '0',
        'resultRecordCount' : '1000'
    }

    def fetch_data(url, params):
        
        all_records = []

        while True:
            response = requests.get(url, params=params)
            data = response.json()
            all_records.extend(data['features'])

            if len(data['features']) < int(params['resultRecordCount']):

                break

            params['resultOffset'] = str(int(params['resultOffset']) + int(params['resultRecordCount']))
        
        return all_records

    records = fetch_data(url = url, params = params)

    ## should probably revist some of this - dictionary unpack 
    ## to get at the geoms
    df = pl.DataFrame([{
        **feature['properties'],
        'geometry': feature['geometry']} for feature in records])
    
    ## works to here 

    ## rename all columns to lowercase
    df.columns = [col.lower() for col in df.columns]

    ## order columns so everything joins down the line with all_company 
    df = df.select([
        "id", "company", "status", "statusstart", "latesteventstart", "latesteventend",
        "latitude", "longitude", "receivingwatercourse", "lastupdated", "objectid","geometry"
    ])

    ## what columns are timeseries - work this up so not using named columns 
    ts = ['statusstart', 'latesteventstart', 'latesteventend', 'lastupdated']
    

    ## reformat - return_dtype solves the problem is all rows are null
    df = df.with_columns(
        pl.when(pl.col('status') == 1).then(pl.lit('start'))
        .when(pl.col('status') == 0).then(pl.lit('end'))
        .when(pl.col('status') == -1).then(pl.lit('offline'))
        .otherwise(pl.lit('NA'))
        .alias('status'),
        *[pl.from_epoch(pl.col(col).map_elements(lambda x: int(str(x)[:10]), return_dtype=pl.Int64), time_unit="s") for col in ts]
        )


    return(df)


def all_company():

    ang = "https://services3.arcgis.com/VCOY1atHWVcDlvlJ/arcgis/rest/services/stream_service_outfall_locations_view/FeatureServer/0/query"
    yks = "https://services-eu1.arcgis.com/1WqkK5cDKUbF0CkH/arcgis/rest/services/Yorkshire_Water_Storm_Overflow_Activity/FeatureServer/0/query"
    nwl = "https://services-eu1.arcgis.com/MSNNjkZ51iVh8yBj/arcgis/rest/services/Northumbrian_Water_Storm_Overflow_Activity_2_view/FeatureServer/0/query"
    svt = "https://services1.arcgis.com/NO7lTIlnxRMMG9Gw/arcgis/rest/services/Severn_Trent_Water_Storm_Overflow_Activity/FeatureServer/0/query"
    sou = "https://services-eu1.arcgis.com/XxS6FebPX29TRGDJ/arcgis/rest/services/Southern_Water_Storm_Overflow_Activity/FeatureServer/0/query"
    sww = "https://services-eu1.arcgis.com/OMdMOtfhATJPcHe3/arcgis/rest/services/NEH_outlets_PROD/FeatureServer/0/query"
    tha = "https://services2.arcgis.com/g6o32ZDQ33GpCIu3/arcgis/rest/services/Thames_Water_Storm_Overflow_Activity_(Production)_view/FeatureServer/0/query"
    uu = "https://services5.arcgis.com/5eoLvR0f8HKb7HWP/arcgis/rest/services/United_Utilities_Storm_Overflow_Activity/FeatureServer/0/query"
    wes = "https://services.arcgis.com/3SZ6e0uCvPROr4mS/arcgis/rest/services/Wessex_Water_Storm_Overflow_Activity/FeatureServer/0/query"
    
    urls_list = [ang,yks,nwl,svt,sou,sww,tha,uu,wes]
    
    dataframes_list = []
    
    ## work through the list of urls with the single_company function
    for url in urls_list:
        df = single_company(url)
        dataframes_list.append(df)
    
    ## return df from the list, everything will join given above alignment
    dat = pl.DataFrame(pl.concat(dataframes_list))
    
    return dat

def urls():

    urls = {
        "anglian": "https://services3.arcgis.com/VCOY1atHWVcDlvlJ/arcgis/rest/services/stream_service_outfall_locations_view/FeatureServer/0/query",
        "yorkshire": "https://services-eu1.arcgis.com/1WqkK5cDKUbF0CkH/arcgis/rest/services/Yorkshire_Water_Storm_Overflow_Activity/FeatureServer/0/query",
        "northumbrian": "https://services-eu1.arcgis.com/MSNNjkZ51iVh8yBj/arcgis/rest/services/Northumbrian_Water_Storm_Overflow_Activity_2_view/FeatureServer/0/query",
        "severntrent": "https://services1.arcgis.com/NO7lTIlnxRMMG9Gw/arcgis/rest/services/Severn_Trent_Water_Storm_Overflow_Activity/FeatureServer/0/query",
        "southern": "https://services-eu1.arcgis.com/XxS6FebPX29TRGDJ/arcgis/rest/services/Southern_Water_Storm_Overflow_Activity/FeatureServer/0/query",
        "southwest": "https://services-eu1.arcgis.com/OMdMOtfhATJPcHe3/arcgis/rest/services/NEH_outlets_PROD/FeatureServer/0/query",
        "thames": "https://services2.arcgis.com/g6o32ZDQ33GpCIu3/arcgis/rest/services/Thames_Water_Storm_Overflow_Activity_(Production)_view/FeatureServer/0/query",
        "unitedutilities": "https://services5.arcgis.com/5eoLvR0f8HKb7HWP/arcgis/rest/services/United_Utilities_Storm_Overflow_Activity/FeatureServer/0/query",
        "wessex": "https://services.arcgis.com/3SZ6e0uCvPROr4mS/arcgis/rest/services/Wessex_Water_Storm_Overflow_Activity/FeatureServer/0/query"
    }

    return urls