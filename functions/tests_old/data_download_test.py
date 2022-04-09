import geopandas as gpd
import pandas as pd
# import fiona
import pytest
import urllib
from unittest import mock
import inspect

from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from shapely.geometry.point import Point
from shapely.geometry.collection import GeometryCollection
from shapely import wkt

# from .data_download import download_and_unzip, download_unzip_link_manual

from .data_download import DownloadFunctions, DownloadSetUp, DataDownload, DataGroupDownloadSetup


'''
#########################################################
fixtures
#########################################################
''' 

@pytest.fixture
def tmp_dir(tmpdir):
    f1 = tmpdir / "mydir"
    f1.mkdir()
    return f1

@pytest.fixture
def zip_path(tmp_dir):
    return tmp_dir / "spatial_download.zip"

@pytest.fixture
def download_config():
    return {
        "occurrence": {
            "name":"nt_petroleum",
            "data_source":"ogr",
            "import_style":"link",
            "link":"http://geoscience.nt.gov.au/contents/prod/Downloads/Drilling/PETROLEUM_WELLS_shp.zip",
            "extension":"nt_petroleum/",
            "created_extension":"nt_petroleum/",
            "groups":
                [
                    {
                        "type":"none",
                        "output":"NT_2",
                        "crs":"EPSG:4283",
                        "files":["PETROLEUM_WELLS"]
                    }
                ]
        },
        "tenement": {
            "name":"nsw_application",
            "data_source":"ogr",
            "import_style":"link",
            "link":"https://gs.geoscience.nsw.gov.au/geoserver/gsnsw/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=gsnsw:bl_titleappl&outputFormat=shape-zip",
            "extension":"",
            "created_extension":"",
            "groups":
                [
                    {
                        "type":"none",
                        "output":"NSW_2",
                        "crs": "EPSG:4326",
                        "files":["bl_titleappl"]
                    }
                ]
        }
    }



'''
#########################################################
tests
#########################################################
'''

''' test for when the correct link is provided '''
def test_download_and_unzip_correct_link(zip_path, tmp_dir):
    ogr_link = "http://geoscience.nt.gov.au/contents/prod/Downloads/Drilling/PETROLEUM_WELLS_shp.zip"
    sc = DownloadFunctions('occurrence')
    sc.download_and_unzip(ogr_link,zip_path,tmp_dir)
    assert len(tmp_dir.listdir()) == 5


''' test for errors when an incorrect link is provided '''
@pytest.mark.parametrize(
    "link,error", 
    [
        ("", ValueError), 
        ("http://geoscience.nt.gov.au/contents/prod/Downloads/Drilling/PETROLEUM_WELLS_shpppp.zip", urllib.error.HTTPError), 
        ("http://this_does_not_exist/fake.zip", urllib.error.URLError)
    ]
)
def test_download_and_unzip_wrong_link(zip_path, tmp_dir, link, error):
    # URLError: errors in URLs, HTTPError: authentication request errors, ValueError: no link provided
    sc = DownloadFunctions('occurrence')
    with pytest.raises(error):
        sc.download_and_unzip(link,zip_path,tmp_dir)
    assert len(tmp_dir.listdir()) == 0


''' test types are correct '''
@pytest.mark.parametrize(
    "variable,vtype", 
    [
        ("format_configs", dict), 
        ("temp_links", dict), 
        ("download_schedule_path", str),
        ("dl_schedule_config", dict)
    ]
)
def test_DownloadSetUp(variable, vtype):
    sc = DownloadSetUp()
    assert type(getattr(sc,variable)) == vtype


@pytest.mark.parametrize(
    "value,error", 
    [
        ("tenement", None), 
        ("", KeyError), 
        ("blahblah", KeyError)
    ]
)
def test_DataGroupDownloadSetup(value,error):
    if error:
        with pytest.raises(error):
            DataGroupDownloadSetup(value)
    else:
        DataGroupDownloadSetup(value)



''' test lengths are not zero '''
@pytest.mark.parametrize(
    "variable,length", 
    [
        ("format_configs", 0), 
        ("temp_links", 0), 
        ("download_schedule_path", 0),
        ("dl_schedule_config", 0)
    ]
)
def test_DownloadSetUp_length(variable, length):
    sc = DownloadSetUp()
    assert len(getattr(sc,variable)) > length




''' check no errors arise in full method '''
def test_download_for_datagroup():
    DataDownload().download_for_datagroup()




''' test exception when incorrect argument is presented '''
@pytest.mark.parametrize(
    "value,error", 
    [
        ("", KeyError), 
        (None, KeyError)
    ]
)
def test_get_groups_that_require_downloading(value,error):
    sc = DownloadFunctions('tenement')
    with pytest.raises(error):
        sc.get_groups_that_require_downloading(value)






''' return a controlled tenement geopandas dataframe '''
def polygon_data_mock():
    data = [
            ['24199','MULTIPOLYGON (((149.25930466 -33.76687852,149.2716321 -33.76845544,149.26733531 -33.78607164,149.26192448 -33.79835232,149.26054699 -33.80551866,149.24763383 -33.81271551,149.2421643 -33.81132737,149.23944887 -33.80702173,149.24049816 -33.7987976,149.2438712 -33.78568929,149.25930466 -33.76687852)))'],
            ['27274','POLYGON ((151.48330887 -33.20000304,151.48323458 -33.20560888,151.47527862 -33.20454595,151.47055221 -33.20537177,151.46981045 -33.20914234,151.47928171 -33.21044277,151.47984895 -33.21036678,151.48713463 -33.20902124,151.46876866 -33.22749048,151.45003232 -33.22472017,151.45008219 -33.21308165,151.46357303 -33.20001226,151.48330887 -33.20000304))']
        ]
    df = pd.DataFrame(data, columns=['id','geometry'])
    df['geometry'] = df['geometry'].astype(str)
    df['geometry'] = df['geometry'].apply(wkt.loads)
    return gpd.GeoDataFrame(df,geometry="geometry",crs="EPSG:4202")

''' return a controlled occurrence geopandas dataframe '''
def point_data_mock():
    data = [
            ['24199','POINT Z (145.28243437182 -41.8918611701944 0)'],
            ['27274','POINT (143.28243437182 -40.8918611701944)']
        ]
    df = pd.DataFrame(data, columns=['id','geometry'])
    df['geometry'] = df['geometry'].astype(str)
    df['geometry'] = df['geometry'].apply(wkt.loads)
    return gpd.GeoDataFrame(df,geometry="geometry",crs="EPSG:4202")


''' test the correct columns are returned, correct length and all POINT Z values have been converted '''
@mock.patch('functions.data_download.DownloadFunctions.convert_data_to_df', return_value=point_data_mock(), autospec=False)
def test_merge_and_export_to_csv__point_data(mock_gdf, download_config):
    data_import_group = download_config['occurrence']
    group = data_import_group['groups'][0]
    sc = DownloadFunctions('occurrence')
    df = sc.merge_and_export_to_csv(data_import_group,group)

    assert df.columns.tolist() == ['id','geometry']
    assert len(df) == 2
    assert len(df[df['geometry'].str.contains("POINT Z")]) == 0


''' test the correct columns are returned, correct length and all POLYGON values have been converted to MULTIPOLYGON '''
@mock.patch('functions.data_download.DownloadFunctions.convert_data_to_df', return_value=polygon_data_mock(), autospec=False)
def test_merge_and_export_to_csv__polygon_data(mock_gdf, download_config):
    data_import_group = download_config['tenement']
    group = data_import_group['groups'][0]
    sc = DownloadFunctions('tenement')
    df = sc.merge_and_export_to_csv(data_import_group,group)

    assert df.columns.tolist() == ['id','geometry']
    assert len(df) == 2
    assert len(df[~df['geometry'].str.contains("MULTIPOLYGON")]) == 0






