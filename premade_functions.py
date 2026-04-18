import geopandas as gpd
import glob
import os
import pandas as pd
import rasterio
import typing
import numpy as np
from sqlalchemy import create_engine, text
import folium
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from PIL import Image
from folium import raster_layers

def retrieve_shp(base_path: str) -> gpd.GeoDataFrame:

    """
    Search through a base directory path for all files and retrieve the shapefile file, downloading it into a geopandas GeoDataFrame

    :param base_path: directory location where shapefile files are stored
    :type base_path: str

    :return gpd.GeoDataFrame:
    """

    if os.path.exists(base_path) == False:
        raise FileNotFoundError('The directory provided in the base_path variable does not exist')

    all_files = glob.glob(os.path.join(base_path, "*"))
    shp_dwnld_path = next(path for path in all_files if path.endswith('.shp'))
    shp = gpd.read_file(shp_dwnld_path)

    return shp

def retrieve_raster_vals(raster_path: str, df: typing.Union[pd.DataFrame, gpd.GeoDataFrame], raster_band: int = 1) -> pd.DataFrame:

    """
    For a given dataframe, find the values in a raster 

    :param raster_path: full file path for the raster file (in a format accepted in the rasterio library)
    :type raster_path: str
    :param raster_band: define the band of the raster necessary, default value is 1
    :type raster_band: int
    :param df: the dataframe with mandatory columns with the following names: Longitude, Latitude
    :type df: pd.DataFrame | gpd.GeoDataFrame

    :return gpd.GeoDataFrame:
    """

    if os.path.exists(raster_path) == False:
        raise FileNotFoundError('The directory provided in the base_path variable does not exist')
    
    if (isinstance(df, pd.DataFrame) == False) or (isinstance(df, gpd.GeoDataFrame) == False):
        raise TypeError('The input provided for the df variable is not a valid type')
    
    if (isinstance(raster_band, int) == False):
        raise TypeError('The raster_band input must be an integer')
    
    
    if any(col for col in ['LONGITUDE', 'LATITUDE']) not in [str(name).strip().upper() for name in df.columns.tolist()]:
        raise KeyError(f'Either LONGITUDE or LATITUDE (or both) are not available in the provided df variable')
    
    # clean data
    df.columns = [str(col).strip().upper() for col in df]
    df['LONGITUDE'] = pd.to_numeric(df['LONGITUDE'], errors='coerce').fillna(0)
    df['LATITUDE'] = pd.to_numeric(df['LATITUDE'], errors='coerce').fillna(0)

    with rasterio.open(raster_path) as src:

        if raster_band not in src.indexes:
            raise KeyError(f'The input provided (or default of 1) is not available in the raster band data. Available bands = {src.indexes}')
        
        band = src.read(raster_band)
        transform = src.transform
        crs = src.crs

        x_coords = df['LONGITUDE']
        y_coords = df['LATITUDE']
        rows, cols = rasterio.transform.rowcol[transform, x_coords.to_numpy(), y_coords.to_numpy()]
        valid_mask = (rows >=0) & (rows < band.shape[0]) & (cols >= 0) & (cols < band.shape[1])
        raster_values = np.full(len(rows, 0))
        raster_values[valid_mask] = band[rows[valid_mask], cols[valid_mask]]

    df['Raster_Values'] = raster_values

    return df

def dwnld_sql_table(server_name: str, database_name: str, sql_query) -> pd.DataFrame:
    
    '''
    LAPTOP-D4S4DOS8
    Download a table stored on a SQL server.

    :param server_name: Name of the server
    :type server_name: str
    :param database_name: Name of the database
    :type database_name: str
    :param sql_query: Query
    :type sql_query: str
    :return pd.DataFrame:
    '''
    
    if not any(isinstance(var, str) for var in [server_name, database_name]):
        raise ValueError("serve_name and database_name must both be strings.")

    engine = create_engine(f'mssql+pyodbc://{server_name}/{database_name}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+server', pool_size=10, max_overflow=20)
    with engine.connect() as connection:
        df = pd.read_sql(sql_query, connection)

    return df

def dwnld_sql_table2(server_name: str, database_name: str, table_name: str, sql_query: str, login_name: str) -> pd.DataFrame:
    
    '''
    LAPTOP-D4S4DOS8
    Download a table stored on a SQL server.

    :param server_name: Name of the server
    :type server_name: str
    :param database_name: Name of the database
    :type database_name: str
    :param sql_query: Query
    :type sql_query: str
    :param table_name: Table name
    :type table_name: str
    :param login_name: Query
    :type login_name: str
    :return pd.DataFrame:
    '''

    if not any(isinstance(var, str) for var in [server_name, database_name, sql_query, login_name]):
        raise ValueError("All inputs must be str type")

    engine = create_engine(fr'mssql+pymssql://{login_name}:@{server_name}/{database_name}')
    with engine.connect() as connection:
        all_db = connection.execute(text("SELECT name FROM sys.databases"))
        all_db_list = [db[0] for db in all_db.fetchall()]
        if database_name not in all_db_list:
            raise KeyError('database_name is not a valid database for the given SQL connection')
        connection.execute(text(f"USE [{database_name}]"))
        all_tb = connection.execute(text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo'"))
        all_tb_list = [table[0] for table in all_tb]
        if table_name not in all_tb_list:
            raise KeyError('table_name is not a valid table for the given database')
        query = connection.execute(text(sql_query))
        data = query.fetchall()
        columns = data.keys()
        df = pd.DataFrame(data, columns=columns)

    return df
        
def shp_folium_plt(m, colormap: str, layer_name: str, gdf: gpd.GeoDataFrame, col_name: str):
    
    """
    Add a shapefile to a folium map object, using a customizable colormap

    :param m: Folium map object
    :type m: Folium object
    :param colormap: colormap name from the plotly colormap options
    :type colormap:str
    :param layer_name: Name to appear in the layer control of the folium map
    :type layer_name: str
    :param gdf: the geodataframe to be plotted 
    :type gdf: gpd.GeoDataFrame
    :param col_name: Name of the column with the values to be plotted
    :type col_name: str
    :return m:
    """

    if not isinstance(gdf, gpd.GeoDataFrame):
        raise TypeError('gdf parameter is not a GeoDataFrame')
    
    if not col_name in gdf.columns.tolist():
        raise KeyError('col_name is not a column name in the given gdf')
    
    if not isinstance(layer_name, str):
        raise TypeError('layer_name is not a str object')
    
    gdf[col_name] = pd.to_numeric(gdf[col_name], errors='coerce').fillna(0)
    
    try:
        colormap = plt.get_cmap(colormap)
        vmax = gdf[col_name].max()
        vmin = gdf[col_name].min()
    except Exception:
        raise KeyError('colormap is not an available plotly colormap. see: https://plotly.com/python/builtin-colorscales/')

    polygon_group = folium.FeatureGroup(name=layer_name, show=True)

    if vmin == vmax:
        print('hdh')
        folium.GeoJson(
            gdf, 
            style_function=lambda feature: {
                'fillColor': colormap(0),
                'color': 'none',
                'weight': 1,
                'fillOOpacity': 0.5,
            },
            highlight_function=lambda feature: {
                'fillColor': colormap(0),
                'color': 'none',
                'weight': 1,
                'fillOOpacity': 0.9
            },
            name = layer_name,
            tooltip = folium.features.GeoJsonTooltip(fields=[col_name])
        ).add_to(polygon_group)
        polygon_group.add_to(m)
        return m

    folium.GeoJson(
        gdf, 
        style_function=lambda feature: {
            'fillColor': colormap((feature['properties'][col_name] - vmin) / (vmax - vmin)),
            'color': 'none',
            'weight': 1,
            'fillOOpacity': 0.5,
        },
        highlight_function=lambda feature: {
            'fillColor': colormap((feature['properties'][col_name] - vmin) / (vmax - vmin)),
            'color': 'none',
            'weight': 1,
            'fillOOpacity': 0.9
        },
        name = layer_name,
        tooltip = folium.features.GeoJsonTooltip(fields=[col_name])
    ).add_to(polygon_group)
    polygon_group.add_to(m)

    return m

def add_base_layers_m(m):
    
    """
    Add on the usual base maps to a folium output

    :param m: Folium map object
    :type m: Folium map object
    :returns Folium map object:
    """

    carto_positron = folium.TileLayer('CartoDB positron', name="CartoDB Positron")
    carto_positron.add_to(m)
    carto_dark_matter = folium.TileLayer('CartoDB dark_matter', name="CartoDB Dark Matter")
    carto_dark_matter.add_to(m)
    esri_world_imagery = folium.TileLayer('Esri WorldImagery', name="Esri WorldImagery")
    esri_world_imagery.add_to(m)
    open_topo_map = folium.TileLayer('OpenTopoMap', name="OpenTopoMap")
    open_topo_map.add_to(m)
    esri_world_streetmap = folium.TileLayer('Esri WorldStreetMap', name="Esri WorldStreetMap")
    esri_world_streetmap.add_to(m)

    folium.LayerControl(position='topright',collapsed=True, autoZIndex=True).add_to(m)

    return m

def add_raster_to_folium(m, raster_path: str, colormap: str, show_min_val: bool, raster_band: int = 1):
    
    """
    :param m: Folium map object
    :type m: Folium object
    :param raster_path: full file path for the raster file (in a format accepted in the rasterio library)
    :type raster_path: str
    :param colormap: colormap name from the plotly colormap options
    :type colormap:str
    :param show_min_val: Show the minimum value on the image or not
    :type show_min_val: bool
    :param raster_band: define the band of the raster necessary, default value is 1
    :type raster_band: int
    """

    if os.path.exists(raster_path) == False:
        raise FileNotFoundError('The directory provided in the base_path variable does not exist')
    try:
        colormap = plt.get_cmap(colormap)
    except Exception:
        raise KeyError('colormap is not an available plotly colormap. see: https://plotly.com/python/builtin-colorscales/')

    
    with rasterio.open(raster_path) as src:
        if raster_band not in src.indexes:
            raise KeyError(f'The input provided (or default of 1) is not available in the raster band data. Available bands = {src.indexes}')
        
        image_data = src.read(raster_band)
        no_data_val = src.nodata
        bounds = src.bounds

        masked_data = np.ma.masked_equal(image_data)

    norm = mcolors.Normalize(vmin = np.min(masked_data), vmax = np.max(masked_data))
    colored_image = colormap(norm(masked_data))
    alpha_channel = np.dstack([colored_image[:, :, :3], alpha_channel])
    colored_image = (colored_image * 255).astype(np.uint8)
    image = Image.fromarray(colored_image)
    image.save(raster_path.replace(os.path.basename(raster_path), "png_image.png"), format="PNG")

    raster_overlay = raster_layers.ImageOverlay(
        image = raster_path.replace(os.path.basename(raster_path), "png_image.png"),
        bounds = [[bounds[1], bounds[0], bounds[3], bounds[2]]],
        opacity = 1
    )
    raster_overlay.add_to(m)

    return m

