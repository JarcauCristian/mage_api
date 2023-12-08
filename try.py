import folium
from IPython.display import display
import pandas as pd
import requests
from sedimark.sedimark_demo import secret
from sedimark.sedimark_demo import connector

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test







@custom
def transform_custom(data, *args, **kwargs):
    """
    Args:
        data: The output from the upstream parent block (if applicable)
        args: The output from any additional upstream blocks

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """


    df=data[0]
    df_coordinates=data[1]
    m = folium.Map(location=[df_coordinates['latitude'].iloc[0], df_coordinates['longitude'].iloc[0]], zoom_start=10)


    print(df_coordinates.latitude)

    for item in df_coordinates.values:
        print(item)
        folium.Marker(
                location=[item[0], item[1]],
                popup=f"<b>Station: {item[0]}</b><br>Latitude: {item[1]}</br><br>Longitude: {item[2]}</br>",
                # popup=f"<b>{station['libelle_station']}</b><br>{station['code_region']}</br><br>{station['etat_station']}</br><br>{station['libelle_cours_eau']}<br><a href='{station['uri_station']}' target='_blank'>More info</a>",
                zoom_on_click=True).add_to(m)

    display(m)

#     return data


# @test
# def test_output(output, *args) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert output is not None, 'The output is undefined'
