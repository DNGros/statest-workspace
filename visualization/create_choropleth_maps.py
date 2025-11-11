#!/usr/bin/env python3
"""Create choropleth maps showing state-level statistics."""

import sys
from pathlib import Path
import polars as pl
import folium
import json


# US state coordinates for map centers
STATE_COORDS = {
    'alabama': [32.806671, -86.791130],
    'alaska': [61.370716, -152.404419],
    'arizona': [33.729759, -111.431221],
    'arkansas': [34.969704, -92.373123],
    'california': [36.116203, -119.681564],
    'colorado': [39.059811, -105.311104],
    'connecticut': [41.597782, -72.755371],
    'delaware': [39.318523, -75.507141],
    'florida': [27.766279, -81.686783],
    'georgia': [33.040619, -83.643074],
    'hawaii': [21.094318, -157.498337],
    'idaho': [44.240459, -114.478828],
    'illinois': [40.349457, -88.986137],
    'indiana': [39.849426, -86.258278],
    'iowa': [42.011539, -93.210526],
    'kansas': [38.526600, -96.726486],
    'kentucky': [37.668140, -84.670067],
    'louisiana': [31.169546, -91.867805],
    'maine': [44.693947, -69.381927],
    'maryland': [39.063946, -76.802101],
    'massachusetts': [42.230171, -71.530106],
    'michigan': [43.326618, -84.536095],
    'minnesota': [45.694454, -93.900192],
    'mississippi': [32.741646, -89.678696],
    'missouri': [38.456085, -92.288368],
    'montana': [46.921925, -110.454353],
    'nebraska': [41.125370, -98.268082],
    'nevada': [38.313515, -117.055374],
    'ohio': [40.388783, -82.764915],
    'oklahoma': [35.565342, -96.928917],
    'oregon': [44.572021, -122.070938],
    'pennsylvania': [40.590752, -77.209755],
    'tennessee': [35.747845, -86.692345],
    'texas': [31.054487, -97.563461],
    'utah': [40.150032, -111.862434],
    'vermont': [44.045876, -72.710686],
    'virginia': [37.769337, -78.169968],
    'washington': [47.400902, -121.490494],
    'wisconsin': [44.268543, -89.616508],
    'wyoming': [42.755966, -107.302490],
}


def create_state_colors_map(csv_path: Path, output_path: Path, 
                            metric: str = 'ego_score',
                            title: str = "State Ego Score"):
    """Create a map with states colored by a metric.
    
    Args:
        csv_path: Path to state_ego_humility.csv
        output_path: Where to save the map
        metric: Column name to use for coloring ('ego_score', 'self_pct', 'other_pct')
        title: Title for the map
    """
    df = pl.read_csv(csv_path)
    
    # Create map centered on US
    m = folium.Map(location=[39.8283, -98.5795], zoom_start=4, tiles='CartoDB Positron')
    
    # Add markers for each state
    for row in df.iter_rows(named=True):
        state = row['state']
        
        if state not in STATE_COORDS:
            continue
        
        value = row[metric]
        
        # Determine color based on metric value
        if metric == 'ego_score':
            # Red for high ego, green for low ego (humble)
            if value > 0.5:
                color = 'red'
            elif value > 0.3:
                color = 'orange'
            elif value > 0.2:
                color = 'lightgreen'
            else:
                color = 'green'
        elif metric == 'self_pct':
            # More self-naming = redder
            if value > 0.3:
                color = 'red'
            elif value > 0.2:
                color = 'orange'
            elif value > 0.1:
                color = 'lightgreen'
            else:
                color = 'green'
        else:  # other_pct
            # More other-naming = greener
            if value > 1.0:
                color = 'green'
            elif value > 0.7:
                color = 'lightgreen'
            elif value > 0.4:
                color = 'orange'
            else:
                color = 'red'
        
        # Create popup text
        popup_text = f"""
        <b>{state.title()}</b><br>
        Ego Score: {row['ego_score']:.3f}<br>
        Self-named: {row['self_named_streets']:,} ({row['self_pct']:.2f}%)<br>
        Other-state-named: {row['other_state_streets']:,} ({row['other_pct']:.2f}%)<br>
        Total streets: {row['total_streets']:,}
        """
        
        # Add circle marker
        folium.CircleMarker(
            location=STATE_COORDS[state],
            radius=15,
            popup=folium.Popup(popup_text, max_width=300),
            tooltip=state.title(),
            color='black',
            fillColor=color,
            fillOpacity=0.7,
            weight=2
        ).add_to(m)
        
        # Add state label
        folium.Marker(
            location=STATE_COORDS[state],
            icon=folium.DivIcon(html=f"""
                <div style="font-size: 10pt; color: black; font-weight: bold; 
                            text-shadow: 1px 1px 2px white, -1px -1px 2px white;">
                    {state[:2].upper()}
                </div>
            """)
        ).add_to(m)
    
    # Add title
    title_html = f'''
    <div style="position: fixed; 
                top: 10px; left: 50px; width: 400px; height: 90px; 
                background-color: white; border:2px solid grey; z-index:9999; 
                font-size:14px; padding: 10px">
        <h4>{title}</h4>
        <p style="font-size: 11px; margin: 5px 0;">
            <span style="color: red;">●</span> High ego (self-naming) &nbsp;
            <span style="color: orange;">●</span> Medium &nbsp;
            <span style="color: lightgreen;">●</span> Low-medium &nbsp;
            <span style="color: green;">●</span> Low ego (humble)
        </p>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(title_html))
    
    m.save(str(output_path))
    print(f"Saved {title} map to {output_path}")
    return m


def create_popularity_map(csv_path: Path, output_path: Path):
    """Create a map showing how popular each state's name is nationwide."""
    df = pl.read_csv(csv_path)
    
    # Create map centered on US
    m = folium.Map(location=[39.8283, -98.5795], zoom_start=4, tiles='CartoDB Positron')
    
    # Get max value for scaling
    max_count = df['street_count'].max()
    
    # Add markers for each state
    for row in df.iter_rows(named=True):
        state = row['state_name']
        
        if state not in STATE_COORDS:
            continue
        
        count = row['street_count']
        
        # Scale radius by popularity (log scale for better visualization)
        import math
        radius = 5 + 20 * math.log(count + 1) / math.log(max_count + 1)
        
        # Color by popularity
        if count > 5000:
            color = 'darkred'
        elif count > 2000:
            color = 'red'
        elif count > 1000:
            color = 'orange'
        elif count > 500:
            color = 'yellow'
        else:
            color = 'lightblue'
        
        popup_text = f"""
        <b>{state.title()}</b><br>
        Appears in {count:,} street names nationwide<br>
        Rank: #{df.with_row_count().filter(pl.col('state_name') == state)['row_nr'][0] + 1}
        """
        
        folium.CircleMarker(
            location=STATE_COORDS[state],
            radius=radius,
            popup=folium.Popup(popup_text, max_width=300),
            tooltip=f"{state.title()}: {count:,} streets",
            color='black',
            fillColor=color,
            fillOpacity=0.6,
            weight=1
        ).add_to(m)
    
    # Add title
    title_html = '''
    <div style="position: fixed; 
                top: 10px; left: 50px; width: 400px; height: 90px; 
                background-color: white; border:2px solid grey; z-index:9999; 
                font-size:14px; padding: 10px">
        <h4>State Name Popularity in Street Names</h4>
        <p style="font-size: 11px; margin: 5px 0;">
            Circle size = number of streets named after this state nationwide<br>
            <span style="color: darkred;">●</span> >5000 &nbsp;
            <span style="color: red;">●</span> 2000-5000 &nbsp;
            <span style="color: orange;">●</span> 1000-2000 &nbsp;
            <span style="color: yellow;">●</span> 500-1000 &nbsp;
            <span style="color: lightblue;">●</span> <500
        </p>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(title_html))
    
    m.save(str(output_path))
    print(f"Saved popularity map to {output_path}")
    return m


def main():
    """Create all choropleth maps."""
    output_dir = Path(__file__).parent / "output"
    
    print("="*80)
    print("CREATING CHOROPLETH MAPS")
    print("="*80)
    print()
    
    # Map 1: Ego score
    create_state_colors_map(
        output_dir / 'state_ego_humility.csv',
        output_dir / 'state_colors_map_ego_score.html',
        metric='ego_score',
        title='State Ego Score (Self-Naming vs Other-State-Naming)'
    )
    
    # Map 2: Self-naming percentage
    create_state_colors_map(
        output_dir / 'state_ego_humility.csv',
        output_dir / 'state_colors_map_self_pct.html',
        metric='self_pct',
        title='Percentage of Streets Named After Own State'
    )
    
    # Map 3: Other-state-naming percentage
    create_state_colors_map(
        output_dir / 'state_ego_humility.csv',
        output_dir / 'state_colors_map_other_pct.html',
        metric='other_pct',
        title='Percentage of Streets Named After Other States'
    )
    
    # Map 4: State name popularity
    create_popularity_map(
        output_dir / 'state_name_popularity.csv',
        output_dir / 'state_name_popularity_map.html'
    )
    
    print()
    print("="*80)
    print("ALL CHOROPLETH MAPS COMPLETE!")
    print("="*80)


if __name__ == "__main__":
    main()

