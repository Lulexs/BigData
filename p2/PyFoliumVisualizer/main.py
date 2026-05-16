import json
import math
import uuid
from datetime import datetime

import folium
from folium.plugins import TimestampedGeoJson
import branca.colormap as cm
from kafka import KafkaConsumer


BOOTSTRAP_SERVERS = "localhost:9094"
TOPIC = "ANALYSIS_RESULTS"

OUTPUT_HTML = "analysis_result_timeline_map.html"

CONSUMER_TIMEOUT_MS = 10_000
RADIUS_METERS = 200

POINTS = [
    {"name": "Tvrdjava", "lat": 43.323273, "lon": 21.895323},
    {"name": "Narodno pozoriste", "lat": 43.320522, "lon": 21.900467},
    {"name": "Palilulska rampa", "lat": 43.313094, "lon": 21.898391},
    {"name": "Cele-kula", "lat": 43.312478, "lon": 21.924012},
    {"name": "Dualsoft kancelarija", "lat": 43.323843, "lon": 21.929969},
    {"name": "Elektronski fakultet", "lat": 43.330180, "lon": 21.892056},
    {"name": "Gradska bolnica", "lat": 43.317720, "lon": 21.911314},
    {"name": "Park Svetog Save", "lat": 43.319180, "lon": 21.919740},
]


def point_key(lat, lon):
    return round(float(lat), 6), round(float(lon), 6)


POINT_NAMES = {
    point_key(p["lat"], p["lon"]): p["name"]
    for p in POINTS
}


def point_name(lat, lon):
    return POINT_NAMES.get(point_key(lat, lon), f"{lat}, {lon}")


def parse_dt(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def iso_z(value):
    if isinstance(value, str):
        return value
    return value.isoformat().replace("+00:00", "Z")


def fmt_num(value, digits=2):
    if value is None:
        return "N/A"
    return f"{float(value):.{digits}f}"


def circle_polygon_lon_lat(lat, lon, radius_m=200, points=64):
    earth_radius_m = 6_371_000

    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)
    angular_distance = radius_m / earth_radius_m

    coords = []

    for i in range(points):
        bearing = 2 * math.pi * i / points

        dest_lat = math.asin(
            math.sin(lat_rad) * math.cos(angular_distance)
            + math.cos(lat_rad) * math.sin(angular_distance) * math.cos(bearing)
        )

        dest_lon = lon_rad + math.atan2(
            math.sin(bearing) * math.sin(angular_distance) * math.cos(lat_rad),
            math.cos(angular_distance) - math.sin(lat_rad) * math.sin(dest_lat),
        )

        coords.append([math.degrees(dest_lon), math.degrees(dest_lat)])

    coords.append(coords[0])

    return coords


def read_kafka_messages():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id=f"folium-timeline-{uuid.uuid4()}",
        enable_auto_commit=False,
        consumer_timeout_ms=CONSUMER_TIMEOUT_MS,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )

    messages = []

    for msg in consumer:
        messages.append(msg.value)

    consumer.close()

    print(f"Read {len(messages)} messages.")
    return messages



def build_colormap(values, colors, caption):
    clean_values = [float(v) for v in values if v is not None]

    if not clean_values:
        return cm.LinearColormap(colors=colors, vmin=0, vmax=1, caption=caption)

    vmin = min(clean_values)
    vmax = max(clean_values)

    if vmin == vmax:
        vmax = vmin + 1

    return cm.LinearColormap(
        colors=colors,
        vmin=vmin,
        vmax=vmax,
        caption=caption,
    )



def traffic_popup(event):
    lat = float(event["location_x"])
    lon = float(event["location_y"])
    name = point_name(lat, lon)

    return f"""
    <div style="font-family: Arial; font-size: 13px; width: 260px;">
        <h4>{name}</h4>

        <b>Type:</b> Traffic<br>
        <b>Window:</b><br>
        {event.get("window_start")}<br>
        → {event.get("window_end")}<br><br>

        <b>Distinct cars:</b> {event.get("distinct_cars_count")}<br><br>

        <b>Coordinates</b><br>
        Lat: {lat}<br>
        Lon: {lon}
    </div>
    """


def pollution_popup(event):
    lat = float(event["location_x"])
    lon = float(event["location_y"])
    name = point_name(lat, lon)

    return f"""
    <div style="font-family: Arial; font-size: 13px; width: 280px;">
        <h4>{name}</h4>

        <b>Type:</b> Pollution<br>
        <b>Window:</b><br>
        {event.get("window_start")}<br>
        → {event.get("window_end")}<br><br>

        <b>Average values</b><br>
        CO2: {fmt_num(event.get("avg_co2"))}<br>
        CO: {fmt_num(event.get("avg_co"))}<br>
        HC: {fmt_num(event.get("avg_hc"), 4)}<br>
        NOx: {fmt_num(event.get("avg_nox"), 4)}<br>
        PMx: {fmt_num(event.get("avg_pmx"), 4)}<br>
        Noise: {fmt_num(event.get("avg_noise"))} dB<br><br>

        <b>Max values</b><br>
        CO2: {fmt_num(event.get("max_co2"))}<br>
        CO: {fmt_num(event.get("max_co"))}<br>
        HC: {fmt_num(event.get("max_hc"), 4)}<br>
        NOx: {fmt_num(event.get("max_nox"), 4)}<br>
        PMx: {fmt_num(event.get("max_pmx"), 4)}<br>
        Noise: {fmt_num(event.get("max_noise"))} dB<br><br>

        <b>Coordinates</b><br>
        Lat: {lat}<br>
        Lon: {lon}
    </div>
    """


def make_traffic_feature(event, traffic_colormap):
    lat = float(event["location_x"])
    lon = float(event["location_y"])
    name = point_name(lat, lon)

    count = event.get("distinct_cars_count")
    color = traffic_colormap(float(count or 0))

    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                circle_polygon_lon_lat(lat, lon, RADIUS_METERS)
            ],
        },
        "properties": {
            "time": event["window_start"],
            "popup": traffic_popup(event),

            "style": {
                "color": color,
                "weight": 5,
                "fillColor": color,
                "fillOpacity": 0.08,
                "opacity": 0.95,
            },
        },
    }


def make_pollution_feature(event, pollution_colormap):
    lat = float(event["location_x"])
    lon = float(event["location_y"])

    avg_co2 = event.get("avg_co2")
    color = pollution_colormap(float(avg_co2 or 0))

    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                circle_polygon_lon_lat(lat, lon, RADIUS_METERS)
            ],
        },
        "properties": {
            "time": event["window_start"],
            "popup": pollution_popup(event),

            "style": {
                "color": color,
                "weight": 1,
                "fillColor": color,
                "fillOpacity": 0.35,
                "opacity": 0.6,
            },
        },
    }



def create_timeline_map(messages):
    usable_messages = [
        m for m in messages
        if m.get("analysis_type") in {"traffic", "pollution"}
        and m.get("window_start")
        and m.get("location_x") is not None
        and m.get("location_y") is not None
    ]

    if not usable_messages:
        raise RuntimeError("No usable traffic/pollution messages found.")

    lats = [float(m["location_x"]) for m in usable_messages]
    lons = [float(m["location_y"]) for m in usable_messages]

    center_lat = sum(lats) / len(lats)
    center_lon = sum(lons) / len(lons)

    traffic_values = [
        m.get("distinct_cars_count")
        for m in usable_messages
        if m.get("analysis_type") == "traffic"
    ]

    pollution_values = [
        m.get("avg_co2")
        for m in usable_messages
        if m.get("analysis_type") == "pollution"
    ]

    traffic_colormap = build_colormap(
        traffic_values,
        colors=["green", "yellow", "red"],
        caption="Traffic: distinct cars per window",
    )

    pollution_colormap = build_colormap(
        pollution_values,
        colors=["blue", "purple", "red"],
        caption="Pollution: avg CO2 per window",
    )

    features = []

    for event in usable_messages:
        if event["analysis_type"] == "traffic":
            features.append(make_traffic_feature(event, traffic_colormap))
        elif event["analysis_type"] == "pollution":
            features.append(make_pollution_feature(event, pollution_colormap))

    feature_collection = {
        "type": "FeatureCollection",
        "features": features,
    }

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=14,
        tiles="OpenStreetMap",
    )

    marker_layer = folium.FeatureGroup(name="Point names", show=True)

    for p in POINTS:
        folium.Marker(
            location=[p["lat"], p["lon"]],
            tooltip=p["name"],
            popup=f"""
            <b>{p["name"]}</b><br>
            Lat: {p["lat"]}<br>
            Lon: {p["lon"]}
            """,
            icon=folium.Icon(color="blue", icon="info-sign"),
        ).add_to(marker_layer)

    marker_layer.add_to(m)

    TimestampedGeoJson(
        data=feature_collection,
        period="PT1M",
        duration="PT1M",
        transition_time=250,
        auto_play=False,
        loop=False,
        loop_button=True,
        time_slider_drag_update=True,

        date_options="YYYY-MM-DD HH:mm:ss",
    ).add_to(m)

    traffic_colormap.add_to(m)
    pollution_colormap.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    
    return m


def main():
    messages = read_kafka_messages()

    m = create_timeline_map(messages)
    m.save(OUTPUT_HTML)



if __name__ == "__main__":
    main()