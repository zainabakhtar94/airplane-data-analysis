import re
from bson import json_util
from elasticsearch7 import Elasticsearch
from flask import Flask, render_template, request
from pymongo import MongoClient, ASCENDING
import config


elastic = Elasticsearch(config.ELASTIC_URL)


# Process elasticsearch hits and return flights records
def process_search(results):
    records = []
    if results["hits"] and results["hits"]["hits"]:
        total = results["hits"]["total"]
        hits = results["hits"]["hits"]
        for hit in hits:
            record = hit["_source"]
            records.append(record)
    return records, total


# Calculate offsets for fetching lists of flights from MongoDB
def get_navigation_offsets(offset1, offset2, increment):
    offsets = {}
    offsets["Previous"] = {
        "top_offset": max(offset2 - increment, 0),
        "bottom_offset": max(offset1 - increment, 0),
    }  # Don't go < 0
    offsets["Next"] = {
        "top_offset": offset2 + increment,
        "bottom_offset": offset1 + increment,
    }
    return offsets


# Strip the existing start and end parameters from the query string
def strip_place(url):
    try:
        p = re.match("(.+)&start=.+&end=.+", url).group(1)
    except AttributeError as e:
        return url
    return p


# Set up Flask and Mongo
app = Flask(__name__)
client = MongoClient("mongo")


# Chapter 5 controller: Fetch a flight and display it
@app.route("/on_time_performance")
def on_time_performance():

    carrier = request.args.get("Carrier")
    flight_date = request.args.get("FlightDate")
    flight_num = request.args.get("FlightNum")

    flight = client.agile_data_science.on_time_performance.find_one(
        {"Carrier": carrier, "FlightDate": flight_date, "FlightNum": flight_num}
    )

    return render_template("flight.html", flight=flight)


# Chapter 5 controller: Fetch all flights between cities on a given day and display them
@app.route("/flights/<origin>/<dest>/<flight_date>")
def list_flights(origin, dest, flight_date):

    flights = client.agile_data_science.on_time_performance.find(
        {"Origin": origin, "Dest": dest, "FlightDate": flight_date},
        sort=[
            ("DepTime", 1),
            ("ArrTime", 1),
        ],
    )
    flight_count = flights.count()

    return render_template(
        "flights.html",
        flights=flights,
        flight_date=flight_date,
        flight_count=flight_count,
    )


@app.route("/flights/search")
@app.route("/flights/search/")
def search_flights():

    # Search parameters
    carrier = request.args.get("Carrier")
    flight_date = request.args.get("FlightDate")
    origin = request.args.get("Origin")
    dest = request.args.get("Dest")
    tail_number = request.args.get("TailNum")
    flight_number = request.args.get("FlightNum")

    # Pagination parameters
    start = request.args.get("start") or 0
    start = int(start)
    end = request.args.get("end") or config.RECORDS_PER_PAGE
    end = int(end)

    # Navigation path and offset setup
    nav_path = strip_place(request.url)
    nav_offsets = get_navigation_offsets(start, end, config.RECORDS_PER_PAGE)

    # Build the base of our elasticsearch query
    query = {
        "query": {"bool": {"must": []}},
        "sort": [{"FlightDate": "asc"}, "_score"],
        "from": start,
        "size": config.RECORDS_PER_PAGE,
    }

    # Add any search parameters present
    if carrier:
        query["query"]["bool"]["must"].append({"match": {"Carrier": carrier}})
    if flight_date:
        query["query"]["bool"]["must"].append({"match": {"FlightDate": flight_date}})
    if origin:
        query["query"]["bool"]["must"].append({"match": {"Origin": origin}})
    if dest:
        query["query"]["bool"]["must"].append({"match": {"Dest": dest}})
    if tail_number:
        query["query"]["bool"]["must"].append({"match": {"TailNum": tail_number}})
    if flight_number:
        query["query"]["bool"]["must"].append({"match": {"FlightNum": flight_number}})

    # Query elasticsearch, process to get records and count
    results = elastic.search(query)
    flights, flight_count = process_search(results)

    # Persist search parameters in the form template
    return render_template(
        "search.html",
        flights=flights,
        flight_date=flight_date,
        flight_count=flight_count,
        nav_path=nav_path,
        nav_offsets=nav_offsets,
        carrier=carrier,
        origin=origin,
        dest=dest,
        tail_number=tail_number,
        flight_number=flight_number,
    )


# Controller: Fetch a flight table
@app.route("/total_flights")
def total_flights():
    total_flights = client.agile_data_science.flights_by_month.find(
        {}, sort=[("Year", 1), ("Month", 1)]
    )
    return render_template("total_flights.html", total_flights=total_flights)


@app.route("/busy_airports.json")
def busy_airports_json():
    airports = client.agile_data_science.busy_airports.find({}, sort=[("count", -1)])
    return json_util.dumps(airports, ensure_ascii=False)


# Controller: Fetch a flight chart
@app.route("/busy_airports")
def busy_airports():
    airports = client.agile_data_science.busy_airports.find({}, sort=[("count", -1)])
    return render_template("top_routes.html", airports=airports)


@app.route("/busy_airports_chart")
def busy_airports_chart():
    airports = client.agile_data_science.busy_airports.find({}, sort=[("count", -1)])
    return render_template("top_routes_chart.html", airports=airports)


# Serve the chart's data via an asynchronous request (formerly known as 'AJAX')
@app.route("/total_flights.json")
def total_flights_json():
    total_flights = client.agile_data_science.flights_by_month.find(
        {}, sort=[("Year", 1), ("Month", 1)]
    )
    return json_util.dumps(total_flights, ensure_ascii=False)


# Controller: Fetch a flight chart
@app.route("/total_flights_chart")
def total_flights_chart():
    total_flights = client.agile_data_science.flights_by_month.find(
        {}, sort=[("Year", 1), ("Month", 1)]
    )
    return render_template("total_flights_chart.html", total_flights=total_flights)


# Controller: Fetch a flight chart
@app.route("/top_routes_chart")
def top_routes_chart():
    return render_template("top_routes_chart.html")


# Controller: Fetch a top routes chart's data
@app.route("/top_routes.json")
def top_routes_json():
    top_routes = client.agile_data_science.top_routes.find(
        {}, sort=[("Year", 1), ("Month", 1)]
    )
    return json_util.dumps(top_routes, ensure_ascii=False)


# Controller: Fetch a flight chart
@app.route("/flight_delay_weekly.json")
def flight_delay_weekly_json():
    weekly_delay = client.agile_data_science.flights_delay_weekly.find(
        {}, sort=[("DayOfWeek", 1)]
    )
    return json_util.dumps(weekly_delay, ensure_ascii=False)


# Controller: Fetch a flight chart
@app.route("/flight_delay_weekly")
def flight_delay_weekly():
    weekly_delay = client.agile_data_science.flights_delay_weekly.find(
        {}, sort=[("DayOfWeek", 1)]
    )
    return render_template("flights_delay_weekly.html", total_flights=weekly_delay)


# Controller: Fetch a flight chart 2.0
@app.route("/total_flights_chart_2")
def total_flights_chart_2():
    total_flights = client.agile_data_science.flights_by_month.find(
        {}, sort=[("Year", 1), ("Month", 1)]
    )
    return render_template("total_flights_chart_2.html", total_flights=total_flights)


# Controller: Fetch an airplane and display its flights
@app.route("/airplane/flights/<tail_number>")
def flights_per_airplane(tail_number):
    flights = client.agile_data_science.flights_per_airplane.find_one(
        {"TailNum": tail_number}
    )
    return render_template(
        "flights_per_airplane.html", flights=flights, tail_number=tail_number
    )


@app.route("/v2/airplane/flights/<tail_number>")
def flights_per_airplane_v2(tail_number):
    flights = client.agile_data_science.flights_per_airplane.find_one(
        {"TailNum": tail_number}
    )
    descriptions = client.agile_data_science.flights_per_airplane_v2.find_one(
        {"TailNum": tail_number}
    )
    print(descriptions["Description"])
    if descriptions is None:
        descriptions = []
    images = client.agile_data_science.airplane_images.find_one(
        {"TailNum": tail_number}
    )
    if images is None:
        images = []

    return render_template(
        "flights_per_airplane_2.html",
        flights=flights,
        images=images,
        descriptions=descriptions["Description"],
        tail_number=tail_number,
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
